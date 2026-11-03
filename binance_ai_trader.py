import json
import math
import os
import queue
import re
import threading
import time
import uuid
from decimal import Decimal, ROUND_DOWN
import anthropic
import requests
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from dotenv import load_dotenv

load_dotenv()

# --- 1. КЛЮЧІ ТА НАЛАШТУВАННЯ БЕЗПЕКИ ---
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "").strip()
missing_keys = [
    name for name, value in (
        ("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY),
        ("BINANCE_API_KEY", BINANCE_API_KEY),
        ("BINANCE_SECRET_KEY", BINANCE_SECRET_KEY),
    )
    if not value
]
if missing_keys:
    raise RuntimeError(
        "Не задано обов'язкові змінні середовища: " + ", ".join(missing_keys)
    )

# Налаштування
MIN_BINANCE_NOTIONAL_USDT = 5.5   # Мінімальний ордер Binance (5 USDT) із невеликим запасом
CHECK_INTERVAL_SECONDS = 10      # Перевіряємо стан кожні 10 секунд
TOP_MARKET_COINS_LIMIT = 20       # Скануємо найліквідніші 20 монет
MARKET_SCAN_INTERVAL_SECONDS = 60
API_RETRY_SECONDS = 10
MEMORY_FILE = os.getenv("MEMORY_FILE", "trade_memory.json")

client_anthropic = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY, ping=False)

# Синхронізація часу з сервером Binance (повторюється автоматично, бо годинник ПК дрейфує)
TIME_RESYNC_SECONDS = 60
TIME_SAFETY_MARGIN_MS = 300   # тримаємо локальний час трохи "позаду" сервера: Binance відхиляє запити з майбутнього
_last_time_sync = 0.0
API_ERROR_THRESHOLD = 10
API_ERROR_COUNTS = {}
API_ERROR_TOTAL = 0
API_ERROR_LAST = None
API_EMERGENCY_TRIGGERED = False
API_ERROR_LOCK = threading.Lock()
CURRENT_MEMORY = None
BINANCE_COOLDOWN_UNTIL = 0.0
MARKET_TICKER_CACHE = {}
OCO_RECONCILE_INTERVAL_SECONDS = 30
_last_oco_reconcile = 0.0


class BinanceCooldownError(RuntimeError):
    pass


def _api_error_signature(error):
    if isinstance(error, BinanceAPIException):
        return f"binance_api:{error.code}"
    return f"binance_transport:{type(error).__name__}"


def _record_api_error(error, api_name):
    global API_ERROR_TOTAL, API_ERROR_LAST, API_EMERGENCY_TRIGGERED, BINANCE_COOLDOWN_UNTIL
    now = time.time()
    signature = f"{api_name}:{_api_error_signature(error) if api_name == 'binance' else type(error).__name__}"
    with API_ERROR_LOCK:
        API_ERROR_TOTAL += 1
        API_ERROR_LAST = signature
        record = API_ERROR_COUNTS.setdefault(signature, {"count": 0})
        record["count"] += 1
        error_count = API_ERROR_TOTAL

    if isinstance(error, BinanceAPIException) and error.code == -1003:
        message = str(getattr(error, "message", error))
        match = re.search(r"banned until\s+(\d+)", message, re.IGNORECASE)
        if match:
            BINANCE_COOLDOWN_UNTIL = max(BINANCE_COOLDOWN_UNTIL, int(match.group(1)) / 1000)
        else:
            BINANCE_COOLDOWN_UNTIL = max(BINANCE_COOLDOWN_UNTIL, now + 300)
        retry_in = max(0, int(BINANCE_COOLDOWN_UNTIL - now))
        print(
            f"⛔ Binance rate-limit ban (-1003): запити призупинені "
            f"приблизно на {retry_in} с. Аварійний продаж не запускається, "
            "поки біржа блокує запити."
        )

    print(
        f"⚠️ API-помилка {error_count}/{API_ERROR_THRESHOLD} "
        f"за цей запуск ({signature})."
    )
    if error_count >= API_ERROR_THRESHOLD:
        API_EMERGENCY_TRIGGERED = True

def sync_server_time():
    global _last_time_sync
    try:
        before = time.time() * 1000
        server_time = binance_client.get_server_time()['serverTime']
        after = time.time() * 1000
        local_mid = (before + after) / 2  # враховуємо затримку мережі
        binance_client.timestamp_offset = int(server_time - local_mid - TIME_SAFETY_MARGIN_MS)
        _last_time_sync = time.time()
    except Exception as e:
        print(f"⚠️ Не вдалося синхронізувати час: {e}")

_original_request = binance_client._request

def _request_with_time_sync(method, uri, signed, *args, **kwargs):
    if time.time() < BINANCE_COOLDOWN_UNTIL:
        retry_in = int(BINANCE_COOLDOWN_UNTIL - time.time())
        raise BinanceCooldownError(f"Binance тимчасово заблокував запити; повтор приблизно через {retry_in} с.")
    if signed and time.time() - _last_time_sync > TIME_RESYNC_SECONDS:
        sync_server_time()
        if time.time() < BINANCE_COOLDOWN_UNTIL:
            retry_in = int(BINANCE_COOLDOWN_UNTIL - time.time())
            raise BinanceCooldownError(f"Binance тимчасово заблокував запити; повтор приблизно через {retry_in} с.")
    try:
        return _original_request(method, uri, signed, *args, **kwargs)
    except (BinanceAPIException, BinanceRequestException, requests.RequestException) as e:
        if isinstance(e, BinanceAPIException) and e.code == -1021:
            sync_server_time()  # наступний запит піде з виправленим часом
        _record_api_error(e, "binance")
        raise

binance_client._request = _request_with_time_sync

STABLECOINS = {"USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "DAIUSDT", "EURUSDT", "GBPUSDT", "BUSDUSDT"}

# --- Ліміти ризику (діють у коді незалежно від рішень Claude) ---
MAX_OPEN_POSITIONS = 8             # Одночасно відкритих позицій
MAX_POSITION_PCT_OF_EQUITY = 25.0  # Макс. розмір однієї позиції, % капіталу
MAX_RISK_PER_TRADE_PCT = 1.0       # Макс. втрата при спрацюванні SL, % капіталу
MAX_SL_DISTANCE_PCT = 5.0          # Стоп не далі ніж на 5% від ціни входу
MIN_REWARD_TO_RISK = 1.5           # Мінімальне співвідношення прибуток/ризик
DAILY_LOSS_LIMIT_PCT = 3.0         # Після втрати 3% капіталу за добу нові BUY зупиняються
SYMBOL_COOLDOWN_MINUTES = 60       # Пауза по монеті після збиткової угоди
MIN_MOMENTUM_SCORE = 3             # Мінімальна оцінка тренду для входу
MAX_ENTRY_RSI = 72                 # Не купуємо перегріті монети
MIN_VOLUME_24H_USDT = 5_000_000    # Мінімальна ліквідність
PERFORMANCE_TARGET_USDT = 30.0     # Ціль реалізованого прибутку за вікно
PERFORMANCE_WINDOW_DAYS = 6        # Цільове вікно у днях
MARKET_SNAPSHOT = {}               # Останні ринкові дані по символах
KNOWN_USDT_SYMBOLS = set()
SL_TP_POLL_SECONDS = 10         # Перевірка ручного SL/TP без зайвих запитів
ADMIN_ACTIONS = queue.Queue()
ADMIN_SNAPSHOT_LOCK = threading.Lock()
ADMIN_SNAPSHOT_JSON = "{}"

# --- Менеджер пам'яті ---
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "bot_state").strip()
SUPABASE_ROW_ID = os.getenv("SUPABASE_ROW_ID", "trade_memory").strip()
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

def _supabase_headers(extra=None):
    headers = {"apikey": SUPABASE_KEY, "Content-Type": "application/json"}
    # Нові ключі sb_secret_* не є JWT і передаються лише в apikey; старі JWT-ключі — ще й як Bearer
    if SUPABASE_KEY.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {SUPABASE_KEY}"
    if extra:
        headers.update(extra)
    return headers

def _supabase_endpoint():
    return f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"

def _read_raw_memory():
    """Повертає збережений стан (dict) або None, якщо його ще немає."""
    if USE_SUPABASE:
        response = requests.get(
            _supabase_endpoint(),
            headers=_supabase_headers(),
            params={"id": f"eq.{SUPABASE_ROW_ID}", "select": "data"},
            timeout=15,
        )
        response.raise_for_status()
        rows = response.json()
        if rows:
            return rows[0]["data"]
        # Перший запуск: переносимо локальний файл, якщо він є
    if os.path.exists(MEMORY_FILE):
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def _write_raw_memory(memory):
    if USE_SUPABASE:
        response = requests.post(
            _supabase_endpoint(),
            headers=_supabase_headers({"Prefer": "resolution=merge-duplicates,return=minimal"}),
            params={"on_conflict": "id"},
            json={
                "id": SUPABASE_ROW_ID,
                "data": memory,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            timeout=15,
        )
        response.raise_for_status()
        return
    temp_file = f"{MEMORY_FILE}.tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(memory, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, MEMORY_FILE)
    finally:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError:
                pass

def load_memory():
    """Завантажує тривалу пам'ять бота (Supabase або локальний файл)"""
    try:
        memory = _read_raw_memory()
        if memory is None:
            return {
                "active_trades": {},
                "trade_history": [],
                "total_pnl_usdt": 0.0
            }
        if not isinstance(memory, dict):
            raise ValueError("кореневе значення має бути JSON-об'єктом")
        memory.setdefault("active_trades", {})
        memory.setdefault("trade_history", [])
        memory.setdefault("total_pnl_usdt", 0.0)
        if not isinstance(memory["active_trades"], dict) or not isinstance(memory["trade_history"], list):
            raise ValueError("неправильна структура active_trades або trade_history")
        if memory.get("pending_order") and not memory.get("emergency_stop"):
            pending = memory["pending_order"]
            raise RuntimeError(
                "Знайдено незавершений ордер "
                f"{pending.get('client_order_id', 'без ID')} для "
                f"{pending.get('symbol', 'невідомого символу')}. "
                "Звірте його стан у Binance перед очищенням pending_order у пам'яті."
            )
        return memory
    except Exception as e:
        if isinstance(e, requests.RequestException):
            _record_api_error(e, "supabase")
        raise RuntimeError(f"Не вдалося безпечно завантажити пам'ять угод: {e}") from e

def save_memory(memory):
    """Зберігає оновлену пам'ять (Supabase або локальний файл)"""
    try:
        _write_raw_memory(memory)
    except Exception as e:
        if isinstance(e, requests.RequestException):
            _record_api_error(e, "supabase")
        raise RuntimeError(f"Не вдалося зберегти пам'ять угод: {e}") from e

def place_order_with_intent(memory, side, symbol, **order_params):
    """Записує намір до надсилання ордера, щоб невизначений стан блокував нові угоди."""
    if memory.get("pending_order"):
        raise RuntimeError("Є незавершений ордер; нові ордери заблоковано.")
    client_order_id = f"bot{uuid.uuid4().hex}"
    memory["pending_order"] = {
        "client_order_id": client_order_id,
        "side": side,
        "symbol": symbol,
        "created_at": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    save_memory(memory)
    if side not in ("BUY", "SELL"):
        memory.pop("pending_order", None)
        save_memory(memory)
        raise ValueError(f"Непідтримувана сторона ордера: {side}")
    send = binance_client.order_market_buy if side == "BUY" else binance_client.order_market_sell
    # Рядки без наукової нотації (1e-05), інакше Binance відхиляє параметр
    order_params = {
        k: (plain_number(v) if k in ("quantity", "quoteOrderQty") else v)
        for k, v in order_params.items()
    }
    try:
        return send(symbol=symbol, newClientOrderId=client_order_id, **order_params)
    except BinanceAPIException as e:
        # -1007: статус ордера невідомий, маркер лишаємо; інші коди - ордер відхилено
        if e.code != -1007:
            memory.pop("pending_order", None)
            save_memory(memory)
        raise

def commit_order_memory(memory):
    """Фіксує оновлений стан пам'яті після застосування результату виконання."""
    memory.pop("pending_order", None)
    save_memory(memory)

def plain_number(value):
    """Число у вигляді рядка без наукової нотації."""
    text = format(Decimal(str(value)), 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text

def get_symbol_tick_size(symbol):
    """Повертає крок ціни (tickSize) для символу."""
    info = binance_client.get_symbol_info(symbol)
    if not info:
        raise ValueError(f"Binance не повернув інформацію для {symbol}")
    for f in info['filters']:
        if f['filterType'] == 'PRICE_FILTER' and float(f['tickSize']) > 0:
            return float(f['tickSize'])
    raise ValueError(f"Для {symbol} не знайдено PRICE_FILTER")

# ================= БІРЖОВІ SL/TP (OCO) =================
OCO_RETRY_SECONDS = 60
_oco_retry_at = {}
_oco_warned = set()

def warn_once(key, message):
    """Друкує попередження про неможливість виставити OCO лише один раз."""
    if key not in _oco_warned:
        _oco_warned.add(key)
        print(message)

def cancel_oco(memory, symbol):
    """Скасовує OCO позиції. True — скасовано або його не було; False — він уже виконався/змінився."""
    trade = memory.get("active_trades", {}).get(symbol)
    oco = trade.get("oco") if trade else None
    if not oco:
        return True
    try:
        binance_client.v3_delete_order_list(symbol=symbol, orderListId=oco["list_id"])
    except Exception as e:
        print(f"⚠️ Не вдалося скасувати OCO {symbol}: {e}; звіряємо стан...")
        reconcile_oco_orders(memory, symbol)
        trade = memory.get("active_trades", {}).get(symbol)
        if trade and trade.get("oco"):
            raise
        return False
    trade.pop("oco", None)
    save_memory(memory)
    return True

def ensure_oco(memory, symbol):
    """Виставляє (або перевиставляє) біржовий OCO зі SL/TP на всю облікову кількість позиції."""
    trade = memory.get("active_trades", {}).get(symbol)
    if not trade:
        return False
    tp = float(trade.get("take_profit", 0.0))
    sl = float(trade.get("stop_loss", 0.0))
    if tp <= 0 or sl <= 0:
        return False
    if not cancel_oco(memory, symbol):
        return False

    base_asset = symbol[:-4]
    free_qty = 0.0
    for b in binance_client.get_account()["balances"]:
        if b["asset"] == base_asset:
            free_qty = float(b["free"])
            break
    qty = round_step_size(min(free_qty, float(trade.get("qty", free_qty))), get_symbol_lot_size(symbol))
    price = float(binance_client.get_ticker(symbol=symbol)["lastPrice"])
    if qty <= 0 or qty * price < get_symbol_min_notional(symbol):
        warn_once((symbol, "small"), f"⚠️ {symbol}: позиція замала для OCO; діє перевірка SL/TP опитуванням.")
        return False
    if not sl < price < tp:
        warn_once((symbol, "range"), f"⚠️ {symbol}: ціна {price} поза діапазоном SL/TP для OCO; діє перевірка опитуванням.")
        return False

    tick = get_symbol_tick_size(symbol)
    tp_price = round_step_size(tp, tick)
    sl_stop = round_step_size(sl, tick)
    sl_limit = round_step_size(sl * 0.995, tick)
    if sl_limit <= 0 or sl_stop <= 0:
        return False
    result = binance_client.create_oco_order(
        symbol=symbol,
        side="SELL",
        quantity=plain_number(qty),
        aboveType="LIMIT_MAKER",
        abovePrice=plain_number(tp_price),
        belowType="STOP_LOSS_LIMIT",
        belowStopPrice=plain_number(sl_stop),
        belowPrice=plain_number(sl_limit),
        belowTimeInForce="GTC",
    )
    trade["oco"] = {"list_id": result["orderListId"], "qty": qty}
    _oco_warned.discard((symbol, "small"))
    _oco_warned.discard((symbol, "range"))
    save_memory(memory)
    print(f"🛡️ {symbol}: біржовий OCO виставлено (TP {tp_price} / SL {sl_stop}, кількість {qty}).")
    return True

def ensure_oco_safe(memory, symbol, force=False):
    """Те саме, що ensure_oco, але без винятків і з паузою між повторами."""
    now = time.time()
    if not force and _oco_retry_at.get(symbol, 0) > now:
        return False
    _oco_retry_at[symbol] = now + OCO_RETRY_SECONDS
    try:
        return ensure_oco(memory, symbol)
    except Exception as e:
        print(f"⚠️ Не вдалося виставити OCO для {symbol}: {e}")
        return False

def reconcile_oco_orders(memory, only_symbol=None):
    """Фіксує в пам'яті OCO, що виконалися на біржі (PnL, залишок позиції)."""
    global _last_oco_reconcile
    now = time.time()
    if only_symbol is None:
        if now - _last_oco_reconcile < OCO_RECONCILE_INTERVAL_SECONDS:
            return False
        _last_oco_reconcile = now
    changed = False
    for symbol, trade in list(memory.get("active_trades", {}).items()):
        if only_symbol and symbol != only_symbol:
            continue
        oco = trade.get("oco")
        if not oco:
            continue
        try:
            order_list = binance_client.v3_get_order_list(orderListId=oco["list_id"])
            if order_list.get("listOrderStatus") == "EXECUTING":
                continue
            orders = [
                binance_client.get_order(symbol=symbol, orderId=o["orderId"])
                for o in order_list.get("orders", [])
            ]
            trade.pop("oco", None)
            changed = True
            filled = [o for o in orders if float(o.get("executedQty", 0)) > 0]
            if not filled:
                print(f"ℹ️ OCO {symbol} завершено без виконання; захист буде виставлено знову.")
                continue
            order = filled[0]
            avg_sell, executed_q, comm, _ = calculate_executed_fill_details(order)
            buy_price = float(trade.get("buy_price", avg_sell))
            pnl_usdt = ((avg_sell - buy_price) * executed_q) - comm
            reason = "Take-Profit (OCO)" if order.get("type") == "LIMIT_MAKER" else "Stop-Loss (OCO)"
            memory["total_pnl_usdt"] = memory.get("total_pnl_usdt", 0.0) + pnl_usdt
            memory.setdefault("trade_history", []).append({
                "symbol": symbol,
                "buy_price": buy_price,
                "sell_price": avg_sell,
                "qty": executed_q,
                "pnl_usdt": round(pnl_usdt, 2),
                "reason": reason,
                "time": time.strftime('%Y-%m-%d %H:%M:%S')
            })
            remaining = max(0.0, float(trade.get("qty", executed_q)) - executed_q)
            if remaining > 0:
                trade["qty"] = remaining
                trade["amount_usdt"] = round(remaining * buy_price, 2)
            else:
                memory["active_trades"].pop(symbol, None)
            print(f"🎯 {reason} для {symbol} спрацював на біржі. PnL: {pnl_usdt:+.2f} USDT")
        except Exception as e:
            print(f"⚠️ Помилка звірки OCO для {symbol}: {e}")
    if changed:
        save_memory(memory)
    return changed

def protect_all_positions(memory):
    """Виставляє OCO для позицій, які його ще не мають."""
    for symbol, trade in list(memory.get("active_trades", {}).items()):
        if not trade.get("oco"):
            ensure_oco_safe(memory, symbol)

def round_step_size(quantity, step_size):
    """Округлює кількість униз до точного кратного step_size."""
    quantity = Decimal(str(quantity))
    step_size = Decimal(str(step_size))
    if step_size <= 0:
        raise ValueError("step_size має бути більшим за нуль")
    steps = (quantity / step_size).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * step_size)

def get_symbol_lot_size(symbol):
    """Отримує мінімальний крок кількості (stepSize) для монети"""
    try:
        info = binance_client.get_symbol_info(symbol)
        if not info:
            raise ValueError(f"Binance не повернув інформацію для {symbol}")
        filters = info['filters']
        for filter_type in ('MARKET_LOT_SIZE', 'LOT_SIZE'):
            for f in filters:
                if f['filterType'] == filter_type and float(f['stepSize']) > 0:
                    return float(f['stepSize'])
        raise ValueError(f"Для {symbol} не знайдено фільтр MARKET_LOT_SIZE або LOT_SIZE")
    except Exception as e:
        raise RuntimeError(f"Не вдалося отримати LOT_SIZE для {symbol}: {e}") from e

def get_symbol_min_notional(symbol):
    """Повертає мінімальну суму ордера для символу, не нижчу за локальний ліміт."""
    try:
        info = binance_client.get_symbol_info(symbol)
        if not info:
            raise ValueError(f"Binance не повернув інформацію для {symbol}")
        for f in info['filters']:
            applies_to_market = (
                f.get('applyToMarket', True)
                if f['filterType'] == 'MIN_NOTIONAL'
                else f.get('applyMinToMarket', True)
            )
            if f['filterType'] in {'MIN_NOTIONAL', 'NOTIONAL'} and applies_to_market:
                return max(
                    MIN_BINANCE_NOTIONAL_USDT,
                    float(f.get('minNotional', MIN_BINANCE_NOTIONAL_USDT))
                )
        return MIN_BINANCE_NOTIONAL_USDT
    except Exception as e:
        raise RuntimeError(f"Не вдалося отримати мінімальну суму для {symbol}: {e}") from e

def calculate_executed_fill_details(order):
    """Вираховує точну середню ціну, кількість і комісію з реальних fills ордера"""
    fills = order.get('fills', [])
    if not fills:
        order_id = order.get('orderId')
        symbol = order.get('symbol')
        if order_id is not None and symbol:
            fills = binance_client.get_my_trades(
                symbol=symbol,
                orderId=int(order_id)
            )
    if not fills:
        qty = float(order.get('executedQty', 0))
        cummulative_quote = float(order.get('cummulativeQuoteQty', 0))
        avg_price = (cummulative_quote / qty) if qty > 0 else float(order.get('price', 0))
        if qty > 0:
            raise RuntimeError(
                f"Немає даних про виконання та комісію для ордера {order.get('orderId')}; "
                "відмовляюся записувати неточний PnL."
            )
        return avg_price, qty, 0.0, 0.0

    symbol = order.get('symbol', '')
    base_asset = symbol[:-4] if symbol.endswith('USDT') else ''
    is_buy = order.get('side') == 'BUY' or (
        'isBuyer' in fills[0] and bool(fills[0]['isBuyer'])
    )
    total_qty = 0.0
    total_cost = 0.0
    total_commission_usdt = 0.0
    base_commission_qty = 0.0
    base_commission_usdt = 0.0

    for fill in fills:
        p = float(fill['price'])
        q = float(fill.get('qty', fill.get('quantity', 0)))
        total_qty += q
        total_cost += p * q

        comm = float(fill.get('commission', 0))
        asset = fill.get('commissionAsset', fill.get('commission_asset', ''))
        if asset == 'USDT':
            total_commission_usdt += comm
        elif asset == base_asset:
            base_commission_usdt += comm * p
            total_commission_usdt += comm * p
            if is_buy:
                base_commission_qty += comm
        elif comm > 0:
            try:
                commission_price = float(
                    binance_client.get_ticker(symbol=f"{asset}USDT")['lastPrice']
                )
                total_commission_usdt += comm * commission_price
            except Exception as e:
                raise RuntimeError(
                    f"Не вдалося оцінити комісію {comm} {asset} у USDT"
                ) from e

    executed_qty = total_qty - base_commission_qty
    if is_buy and executed_qty > 0:
        other_fees = total_commission_usdt - base_commission_usdt
        avg_price = (total_cost + other_fees) / executed_qty
    else:
        avg_price = (total_cost / total_qty) if total_qty > 0 else 0.0
    return avg_price, executed_qty, total_commission_usdt, base_commission_qty

def get_top_usdt_symbols(limit=50):
    """Отримує ТОП-монети за об'ємом торгів"""
    MARKET_TICKER_CACHE.clear()
    try:
        tickers = binance_client.get_ticker()
        MARKET_TICKER_CACHE.update({
            ticker["symbol"]: ticker
            for ticker in tickers
            if isinstance(ticker, dict) and "symbol" in ticker
        })
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if symbol.endswith('USDT') and symbol not in STABLECOINS:
                usdt_pairs.append({
                    'symbol': symbol,
                    'quoteVolume': float(t['quoteVolume'])
                })
        usdt_pairs.sort(key=lambda x: x['quoteVolume'], reverse=True)
        return [p['symbol'] for p in usdt_pairs[:limit]]
    except Exception as e:
        print(f"⚠️ Помилка отримання монет: {e}")
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

_cached_model = None

def get_best_available_model():
    """Автовизначення моделі Anthropic (результат кешується)"""
    global _cached_model
    if _cached_model:
        return _cached_model
    try:
        response = client_anthropic.models.list()
        model_ids = [m.id for m in response.data]
        for preferred in ["haiku", "sonnet"]:
            for m in model_ids:
                if preferred in m:
                    _cached_model = m
                    return m
        if model_ids:
            _cached_model = model_ids[0]
            return _cached_model
    except Exception as e:
        if isinstance(e, anthropic.APIError):
            _record_api_error(e, "anthropic")
        print(f"⚠️ Помилка вибору моделі: {e}")
    return "claude-haiku-5-5"

def _ema(values, period):
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema

def _rsi(closes, period=14):
    gains, losses = [], []
    for prev, cur in zip(closes[:-1], closes[1:]):
        change = cur - prev
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for g, l in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

def _atr(klines, period=14):
    true_ranges = []
    for prev, cur in zip(klines[:-1], klines[1:]):
        high, low, prev_close = float(cur[2]), float(cur[3]), float(prev[4])
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    recent = true_ranges[-period:]
    return sum(recent) / len(recent)

def scan_market_watchlist(symbols):
    """Збирає ринкові дані з індикаторами і оцінкою перспективності"""
    print(f"🔍 Скануємо ринок Binance ({len(symbols)} монет)...")
    market_overview = []
    for symbol in symbols:
        try:
            ticker = MARKET_TICKER_CACHE.get(symbol)
            if ticker is None:
                ticker = binance_client.get_ticker(symbol=symbol)
            klines = binance_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1HOUR, limit=60)
            if len(klines) < 30:
                continue
            closes = [float(k[4]) for k in klines]
            volumes = [float(k[7]) for k in klines]  # обсяг у USDT
            price = float(ticker["lastPrice"])
            ema9, ema21 = _ema(closes, 9), _ema(closes, 21)
            rsi = _rsi(closes)
            atr = _atr(klines)
            avg_volume = sum(volumes[-25:-1]) / 24
            volume_ratio = volumes[-1] / avg_volume if avg_volume > 0 else 0.0
            change_4h = (price / closes[-5] - 1) * 100 if closes[-5] > 0 else 0.0
            atr_pct = atr / price * 100 if price > 0 else 0.0

            score = 0
            score += 2 if ema9 > ema21 else -2
            score += 1 if price > ema9 else 0
            if 50 <= rsi <= 68:
                score += 2
            elif rsi > 75:
                score -= 2
            elif rsi < 35:
                score -= 1
            score += 1 if volume_ratio >= 1.5 else 0
            score += 1 if 0 < change_4h <= 6 else 0
            score -= 1 if change_4h > 10 else 0
            score += 1 if 0.3 <= atr_pct <= 3.0 else 0

            market_overview.append({
                "symbol": symbol,
                "current_price": price,
                "price_change_24h_pct": float(ticker["priceChangePercent"]),
                "change_4h_pct": round(change_4h, 2),
                "volume_24h_usdt": float(ticker["quoteVolume"]),
                "volume_ratio_1h_vs_avg": round(volume_ratio, 2),
                "high_24h": float(ticker["highPrice"]),
                "low_24h": float(ticker["lowPrice"]),
                "rsi_14": round(rsi, 1),
                "ema9_above_ema21": ema9 > ema21,
                "atr_pct": round(atr_pct, 2),
                "momentum_score": score,
                "suggested_stop_loss": price - 1.5 * atr,
                "suggested_take_profit": price + 2.5 * atr,
                "recent_1h_prices": closes[-6:]
            })
        except Exception:
            continue
    market_overview.sort(key=lambda x: x["momentum_score"], reverse=True)
    MARKET_SNAPSHOT.clear()
    MARKET_SNAPSHOT.update({m["symbol"]: m for m in market_overview})
    return market_overview

def get_account_balances_and_positions(memory_symbols=None):
    """Збирає дані про всі наявні монети на гаманці"""
    wallet_info = {
        "usdt_free_balance": 0.0,
        "holdings": [],
        "asset_quantities": {},
        "error": False
    }
    try:
        account = binance_client.get_account()
        global KNOWN_USDT_SYMBOLS
        if not KNOWN_USDT_SYMBOLS:
            KNOWN_USDT_SYMBOLS = {
                s["symbol"] for s in binance_client.get_exchange_info()["symbols"]
                if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
            }
        for b in account["balances"]:
            asset = b["asset"]
            free_qty = float(b["free"])
            locked_qty = float(b["locked"])
            total_qty = free_qty + locked_qty

            wallet_info["asset_quantities"][asset] = total_qty

            if asset == "USDT":
                wallet_info["usdt_free_balance"] = free_qty
            elif total_qty > 0 and (
                asset not in ["BNB", "USDC", "FDUSD", "BUSD"]
                or f"{asset}USDT" in (memory_symbols or ())
            ):
                symbol = f"{asset}USDT"
                if symbol not in (memory_symbols or ()) and symbol not in KNOWN_USDT_SYMBOLS:
                    continue
                try:
                    ticker = binance_client.get_ticker(symbol=symbol)
                    current_price = float(ticker["lastPrice"])
                    value_usdt = total_qty * current_price

                    if value_usdt >= 5.0:
                        wallet_info["holdings"].append({
                            "asset": asset,
                            "symbol": symbol,
                            "quantity": total_qty,
                            "free_quantity": free_qty,
                            "current_price": current_price,
                            "value_usdt": round(value_usdt, 2)
                        })
                except Exception as e:
                    if symbol in (memory_symbols or ()):
                        raise RuntimeError(f"Не вдалося отримати ціну {symbol}: {e}") from e
                    print(f"⚠️ Пропущено {asset}: немає ціни {symbol} ({e})")
                    continue
    except Exception as e:
        print(f"⚠️ Помилка отримання даних гаманця: {e}")
        wallet_info["error"] = True
    return wallet_info

def sync_memory_with_wallet(memory, wallet_info):
    """Звіряє кількість облікованих монет із підтвердженим балансом Binance."""
    if wallet_info.get("error", False):
        print("⚠️ Синхронізацію пам'яті пропущено через помилку запиту до Binance.")
        return

    asset_quantities = wallet_info.get("asset_quantities", {})
    memory_symbols = list(memory["active_trades"].keys())
    
    for sym in memory_symbols:
        base_asset = sym.replace("USDT", "")
        wallet_qty = float(asset_quantities.get(base_asset, 0.0))
        tracked_qty = float(memory["active_trades"][sym].get("qty", wallet_qty))
        if wallet_qty <= 0:
            print(f"🧹 [Синхронізація] Актив {base_asset} відсутній на балансі; видаляємо із пам'яті.")
            memory["active_trades"].pop(sym, None)
        elif tracked_qty > wallet_qty:
            print(
                f"🧹 [Синхронізація] Кількість {sym} зменшена "
                f"з {tracked_qty} до фактичних {wallet_qty}."
            )
            memory["active_trades"][sym]["qty"] = wallet_qty
            if "buy_price" in memory["active_trades"][sym]:
                memory["active_trades"][sym]["amount_usdt"] = round(
                    wallet_qty * float(memory["active_trades"][sym]["buy_price"]), 2
                )

    save_memory(memory)

def check_auto_sl_tp(memory):
    """Виправлено: Перевірка та автоматичне закриття позицій за рівнями Stop-Loss та Take-Profit"""
    active_trades = list(memory.get("active_trades", {}).items())
    for symbol, trade in active_trades:
        tp = trade.get("take_profit", 0.0)
        sl = trade.get("stop_loss", 0.0)
        if tp <= 0 and sl <= 0:
            continue
        if trade.get("oco"):
            continue  # SL/TP виконує біржа
        try:
            ticker = binance_client.get_ticker(symbol=symbol)
            current_price = float(ticker["lastPrice"])
            
            trigger_reason = None
            if tp > 0 and current_price >= tp:
                trigger_reason = f"Take-Profit ({tp})"
            elif sl > 0 and current_price <= sl:
                trigger_reason = f"Stop-Loss ({sl})"

            if trigger_reason:
                print(f"🎯 Спрацював автоматичний {trigger_reason} для {symbol} за ціною {current_price}!")
                base_asset = symbol.replace("USDT", "")
                account = binance_client.get_account()
                for b in account["balances"]:
                    if b["asset"] == base_asset:
                        free_qty = float(b["free"])
                        tracked_qty = float(trade.get("qty", free_qty))
                        qty = min(free_qty, tracked_qty)
                        if qty > 0:
                            step_size = get_symbol_lot_size(symbol)
                            safe_qty = round_step_size(qty, step_size)
                            min_notional = get_symbol_min_notional(symbol)
                            if (
                                safe_qty <= 0
                                or safe_qty * current_price < min_notional
                            ):
                                print(f"🧹 {symbol}: залишок {safe_qty or qty} (~{qty * current_price:.4f} USDT) менший за мінімальний ордер; прибираємо з пам'яті як пил.")
                                memory["active_trades"].pop(symbol, None)
                                save_memory(memory)
                                break
                            order = place_order_with_intent(
                                memory, "SELL", symbol, quantity=safe_qty
                            )
                            avg_sell_p, executed_q, comm, base_fee_qty = calculate_executed_fill_details(order)
                            
                            buy_price = trade.get("buy_price", current_price)
                            pnl_usdt = ((avg_sell_p - buy_price) * executed_q) - comm
                            memory["total_pnl_usdt"] = memory.get("total_pnl_usdt", 0.0) + pnl_usdt
                            memory["trade_history"].append({
                                "symbol": symbol,
                                "buy_price": buy_price,
                                "sell_price": avg_sell_p,
                                "qty": executed_q,
                                "pnl_usdt": round(pnl_usdt, 2),
                                "reason": trigger_reason,
                                "time": time.strftime('%Y-%m-%d %H:%M:%S')
                            })
                            remaining_qty = max(0.0, tracked_qty - executed_q - base_fee_qty)
                            if remaining_qty > 0:
                                memory["active_trades"][symbol]["qty"] = remaining_qty
                            else:
                                memory["active_trades"].pop(symbol, None)
                            commit_order_memory(memory)
                            print(f"✅ {symbol} успішно закрито по {trigger_reason}. PnL: {pnl_usdt:+.2f} USDT")
                            if remaining_qty > 0:
                                ensure_oco_safe(memory, symbol, force=True)
                        break
        except Exception as e:
            print(f"⚠️ Помилка перевірки SL/TP для {symbol}: {e}")

def free_up_usdt_if_needed(required_amount=MIN_BINANCE_NOTIONAL_USDT, memory=None, exclude_symbol=None):
    """Виправлення: Закриває ДІЙСНО найменш вигідну (найгіршу за PnL) позицію"""
    wallet_info = get_account_balances_and_positions(memory.get("active_trades", {}) if memory else None)
    current_balance = wallet_info["usdt_free_balance"]
    
    if current_balance >= required_amount:
        return current_balance

    print(f"⚠️ Балансу ({current_balance:.2f} USDT) недостатньо для нової угоди ({required_amount} USDT).")
    print("🔄 Шукаємо та закриваємо найменш вигідну відкриту позицію для вивільнення коштів...")

    if wallet_info.get("error", False):
        print("❌ Не вдалося перевірити баланс Binance; позиції не продаватимуться.")
        return current_balance

    holdings = []
    for holding in wallet_info["holdings"]:
        free_qty = float(holding.get("free_quantity", 0))
        symbol = holding["symbol"]
        if symbol == exclude_symbol:
            continue
        # Продаємо лише позиції, куплені ботом
        if not memory or symbol not in memory.get("active_trades", {}):
            continue
        if memory["active_trades"][symbol].get("oco"):
            free_qty = float(holding.get("quantity", free_qty))  # кількість заблокована OCO
        free_qty = min(
            free_qty,
            float(memory["active_trades"][symbol].get("qty", free_qty))
        )
        if free_qty * holding["current_price"] >= get_symbol_min_notional(symbol):
            holdings.append(holding)
    if not holdings:
        print("❌ На гаманці немає активних криптовалютних позицій для закриття.")
        return current_balance

    # Обчислюємо PnL для кожної позиції
    evaluated_holdings = []
    for h in holdings:
        sym = h["symbol"]
        curr_p = h["current_price"]
        buy_p = curr_p
        if memory and sym in memory.get("active_trades", {}):
            buy_p = memory["active_trades"][sym].get("buy_price", curr_p)
        pnl_pct = ((curr_p - buy_p) / buy_p) * 100.0 if buy_p > 0 else 0.0
        evaluated_holdings.append((pnl_pct, h, buy_p))

    # Сортуємо за зростанням PnL (найгірший PnL перший)
    evaluated_holdings.sort(key=lambda x: x[0])
    worst_pnl, worst_holding, buy_price = evaluated_holdings[0]

    symbol = worst_holding["symbol"]
    qty = worst_holding["free_quantity"]
    if memory and memory.get("active_trades", {}).get(symbol, {}).get("oco"):
        qty = worst_holding["quantity"]
    if memory and symbol in memory.get("active_trades", {}):
        qty = min(qty, float(memory["active_trades"][symbol].get("qty", qty)))

    try:
        step_size = get_symbol_lot_size(symbol)
        safe_qty = round_step_size(qty, step_size)
        min_notional = get_symbol_min_notional(symbol)
        if safe_qty <= 0 or safe_qty * worst_holding["current_price"] < min_notional:
            print(f"⚠️ Доступна кількість {symbol} нижча за мінімальний обсяг Binance.")
            return current_balance
        print(f"💰 Закриваємо найменш вигідну позицію по {symbol} (PnL: {worst_pnl:.2f}%)...")
        if not cancel_oco(memory, symbol):
            return current_balance
        order = place_order_with_intent(
            memory, "SELL", symbol, quantity=safe_qty
        )
        avg_sell_p, executed_q, comm, base_fee_qty = calculate_executed_fill_details(order)

        if memory and symbol in memory.get("active_trades", {}):
            pnl_usdt = ((avg_sell_p - buy_price) * executed_q) - comm
            memory["total_pnl_usdt"] = memory.get("total_pnl_usdt", 0.0) + pnl_usdt
            memory["trade_history"].append({
                "symbol": symbol,
                "buy_price": buy_price,
                "sell_price": avg_sell_p,
                "qty": executed_q,
                "pnl_usdt": round(pnl_usdt, 2),
                "reason": "Freeing up USDT",
                "time": time.strftime('%Y-%m-%d %H:%M:%S')
            })
            tracked_qty = float(memory["active_trades"][symbol].get("qty", executed_q))
            remaining_qty = max(0.0, tracked_qty - executed_q - base_fee_qty)
            if remaining_qty > 0:
                memory["active_trades"][symbol]["qty"] = remaining_qty
            else:
                memory["active_trades"].pop(symbol, None)
        commit_order_memory(memory)
        if symbol in memory.get("active_trades", {}):
            ensure_oco_safe(memory, symbol, force=True)

        time.sleep(2)
        return get_account_balances_and_positions(memory.get("active_trades", {}) if memory else None)["usdt_free_balance"]
    except Exception as e:
        print(f"❌ Помилка при закритті позиції для вивільнення коштів: {e}")
        if memory and symbol in memory.get("active_trades", {}) and not memory["active_trades"][symbol].get("oco"):
            ensure_oco_safe(memory, symbol, force=True)

    return current_balance

def ask_claude_to_select_and_trade(market_data, wallet_info, memory):
    """ШІ аналізує ринок з урахуванням пам'яті"""
    model_to_use = get_best_available_model()
    goal = memory.get("performance_goal", {})

    prompt = f"""
    ТВОЯ ТРИВАЛА ПАМ'ЯТЬ (ІСТОРІЯ ТА АКТИВНІ ПОЗИЦІЇ):
    - Загальний реалізований PnL: {memory.get('total_pnl_usdt', 0.0):.2f} USDT
    - Цільове вікно: {goal.get('profit_usdt', 0.0):+.2f}/{PERFORMANCE_TARGET_USDT:.2f} USDT,
      статус {goal.get('status', 'active')}, залишилось приблизно {goal.get('seconds_remaining', 0) // 3600} год.
    - Деталі активних куплених позицій (з твоєї пам'яті):
    {json.dumps(memory.get('active_trades', {}), indent=2)}
    - Останні завершені угоди:
    {json.dumps(memory.get('trade_history', [])[-5:], indent=2)}

    ПОТОЧНИЙ СТАН ГАМАНЦЯ НА BINANCE:
    - Вільний баланс USDT: {wallet_info['usdt_free_balance']} USDT
    - Наявні монети (>5 USDT):
    {json.dumps(wallet_info['holdings'], indent=2)}

    ПОТОЧНИЙ СТАН ТОП-{len(market_data)} МОНЕТ НА РИНКУ (відсортовано за momentum_score від найперспективнішої;
    є RSI, EMA, ATR, співвідношення обсягу та орієнтовні SL/TP на основі ATR):
    {json.dumps(market_data, indent=2)}
    
    Завдання:
    1. Використовуй пам'ять про ціну входу, щоб розрахувати прибуток/збиток перед продажем.
    2. Якщо позиція досягла цілі або тренд зламався — обери "SELL".
    3. Якщо вільний баланс USDT достатній для угоди, обери НАЙПЕРСПЕКТИВНІШУ монету зі списку
       (сильний тренд EMA9>EMA21, RSI 50-68, зростаючий обсяг, без перегрітого росту) і обери "BUY".
       Не обирай HOLD лише через загальну обережність: HOLD допустимий, тільки якщо ВСІ монети
       мають слабкий тренд або перегріті (momentum_score <= 2).
    4. Для BUY обов'язково задай stop_loss нижче ціни і take_profit вище ціни; бери за основу
       suggested_stop_loss/suggested_take_profit, співвідношення прибуток/ризик не менше 1.5.
       quantity_pct — частка вільного USDT (50-100); код сам обмежить розмір позиції лімітами ризику.
       ЖОРСТКІ ЛІМІТИ (угода буде відхилена кодом): не більше {MAX_OPEN_POSITIONS} відкритих позицій,
       стоп не далі {MAX_SL_DISTANCE_PCT}% від ціни, прибуток/ризик ≥ {MIN_REWARD_TO_RISK},
       momentum_score ≥ {MIN_MOMENTUM_SCORE}, RSI ≤ {MAX_ENTRY_RSI}, обсяг 24г ≥ {MIN_VOLUME_24H_USDT} USDT.
    5. У полі "reasoning" коротко опиши, чому саме ця монета найкраща, і чому відкинуті інші топ-кандидати.
    """

    system_instruction = (
        f"Ти — помічник для аналізу спотового крипторинку. Є операційна ціль "
        f"+{PERFORMANCE_TARGET_USDT:.0f} USDT реалізованого PnL за {PERFORMANCE_WINDOW_DAYS} днів. "
        "Це не гарантія і не привід форсувати угоди: ніколи не збільшуй ризик, не вигадуй сигнали "
        "і не порушуй ліміти з коду. Пріоритет — збереження капіталу; якщо сетапу немає, обирай HOLD. "
        "Код автоматично призупинить нові покупки після завершення вікна без цілі, "
        "але продовжить супровід відкритих позицій."
    )

    last_error = None
    for attempt in range(1, 4):
        try:
            with client_anthropic.messages.stream(
                model=model_to_use,
                max_tokens=4000,
                system=system_instruction,
                messages=[{"role": "user", "content": prompt}],
                output_config={
                    "format": {
                        "type": "json_schema",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "action": {"type": "string", "enum": ["BUY", "SELL", "HOLD"]},
                                "symbol": {"type": "string"},
                                "quantity_pct": {"type": "number"},
                                "take_profit": {"type": "number"},
                                "stop_loss": {"type": "number"},
                                "reasoning": {"type": "string"}
                            },
                            "required": ["action", "symbol", "quantity_pct", "take_profit", "stop_loss", "reasoning"],
                            "additionalProperties": False
                        }
                    }
                }
            ) as stream:
                response_text = "".join(list(stream.text_stream))
            decision = json.loads(response_text)
            if not isinstance(decision, dict):
                raise ValueError("Claude повернув не JSON-об'єкт")
            return decision
        except (ValueError, anthropic.APIError) as e:
            last_error = e
            if isinstance(e, anthropic.APIError):
                _record_api_error(e, "anthropic")
            print(f"⚠️ Некоректна відповідь Claude (спроба {attempt}/3): {e}")
            time.sleep(2)

    return {
        "action": "HOLD",
        "symbol": "",
        "quantity_pct": 0,
        "take_profit": 0,
        "stop_loss": 0,
        "reasoning": f"Claude не повернув коректну відповідь після 3 спроб ({last_error}); безпечне рішення — HOLD.",
    }

def verify_trade_immediately(symbol, bought_qty, memory, buy_price):
    """Виправлення: Продає ТІЛЬКИ щойно куплену кількість (bought_qty) при скасуванні"""
    print(f"🔎 Миттєва перевірка графіка для {symbol} після відкриття ордера...")
    time.sleep(3)
    try:
        klines = binance_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=5)
        recent_1m_prices = [float(k[4]) for k in klines]
        ticker = binance_client.get_ticker(symbol=symbol)

        check_prompt = f"""
        Щойно було відкрито угоду BUY по монеті {symbol}.
        Поточна ціна: {ticker['lastPrice']}
        Останні 1-хвилинні ціни свічок: {recent_1m_prices}

        Проаналізуй динаміку останніх кількох хвилин.
        Чи підтверджується позитивний рух, чи бачиш ти негайний хибний пробій/небезпеку?
        
        Відповідай коротким JSON у форматі: {{"keep_trade": true, "reason": "коротке пояснення"}}
        """

        model_to_use = get_best_available_model()
        try:
            response = client_anthropic.messages.create(
                model=model_to_use,
                max_tokens=300,
                messages=[{"role": "user", "content": check_prompt}]
            )
        except anthropic.APIError as e:
            _record_api_error(e, "anthropic")
            raise

        raw_text = ""
        for block in response.content:
            if getattr(block, 'type', None) == 'text':
                raw_text = block.text
                break
        if not raw_text:
            raw_text = str(response.content)

        keep_trade = True
        reason = "Динаміка стабільна"

        try:
            clean_json = raw_text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean_json)
            keep_trade = result.get("keep_trade", True)
            reason = result.get("reason", reason)
        except Exception:
            if "false" in raw_text.lower():
                keep_trade = False
                reason = "Виявлено ризик (парсинг відновив негативний сигнал)"

        if not keep_trade:
            print(f"⚠️ ШІ виявив ризик після перевірки: {reason}")
            print(f"🚨 Скасовуємо (закриваємо) щойно куплену позицію {bought_qty} {symbol}!")
            step_size = get_symbol_lot_size(symbol)
            safe_qty = round_step_size(bought_qty, step_size)
            if safe_qty > 0:
                order = place_order_with_intent(
                    memory, "SELL", symbol, quantity=safe_qty
                )
                avg_sell_price, executed_qty, commission_usdt, base_fee_qty = calculate_executed_fill_details(order)
                pnl_usdt = ((avg_sell_price - buy_price) * executed_qty) - commission_usdt
                memory["total_pnl_usdt"] = memory.get("total_pnl_usdt", 0.0) + pnl_usdt
                memory.setdefault("trade_history", []).append({
                    "symbol": symbol,
                    "buy_price": buy_price,
                    "sell_price": avg_sell_price,
                    "qty": executed_qty,
                    "pnl_usdt": round(pnl_usdt, 2),
                    "reason": "Immediate validation",
                    "time": time.strftime('%Y-%m-%d %H:%M:%S')
                })
                if symbol in memory.get("active_trades", {}):
                    active = memory["active_trades"][symbol]
                    previous_qty = float(active.get("qty", executed_qty))
                    remaining_qty = max(0.0, previous_qty - executed_qty - base_fee_qty)
                    if remaining_qty > 0:
                        previous_cost = previous_qty * float(active.get("buy_price", buy_price))
                        active["qty"] = remaining_qty
                        active["buy_price"] = max(
                            0.0, (previous_cost - executed_qty * buy_price) / remaining_qty
                        )
                        active["amount_usdt"] = round(
                            remaining_qty * active["buy_price"], 2
                        )
                    else:
                        memory["active_trades"].pop(symbol, None)
                commit_order_memory(memory)
                print(f"✅ Щойно куплену частку по {symbol} успішно продано.")
        else:
            print(f"👍 Перевірка успішна: {reason}. Залишаємо позицію!")

    except Exception as e:
        print(f"⚠️ Не вдалося провести миттєву перевірку: {e}")

def _history_time(entry):
    try:
        return time.mktime(time.strptime(entry.get("time", ""), '%Y-%m-%d %H:%M:%S'))
    except (ValueError, TypeError):
        return None

def update_performance_goal(memory):
    """Веде цільове вікно; пропуск цілі блокує нові BUY, але не супровід позицій."""
    now = time.time()
    goal = memory.get("performance_goal")
    if not isinstance(goal, dict):
        goal = {
            "started_at": now,
            "baseline_realized_pnl_usdt": float(memory.get("total_pnl_usdt", 0.0)),
            "status": "active",
        }
        memory["performance_goal"] = goal
        save_memory(memory)
        print(
            f"🎯 Почався період цілі: {PERFORMANCE_TARGET_USDT:.2f} USDT "
            f"за {PERFORMANCE_WINDOW_DAYS} днів."
        )

    started_at = float(goal["started_at"])
    baseline = float(goal["baseline_realized_pnl_usdt"])
    profit = float(memory.get("total_pnl_usdt", 0.0)) - baseline
    deadline = started_at + PERFORMANCE_WINDOW_DAYS * 24 * 60 * 60
    changed = False

    if goal.get("status") == "active" and profit >= PERFORMANCE_TARGET_USDT:
        goal["status"] = "achieved"
        changed = True
        print(f"✅ Ціль прибутку виконана: +{profit:.2f} USDT.")
    elif goal.get("status") == "active" and now >= deadline:
        goal["status"] = "paused"
        changed = True
        print(
            f"⏸️ Ціль не досягнута за {PERFORMANCE_WINDOW_DAYS} днів "
            f"(результат {profit:+.2f} USDT). Нові BUY призупинені; "
            "моніторинг і захист відкритих позицій продовжуються."
        )

    if changed:
        save_memory(memory)
    goal["profit_usdt"] = round(profit, 2)
    goal["seconds_remaining"] = max(0, int(deadline - now))
    return goal

def check_buy_risk_limits(memory, symbol, current_price, take_profit, stop_loss, equity):
    """Повертає текст причини відмови або None, якщо BUY проходить усі ліміти ризику."""
    if memory.get("performance_goal", {}).get("status") == "paused":
        return "цільове вікно завершилося без цілі; нові покупки призупинені"

    if symbol not in memory.get("active_trades", {}) and len(memory.get("active_trades", {})) >= MAX_OPEN_POSITIONS:
        return f"вже відкрито {MAX_OPEN_POSITIONS} позиції (ліміт)"

    sl_distance_pct = (current_price - stop_loss) / current_price * 100
    if sl_distance_pct > MAX_SL_DISTANCE_PCT:
        return f"стоп занадто далеко ({sl_distance_pct:.1f}% > {MAX_SL_DISTANCE_PCT}%)"
    reward_to_risk = (take_profit - current_price) / (current_price - stop_loss)
    if reward_to_risk < MIN_REWARD_TO_RISK:
        return f"співвідношення прибуток/ризик {reward_to_risk:.2f} < {MIN_REWARD_TO_RISK}"

    snapshot = MARKET_SNAPSHOT.get(symbol)
    if not snapshot:
        return "немає свіжих ринкових даних по монеті"
    if snapshot["momentum_score"] < MIN_MOMENTUM_SCORE:
        return f"слабкий тренд (momentum_score {snapshot['momentum_score']} < {MIN_MOMENTUM_SCORE})"
    if snapshot["rsi_14"] > MAX_ENTRY_RSI:
        return f"монета перегріта (RSI {snapshot['rsi_14']} > {MAX_ENTRY_RSI})"
    if snapshot["volume_24h_usdt"] < MIN_VOLUME_24H_USDT:
        return "низька ліквідність"

    now = time.time()
    today = time.strftime('%Y-%m-%d')
    daily_pnl = sum(
        float(h.get("pnl_usdt", 0.0))
        for h in memory.get("trade_history", [])
        if str(h.get("time", "")).startswith(today)
    )
    if equity > 0 and daily_pnl <= -equity * DAILY_LOSS_LIMIT_PCT / 100:
        return f"досягнуто денний ліміт збитку ({daily_pnl:.2f} USDT)"

    for h in memory.get("trade_history", [])[-30:]:
        t = _history_time(h)
        if (
            h.get("symbol") == symbol
            and float(h.get("pnl_usdt", 0.0)) < 0
            and t is not None
            and now - t < SYMBOL_COOLDOWN_MINUTES * 60
        ):
            return f"пауза по {symbol} після збиткової угоди"
    return None

def execute_trade_safely(trade_decision, memory):
    """Виправлення: Точний розрахунок PnL за реальним ордером та підтримка часткових продажів"""
    action = trade_decision.get("action")
    if action not in {"BUY", "SELL", "HOLD"}:
        print(f"❌ Невідома дія від ШІ: {action!r}")
        return
    if action == "HOLD":
        print("⏳ Рішення: HOLD. Очікуємо наступного сканування.")
        print(f"💡 Чому HOLD: {trade_decision.get('reasoning')}")
        return

    symbol = str(trade_decision.get("symbol", "")).strip().upper()

    if not symbol.endswith("USDT"):
        symbol = f"{symbol}USDT"
    if len(symbol) <= 4 or not symbol.isalnum():
        print(f"❌ Некоректний символ від ШІ: {symbol!r}")
        return

    try:
        ai_pct = float(trade_decision.get("quantity_pct", 100.0))
        take_profit = float(trade_decision.get("take_profit", 0.0))
        stop_loss = float(trade_decision.get("stop_loss", 0.0))
    except (TypeError, ValueError):
        print("❌ Відхилено рішення: quantity_pct, take_profit і stop_loss мають бути числами.")
        return

    if not math.isfinite(ai_pct) or not 0 < ai_pct <= 100:
        print(f"❌ Відхилено рішення: quantity_pct має бути в межах (0, 100], отримано {ai_pct}.")
        return
    if (
        not math.isfinite(take_profit)
        or not math.isfinite(stop_loss)
        or take_profit < 0
        or stop_loss < 0
    ):
        print("❌ Відхилено рішення: рівні Take Profit і Stop Loss мають бути скінченними невід'ємними числами.")
        return

    print(f"\n================ AI ВИБІР ТА АНАЛІЗ ================")
    print(f"🪙 Обрана монета: {symbol}")
    print(f"🎯 Дія: {action}")
    print(f"💡 Обґрунтування: {trade_decision.get('reasoning')}")
    print(f"📈 Take Profit: {take_profit} | Stop Loss: {stop_loss}")
    print(f"=====================================================")

    try:
        # === БЛОК ПРОДАЖУ (SELL) ===
        if action == "SELL":
            if not cancel_oco(memory, symbol):
                print(f"ℹ️ OCO {symbol} уже виконався на біржі; продаж скасовано.")
                return
            base_asset = symbol.replace("USDT", "")
            account = binance_client.get_account()
            asset_balance = 0.0
            for asset in account["balances"]:
                if asset["asset"] == base_asset:
                    asset_balance = float(asset["free"])
                    break
            
            ticker = binance_client.get_ticker(symbol=symbol)
            current_price = float(ticker["lastPrice"])
            sellable_qty = asset_balance
            if symbol in memory.get("active_trades", {}):
                tracked_qty = float(memory["active_trades"][symbol].get("qty", asset_balance))
                sellable_qty = min(asset_balance, tracked_qty)
            sell_qty = sellable_qty * (ai_pct / 100.0)
            if sell_qty <= 0:
                print(f"⚠️ Немає доступного балансу {base_asset} для продажу.")
                return
            min_notional = get_symbol_min_notional(symbol)
            if sell_qty * current_price < min_notional:
                print(f"⚠️ Обсяг продажу {symbol} ({sell_qty * current_price:.2f} USDT) менший за мінімум; продаж відхилено.")
                return
            
            if sell_qty > 0:
                step_size = get_symbol_lot_size(symbol)
                safe_sell_qty = round_step_size(sell_qty, step_size)
                if safe_sell_qty > 0 and safe_sell_qty * current_price >= min_notional:
                    order = place_order_with_intent(
                        memory, "SELL", symbol, quantity=safe_sell_qty
                    )
                    avg_sell_price, executed_qty, commission_usdt, base_fee_qty = calculate_executed_fill_details(order)
                    print(f"✅ Продано {executed_qty} {base_asset} за середньою ціною {avg_sell_price:.4f} (Комісія: {commission_usdt:.4f} USDT)")

                    # --- Частковий або повний розрахунок PnL ---
                    if symbol in memory["active_trades"]:
                        active = memory["active_trades"][symbol]
                        buy_price = active.get("buy_price", avg_sell_price)
                        stored_qty = float(active.get("qty", executed_qty))

                        pnl_usdt = ((avg_sell_price - buy_price) * executed_qty) - commission_usdt
                        memory["total_pnl_usdt"] = memory.get("total_pnl_usdt", 0.0) + pnl_usdt

                        memory["trade_history"].append({
                            "symbol": symbol,
                            "buy_price": buy_price,
                            "sell_price": avg_sell_price,
                            "qty": executed_qty,
                            "pnl_usdt": round(pnl_usdt, 2),
                            "time": time.strftime('%Y-%m-%d %H:%M:%S')
                        })

                        # Якщо це був частковий продажу — зберігаємо залишок позиції
                        remaining_qty = max(0.0, stored_qty - executed_qty - base_fee_qty)
                        if remaining_qty > 0:
                            memory["active_trades"][symbol]["qty"] = remaining_qty
                            memory["active_trades"][symbol]["amount_usdt"] = round(remaining_qty * buy_price, 2)
                        else:
                            memory["active_trades"].pop(symbol, None)

                        print(f"🧠 [Пам'ять оновлено] Фінансовий результат: {pnl_usdt:+.2f} USDT")
                    commit_order_memory(memory)

        # === БЛОК ПОКУПКИ (BUY) ===
        elif action == "BUY":
            current_price = float(binance_client.get_ticker(symbol=symbol)["lastPrice"])
            if take_profit <= current_price or stop_loss <= 0 or stop_loss >= current_price:
                print("❌ Відхилено BUY: потрібні Take Profit вище та Stop Loss (>0) нижче поточної ціни.")
                return

            wallet = get_account_balances_and_positions(memory.get("active_trades", {}))
            if wallet.get("error"):
                print("❌ Не вдалося отримати баланс Binance; BUY скасовано.")
                return
            balance = wallet["usdt_free_balance"]
            equity = balance + sum(h["value_usdt"] for h in wallet["holdings"])

            refusal = check_buy_risk_limits(
                memory, symbol, current_price, take_profit, stop_loss, equity
            )
            if refusal:
                print(f"🛑 [Ризик-менеджмент] BUY {symbol} відхилено: {refusal}.")
                return

            min_notional = get_symbol_min_notional(symbol)
            # Розмір позиції: за запитом ШІ, але не більше лімітів позиції та ризику на угоду
            risk_per_unit_pct = (current_price - stop_loss) / current_price
            max_by_risk = equity * (MAX_RISK_PER_TRADE_PCT / 100) / risk_per_unit_pct
            max_by_size = equity * (MAX_POSITION_PCT_OF_EQUITY / 100)
            existing_value = 0.0
            for h in wallet["holdings"]:
                if h["symbol"] == symbol:
                    existing_value = h["value_usdt"]
            allocation_usdt = min(
                balance * (ai_pct / 100.0), max_by_risk, max_by_size - existing_value, balance
            )
            allocation_usdt = float(
                (Decimal(str(max(allocation_usdt, 0.0))) * 100).to_integral_value(rounding=ROUND_DOWN) / 100
            )
            print(
                f"🛡️ [Ризик-менеджмент] Капітал {equity:.2f} USDT; розмір позиції {allocation_usdt:.2f} USDT "
                f"(макс. збиток по SL ≈ {allocation_usdt * risk_per_unit_pct:.2f} USDT)."
            )
            if allocation_usdt < min_notional:
                print(f"⚠️ Дозволена сума ({allocation_usdt:.2f} USDT) нижча за мінімум Binance; купівля {symbol} не виконується.")
                return

            order = place_order_with_intent(
                memory,
                "BUY",
                symbol,
                quoteOrderQty=allocation_usdt
            )
            
            avg_buy_price, executed_qty, commission_usdt, _ = calculate_executed_fill_details(order)
            print(f"✅ Куплено {executed_qty} {symbol} за ціною {avg_buy_price:.4f} (Комісія: {commission_usdt:.4f} USDT)")

            if executed_qty <= 0:
                raise RuntimeError(f"Ордер BUY {symbol} не повернув виконану кількість.")

            # Повторні покупки об'єднуються із розрахунком середньої ціни входу.
            previous = memory.setdefault("active_trades", {}).get(symbol)
            if previous:
                previous_qty = float(previous.get("qty", 0.0))
                combined_qty = previous_qty + executed_qty
                combined_cost = (
                    previous_qty * float(previous.get("buy_price", avg_buy_price))
                    + executed_qty * avg_buy_price
                )
                memory["active_trades"][symbol] = {
                    **previous,
                    "buy_price": combined_cost / combined_qty,
                    "qty": combined_qty,
                    "amount_usdt": round(
                        float(previous.get("amount_usdt", 0.0)) + allocation_usdt, 2
                    ),
                    "take_profit": take_profit or previous.get("take_profit", 0.0),
                    "stop_loss": stop_loss or previous.get("stop_loss", 0.0),
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                }
            else:
                memory["active_trades"][symbol] = {
                    "buy_price": avg_buy_price,
                    "qty": executed_qty,
                    "amount_usdt": round(allocation_usdt, 2),
                    "take_profit": take_profit,
                    "stop_loss": stop_loss,
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                }
            commit_order_memory(memory)
            print(f"🧠 [Пам'ять оновлено] Купівля записана (SL: {stop_loss} | TP: {take_profit})")

            # Миттєва перевірка (передаємо точний куплений об'єм)
            verify_trade_immediately(symbol, executed_qty, memory, avg_buy_price)
            ensure_oco_safe(memory, symbol, force=True)

    except BinanceAPIException as e:
        print(f"❌ Помилка Binance API: {e}")
    except Exception as e:
        print(f"❌ Помилка: {e}")
    finally:
        # Після SELL (у т.ч. відхиленого/часткового) повертаємо біржовий захист залишку
        if action == "SELL":
            trade = memory.get("active_trades", {}).get(symbol)
            if trade and not trade.get("oco"):
                ensure_oco_safe(memory, symbol, force=True)


def set_current_memory(memory):
    global CURRENT_MEMORY, API_EMERGENCY_TRIGGERED
    CURRENT_MEMORY = memory
    if memory.get("emergency_stop"):
        API_EMERGENCY_TRIGGERED = True


def save_emergency_state(memory):
    try:
        save_memory(memory)
    except Exception as e:
        print(f"❌ Не вдалося зберегти стан аварійної зупинки: {e}")


def emergency_liquidate_all(memory):
    """Cancel orders and market-sell all sellable non-USDT spot balances."""
    global API_EMERGENCY_TRIGGERED
    stop = memory["emergency_stop"]
    stop["attempts"] = int(stop.get("attempts", 0)) + 1
    stop["last_attempt_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    stop["status"] = "liquidating"
    retryable_failure = False
    unsold = []
    print(
        f"🚨 АВАРІЙНА ЛІКВІДАЦІЯ, спроба {stop['attempts']}: "
        f"{stop['reason']}"
    )

    try:
        open_orders = binance_client.get_open_orders()
        symbols_to_cancel = {order["symbol"] for order in open_orders}
    except Exception as e:
        print(f"❌ Не вдалося отримати відкриті ордери для скасування: {e}")
        symbols_to_cancel = set()
        retryable_failure = True
    symbols_to_cancel.update(
        symbol for symbol, trade in memory.get("active_trades", {}).items()
        if trade.get("oco")
    )
    for symbol in sorted(symbols_to_cancel):
        try:
            binance_client.cancel_all_open_orders(symbol=symbol)
            trade = memory.get("active_trades", {}).get(symbol)
            if trade:
                trade.pop("oco", None)
            save_emergency_state(memory)
        except Exception as e:
            print(f"⚠️ Не вдалося скасувати ордери {symbol}: {e}")
            retryable_failure = True

    try:
        exchange_info = binance_client.get_exchange_info()
        account = binance_client.get_account()
    except Exception as e:
        stop["last_error"] = str(e)
        stop["status"] = "liquidating"
        print(f"❌ Не вдалося отримати дані Binance для аварійного продажу: {e}")
        save_emergency_state(memory)
        return

    usdt_pairs = {}
    for info in exchange_info.get("symbols", []):
        if info.get("quoteAsset") != "USDT" or info.get("status") != "TRADING":
            continue
        filters = {item.get("filterType"): item for item in info.get("filters", [])}
        lot_filter = filters.get("MARKET_LOT_SIZE", {})
        if float(lot_filter.get("stepSize", 0) or 0) <= 0:
            lot_filter = filters.get("LOT_SIZE", {})
        notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        usdt_pairs[info["baseAsset"]] = {
            "symbol": info["symbol"],
            "step": float(lot_filter.get("stepSize", 0) or 0),
            "min_qty": float(lot_filter.get("minQty", 0) or 0),
            "max_qty": float(lot_filter.get("maxQty", 0) or 0),
            "min_notional": float(notional_filter.get("minNotional", 0) or 0),
        }

    for balance in account.get("balances", []):
        asset = balance["asset"]
        if asset == "USDT":
            continue
        free_qty = float(balance["free"])
        locked_qty = float(balance["locked"])
        total_qty = free_qty + locked_qty
        if total_qty <= 0:
            continue
        pair = usdt_pairs.get(asset)
        if not pair:
            unsold.append(f"{asset}: немає активної USDT-пари")
            continue
        if free_qty <= 0:
            unsold.append(f"{asset}: кошти заблоковані ордерами")
            continue
        try:
            price = float(binance_client.get_ticker(symbol=pair["symbol"])["lastPrice"])
            quantity = round_step_size(free_qty, pair["step"])
            if pair["max_qty"] > 0:
                quantity = min(quantity, pair["max_qty"])
                quantity = round_step_size(quantity, pair["step"])
            min_notional = pair["min_notional"]
            if quantity < pair["min_qty"] or quantity <= 0 or quantity * price < min_notional:
                unsold.append(f"{asset}: баланс нижче мінімуму ордера")
                continue
            order = binance_client.order_market_sell(
                symbol=pair["symbol"],
                quantity=plain_number(quantity),
                newClientOrderId=f"emg{uuid.uuid4().hex[:30]}",
            )
            avg_price, executed_qty, commission, base_fee_qty = calculate_executed_fill_details(order)
            trade = memory.get("active_trades", {}).get(pair["symbol"])
            if trade and executed_qty > 0:
                tracked_qty = float(trade.get("qty", 0.0))
                attributed_qty = min(tracked_qty, executed_qty)
                buy_price = float(trade.get("buy_price", avg_price))
                attributed_commission = commission * attributed_qty / executed_qty
                pnl = (avg_price - buy_price) * attributed_qty - attributed_commission
                memory["total_pnl_usdt"] = float(memory.get("total_pnl_usdt", 0.0)) + pnl
                memory.setdefault("trade_history", []).append({
                    "symbol": pair["symbol"],
                    "buy_price": buy_price,
                    "sell_price": avg_price,
                    "qty": attributed_qty,
                    "pnl_usdt": round(pnl, 2),
                    "reason": "Emergency liquidation",
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                })
                remaining = max(0.0, tracked_qty - executed_qty - base_fee_qty)
                if remaining > 0:
                    trade["qty"] = remaining
                    trade["amount_usdt"] = round(remaining * buy_price, 2)
                else:
                    memory["active_trades"].pop(pair["symbol"], None)
            print(
                f"🚨 Продано {executed_qty:g} {asset} за {avg_price:g} USDT "
                f"(комісія {commission:g} USDT)."
            )
            stop.setdefault("sold_assets", {})[asset] = (
                float(stop.setdefault("sold_assets", {}).get(asset, 0.0)) + executed_qty
            )
            save_emergency_state(memory)
        except Exception as e:
            print(f"❌ Аварійний продаж {asset} не вдався: {e}")
            stop["last_error"] = str(e)
            retryable_failure = True

    try:
        final_account = binance_client.get_account()
    except Exception as e:
        stop["last_error"] = str(e)
        retryable_failure = True
        final_account = None

    if final_account is not None:
        final_balances = {
            item["asset"]: float(item["free"]) + float(item["locked"])
            for item in final_account.get("balances", [])
        }
        for symbol, trade in list(memory.get("active_trades", {}).items()):
            asset = symbol[:-4] if symbol.endswith("USDT") else symbol
            actual_qty = final_balances.get(asset, 0.0)
            if actual_qty <= 0:
                memory["active_trades"].pop(symbol, None)
            else:
                trade["qty"] = min(float(trade.get("qty", actual_qty)), actual_qty)
        for asset, quantity in final_balances.items():
            if asset == "USDT" or quantity <= 0:
                continue
            pair = usdt_pairs.get(asset)
            if not pair:
                continue
            try:
                price = float(binance_client.get_ticker(symbol=pair["symbol"])["lastPrice"])
            except Exception:
                retryable_failure = True
                continue
            if quantity * price < pair["min_notional"] or quantity < pair["min_qty"]:
                unsold.append(f"{asset}: залишок нижче мінімуму ордера")
            elif not any(item.startswith(f"{asset}:") for item in unsold):
                retryable_failure = True

    stop["unsold_assets"] = sorted(set(unsold))
    if retryable_failure:
        stop["status"] = "liquidating"
        print("⏳ Аварійна ліквідація не завершена; повтор через 60 секунд.")
    elif unsold:
        stop["status"] = "blocked"
        print("⛔ Є активи, які Binance не дозволяє продати автоматично:")
        for item in stop["unsold_assets"]:
            print(f"   • {item}")
    else:
        stop["status"] = "completed"
        print("🛑 Аварійна ліквідація завершена. Звичайну торгівлю вимкнено.")
    save_emergency_state(memory)
    API_EMERGENCY_TRIGGERED = True


def process_emergency_stop(memory):
    global API_EMERGENCY_TRIGGERED
    set_current_memory(memory)
    stop = memory.get("emergency_stop")
    if not stop:
        if API_ERROR_TOTAL < API_ERROR_THRESHOLD and not API_EMERGENCY_TRIGGERED:
            return False
        signature = API_ERROR_LAST or "api_errors_this_run"
        stop = {
            "status": "liquidating",
            "reason": (
                f"Накопичено {API_ERROR_TOTAL} помилок API за цей запуск "
                f"(останній тип: {signature})"
            ),
            "error_signature": signature,
            "triggered_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "error_count": API_ERROR_TOTAL,
            "attempts": 0,
        }
        memory["emergency_stop"] = stop
        print(f"🚨 {stop['reason']}; нові угоди вимкнено.")
        save_emergency_state(memory)
        API_EMERGENCY_TRIGGERED = True

    if stop.get("status") not in {"completed", "blocked"}:
        emergency_liquidate_all(memory)
    else:
        print(f"🛑 Аварійна зупинка активна (стан: {stop['status']}); нові угоди вимкнено.")
    return True


DEFAULT_KEEP_ALIVE_URL = "https://binance-jcm1.onrender.com"
KEEP_ALIVE_INTERVAL_SECONDS = 600


def publish_admin_snapshot(memory=None, wallet=None, status=None):
    global ADMIN_SNAPSHOT_JSON
    memory = memory or CURRENT_MEMORY or {}
    wallet = wallet or {}
    snapshot = {
        "status": status or (
            "emergency stop" if memory.get("emergency_stop")
            else "paused" if memory.get("admin_paused")
            else "running"
        ),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "wallet": {
            "usdt_free_balance": wallet.get("usdt_free_balance", 0.0),
            "holdings": wallet.get("holdings", []),
            "error": wallet.get("error", False),
        },
        "active_trades": {
            symbol: dict(trade)
            for symbol, trade in memory.get("active_trades", {}).items()
        },
        "total_pnl_usdt": memory.get("total_pnl_usdt", 0.0),
        "api_errors_this_run": API_ERROR_TOTAL,
        "trade_history": memory.get("trade_history", [])[-30:],
        "emergency_stop": dict(memory["emergency_stop"])
        if memory.get("emergency_stop") else None,
    }
    with ADMIN_SNAPSHOT_LOCK:
        ADMIN_SNAPSHOT_JSON = json.dumps(snapshot, ensure_ascii=False)


def get_admin_snapshot():
    with ADMIN_SNAPSHOT_LOCK:
        return json.loads(ADMIN_SNAPSHOT_JSON)


def submit_admin_action(action):
    if action not in {"pause", "resume", "emergency_sell"}:
        raise ValueError(f"Непідтримувана адміністративна команда: {action}")
    ADMIN_ACTIONS.put(action)


def process_admin_actions(memory):
    changed = False
    emergency_requested = False
    while True:
        try:
            action = ADMIN_ACTIONS.get_nowait()
        except queue.Empty:
            break
        if action == "pause":
            memory["admin_paused"] = True
            print("⏸️ Адміністратор призупинив нові угоди; супровід позицій триває.")
        elif action == "resume":
            if memory.get("emergency_stop"):
                print("⛔ Аварійна зупинка активна; відновлення заблоковане до ручної перевірки.")
            else:
                memory["admin_paused"] = False
                print("▶️ Адміністратор відновив роботу бота.")
        elif action == "emergency_sell":
            memory["emergency_stop"] = {
                "status": "liquidating",
                "reason": "Аварійний продаж запущений адміністратором",
                "error_signature": "admin_requested",
                "triggered_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "error_count": 0,
                "attempts": 0,
            }
            emergency_requested = True
            print("🚨 Адміністратор запросив аварійну ліквідацію всіх монет.")
        changed = True
    if changed:
        save_memory(memory)
        publish_admin_snapshot(memory)
    return emergency_requested


def admin_advisor(message, history):
    state = get_admin_snapshot()
    context = {
        "status": state["status"],
        "wallet": state["wallet"],
        "active_trades": state["active_trades"],
        "total_pnl_usdt": state["total_pnl_usdt"],
        "trade_history": state["trade_history"],
        "emergency_stop": state["emergency_stop"],
    }
    messages = [{
        "role": "user",
        "content": (
            "Поточний знімок стану бота (JSON):\n"
            + json.dumps(context, ensure_ascii=False)
            + "\nВикористовуй цей стан як актуальний контекст для подальшої розмови."
        ),
    }]
    messages.extend(
        {"role": turn["role"], "content": turn["content"]}
        for turn in history[-10:]
        if turn.get("role") in {"user", "assistant"}
    )
    messages.append({"role": "user", "content": message})
    try:
        response = client_anthropic.messages.create(
            model=get_best_available_model(),
            max_tokens=1200,
            system=(
                "Ти — аналітичний помічник адміністратора криптотрейдинг-бота. "
                "Відповідай на запит і розглядай повідомлення адміністратора як ідеї для аналізу, "
                "а не як команду до виконання. Ти не маєш доступу до інструментів і не можеш "
                "виконувати, ініціювати чи обіцяти угоди, змінювати налаштування або стан бота. "
                "Не проси API-ключі, паролі чи коди автентифікатора. Вказуй невизначеність і ризики."
            ),
            messages=messages,
        )
    except anthropic.APIError as e:
        _record_api_error(e, "anthropic")
        raise
    return "\n".join(
        block.text for block in response.content
        if getattr(block, "type", None) == "text"
    ).strip()


def start_health_server():
    # Render Web Service вимагає відкритий порт; він також обслуговує адмін-панель.
    port_value = os.getenv("PORT")
    if not port_value:
        return
    try:
        from admin_dashboard import create_admin_app
        from waitress import serve

        app = create_admin_app(get_admin_snapshot, submit_admin_action, admin_advisor)
        port = int(port_value)
        threading.Thread(
            target=serve,
            kwargs={"app": app, "host": "0.0.0.0", "port": port, "threads": 4},
            daemon=True,
        ).start()
    except Exception as e:
        print(f"⚠️ Не вдалося запустити вебсервер адміністратора: {e}")
        return
    print(f"🌐 Health та адмін-панель слухають порт {port_value}.")


def keep_alive_loop(url):
    # Render Free засинає після 15 хв без вхідного трафіку; пінг публічного URL цьому запобігає
    while True:
        time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)
        try:
            requests.get(url, timeout=20)
        except requests.RequestException as e:
            print(f"⚠️ Keep-alive пінг не вдався: {e}")


def start_keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("KEEP_ALIVE_URL") or DEFAULT_KEEP_ALIVE_URL
    if not os.getenv("PORT") or not url:
        return
    threading.Thread(target=keep_alive_loop, args=(url,), daemon=True).start()
    print(f"🔔 Keep-alive пінг кожні {KEEP_ALIVE_INTERVAL_SECONDS // 60} хв: {url}")


def log_public_ip():
    # Потрібно, щоб додати вихідну IP-адресу хостингу до білого списку Binance
    for url in ("https://api.ipify.org", "https://ifconfig.me/ip"):
        try:
            ip = requests.get(url, timeout=10).text.strip()
            print(f"🌍 Вихідна публічна IP цього бота: {ip} (додайте її в Binance API → trusted IPs)")
            return
        except requests.RequestException:
            continue
    print("⚠️ Не вдалося визначити публічну IP-адресу.")


if __name__ == "__main__":
    print("🤖 Бот запущений з виправленими багами (точний PnL, SL/TP, без втрати пам'яті).")
    start_health_server()
    start_keep_alive()
    log_public_ip()
    last_market_scan_at = 0.0
    
    while True:
        try:
            print(f"\n--- Новий цикл перевірки ринку [{time.strftime('%Y-%m-%d %H:%M:%S')}] ---")
            
            memory = (
                CURRENT_MEMORY
                if API_EMERGENCY_TRIGGERED and CURRENT_MEMORY is not None
                else load_memory()
            )
            set_current_memory(memory)
            process_admin_actions(memory)
            publish_admin_snapshot(memory)
            if time.time() < BINANCE_COOLDOWN_UNTIL:
                retry_in = max(1, int(BINANCE_COOLDOWN_UNTIL - time.time()))
                print(f"⏸️ Binance заблокував IP; без нових API-запитів ще приблизно {retry_in} с.")
                time.sleep(min(API_RETRY_SECONDS, retry_in))
                continue
            if process_emergency_stop(memory):
                time.sleep(API_RETRY_SECONDS)
                continue
            update_performance_goal(memory)
            
            # 0. Фіксація OCO, що спрацювали на біржі
            reconcile_oco_orders(memory)
            process_admin_actions(memory)
            if process_emergency_stop(memory):
                time.sleep(API_RETRY_SECONDS)
                continue

            # 1. Опитувальна перевірка SL/TP для позицій без OCO
            check_auto_sl_tp(memory)
            process_admin_actions(memory)
            if process_emergency_stop(memory):
                time.sleep(API_RETRY_SECONDS)
                continue
            
            # 2. Свіжий баланс після можливих продажів і безпечна синхронізація
            wallet_info = get_account_balances_and_positions(memory["active_trades"])
            sync_memory_with_wallet(memory, wallet_info)
            publish_admin_snapshot(memory, wallet_info)
            process_admin_actions(memory)
            if process_emergency_stop(memory):
                time.sleep(API_RETRY_SECONDS)
                continue
            if wallet_info.get("error"):
                print(f"⏳ Дані гаманця недоступні — аналіз Claude пропущено. Повтор через {API_RETRY_SECONDS} с.")
                time.sleep(API_RETRY_SECONDS)
                continue
            protect_all_positions(memory)
            
            print(f"💵 Вільний баланс: {wallet_info['usdt_free_balance']:.4f} USDT")
            print(f"🧠 Загальний зафіксований PnL у пам'яті: {memory.get('total_pnl_usdt', 0.0):+.2f} USDT")
            
            if wallet_info['holdings']:
                print("💼 Наявні монети на гаманці:")
                for h in wallet_info['holdings']:
                    print(f"   • {h['asset']}: {h['quantity']} шт (~{h['value_usdt']} USDT)")
            else:
                print("💼 Суттєві монети на гаманці відсутні.")

            goal = memory.get("performance_goal", {})
            if goal.get("status") == "paused" and not memory["active_trades"]:
                print("⏸️ Цільове вікно закрите; позицій для супроводу немає. Бот переходить у режим очікування.")
                for _ in range(max(1, CHECK_INTERVAL_SECONDS // SL_TP_POLL_SECONDS)):
                    time.sleep(SL_TP_POLL_SECONDS)
                    process_admin_actions(memory)
                    if process_emergency_stop(memory):
                        break
                    publish_admin_snapshot(memory)
                continue

            if goal.get("status") != "paused":
                if memory.get("admin_paused"):
                    print("⏸️ Адміністратор призупинив нові угоди; супровід позицій триває.")
                else:
                    if time.time() - last_market_scan_at >= MARKET_SCAN_INTERVAL_SECONDS:
                        last_market_scan_at = time.time()
                        watchlist = get_top_usdt_symbols(limit=TOP_MARKET_COINS_LIMIT)
                        market_data = scan_market_watchlist(watchlist)
                        process_admin_actions(memory)
                        if process_emergency_stop(memory):
                            time.sleep(API_RETRY_SECONDS)
                            continue

                        if not memory.get("admin_paused") and market_data:
                            trade_decision = ask_claude_to_select_and_trade(market_data, wallet_info, memory)
                            process_admin_actions(memory)
                            if process_emergency_stop(memory):
                                time.sleep(API_RETRY_SECONDS)
                                continue
                            if not memory.get("admin_paused"):
                                execute_trade_safely(trade_decision, memory)
                                if process_emergency_stop(memory):
                                    time.sleep(API_RETRY_SECONDS)
                                    continue
                    else:
                        print("⏳ Повне сканування ринку відкладено, щоб не перевищити ліміт Binance.")
            else:
                print("⏸️ Новий аналіз і BUY призупинені; перевіряю лише наявні позиції.")
            
            print(f"💤 Чекаємо {CHECK_INTERVAL_SECONDS} с до наступного сканування (SL/TP перевіряється кожні {SL_TP_POLL_SECONDS} с)...")
            waited = 0
            while waited < CHECK_INTERVAL_SECONDS:
                time.sleep(SL_TP_POLL_SECONDS)
                waited += SL_TP_POLL_SECONDS
                reconcile_oco_orders(memory)
                check_auto_sl_tp(memory)
                process_admin_actions(memory)
                if process_emergency_stop(memory):
                    break
                publish_admin_snapshot(memory, wallet_info)
            
        except KeyboardInterrupt:
            print("\n🛑 Роботу бота зупинено користувачем.")
            break
        except Exception as e:
            print(f"❌ Помилка в циклі: {e}")
            if API_ERROR_TOTAL >= API_ERROR_THRESHOLD:
                emergency_memory = CURRENT_MEMORY or {
                    "active_trades": {},
                    "trade_history": [],
                    "total_pnl_usdt": 0.0,
                }
                set_current_memory(emergency_memory)
                try:
                    process_emergency_stop(emergency_memory)
                except Exception as emergency_error:
                    print(f"❌ Не вдалося обробити аварійну зупинку: {emergency_error}")
            print(f"⏳ Повторна спроба через {API_RETRY_SECONDS} секунд...")
            time.sleep(API_RETRY_SECONDS)