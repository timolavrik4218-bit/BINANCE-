import binascii
import hmac
import os
import secrets
import threading
import time
from datetime import timedelta
from functools import wraps

import pyotp
from flask import Flask, abort, flash, redirect, render_template_string, request, session, url_for


LOGIN_TEMPLATE = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Вхід до панелі адміністратора</title>
  <style>
    body{font:16px system-ui;background:#0b1220;color:#e5e7eb;max-width:440px;margin:12vh auto;padding:24px}
    section{background:#111c2f;padding:28px;border:1px solid #26344c;border-radius:14px}
    input,button{box-sizing:border-box;width:100%;padding:12px;margin:7px 0;border-radius:8px}
    input{background:#0b1220;border:1px solid #34445f;color:#fff}
    button{background:#2563eb;border:0;color:white;font-weight:700;cursor:pointer}
    .message{color:#fca5a5}.muted{color:#9ca3af;font-size:14px}
  </style>
</head>
<body><section>
  <h1>Панель адміністратора</h1>
  <p class="muted">Увійдіть за логіном, паролем і кодом із Google Authenticator.</p>
  {% for message in get_flashed_messages() %}<p class="message">{{ message }}</p>{% endfor %}
  <form method="post" action="{{ url_for('login') }}">
    <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
    <label>Логін<input name="username" autocomplete="username" required maxlength="128"></label>
    <label>Пароль<input name="password" type="password" autocomplete="current-password" required maxlength="256"></label>
    <label>Код автентифікатора<input name="totp" inputmode="numeric" pattern="[0-9]{6}" autocomplete="one-time-code" required></label>
    <button type="submit">Увійти</button>
  </form>
</section></body></html>
"""


DASHBOARD_TEMPLATE = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>Binance AI Trader — Admin</title>
  <style>
    body{font:15px system-ui;background:#0b1220;color:#e5e7eb;max-width:1100px;margin:30px auto;padding:0 18px}
    header,.card{background:#111c2f;border:1px solid #26344c;border-radius:12px;padding:18px;margin:14px 0}
    header{display:flex;justify-content:space-between;align-items:center}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px}
    table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:8px;border-bottom:1px solid #26344c}
    input,textarea,button{font:inherit;padding:10px;border-radius:7px}
    textarea{width:100%;box-sizing:border-box;background:#0b1220;color:#fff;border:1px solid #34445f;min-height:90px}
    button{border:0;background:#2563eb;color:#fff;font-weight:650;cursor:pointer;margin:4px}
    button.danger{background:#b91c1c}button.neutral{background:#475569}
    .muted{color:#9ca3af}.warning{color:#fbbf24}.error{color:#fca5a5}
    pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#0b1220;padding:12px;border-radius:8px}
    form.inline{display:inline}
  </style>
</head>
<body>
  <header><div><h1>Binance AI Trader</h1><span class="muted">Статус: {{ state.status }} · Оновлено: {{ state.updated_at }}</span></div>
    <form method="post" action="{{ url_for('logout') }}"><input type="hidden" name="csrf_token" value="{{ csrf_token }}"><button class="neutral">Вийти</button></form>
  </header>
  {% for message in get_flashed_messages() %}<p class="warning">{{ message }}</p>{% endfor %}
  {% if state.emergency_stop %}<section class="card error"><strong>Аварійна зупинка: {{ state.emergency_stop.status }}</strong><br>{{ state.emergency_stop.reason }}</section>{% endif %}
  <div class="grid">
    <section class="card"><h2>Баланс і результат</h2>
      {% if state.wallet.error %}<p class="error">Не вдалося отримати актуальні дані гаманця.</p>{% endif %}
      <p>Вільний USDT: <strong>{{ state.wallet.usdt_free_balance }} USDT</strong></p>
      <p>Зафіксований PnL: <strong>{{ state.total_pnl_usdt }} USDT</strong></p>
      <p>Позицій у пам'яті: <strong>{{ state.active_trades|length }}</strong></p>
    </section>
    <section class="card"><h2>Керування</h2>
      <form class="inline" method="post" action="{{ url_for('control') }}"><input type="hidden" name="csrf_token" value="{{ csrf_token }}"><input type="hidden" name="action" value="pause"><button class="neutral">Пауза торгівлі</button></form>
      <form class="inline" method="post" action="{{ url_for('control') }}"><input type="hidden" name="csrf_token" value="{{ csrf_token }}"><input type="hidden" name="action" value="resume"><button>Відновити</button></form>
      <form method="post" action="{{ url_for('control') }}" onsubmit="return confirm('Ринковими ордерами продати всі доступні монети на Spot-гаманці? Це може зафіксувати збиток.');">
        <input type="hidden" name="csrf_token" value="{{ csrf_token }}"><input type="hidden" name="action" value="emergency_sell">
        <button class="danger">Аварійно продати всі монети</button>
      </form>
      <p class="muted">Пауза не скасовує біржові OCO. Після аварійної зупинки торгівля залишається заблокованою — потрібна ручна перевірка стану.</p>
    </section>
  </div>
  <section class="card"><h2>Позиції на гаманці</h2>
    {% if state.wallet.holdings %}<table><tr><th>Актив</th><th>Кількість</th><th>Оцінка</th></tr>
    {% for item in state.wallet.holdings %}<tr><td>{{ item.asset }}</td><td>{{ item.quantity }}</td><td>{{ item.value_usdt }} USDT</td></tr>{% endfor %}</table>
    {% else %}<p class="muted">Немає даних або суттєвих позицій.</p>{% endif %}
  </section>
  <section class="card"><h2>Позиції, які відстежує бот</h2>
    {% if state.active_trades %}<table><tr><th>Символ</th><th>Кількість</th><th>Вхід</th><th>SL</th><th>TP</th></tr>
    {% for symbol, trade in state.active_trades.items() %}<tr><td>{{ symbol }}</td><td>{{ trade.qty }}</td><td>{{ trade.buy_price }}</td><td>{{ trade.stop_loss }}</td><td>{{ trade.take_profit }}</td></tr>{% endfor %}</table>
    {% else %}<p class="muted">Відстежуваних позицій немає.</p>{% endif %}
  </section>
  <section class="card"><h2>Останні угоди</h2>
    {% if state.trade_history %}<table><tr><th>Час</th><th>Символ</th><th>PnL</th><th>Причина</th></tr>
    {% for trade in state.trade_history|reverse %}<tr><td>{{ trade.time }}</td><td>{{ trade.symbol }}</td><td>{{ trade.pnl_usdt }}</td><td>{{ trade.reason }}</td></tr>{% endfor %}</table>
    {% else %}<p class="muted">Історія порожня.</p>{% endif %}
  </section>
  <section class="card"><h2>Повідомлення ШІ (лише поради)</h2>
    <p class="muted">Відповідь не виконує угод і не змінює стан бота. Не надсилайте ключі або паролі.</p>
    {% for turn in chat_history %}<p><strong>{{ "Адміністратор" if turn.role == "user" else "ШІ" }}</strong></p><pre>{{ turn.content }}</pre>{% endfor %}
    <form method="post" action="{{ url_for('chat') }}"><input type="hidden" name="csrf_token" value="{{ csrf_token }}">
      <textarea name="message" maxlength="2000" required placeholder="Ваше запитання або аналітична підказка для ШІ"></textarea>
      <button>Запитати ШІ</button>
    </form>
    {% if answer %}<h3>Відповідь</h3><pre>{{ answer }}</pre>{% endif %}
  </section>
</body></html>
"""


def create_admin_app(snapshot_provider, action_submitter, advisor):
    app = Flask(__name__)
    app.secret_key = os.getenv("ADMIN_SESSION_SECRET", "")
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_SAMESITE="Strict",
        PERMANENT_SESSION_LIFETIME=timedelta(minutes=30),
        MAX_CONTENT_LENGTH=16 * 1024,
    )
    expected_username = os.getenv("ADMIN_USERNAME", "")
    expected_password = os.getenv("ADMIN_PASSWORD", "")
    totp_secret = os.getenv("ADMIN_TOTP_SECRET", "").strip().replace(" ", "")
    configuration_error = None
    if not expected_username or len(expected_password) < 16 or len(totp_secret) < 32:
        configuration_error = "Задайте ADMIN_USERNAME, ADMIN_PASSWORD (16+ символів) та ADMIN_TOTP_SECRET (Base32, 32+ символи)."
    elif len(app.secret_key) < 32:
        configuration_error = "ADMIN_SESSION_SECRET має містити щонайменше 32 символи."
    else:
        try:
            pyotp.TOTP(totp_secret).now()
        except (TypeError, ValueError, binascii.Error):
            configuration_error = "ADMIN_TOTP_SECRET має бути коректним Base32 секретом."

    failed_logins = []
    lockout_until = 0.0
    login_lock = threading.Lock()
    chat_requests = []
    chat_lock = threading.Lock()
    conversations = {}
    conversation_lock = threading.Lock()

    def csrf_token():
        token = session.get("csrf_token")
        if not token:
            token = secrets.token_urlsafe(32)
            session["csrf_token"] = token
        return token

    def check_csrf():
        supplied = request.form.get("csrf_token", "")
        expected = session.get("csrf_token", "")
        if not supplied or not expected or not hmac.compare_digest(supplied, expected):
            abort(400)

    def login_required(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not session.get("authenticated"):
                return redirect(url_for("login"))
            session.permanent = True
            return view(*args, **kwargs)
        return wrapped

    @app.after_request
    def security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
        return response

    @app.get("/")
    def health():
        return "OK", 200

    @app.get("/admin")
    @app.get("/admin/")
    def login():
        if configuration_error:
            return "Admin authentication is not configured.", 503
        if session.get("authenticated"):
            return redirect(url_for("dashboard"))
        return render_template_string(LOGIN_TEMPLATE, csrf_token=csrf_token())

    @app.post("/admin/login")
    def login_post():
        nonlocal lockout_until, failed_logins
        check_csrf()
        if configuration_error:
            return "Admin authentication is not configured.", 503
        now = time.time()
        with login_lock:
            failed_logins = [stamp for stamp in failed_logins if now - stamp < 900]
            blocked = now < lockout_until
        if blocked:
            flash("Забагато невдалих спроб. Спробуйте пізніше.")
            return redirect(url_for("login"))

        username_ok = hmac.compare_digest(request.form.get("username", ""), expected_username)
        password_ok = hmac.compare_digest(request.form.get("password", ""), expected_password)
        code = request.form.get("totp", "")
        totp_ok = bool(code.isdigit() and pyotp.TOTP(totp_secret).verify(code, valid_window=1))
        if username_ok and password_ok and totp_ok:
            with login_lock:
                failed_logins.clear()
                lockout_until = 0.0
            session.clear()
            session["authenticated"] = True
            session["admin_session_id"] = secrets.token_urlsafe(24)
            csrf_token()
            session.permanent = True
            return redirect(url_for("dashboard"))

        with login_lock:
            failed_logins.append(now)
            if len(failed_logins) >= 5:
                lockout_until = now + 900
                failed_logins.clear()
        flash("Невірні дані для входу.")
        return redirect(url_for("login"))

    @app.get("/admin/dashboard")
    @login_required
    def dashboard():
        state = snapshot_provider()
        session_id = session["admin_session_id"]
        with conversation_lock:
            chat_history = list(conversations.get(session_id, []))
        return render_template_string(
            DASHBOARD_TEMPLATE,
            state=state,
            csrf_token=csrf_token(),
            chat_history=chat_history[-20:],
        )

    @app.post("/admin/logout")
    @login_required
    def logout():
        check_csrf()
        session_id = session.get("admin_session_id")
        with conversation_lock:
            conversations.pop(session_id, None)
        session.clear()
        return redirect(url_for("login"))

    @app.post("/admin/control")
    @login_required
    def control():
        check_csrf()
        action = request.form.get("action", "")
        if action not in {"pause", "resume", "emergency_sell"}:
            abort(400)
        action_submitter(action)
        flash("Команду передано боту. Стан оновиться після обробки запиту.")
        return redirect(url_for("dashboard"))

    @app.post("/admin/chat")
    @login_required
    def chat():
        check_csrf()
        message = request.form.get("message", "").strip()
        if not message or len(message) > 2000:
            abort(400)
        now = time.time()
        with chat_lock:
            chat_requests[:] = [stamp for stamp in chat_requests if now - stamp < 60]
            if len(chat_requests) >= 10:
                flash("Забагато запитів до ШІ. Зачекайте хвилину.")
                return redirect(url_for("dashboard"))
            chat_requests.append(now)
        session_id = session["admin_session_id"]
        with conversation_lock:
            history = list(conversations.get(session_id, []))[-10:]
        try:
            answer = advisor(message, history)
        except Exception:
            app.logger.exception("Admin advisory request failed")
            answer = "Не вдалося отримати пораду від ШІ. Перевірте логи сервісу."
        with conversation_lock:
            conversation = conversations.setdefault(session_id, [])
            conversation.extend([
                {"role": "user", "content": message},
                {"role": "assistant", "content": answer},
            ])
            del conversation[:-20]
        return redirect(url_for("dashboard"))

    return app
