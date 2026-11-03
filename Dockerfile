FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY binance_ai_trader.py .
COPY admin_dashboard.py .

CMD ["python", "binance_ai_trader.py"]
