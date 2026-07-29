FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/app/data

WORKDIR /app

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY main.py logic.py healthcheck.py ./

RUN mkdir -p /app/data \
    && chown -R appuser:appuser /app

USER appuser
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=3s --start-period=45s --retries=3 \
    CMD ["python", "/app/healthcheck.py"]

CMD ["python", "-u", "/app/main.py", "bot"]
