FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim
LABEL maintainer="mkuznetcova"
LABEL description="Matrix file distribution bot with SFTP support"
LABEL version="1.0"

RUN groupadd -r botuser && useradd -r -g botuser -d /app -s /sbin/nologin botuser
WORKDIR /app

COPY --from=builder /install /usr/local
COPY main.py config.py ./
RUN mkdir -p /app/logs /app/tmp_downloads /app/config /app/data \
    && chown -R botuser:botuser /app
USER botuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_FILE=/app/logs/bot.log
    FOLDERS_FILE=/app/config/folders.json

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD pgrep -f "python main.py" || exit 1
CMD ["python", "main.py"]