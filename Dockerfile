# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Dependências que o Chromium precisa (o --with-deps completa o resto)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget gnupg \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 \
    libdrm2 libxkbcommon0 libxdamage1 libxcomposite1 libxrandr2 \
    libgbm1 libasound2 libxshmfence1 fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) Instala dependências Python com melhor cache
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir playwright requests \
 && python -m playwright install --with-deps chromium

# 2) Copia o código
COPY . .

# 3) Usuário não-root (boa prática)
RUN useradd -m app && chown -R app:app /app
USER app

# 4) Logs sem buffer (Render, etc.)
CMD ["python", "-u", "extrator.py"]
