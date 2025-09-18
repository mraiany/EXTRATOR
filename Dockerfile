# Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Dependências de sistema necessárias pro Chromium/Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    gnupg \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libxshmfence1 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Se usa requirements.txt
COPY requirements.txt .
RUN pip install -r requirements.txt || true

# Playwright + baixar Chromium com dependências
RUN pip install playwright && playwright install --with-deps chromium

# Copia o projeto
COPY . .

# IMPORTANTE: -u (sem buffer) para os logs aparecerem no Render
CMD ["python", "-u", "extrator.py"]
