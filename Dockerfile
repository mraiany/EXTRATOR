# Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Dependências de sistema base; o `--with-deps` do Playwright completa o resto.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    gnupg \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libdrm2 \
    libxkbcommon0 \
    libxdamage1 \
    libxcomposite1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libxshmfence1 \
    fonts-liberation \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) Instalar dependências Python (aproveita cache de build)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Instalar Playwright e o Chromium com dependências do SO
RUN pip install --no-cache-dir playwright \
 && playwright install --with-deps chromium

# 3) Copiar o projeto
COPY . .

# IMPORTANTE: -u (unbuffered) para logs no Render
CMD ["python", "-u", "extrator.py"]
