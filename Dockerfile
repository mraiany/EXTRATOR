FROM python:3.11-slim

# Variáveis de ambiente base
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Instala dependências nativas e o navegador Chromium com seu driver
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libxkbcommon0 \
    libxdamage1 \
    libxcomposite1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libxshmfence1 \
    fonts-liberation \
    chromium \
    chromium-driver \
 && rm -rf /var/lib/apt/lists/*

# Instala dependências Python
WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
 && pip install -r requirements.txt

# Copia o código
COPY . .

# Define caminhos dos binários do Chrome/Chromium para uso no Python
ENV CHROME_BINARY_PATH=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    HEADLESS=chrome

CMD ["python", "-u", "extrator.py"]
