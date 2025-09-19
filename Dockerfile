# Imagem base leve (Debian bookworm-slim)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Dependências do SO para Chromium rodar em headless + fontes + certificados
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    gnupg \
    # Chromium e driver (driver é opcional se você usar webdriver-manager)
    chromium \
    chromium-driver \
    # libs gráficas/áudio comuns
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

# Deixe essas variáveis se seu código precisar referenciar o binário
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER=/usr/bin/chromedriver

WORKDIR /app

# Instalar dependências Python do projeto
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copiar o código
COPY . .

# Comando de execução (logs sem buffer)
CMD ["python", "-u", "extrator.py"]
