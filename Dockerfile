FROM python:3.11-slim

# Instala dependências do sistema para rodar Chromium
RUN apt-get update && apt-get install -y \
    wget curl unzip gnupg \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxrandr2 \
    libgbm1 libasound2 libpangocairo-1.0-0 libpango-1.0-0 \
    libxdamage1 libxfixes3 libatspi2.0-0 libgtk-3-0 \
    fonts-liberation libappindicator3-1 \
    && rm -rf /var/lib/apt/lists/*

# Instala Python deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala navegadores do Playwright
RUN playwright install --with-deps chromium

# Copia o código
COPY . .

# Comando padrão
CMD ["python", "extrator.py"]
