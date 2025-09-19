FROM python:3.11-slim

# Variáveis de ambiente
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Dependências nativas necessárias para Chrome/Chromedriver em modo headless
RUN apt-get update && apt-get install -y --no-install-recommends \
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
  && rm -rf /var/lib/apt/lists/*

# Instalar dependências Python
WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt

# Copiar código
COPY . .

# (Opcional) Defina um User-Agent padrão aqui se quiser
# ENV USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"

# HEADLESS=chrome é a recomendação para capturar console.log com mais estabilidade
ENV HEADLESS=chrome

CMD ["python", "-u", "extrator.py"]
