# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir requests \
 && playwright install chromium

COPY . .

# já vem com usuário 'pwuser' criado
USER pwuser

CMD ["python", "-u", "extrator.py"]
