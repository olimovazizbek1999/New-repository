# ===== Build stage (optional, keeps runtime slim) =====
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps for scientific stack + requests/ssl + uvicorn
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates gnupg git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . /app

# Expose for Cloud Run
ENV PORT=8080
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT}
