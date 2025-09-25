# Use official Python image
FROM python:3.11-slim

# Install system dependencies (needed for Playwright, lxml, etc.)
RUN apt-get update && apt-get install -y \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    fonts-liberation \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Copy requirements first (for caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Expose Cloud Run port
ENV PORT=8080

# ✅ Run FastAPI with uvicorn, listening on the Cloud Run port
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
