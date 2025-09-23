# Use official slim Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system deps for pandas, lxml, etc.
RUN apt-get update && apt-get install -y \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libatlas-base-dev \
    gfortran \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip (avoids warnings)
RUN pip install --upgrade pip

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose the port Cloud Run expects
EXPOSE 8080

# Run FastAPI app with Uvicorn (JSON exec form)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
