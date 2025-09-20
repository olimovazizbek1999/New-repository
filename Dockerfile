FROM python:3.11-slim


ENV PYTHONDONTWRITEBYTECODE=1 \
PYTHONUNBUFFERED=1


# Build deps for some wheels; keep slim
RUN apt-get update && apt-get install -y --no-install-recommends \
build-essential \
ca-certificates \
&& rm -rf /var/lib/apt/lists/*


WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


COPY . .


ENV PORT=8080


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]