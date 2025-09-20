# Use a slim Python image
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Expose Cloud Run port
EXPOSE 8080

# Run the app
CMD ["python", "main.py"]    # For Flask
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]  # For FastAPI
