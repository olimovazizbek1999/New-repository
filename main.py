from fastapi import FastAPI, Request, UploadFile, Form
from fastapi.responses import HTMLResponse
from services import gcs, processing
import uuid
import os

app = FastAPI()

GCS_BUCKET = os.environ.get("GCS_BUCKET")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/form.html") as f:
        return f.read()

@app.post("/upload")
async def upload_csv(file: UploadFile, email: str = Form(...)):
    job_id = str(uuid.uuid4())
    input_path = f"jobs/{job_id}/input/{file.filename}"

    # Upload CSV to GCS
    gcs.upload_fileobj(GCS_BUCKET, input_path, file.file)

    # Create job manifest
    manifest = {
        "job_id": job_id,
        "status": "queued",
        "email": email,
        "chunks_processed": 0,
        "errors": []
    }
    gcs.upload_json(GCS_BUCKET, f"jobs/{job_id}/manifest.json", manifest)

    # Trigger first processing chunk
    processing.start_job(job_id, GCS_BUCKET, PUBSUB_TOPIC)

    return {"job_id": job_id, "message": "Job started."}

@app.get("/healthz")
async def health():
    return {"status": "ok"}
