import os
import uuid
import json
import base64
import asyncio
from fastapi import FastAPI, Request, UploadFile, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.cloud import pubsub_v1

from services import gcs, processing

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Environment variables
BUCKET = os.environ.get("GCS_BUCKET")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

# Publisher client (for enqueueing jobs)
_publisher = pubsub_v1.PublisherClient() if PUBSUB_TOPIC else None


def enqueue_job(job_id: str, chunk_index: int = 0):
    """Publish a message to Pub/Sub to process a job chunk."""
    if not PUBSUB_TOPIC:
        print("⚠️ PUBSUB_TOPIC not set, skipping enqueue.")
        return
    message = json.dumps({"job_id": job_id, "chunk_index": chunk_index}).encode("utf-8")
    _publisher.publish(PUBSUB_TOPIC, message)
    print(f"📨 Enqueued job {job_id} chunk {chunk_index}")


@app.get("/", response_class=HTMLResponse)
async def form_page(request: Request):
    """Render upload form."""
    return templates.TemplateResponse("form.html", {"request": request})


@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = Form(...), email: str = Form(...)):
    """Handle file upload, save to GCS, enqueue processing."""
    job_id = str(uuid.uuid4())
    filename = file.filename
    input_path = f"jobs/{job_id}/input/{filename}"

    # Upload file to GCS
    contents = await file.read()
    gcs.upload_bytes(BUCKET, input_path, contents)

    # Create job manifest
    manifest = {
        "job_id": job_id,
        "input": f"gs://{BUCKET}/{input_path}",
        "email": email,
        "status": "queued",
        "filename": filename,
        "created_at": str(processing.utcnow()),
        "updated_at": str(processing.utcnow()),
        "total_rows": None,
        "chunk_size": 100,  # ✅ smaller chunks
        "num_chunks": None,
        "processed_chunks": [],
        "error_chunks": [],
        "email_sent": False,
        "final_gcs_path": None,
        "final_signed_url": None,
        "log": [],
    }
    gcs.upload_bytes(BUCKET, f"jobs/{job_id}/manifest.json", json.dumps(manifest).encode("utf-8"))

    # Enqueue first chunk
    enqueue_job(job_id, 0)

    return {"job_id": job_id, "status": "queued", "message": "Job accepted and queued."}


@app.post("/process")
async def process_pubsub(request: Request):
    """Handle Pub/Sub push message quickly and schedule background work."""
    body = await request.json()
    message = body.get("message", {})
    data = base64.b64decode(message.get("data", "")).decode("utf-8")
    payload = json.loads(data)

    job_id = payload["job_id"]
    chunk_index = payload["chunk_index"]

    print(f"⚙️ Received job {job_id}, chunk {chunk_index} → scheduling in background")

    # ✅ Run processing asynchronously (don’t block response)
    asyncio.create_task(processing.process_chunk(job_id, chunk_index))

    # ✅ Immediately ack Pub/Sub
    return {"status": "ok"}


@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    """Return job manifest (status, chunks, signed URL)."""
    try:
        data = gcs.download_bytes(BUCKET, f"jobs/{job_id}/manifest.json")
        manifest = json.loads(data.decode("utf-8"))
        return manifest
    except Exception as e:
        return {"error": str(e)}


@app.get("/healthz")
async def healthz():
    """Health check for Cloud Run."""
    return {"status": "ok"}
