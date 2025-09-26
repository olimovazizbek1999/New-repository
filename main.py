import os
import uuid
import json
import base64
import asyncio
import io
import polars as pl
from fastapi import FastAPI, Request, UploadFile, Form, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from google.cloud import pubsub_v1
from playwright.async_api import async_playwright   # ✅ added

from services import gcs, processing

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Environment variables
BUCKET = os.environ.get("GCS_BUCKET")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")
PROJECT_ID = os.environ.get("GCP_PROJECT")

# Publisher client (for enqueueing jobs)
_publisher = pubsub_v1.PublisherClient() if PUBSUB_TOPIC else None


def enqueue_job(job_id: str, chunk_index: int = 0):
    """Publish a message to Pub/Sub to process a job chunk."""
    if not PUBSUB_TOPIC or not PROJECT_ID:
        print("⚠️ PUBSUB_TOPIC or GCP_PROJECT not set, skipping enqueue (using local mode).")
        return
    topic_path = _publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    message = json.dumps({"job_id": job_id, "chunk_index": chunk_index}).encode("utf-8")
    _publisher.publish(topic_path, message)
    print(f"📨 Enqueued job {job_id} chunk {chunk_index}")


@app.get("/", response_class=HTMLResponse)
async def form_page(request: Request):
    """Render upload form."""
    return templates.TemplateResponse("form.html", {"request": request})


@app.post("/upload")
async def upload_file(file: UploadFile = File(...), email: str = Form(...)):
    """Handle file upload, split into chunks with Polars, save to GCS, enqueue all chunks."""
    try:
        job_id = str(uuid.uuid4())
        filename = file.filename
        input_path = f"jobs/{job_id}/input/{filename}"

        # Upload file to GCS
        contents = await file.read()
        gcs.upload_bytes(BUCKET, input_path, contents)

        # ✅ Use Polars for faster CSV read
        df = pl.read_csv(io.BytesIO(contents))
        # ✅ Default chunk size = 500 (configurable via env)
        chunk_size = int(os.environ.get("CHUNK_SIZE", 500))

        # ✅ Ensure at least 1 chunk, even for small files
        num_chunks = max(1, (df.height + chunk_size - 1) // chunk_size)

        # Save each chunk separately
        for i in range(num_chunks):
            start, end = i * chunk_size, min((i + 1) * chunk_size, df.height)
            chunk = df[start:end]
            chunk_path = f"jobs/{job_id}/chunks/chunk_{i}.csv"
            gcs.upload_bytes(BUCKET, chunk_path, chunk.write_csv().encode("utf-8"))

        # Create job manifest
        manifest = {
            "job_id": job_id,
            "input": f"gs://{BUCKET}/{input_path}",
            "email": email,
            "status": "queued",
            "filename": filename,
            "created_at": str(processing.utcnow()),
            "updated_at": str(processing.utcnow()),
            "total_rows": df.height,
            "chunk_size": chunk_size,
            "num_chunks": num_chunks,
            "processed_chunks": [],
            "error_chunks": [],
            "email_sent": False,
            "final_gcs_path": None,
            "final_signed_url": None,
            "log": [],
        }
        gcs.upload_bytes(
            BUCKET,
            f"jobs/{job_id}/manifest.json",
            json.dumps(manifest).encode("utf-8"),
        )

        # Enqueue ALL chunks upfront for parallel processing
        if PUBSUB_TOPIC and PROJECT_ID:
            for i in range(num_chunks):
                enqueue_job(job_id, i)
        else:
            # ✅ FIX: Run synchronously in local mode so Cloud Run without Pub/Sub still works
            for i in range(num_chunks):
                await processing.process_chunk(job_id, i)

        return {
            "job_id": job_id,
            "status": "queued",
            "message": f"Job accepted with {num_chunks} chunks.",
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/process")
async def process_pubsub(request: Request):
    try:
        # Log the raw request text before parsing
        raw_body = await request.body()
        print("📥 FULL RAW Pub/Sub request:", raw_body.decode("utf-8", errors="ignore"))

        body = await request.json()
        message = body.get("message", {})

        # Try attributes first
        attrs = message.get("attributes", {})
        job_id = attrs.get("job_id")
        chunk_index = attrs.get("chunk_index")

        if job_id is None or chunk_index is None:
            # Fall back to Base64 data
            data = base64.b64decode(message.get("data", "")).decode("utf-8")
            print("📥 Base64 decoded data:", data)

            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                import ast
                payload = ast.literal_eval(data)

            job_id = payload.get("job_id")
            chunk_index = payload.get("chunk_index") or payload.get("chunk.ind_index")

        if job_id is None or chunk_index is None:
            print("⚠️ Invalid payload after parsing:", body)
            return JSONResponse(status_code=400, content={"error": "Invalid payload", "payload": body})

        print(f"⚙️ Received job {job_id}, chunk {chunk_index} → scheduling in background")
        asyncio.create_task(processing.process_chunk(job_id, int(chunk_index)))
        return {"status": "ok"}

    except Exception as e:
        print(f"⚠️ /process error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})




@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    """Return job manifest (status, chunks, signed URL)."""
    try:
        data = gcs.download_bytes(BUCKET, f"jobs/{job_id}/manifest.json")
        manifest = json.loads(data.decode("utf-8"))
        return manifest
    except Exception as e:
        return JSONResponse(status_code=404, content={"error": str(e)})


@app.get("/progress/{job_id}")
async def get_progress(job_id: str):
    """Return job progress info for frontend polling (row + chunk level)."""
    try:
        data = gcs.download_bytes(BUCKET, f"jobs/{job_id}/manifest.json")
        manifest = json.loads(data.decode("utf-8"))
        total_chunks = manifest.get("num_chunks", 1)
        done_chunks = len(manifest.get("processed_chunks", []))

        total_rows = manifest.get("total_rows", 0)
        chunk_size = manifest.get("chunk_size", 500)
        # Approx rows processed = chunks done × chunk size (capped at total_rows)
        approx_rows_done = min(done_chunks * chunk_size, total_rows)

        percent = int((approx_rows_done / total_rows) * 100) if total_rows else 0

        return {
            "job_id": job_id,
            "status": manifest.get("status", "unknown"),
            "progress_percent": percent,
            "processed_chunks": done_chunks,
            "total_chunks": total_chunks,
            "processed_rows": approx_rows_done,
            "total_rows": total_rows,
            "final_signed_url": manifest.get("final_signed_url"),
        }
    except Exception as e:
        return JSONResponse(status_code=404, content={"error": str(e)})


@app.get("/logs/{job_id}")
async def get_logs(job_id: str, limit: int = 20):
    """Return last N logs for a job (default: 20)."""
    try:
        data = gcs.download_bytes(BUCKET, f"jobs/{job_id}/manifest.json")
        manifest = json.loads(data.decode("utf-8"))
        logs = manifest.get("log", [])
        return {
            "job_id": job_id,
            "status": manifest.get("status", "unknown"),
            "logs": logs[-limit:],  # last N entries
        }
    except Exception as e:
        return JSONResponse(status_code=404, content={"error": str(e)})


@app.get("/healthz")
async def healthz():
    """Health check for Cloud Run. Verifies Playwright Chromium works."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto("https://example.com", timeout=15000)
            title = await page.title()
            await browser.close()
        return {"status": "ok", "playwright": True, "title": title}
    except Exception as e:
        return {"status": "degraded", "playwright": False, "error": str(e)}
