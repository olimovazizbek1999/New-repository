from __future__ import annotations

import base64
import json
import os
import uuid
from typing import Optional

import orjson
import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv

# Load .env early
load_dotenv()

from services.gcs import GCSClient
from services.processing import JobOrchestrator

# Templates
templates = Jinja2Templates(directory="templates")

app = FastAPI(title="Lead Processor", version="1.0.0")

def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

BUCKET = env("GCS_BUCKET", "")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "")


# Health endpoint
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


# Home form
@app.get("/", response_class=HTMLResponse)
async def form(request: Request):
    return templates.TemplateResponse("form.html", {"request": request})


@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    email: EmailStr = Form(...),
):
    if not BUCKET:
        raise HTTPException(500, "Server not configured: missing GCS_BUCKET")

    # Generate a job id
    job_id = str(uuid.uuid4())
    filename = file.filename or f"upload.csv"

    gcs = GCSClient(bucket_name=BUCKET)

    # Store input file to GCS
    input_blob = f"jobs/{job_id}/input/{filename}"
    await gcs.async_upload_stream(input_blob, await file.read())

    # Create manifest
    orchestrator = JobOrchestrator(gcs=gcs, pubsub_topic=PUBSUB_TOPIC)
    total_rows, total_chunks = await orchestrator.create_manifest_and_plan(
        job_id=job_id, gcs_input_blob=input_blob, notify_email=str(email)
    )

    # Kick off first chunk (Pub/Sub if available; else immediate)
    await orchestrator.kickoff(job_id)

    return JSONResponse(
        {
            "message": "Upload received; processing started.",
            "job_id": job_id,
            "rows": total_rows,
            "chunks": total_chunks,
            "manifest": f"gs://{BUCKET}/jobs/{job_id}/manifest.json",
        }
    )


# Pub/Sub push endpoint (for chunk processing)
class PubSubPush(BaseModel):
    message: dict
    subscription: Optional[str] = None


def _decode_pubsub_message(msg: dict) -> dict:
    data_b64 = msg.get("data")
    if not data_b64:
        return {}
    payload = base64.b64decode(data_b64).decode("utf-8")
    return json.loads(payload)


@app.post("/process")
async def process_push(body: PubSubPush):
    gcs = GCSClient(bucket_name=BUCKET)
    orchestrator = JobOrchestrator(gcs=gcs, pubsub_topic=PUBSUB_TOPIC)

    data = _decode_pubsub_message(body.message)
    if not data:
        raise HTTPException(400, "Invalid Pub/Sub message")

    await orchestrator.process_chunk(**data)
    return {"status": "processed", "chunk": data.get("chunk_index")}


# Local dev helper
@app.post("/dev/process_once")
async def dev_process_once(payload: dict):
    gcs = GCSClient(bucket_name=BUCKET)
    orchestrator = JobOrchestrator(gcs=gcs, pubsub_topic=PUBSUB_TOPIC)
    await orchestrator.process_chunk(**payload)
    return {"status": "ok"}