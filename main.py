import base64
filename = file.filename or f"upload_{job_id}.csv"
in_path = f"jobs/{job_id}/input/{filename}"


# Upload raw file to GCS
content = await file.read()
gcs.upload_bytes(GCS_BUCKET, in_path, content, content_type="text/csv")
gcs_uri = f"gs://{GCS_BUCKET}/{in_path}"


# Create manifest
manifest = processing.create_manifest_skeleton(job_id, gcs_uri, email)
gcs.write_json(GCS_BUCKET, f"jobs/{job_id}/manifest.json", manifest)


# Kick off first chunk
try:
total_rows, num_chunks = processing.initialize_job_stats(
bucket=GCS_BUCKET, job_id=job_id, input_gcs_uri=gcs_uri
)
manifest.update({"total_rows": total_rows, "num_chunks": num_chunks})
gcs.write_json(GCS_BUCKET, f"jobs/{job_id}/manifest.json", manifest)


if PUBSUB_TOPIC:
processing.publish_next_chunk(job_id, 0)
logger.info(f"Published chunk 0 for job {job_id}")
else:
# Fallback: process only first chunk synchronously
logger.info("PUBSUB_TOPIC not set; processing first chunk synchronously")
processing.process_chunk(GCS_BUCKET, job_id, 0)
except Exception as e:
logger.exception("Failed to start job")
manifest.update({"status": "error", "error": str(e)})
gcs.write_json(GCS_BUCKET, f"jobs/{job_id}/manifest.json", manifest)
raise HTTPException(500, detail=f"Failed to start job: {e}")


return RedirectResponse(url=f"/job/{job_id}", status_code=303)




@app.get("/job/{job_id}")
async def job_status(job_id: str):
manifest = gcs.read_json(GCS_BUCKET, f"jobs/{job_id}/manifest.json")
if not manifest:
raise HTTPException(404, detail="Job not found")
return JSONResponse(manifest)




@app.post("/process")
async def process_pubsub(request: Request):
"""
Pub/Sub push endpoint. Accepts either:
- {"message": {"data": base64(json.dumps({job_id, chunk_index}))}}
- direct JSON: {"job_id":..., "chunk_index":...}
"""
body = await request.json()
try:
if "message" in body and body["message"].get("data"):
payload = json.loads(base64.b64decode(body["message"]["data"]))
else:
payload = body
job_id = payload["job_id"]
chunk_index = int(payload["chunk_index"])
except Exception as e:
raise HTTPException(400, detail=f"Invalid message: {e}")


try:
processing.process_chunk(GCS_BUCKET, job_id, chunk_index)
except Exception as e:
logger.exception("Chunk processing failed")
raise HTTPException(500, detail=str(e))


return {"status": "ok"}




@app.post("/dev/process_once")
async def process_once(job_id: str, chunk_index: int = 0):
"""Local/dev helper to process one chunk synchronously."""
processing.process_chunk(GCS_BUCKET, job_id, chunk_index)
return {"status": "ok", "job_id": job_id, "chunk_index": chunk_index}