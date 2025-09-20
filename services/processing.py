import json


tmp = gcs.download_to_tempfile(bkt, path)
df = _iter_csv_chunk(tmp, chunk_index)
if df.empty:
logger.info(f"Chunk {chunk_index} empty; marking processed")
_update_manifest(job_id, processed_chunks=sorted(set([*man.get('processed_chunks', []), chunk_index])))
return
df = _ensure_columns(df)


_log(job_id, f"chunk {chunk_index}: loaded {len(df)} rows")


# Transformations
try:
_clean_names(df)
except Exception as e:
_log(job_id, f"clean error: {e}")


cache = _load_cache(bucket, job_id)
try:
_scrape_and_fill(df, cache)
except Exception as e:
_log(job_id, f"scrape/fill error: {e}")
_save_cache(bucket, job_id, cache)


try:
_generate_openers(df)
except Exception as e:
_log(job_id, f"opener error: {e}")


# Save output chunk
out_path = f"jobs/{job_id}/out/chunk_{chunk_index}.csv"
try:
gcs.write_df_csv(bucket, out_path, df)
except Exception as e:
_log(job_id, f"save chunk error: {e}")
# Still proceed to mark processed to avoid infinite loop; log error
man.setdefault("error_chunks", []).append(chunk_index)
_update_manifest(job_id, error_chunks=man.get("error_chunks", []))


# Update manifest
processed = sorted(set([*man.get("processed_chunks", []), chunk_index]))
_update_manifest(job_id, processed_chunks=processed)


# If last chunk, merge and deliver
if len(processed) >= int(man.get("num_chunks", 0) or 0):
try:
final_path = gcs.merge_chunks_to_final(bucket, job_id, man.get("filename", "output.csv"))
signed = gcs.generate_signed_url(bucket, final_path, days=7)
_update_manifest(job_id, status="done", final_gcs_path=final_path, final_signed_url=signed)
# Send email
emailer.send_email(
to_email=man.get("email"),
subject=f"Lead Processor: job {job_id} complete",
body=signed,
job_id=job_id,
)
_update_manifest(job_id, email_sent=True)
except Exception as e:
_log(job_id, f"finalize error: {e}")
_update_manifest(job_id, status="error")
return


# Publish next chunk
next_idx = chunk_index + 1
if PUBSUB_TOPIC and os.environ.get("GOOGLE_CLOUD_PROJECT"):
publish_next_chunk(job_id, next_idx)
else:
logger.info("PUB/SUB not configured; next chunk not scheduled automatically")