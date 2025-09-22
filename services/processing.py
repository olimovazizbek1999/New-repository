import os
import io
import json
import pandas as pd
from datetime import datetime, timezone
from services import gcs, openai_utils, emailer, scraper
from .openai_utils import clean_rows, generate_openers
from .emailer import send_email
from .scraper import extract_company_name
from google.cloud import pubsub_v1

# Env vars
BUCKET = os.environ.get("GCS_BUCKET")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

# Publisher for chaining chunks
_publisher = pubsub_v1.PublisherClient() if PUBSUB_TOPIC else None


def utcnow():
    """Get current UTC time (ISO format)."""
    return datetime.now(timezone.utc)


def log_event(manifest, msg):
    """Append event log entry to manifest dict."""
    manifest["log"].append({"time": str(utcnow()), "msg": msg})
    manifest["updated_at"] = str(utcnow())


def load_manifest(job_id):
    """Load job manifest from GCS."""
    data = gcs.download_bytes(BUCKET, f"jobs/{job_id}/manifest.json")
    return json.loads(data.decode("utf-8"))


def save_manifest(job_id, manifest):
    """Save job manifest to GCS."""
    gcs.upload_bytes(BUCKET, f"jobs/{job_id}/manifest.json", json.dumps(manifest).encode("utf-8"))


async def process_chunk(job_id, chunk_index):
    """
    Process a chunk of the job:
      - Read input file
      - Clean names & company
      - Scrape company name
      - Generate email openers
      - Save chunk
      - Enqueue next chunk or finalize
    """
    manifest = load_manifest(job_id)
    log_event(manifest, f"Processing chunk {chunk_index}")

    try:
        # Load full CSV
        input_path = manifest["input"].replace(f"gs://{BUCKET}/", "")
        data = gcs.download_bytes(BUCKET, input_path)
        df = pd.read_csv(io.BytesIO(data))

        total_rows = len(df)
        manifest["total_rows"] = total_rows
        chunk_size = manifest.get("chunk_size", 1000)  # default = 1000
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        manifest["num_chunks"] = num_chunks

        start = chunk_index * chunk_size
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end].copy()
        chunk = chunk.fillna("")


        # ✅ Fix 1: Fill NaN values to prevent NAType JSON errors
        chunk = chunk.fillna("")

        # ✅ Fix 2: Ensure Company column is string type
        if "Company" in chunk.columns:
            chunk["Company"] = chunk["Company"].astype("string")

        if chunk.empty:
            log_event(manifest, f"Chunk {chunk_index} empty, marking processed")
            manifest["processed_chunks"].append(chunk_index)
            save_manifest(job_id, manifest)
            return

        # Clean names/companies with OpenAI
        rows = [
            {"first": r["First Name"], "last": r["Last Name"], "company": r["Company"]}
            for _, r in chunk.iterrows()
        ]
        try:
            cleaned = json.loads(clean_rows(rows))
            for i, c in enumerate(cleaned):
                chunk.iloc[i, chunk.columns.get_loc("First Name")] = c["first"]
                chunk.iloc[i, chunk.columns.get_loc("Last Name")] = c["last"]
                chunk.iloc[i, chunk.columns.get_loc("Company")] = c["company"]
        except Exception as e:
            log_event(manifest, f"clean error: {e}")

        # Scrape website & override company if available
        if "Website" in chunk.columns:
            for idx, row in chunk.iterrows():
                if pd.notna(row["Website"]):
                    try:
                        company_name = extract_company_name(row["Website"])
                        if company_name:
                            chunk.at[idx, "Company"] = company_name
                    except Exception as e:
                        log_event(manifest, f"scrape error {row['Website']}: {e}")

        # Generate openers with OpenAI
        rows = [
            {"company": r["Company"], "website": r.get("Website", ""), "industry": r.get("Industry", "")}
            for _, r in chunk.iterrows()
        ]
        try:
            opened = json.loads(generate_openers(rows))
            for i, o in enumerate(opened):
                chunk.loc[chunk.index[i], "Opener"] = o["opener"]
        except Exception as e:
            log_event(manifest, f"opener error: {e}")

        # Save processed chunk
        out_path = f"jobs/{job_id}/out/chunk_{chunk_index}.csv"
        gcs.upload_bytes(BUCKET, out_path, chunk.to_csv(index=False).encode("utf-8"))
        log_event(manifest, f"Saved chunk {chunk_index} → {out_path}")

        # Mark processed
        manifest["processed_chunks"].append(chunk_index)
        save_manifest(job_id, manifest)

        # Enqueue next chunk or finalize
        if PUBSUB_TOPIC and chunk_index + 1 < num_chunks:
            _publisher.publish(
                PUBSUB_TOPIC,
                json.dumps({"job_id": job_id, "chunk_index": chunk_index + 1}).encode("utf-8"),
            )
            log_event(manifest, f"Enqueued next chunk {chunk_index+1}")
            save_manifest(job_id, manifest)
        elif chunk_index + 1 == num_chunks:
            finalize_job(job_id, manifest)

    except Exception as e:
        log_event(manifest, f"process error: {e}")
        manifest["error_chunks"].append(chunk_index)
        save_manifest(job_id, manifest)


def finalize_job(job_id, manifest=None):
    """Merge chunks, generate signed URL, send email."""
    if manifest is None:
        manifest = load_manifest(job_id)

    try:
        # Merge chunks
        input_paths = [f"jobs/{job_id}/out/chunk_{i}.csv" for i in manifest["processed_chunks"]]
        final_path = f"jobs/{job_id}/final/{manifest['filename'].replace('.csv', '_processed.csv')}"
        gcs.merge_csvs(BUCKET, input_paths, final_path)
        manifest["final_gcs_path"] = f"gs://{BUCKET}/{final_path}"
        log_event(manifest, f"Merged chunks → {final_path}")

        # Signed URL
        signed_url = gcs.generate_signed_url(BUCKET, final_path, expiration=7 * 24 * 3600)
        manifest["final_signed_url"] = signed_url
        log_event(manifest, f"Generated signed URL")

        # Email
        email_body = f"Download link (valid 7 days):\n\n{signed_url}"
        send_email(
            to_addr=manifest["email"],
            subject="Your processed CSV is ready",
            body=email_body,
            cc=["olimovazizbek1999@gmail.com"],
        )
        manifest["email_sent"] = True
        log_event(manifest, "Sent email notification")

        # Update status
        manifest["status"] = "done"
        save_manifest(job_id, manifest)

    except Exception as e:
        log_event(manifest, f"finalize error: {e}")
        manifest["status"] = "error"
        save_manifest(job_id, manifest)
