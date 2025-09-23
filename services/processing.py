import os
import io
import json
import pandas as pd
import logging
import asyncio
from datetime import datetime, timezone
from services import gcs
from .openai_utils import clean_rows, generate_openers   # now async
from .emailer import send_email
from .scraper import extract_company_info                # async httpx version
from google.cloud import pubsub_v1

# Logging
logger = logging.getLogger("services.processing")
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("GCS_BUCKET")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

_publisher = pubsub_v1.PublisherClient() if PUBSUB_TOPIC else None


def utcnow():
    return datetime.now(timezone.utc)


def log_event(manifest, msg):
    manifest["log"].append({"time": str(utcnow()), "msg": msg})
    manifest["updated_at"] = str(utcnow())
    logger.info(msg)


def load_manifest(job_id):
    data = gcs.download_bytes(BUCKET, f"jobs/{job_id}/manifest.json")
    return json.loads(data.decode("utf-8"))


def save_manifest(job_id, manifest):
    gcs.upload_bytes(
        BUCKET,
        f"jobs/{job_id}/manifest.json",
        json.dumps(manifest).encode("utf-8"),
    )


async def process_chunk(job_id, chunk_index):
    manifest = load_manifest(job_id)
    log_event(manifest, f"Processing chunk {chunk_index}")

    cleaned_count = 0
    opener_count = 0

    try:
        # Load CSV
        input_path = manifest["input"].replace(f"gs://{BUCKET}/", "")
        data = gcs.download_bytes(BUCKET, input_path)
        df = pd.read_csv(io.BytesIO(data))

        total_rows = len(df)
        manifest["total_rows"] = total_rows
        chunk_size = manifest.get("chunk_size", 1000)
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        manifest["num_chunks"] = num_chunks

        start = chunk_index * chunk_size
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end].copy().fillna("")

        if chunk.empty:
            log_event(manifest, f"Chunk {chunk_index} empty, marking processed")
            manifest["processed_chunks"].append(chunk_index)
            save_manifest(job_id, manifest)
            return

        # Ensure required columns exist
        for col in ["First Name", "Last Name", "Company", "Opener"]:
            if col not in chunk.columns:
                chunk[col] = "" if col != "Opener" else "No opener generated"

        # --- CLEAN ROWS (async OpenAI) ---
        try:
            rows = [
                {"first": r["First Name"], "last": r["Last Name"], "company": r["Company"]}
                for _, r in chunk.iterrows()
            ]
            cleaned = await clean_rows(rows, job_id)

            for i, c in enumerate(cleaned):
                if i < len(chunk):
                    chunk.at[chunk.index[i], "First Name"] = c.get("first", "") or "Unknown"
                    chunk.at[chunk.index[i], "Last Name"] = c.get("last", "") or "Unknown"
                    chunk.at[chunk.index[i], "Company"] = c.get("company", "") or "Unknown"
                    cleaned_count += 1
            log_event(manifest, f"Applied cleaning to {cleaned_count} rows")
        except Exception as e:
            log_event(manifest, f"⚠️ OpenAI clean_rows failed: {e}")

        # --- SCRAPE WEBSITES (async httpx) ---
        if "Website" in chunk.columns:
            scrape_tasks = [
                extract_company_info(row["Website"])
                for idx, row in chunk.iterrows() if row.get("Website")
            ]
            results = await asyncio.gather(*scrape_tasks, return_exceptions=True)

            for (idx, row), info in zip(chunk.iterrows(), results):
                if isinstance(info, dict):
                    if info.get("name"):
                        chunk.at[idx, "Company"] = info["name"]
                    chunk.at[idx, "ScrapedText"] = info.get("text", "")
                else:
                    log_event(manifest, f"scrape error {row['Website']}: {info}")
                    chunk.at[idx, "ScrapedText"] = ""

        # --- GENERATE OPENERS (async OpenAI) ---
        try:
            rows = [
                {
                    "company": r.get("Company", ""),
                    "website": r.get("Website", ""),
                    "industry": r.get("Industry", ""),
                    "text": r.get("ScrapedText", ""),
                }
                for _, r in chunk.iterrows()
            ]
            opened = await generate_openers(rows, job_id)

            for i, o in enumerate(opened):
                if i < len(chunk):
                    chunk.at[chunk.index[i], "Opener"] = (
                        o.get("opener", "") or "No opener generated"
                    )
                    opener_count += 1
            log_event(manifest, f"Applied openers to {opener_count} rows")
        except Exception as e:
            log_event(manifest, f"⚠️ OpenAI generate_openers failed: {e}")

        # Drop ScrapedText before saving
        if "ScrapedText" in chunk.columns:
            chunk = chunk.drop(columns=["ScrapedText"])

        out_path = f"jobs/{job_id}/out/chunk_{chunk_index}.csv"
        gcs.upload_bytes(BUCKET, out_path, chunk.to_csv(index=False).encode("utf-8"))
        log_event(manifest, f"Saved chunk {chunk_index} → {out_path}")

        manifest["processed_chunks"].append(chunk_index)
        save_manifest(job_id, manifest)

        # Enqueue next chunk or finalize
        if PUBSUB_TOPIC and chunk_index + 1 < num_chunks:
            _publisher.publish(
                PUBSUB_TOPIC,
                json.dumps({"job_id": job_id, "chunk_index": chunk_index + 1}).encode("utf-8"),
            )
            log_event(manifest, f"Enqueued next chunk {chunk_index+1}")
        elif chunk_index + 1 == num_chunks:
            finalize_job(job_id, manifest)

    except Exception as e:
        log_event(manifest, f"process error: {e}")
        manifest["error_chunks"].append(chunk_index)
        save_manifest(job_id, manifest)


def finalize_job(job_id, manifest=None):
    if manifest is None:
        manifest = load_manifest(job_id)

    try:
        input_paths = [f"jobs/{job_id}/out/chunk_{i}.csv" for i in manifest["processed_chunks"]]
        final_path = f"jobs/{job_id}/final/{manifest['filename'].replace('.csv', '_processed.csv')}"
        gcs.merge_csvs(BUCKET, input_paths, final_path)
        manifest["final_gcs_path"] = f"gs://{BUCKET}/{final_path}"
        log_event(manifest, f"Merged chunks → {final_path}")

        # Remove ScrapedText column if present
        data = gcs.download_bytes(BUCKET, final_path)
        df = pd.read_csv(io.BytesIO(data))
        if "ScrapedText" in df.columns:
            df = df.drop(columns=["ScrapedText"])
            gcs.upload_bytes(BUCKET, final_path, df.to_csv(index=False).encode("utf-8"))
            log_event(manifest, "Removed internal ScrapedText column from final CSV")

        signed_url = gcs.generate_signed_url(BUCKET, final_path, expiration=7 * 24 * 3600)
        manifest["final_signed_url"] = signed_url
        log_event(manifest, "Generated signed URL")

        email_body = f"Download link (valid 7 days):\n\n{signed_url}"
        send_email(
            to=manifest["email"],
            subject="Your processed CSV is ready",
            body=email_body,
            cc=["olimovazizbek1999@gmail.com"],
        )
        manifest["email_sent"] = True
        log_event(manifest, "Sent email notification")

        manifest["status"] = "done"
        log_event(
            manifest,
            f"Job {job_id} finished successfully → {manifest.get('total_rows', 0)} rows processed",
        )
        save_manifest(job_id, manifest)

    except Exception as e:
        log_event(manifest, f"finalize error: {e}")
        manifest["status"] = "error"
        save_manifest(job_id, manifest)
