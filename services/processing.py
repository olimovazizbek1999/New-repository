import os
import io
import json
import re
import polars as pl
import logging
import asyncio
import unicodedata
import random
from datetime import datetime, timezone
from ftfy import fix_text
from services import gcs, openai_utils, emailer, scraper
from google.cloud import pubsub_v1
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger("services.processing")
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("GCS_BUCKET")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

_publisher = pubsub_v1.PublisherClient() if PUBSUB_TOPIC else None

# ✅ Logging optimization
MAX_LOGS = 100
LOG_FLUSH_INTERVAL = 5

# ✅ Pool of polite fallback openers
FALLBACK_OPENERS = [
    "I admire the work your company is doing and appreciate the value you bring.",
    "Your dedication to delivering quality services is truly impressive and inspiring.",
    "It’s clear your company is making a positive impact in your field, which I respect.",
]

def get_fallback_opener():
    return random.choice(FALLBACK_OPENERS)


# ---------------- Helpers ---------------- #

def utcnow():
    return datetime.now(timezone.utc)


def preclean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", str(text))
    text = fix_text(text)
    text = re.sub(r"[^\w\s\-&.,]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def log_event(manifest, msg, save=False):
    manifest.setdefault("_log_buffer", [])
    manifest["_log_buffer"].append({"time": str(utcnow()), "msg": msg})
    manifest["updated_at"] = str(utcnow())
    logger.info(msg)

    if save or len(manifest["_log_buffer"]) >= LOG_FLUSH_INTERVAL:
        existing_logs = manifest.get("log", [])
        manifest["log"] = (existing_logs + manifest["_log_buffer"])[-MAX_LOGS:]
        manifest["_log_buffer"] = []
        save_manifest(manifest["job_id"], manifest)


def load_manifest(job_id):
    data = gcs.download_bytes(BUCKET, f"jobs/{job_id}/manifest.json")
    manifest = json.loads(data.decode("utf-8"))
    manifest.setdefault("_log_buffer", [])  # ✅ ensure log buffer exists
    return manifest


def save_manifest(job_id, manifest):
    gcs.upload_bytes(
        BUCKET,
        f"jobs/{job_id}/manifest.json",
        json.dumps({k: v for k, v in manifest.items() if k != "_log_buffer"}).encode("utf-8"),
    )


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=4, max=60))
async def retry_clean_rows(rows, job_id):
    return await openai_utils.clean_rows(rows, job_id)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=4, max=60))
async def retry_generate_openers(rows, job_id):
    return await openai_utils.generate_openers(rows, job_id)


def fallback_clean(text: str) -> str:
    return preclean_text(text)


# ---------------- AI Output Validation ---------------- #

def validate_opener(opener: str, scraped_text: str, seen: set[str]) -> tuple[str, str]:
    """Validate AI-generated opener. Returns (final_opener, error_message)."""
    if not opener:
        return get_fallback_opener(), "empty opener"

    # Sentence + word count
    sentences = [s.strip() for s in opener.split(".") if s.strip()]
    if len(sentences) != 2:
        return get_fallback_opener(), "not 2 sentences"
    if not all(10 <= len(s.split()) <= 18 for s in sentences):
        return get_fallback_opener(), "sentence length out of bounds"

    # Forbidden patterns
    forbidden = ["lorem ipsum", "undefined", "error", "INSERT", "<", ">"]
    if any(bad in opener.lower() for bad in forbidden):
        return get_fallback_opener(), "forbidden phrase"

    # Length
    if len(opener) > 200:
        return get_fallback_opener(), "too long"

    # Context check (at least one word from scraped text)
    words = set(scraped_text.lower().split())
    overlap = sum(1 for w in opener.lower().split() if w in words)
    if overlap == 0 and scraped_text:
        return get_fallback_opener(), "no context match"

    # Deduplication
    if opener in seen:
        return get_fallback_opener(), "duplicate opener"
    seen.add(opener)

    return opener, ""


# ---------------- Main Processing ---------------- #

async def process_chunk(job_id, chunk_index):
    manifest = load_manifest(job_id)
    manifest["job_id"] = job_id
    log_event(manifest, f"Processing chunk {chunk_index}")

    try:
        input_path = f"jobs/{job_id}/chunks/chunk_{chunk_index}.csv"
        data = gcs.download_bytes(BUCKET, input_path)

        df = pl.read_csv(io.BytesIO(data))
        if df.height == 0:
            log_event(manifest, f"Chunk {chunk_index} empty, skipping", save=True)
            manifest["processed_chunks"].append(chunk_index)
            save_manifest(job_id, manifest)
            return

        # ✅ Ensure required columns exist
        for col in ["First Name", "Last Name", "Company", "Opener", "Error", "ScrapedText"]:
            if col not in df.columns:
                if col == "Opener":
                    df = df.with_columns(pl.lit(get_fallback_opener()).alias("Opener"))
                elif col == "Error":
                    df = df.with_columns(pl.lit("").alias("Error"))
                else:
                    df = df.with_columns(pl.lit("").alias(col))

        # --- CLEAN ROWS ---
        try:
            rows = [
                {
                    "first": preclean_text(r["First Name"]),
                    "last": preclean_text(r["Last Name"]),
                    "company": preclean_text(r["Company"]),
                }
                for r in df.iter_rows(named=True)
            ]
            cleaned = await retry_clean_rows(rows, job_id)
            df = df.with_columns([
                pl.Series("First Name", [c.get("first") or fallback_clean(r["First Name"]) for c, r in zip(cleaned, df.iter_rows(named=True))]),
                pl.Series("Last Name", [c.get("last") or fallback_clean(r["Last Name"]) for c, r in zip(cleaned, df.iter_rows(named=True))]),
                pl.Series("Company", [c.get("company") or fallback_clean(r["Company"]) for c, r in zip(cleaned, df.iter_rows(named=True))]),
            ])
            log_event(manifest, f"Applied cleaning to {len(cleaned)} rows")
        except Exception as e:
            log_event(manifest, f"⚠️ clean_rows failed: {e}")
            df = df.with_columns([
                pl.Series("First Name", [fallback_clean(r["First Name"]) for r in df.iter_rows(named=True)]),
                pl.Series("Last Name", [fallback_clean(r["Last Name"]) for r in df.iter_rows(named=True)]),
                pl.Series("Company", [fallback_clean(r["Company"]) for r in df.iter_rows(named=True)]),
                pl.Series("Error", ["clean_rows failed" for _ in range(df.height)]),
            ])

        # --- SCRAPE WEBSITES ---
        scrape_success, scrape_fail = 0, 0
        if "Website" in df.columns:
            websites = {preclean_text(r["Website"]): None for r in df.iter_rows(named=True) if r.get("Website")}
            scrape_tasks = {url: scraper.extract_company_info(url) for url in websites.keys()}
            results = await asyncio.gather(*scrape_tasks.values(), return_exceptions=True)

            for url, info in zip(scrape_tasks.keys(), results):
                websites[url] = info if isinstance(info, dict) else {"name": "", "text": ""}

            scraped_company, scraped_text = [], []
            for r in df.iter_rows(named=True):
                url = preclean_text(r.get("Website", ""))
                if url and url in websites:
                    info = websites[url]
                    scraped_company.append(info.get("name", "") or r.get("Company", ""))
                    text_val = info.get("text", "")[:800]
                    scraped_text.append(text_val)
                    if text_val:
                        scrape_success += 1
                    else:
                        scrape_fail += 1
                else:
                    scraped_company.append(r.get("Company", ""))
                    scraped_text.append("")
            df = df.with_columns([
                pl.Series("Company", scraped_company),
                pl.Series("ScrapedText", scraped_text),
            ])
        log_event(manifest, f"Scraping done → {scrape_success} success, {scrape_fail} fail")

        # --- GENERATE OPENERS + VALIDATION ---
        try:
            rows = [
                {
                    "company": preclean_text(r.get("Company", "")),
                    "website": preclean_text(r.get("Website", "")),
                    "text": preclean_text(r.get("ScrapedText", "")),
                }
                for r in df.iter_rows(named=True)
            ]
            opened = await retry_generate_openers(rows, job_id)

            seen = set()
            validated_openers, errors = [], []
            for r, o in zip(rows, opened):
                opener, err = validate_opener(o.get("opener", ""), r.get("text", ""), seen)
                validated_openers.append(opener)
                errors.append(err or o.get("error", ""))

            df = df.with_columns([
                pl.Series("Opener", validated_openers),
                pl.Series("Error", errors),
            ])
            log_event(manifest, f"Applied openers to {len(validated_openers)} rows with validation")
        except Exception as e:
            log_event(manifest, f"⚠️ generate_openers failed: {e}")
            df = df.with_columns([
                pl.Series("Opener", [get_fallback_opener() for _ in range(df.height)]),
                pl.Series("Error", [f"generate_openers failed: {e}" for _ in range(df.height)]),
            ])

        if "ScrapedText" in df.columns:
            df = df.drop("ScrapedText")

        out_path = f"jobs/{job_id}/out/chunk_{chunk_index}.csv"
        gcs.upload_bytes(BUCKET, out_path, df.write_csv().encode("utf-8"))
        log_event(manifest, f"Saved chunk {chunk_index} → {out_path}", save=True)

        manifest["processed_chunks"].append(chunk_index)
        save_manifest(job_id, manifest)

        if len(manifest["processed_chunks"]) == manifest["num_chunks"]:
            finalize_job(job_id, manifest)

    except Exception as e:
        log_event(manifest, f"process error: {e}", save=True)
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

        signed_url = gcs.generate_signed_url(BUCKET, final_path, expiration=7 * 24 * 3600)
        manifest["final_signed_url"] = signed_url
        log_event(manifest, "Generated signed URL")

        emailer.send_email(
            to=manifest["email"],
            subject="Your processed CSV is ready",
            body=f"Download link (valid 7 days):\n\n{signed_url}",
            cc=[os.environ.get("EMAIL_CC", "olimovazizbek1999@gmail.com")],
        )
        manifest["email_sent"] = True
        manifest["status"] = "done"
        log_event(manifest, f"Job {job_id} finished successfully → {manifest.get('total_rows', 0)} rows processed", save=True)
        save_manifest(job_id, manifest)

    except Exception as e:
        log_event(manifest, f"finalize error: {e}", save=True)
        manifest["status"] = "error"
        save_manifest(job_id, manifest)
