import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from . import gcs, emailer, scraper, openai_utils

logger = logging.getLogger(__name__)

# Config
CHUNK_ROWS = int(os.environ.get("CHUNK_ROWS", "10000"))
BATCH_CLEAN = int(os.environ.get("BATCH_CLEAN", "100"))
BATCH_OPENER = int(os.environ.get("BATCH_OPENER", "20"))
OPENAI_RPS = float(os.environ.get("OPENAI_RPS", "3"))
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")
GCP_PROJECT = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")

REQUIRED_COLUMNS = [
    "First Name", "Last Name", "Title", "Company", "Email", "# Employees", "Industry", "Keywords",
    "Person Linkedin Url", "Website", "Country", "a1", "a2", "a3", "Suitable Length", "Opener"
]


def create_manifest_skeleton(job_id: str, input_gcs_uri: str, email: str) -> Dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "job_id": job_id,
        "input": input_gcs_uri,
        "email": email,
        "status": "queued",
        "filename": input_gcs_uri.split("/")[-1],
        "created_at": now,
        "updated_at": now,
        "total_rows": None,
        "chunk_size": CHUNK_ROWS,
        "num_chunks": None,
        "processed_chunks": [],
        "error_chunks": [],
        "email_sent": False,
        "final_gcs_path": None,
        "final_signed_url": None,
        "log": [],
    }


def _read_input_to_count(bucket: str, gcs_uri: str) -> int:
    assert gcs_uri.startswith("gs://")
    parts = gcs_uri[5:].split("/", 1)
    bkt, path = parts[0], parts[1]
    tmp = gcs.download_to_tempfile(bkt, path)
    cnt = sum(1 for _ in open(tmp, "r", encoding="utf-8", errors="ignore")) - 1
    return max(cnt, 0)


def initialize_job_stats(bucket: str, job_id: str, input_gcs_uri: str) -> Tuple[int, int]:
    total_rows = _read_input_to_count(bucket, input_gcs_uri)
    num_chunks = math.ceil(total_rows / CHUNK_ROWS) if total_rows else 1
    man_path = f"jobs/{job_id}/manifest.json"
    manifest = gcs.read_json(bucket, man_path) or {}
    manifest.update({
        "status": "running",
        "total_rows": total_rows,
        "num_chunks": num_chunks,
        "updated_at": datetime.now(timezone.utc).isoformat()
    })
    gcs.write_json(bucket, man_path, manifest)
    return total_rows, num_chunks


def publish_next_chunk(job_id: str, chunk_index: int):
    if not (PUBSUB_TOPIC and GCP_PROJECT):
        logger.info("Pub/Sub not configured; skipping publish")
        return
    # Lazy import to avoid grpc dependency when not used
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)
    payload = json.dumps({"job_id": job_id, "chunk_index": chunk_index}).encode("utf-8")
    future = publisher.publish(topic_path, payload)
    future.result(timeout=30)
    logger.info(f"Published to {topic_path}: chunk {chunk_index}")


@dataclass
class Cache:
    company_by_url: Dict[str, str]
    homepage_text_by_url: Dict[str, str]


def _load_cache(bucket: str, job_id: str) -> Cache:
    obj = gcs.read_json(bucket, f"jobs/{job_id}/cache.json") or {}
    return Cache(
        company_by_url=obj.get("company_by_url", {}),
        homepage_text_by_url=obj.get("homepage_text_by_url", {}),
    )


def _save_cache(bucket: str, job_id: str, cache: Cache):
    gcs.write_json(bucket, f"jobs/{job_id}/cache.json", {
        "company_by_url": cache.company_by_url,
        "homepage_text_by_url": cache.homepage_text_by_url,
    })


def _update_manifest(job_id: str, **fields):
    man_path = f"jobs/{job_id}/manifest.json"
    manifest = gcs.read_json(GCS_BUCKET, man_path) or {}
    manifest.update(fields)
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
    gcs.write_json(GCS_BUCKET, man_path, manifest)


def _log(job_id: str, message: str):
    man_path = f"jobs/{job_id}/manifest.json"
    manifest = gcs.read_json(GCS_BUCKET, man_path) or {}
    log = manifest.get("log", [])
    log.append({"time": datetime.now(timezone.utc).isoformat(), "msg": message})
    manifest["log"] = log[-500:]
    gcs.write_json(GCS_BUCKET, man_path, manifest)


def _iter_csv_chunk(tmp_path: str, chunk_index: int) -> pd.DataFrame:
    reader = pd.read_csv(tmp_path, chunksize=CHUNK_ROWS)
    for idx, df in enumerate(reader):
        if idx == chunk_index:
            return df
    return pd.DataFrame(columns=REQUIRED_COLUMNS)


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for c in REQUIRED_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[REQUIRED_COLUMNS]


def _batch(items: List, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _clean_names(df: pd.DataFrame):
    rows = [
        {"first": str(r.get("First Name", "")), "last": str(r.get("Last Name", "")), "company": str(r.get("Company", ""))}
        for r in df.to_dict(orient="records")
    ]
    out = []
    for chunk in _batch(rows, BATCH_CLEAN):
        cleaned = openai_utils.batch_clean_names(chunk)
        out.extend(cleaned)
    for i, c in enumerate(out):
        df.at[df.index[i], "First Name"] = c.get("first", "").strip()
        df.at[df.index[i], "Last Name"] = c.get("last", "").strip()
        df.at[df.index[i], "Company"] = c.get("company", "").strip()


def _scrape_and_fill(df: pd.DataFrame, cache: Cache):
    urls: Dict[str, List[int]] = {}
    for i, row in df.iterrows():
        url = str(row.get("Website") or "").strip()
        if url:
            urls.setdefault(url, []).append(i)

    for url, idxs in urls.items():
        if url in cache.homepage_text_by_url:
            company_name = cache.company_by_url.get(url)
            homepage_text = cache.homepage_text_by_url.get(url, "")
        else:
            try:
                html = scraper.fetch(url)
            except Exception as e:
                _log("", f"scrape error for {url}: {e}")
                html = None
            company_name = scraper.extract_company_name(html) if html else None
            homepage_text = scraper.extract_visible_text(html) if html else ""
            if company_name:
                cache.company_by_url[url] = company_name
            cache.homepage_text_by_url[url] = homepage_text
        for i in idxs:
            if company_name:
                df.at[i, "Company"] = company_name
            df.at[i, "_homepage_text"] = homepage_text


def _generate_openers(df: pd.DataFrame):
    items = [
        {"homepage_text": str(r.get("_homepage_text", "")), "company": str(r.get("Company", ""))}
        for r in df.to_dict(orient="records")
    ]
    out: List[str] = []
    for chunk in _batch(items, BATCH_OPENER):
        gens = openai_utils.batch_generate_openers(chunk)
        out.extend(gens)
    for i, txt in enumerate(out):
        df.at[df.index[i], "Opener"] = (txt or "").strip()


def process_chunk(bucket: str, job_id: str, chunk_index: int):
    man = gcs.read_json(bucket, f"jobs/{job_id}/manifest.json") or {}
    input_uri = man.get("input")
    assert input_uri and input_uri.startswith("gs://")
    bkt, path = input_uri[5:].split("/", 1)

    tmp = gcs.download_to_tempfile(bkt, path)
    df = _iter_csv_chunk(tmp, chunk_index)
    if df.empty:
        logger.info(f"Chunk {chunk_index} empty; marking processed")
        _update_manifest(job_id, processed_chunks=sorted(set([*man.get('processed_chunks', []), chunk_index])))
        return

    df = _ensure_columns(df)
    _log(job_id, f"chunk {chunk_index}: loaded {len(df)} rows")

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

    out_path = f"jobs/{job_id}/out/chunk_{chunk_index}.csv"
    try:
        gcs.write_df_csv(bucket, out_path, df)
    except Exception as e:
        _log(job_id, f"save chunk error: {e}")
        man.setdefault("error_chunks", []).append(chunk_index)
        _update_manifest(job_id, error_chunks=man.get("error_chunks", []))

    processed = sorted(set([*man.get("processed_chunks", []), chunk_index]))
    _update_manifest(job_id, processed_chunks=processed)

    if len(processed) >= int(man.get("num_chunks", 0) or 0):
        try:
            final_path = gcs.merge_chunks_to_final(bucket, job_id, man.get("filename", "output.csv"))
            signed = gcs.generate_signed_url(bucket, final_path, days=7)
            _update_manifest(job_id, status="done", final_gcs_path=final_path, final_signed_url=signed)
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

    next_idx = chunk_index + 1
    if PUBSUB_TOPIC and os.environ.get("GOOGLE_CLOUD_PROJECT"):
        publish_next_chunk(job_id, next_idx)
    else:
        logger.info("PUB/SUB not configured; next chunk not scheduled automatically")
