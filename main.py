import csv
import io
import json
import logging
import os
import re
import smtplib
import sys
import time
import uuid
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from typing import List, Optional, Dict, Any, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, PlainTextResponse
from google.cloud import storage
from google.cloud import pubsub_v1
from openai import OpenAI
from pydantic import BaseModel, EmailStr
from tenacity import retry, wait_exponential, stop_after_attempt

# ------------- Logging -------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ------------- Config via env -------------
PROJECT_ID = os.getenv("GCP_PROJECT") or os.getenv("PROJECT_ID", "")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")  # REQUIRED
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "")  # optional; if missing, processes synchronously (dev)
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))  # ~10k rows per job
OPENAI_MODEL_CLEAN = os.getenv("OPENAI_MODEL_CLEAN", "gpt-4o-mini")
OPENAI_MODEL_OPENER = os.getenv("OPENAI_MODEL_OPENER", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
SIGNED_URL_TTL_DAYS = int(os.getenv("SIGNED_URL_TTL_DAYS", "7"))
# Email via SMTP (e.g., SendGrid SMTP relay): set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", "noreply@example.com")
CC_EMAIL = "olimovazizbek1999@gmail.com"

if not GCS_BUCKET:
    logger.error("GCS_BUCKET env var is required.")
if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY not set. OpenAI calls will fail.")

# ------------- Clients -------------
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)

pubsub_publisher = None
if PUBSUB_TOPIC:
    try:
        pubsub_publisher = pubsub_v1.PublisherClient()
        if PUBSUB_TOPIC.startswith("projects/"):
            PUBSUB_TOPIC_PATH = PUBSUB_TOPIC
        else:
            PUBSUB_TOPIC_PATH = pubsub_publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    except Exception as e:
        logger.error(f"Could not init Pub/Sub publisher: {e}")
        pubsub_publisher = None

openai_client = OpenAI(api_key=OPENAI_API_KEY)

# ------------- FastAPI app -------------
app = FastAPI(title="Lead CSV Processor (Cloud Run)")

# ------------- Models -------------
class PubSubPush(BaseModel):
    message: Dict[str, Any]
    subscription: Optional[str] = None

# ------------- Helpers: GCS -------------
def gcs_blob(path: str):
    return bucket.blob(path)

def gcs_write_text(path: str, text: str, content_type: str = "text/plain"):
    b = gcs_blob(path)
    b.upload_from_string(text, content_type=content_type)

def gcs_read_text(path: str) -> str:
    b = gcs_blob(path)
    return b.download_as_text()

def gcs_exists(path: str) -> bool:
    return gcs_blob(path).exists()

def gcs_generate_signed_url(path: str, days: int = 7) -> str:
    blob = gcs_blob(path)
    expiration = timedelta(days=days)
    return blob.generate_signed_url(version="v4", expiration=expiration, method="GET")

# ------------- HTML Form -------------
FORM_HTML = """
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>Lead Processor Upload</title>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; padding: 24px; color: #0f172a; }
      .card { max-width: 720px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 16px; padding: 24px; box-shadow: 0 6px 20px rgba(0,0,0,0.06); }
      label { display:block; margin: 12px 0 6px; font-weight: 600; }
      input,button { width:100%; padding: 12px; border:1px solid #cbd5e1; border-radius: 12px; }
      button { background:#0ea5e9; color:white; font-weight:700; border:none; cursor:pointer; margin-top:16px; }
      button:hover{ background:#0284c7; }
      small { color:#334155; }
    </style>
  </head>
  <body>
    <div class="card">
      <h2>Upload CSV & Notification Email</h2>
      <form action="/upload" method="post" enctype="multipart/form-data">
        <label for="file">CSV File (Columns A–O; headers okay)</label>
        <input type="file" id="file" name="file" accept=".csv" required />

        <label for="email">Your Email</label>
        <input type="email" id="email" name="email" placeholder="you@company.com" required />

        <button type="submit">Start Processing</button>
        <p><small>We’ll email you a signed link when it’s done.</small></p>
      </form>
    </div>
  </body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(FORM_HTML)

# ------------- Email -------------
def send_email(subject: str, body: str, to_email: str, cc_email: Optional[str] = None):
    if not SMTP_HOST:
        logger.warning("SMTP not configured; skipping email send.")
        return

    recipients = [to_email]
    if cc_email:
        recipients.append(cc_email)

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to_email
    if cc_email:
        msg["Cc"] = cc_email

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_FROM, recipients, msg.as_string())
        logger.info(f"Email sent to {recipients}")

# ------------- Web Scraping -------------
USER_AGENT = "Mozilla/5.0 (compatible; LeadProcessorBot/1.0; +https://example.com/bot)"

def _visible_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    # remove script/style/noscript
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()

    texts = soup.stripped_strings
    combined = " ".join(t for t in texts if t)
    # Collapse whitespace
    combined = re.sub(r"\s+", " ", combined).strip()
    return combined[:12000]  # cost control

def _extract_company_name(soup: BeautifulSoup) -> Optional[str]:
    # Candidates: og:site_name, meta name=application-name, <title>, first h1, header site brand
    og_site = soup.find("meta", property="og:site_name")
    if og_site and og_site.get("content"):
        return og_site["content"].strip()

    app_name = soup.find("meta", attrs={"name": "application-name"})
    if app_name and app_name.get("content"):
        return app_name["content"].strip()

    title = soup.title.string.strip() if soup.title and soup.title.string else None
    if title:
        # Clean common separators
        title = re.split(r"\s*[|\-–—•]\s*", title)[0].strip()
        if 2 <= len(title) <= 80:
            return title

    h1 = soup.find("h1")
    if h1:
        txt = re.sub(r"\s+", " ", h1.get_text(strip=True))
        if txt and 2 <= len(txt) <= 80:
            return txt

    return None

def fetch_homepage_text_and_name(url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        if not re.match(r"^https?://", url, re.I):
            url = "https://" + url.strip("/")
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=12)
        if resp.status_code >= 400 or not resp.text:
            return None, None
        soup = BeautifulSoup(resp.text, "lxml")
        visible = _visible_text_from_html(resp.text)
        name = _extract_company_name(soup)
        return visible, name
    except Exception as e:
        logger.warning(f"fetch_homepage_text_and_name error for {url}: {e}")
        return None, None

# ------------- OpenAI Helpers -------------
@retry(wait=wait_exponential(multiplier=1, min=1, max=20), stop=stop_after_attempt(5))
def openai_batch_clean(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    rows: list of dicts with keys: first, last, company
    returns list same length with cleaned fields
    """
    # Compact prompt with JSON schema-ish instructions
    system = (
        "You are a data cleaner. Remove leading/trailing/multiple spaces and irrelevant non-letter symbols "
        "from names and company fields. Keep legitimate punctuation like hyphens or apostrophes inside words. "
        "Return strict JSON array, each item: {\"first\": str, \"last\": str, \"company\": str} in order."
    )
    user = {"rows": rows}
    resp = openai_client.chat.completions.create(
        model=OPENAI_MODEL_CLEAN,
        temperature=0,
        messages=[
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": "Clean the following rows. Respond ONLY with JSON.\n" + json.dumps(user, ensure_ascii=False),
            },
        ],
    )
    content = resp.choices[0].message.content
    try:
        # Find JSON payload (some models may wrap in code fences)
        m = re.search(r"\[.*\]", content, re.S)
        cleaned = json.loads(m.group(0)) if m else json.loads(content)
        assert isinstance(cleaned, list)
        return cleaned
    except Exception as e:
        logger.error(f"Failed to parse OpenAI clean JSON: {e}; content={content[:2000]}")
        # Fallback: return original
        return rows

@retry(wait=wait_exponential(multiplier=1, min=1, max=20), stop=stop_after_attempt(5))
def openai_batch_openers(pairs: List[Dict[str, str]]) -> List[str]:
    """
    pairs: list of dict {homepage_text:str, company_name:str}
    returns list of openers: exactly 2 sentences, 10–18 words each.
    """
    rules = (
        "Write a professional, calm, specific two-sentence email opener about the company, "
        "using ONLY the visible homepage text provided. Tone neutral-positive. No hype, emojis, "
        "or questions. No assumptions about size, funding, or location. Exactly two sentences, "
        "each 10-18 words. Second sentence must flow naturally and end cleanly without a sales pitch."
    )
    # To encourage parallel generation, enumerate with indices
    items = [{"idx": i, "company": p.get("company_name", "")[:80], "text": p.get("homepage_text", "")[:2000]} for i, p in enumerate(pairs)]
    system = "You generate short email openers that strictly follow constraints and output JSON."
    user = {"rules": rules, "items": items}
    resp = openai_client.chat.completions.create(
        model=OPENAI_MODEL_OPENER,
        temperature=0.2,
        messages=[
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": "For each item, produce an opener string following the rules. "
                           "Return ONLY a JSON array of strings in the same order as items.\n"
                           + json.dumps(user, ensure_ascii=False),
            },
        ],
    )
    content = resp.choices[0].message.content
    try:
        m = re.search(r"\[.*\]", content, re.S)
        arr = json.loads(m.group(0)) if m else json.loads(content)
        assert isinstance(arr, list)
        # enforce structure fallback
        fixed = []
        for s in arr:
            if not isinstance(s, str):
                s = ""
            # Attempt to enforce 2 sentences, 10-18 words each.
            sents = re.split(r"(?<=[.!?])\s+", s.strip())
            sents = [x for x in sents if x]
            if len(sents) != 2 or not (10 <= len(sents[0].split()) <= 18 and 10 <= len(sents[1].split()) <= 18):
                # Weak fallback: truncate/repair
                text = "We reviewed your homepage and noted key offerings and value to customers. "
                text += "Your messaging highlights specific solutions and outcomes that appear relevant to your audience."
                s = text
            fixed.append(s)
        return fixed
    except Exception as e:
        logger.error(f"Failed to parse OpenAI opener JSON: {e}; content={content[:1200]}")
        return [
            "We reviewed your homepage and noted key offerings and value to customers. "
            "Your messaging highlights specific solutions and outcomes that appear relevant to your audience."
            for _ in pairs
        ]

# ------------- CSV Processing -------------
def _read_csv_chunk_from_gcs(input_path: str, chunk_index: int, chunk_size: int) -> pd.DataFrame:
    # Stream from GCS using gcsfs via pandas
    with storage_client.bucket(GCS_BUCKET).blob(input_path).open("rb") as f:
        it = pd.read_csv(f, chunksize=chunk_size, dtype=str, keep_default_na=False)
        for i, df in enumerate(it):
            if i == chunk_index:
                df = df.fillna("")
                # Normalize columns by index if headers missing
                df.columns = list(df.columns)
                return df
    return pd.DataFrame()

def _write_chunk_to_gcs(df: pd.DataFrame, output_prefix: str, chunk_index: int):
    path = f"{output_prefix}/chunk_{chunk_index:06d}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    gcs_blob(path).upload_from_string(csv_bytes, content_type="text/csv")
    logger.info(f"Wrote chunk to {path}")

def _merge_chunks(output_prefix: str, merged_path: str):
    # Concatenate in order
    blobs = list(storage_client.list_blobs(GCS_BUCKET, prefix=output_prefix + "/"))
    parts = sorted([b.name for b in blobs if b.name.endswith(".csv")])
    if not parts:
        raise RuntimeError("No chunks to merge.")
    header_written = False
    out_blob = gcs_blob(merged_path)
    with out_blob.open("wb") as out_f:
        for p in parts:
            with gcs_blob(p).open("rb") as in_f:
                for i, line in enumerate(in_f):
                    if header_written and i == 0:
                        continue
                    out_f.write(line)
            header_written = True
    logger.info(f"Merged to {merged_path}")

# ------------- Row-level transforms -------------
def clean_fields_batch(firsts: List[str], lasts: List[str], companies: List[str]) -> Tuple[List[str], List[str], List[str]]:
    rows = [{"first": f or "", "last": l or "", "company": c or ""} for f, l, c in zip(firsts, lasts, companies)]
    cleaned = openai_batch_clean(rows)
    out_f = []
    out_l = []
    out_c = []
    for item in cleaned:
        out_f.append(item.get("first", ""))
        out_l.append(item.get("last", ""))
        out_c.append(item.get("company", ""))
    return out_f, out_l, out_c

def generate_openers_batch(company_names: List[str], homepage_texts: List[str]) -> List[str]:
    pairs = [{"company_name": c or "", "homepage_text": t or ""} for c, t in zip(company_names, homepage_texts)]
    return openai_batch_openers(pairs)

# ------------- Job Orchestration -------------
def _manifest_path(job_id: str) -> str:
    return f"jobs/{job_id}/manifest.json"

def _output_prefix(job_id: str) -> str:
    return f"jobs/{job_id}/out"

def _merged_output_path(job_id: str, base_name: str) -> str:
    stem = base_name.rsplit(".", 1)[0]
    return f"jobs/{job_id}/{stem}_processed.csv"

def publish_next_chunk(job_id: str, chunk_index: int):
    if not pubsub_publisher:
        # Fallback: synchronous call (useful for local/dev)
        logger.info("Pub/Sub not configured; processing synchronously.")
        process_chunk(job_id, chunk_index)
        return

    data = json.dumps({"job_id": job_id, "chunk_index": chunk_index}).encode("utf-8")
    future = pubsub_publisher.publish(PUBSUB_TOPIC_PATH, data, origin="lead-processor")
    future.result(timeout=30)
    logger.info(f"Published chunk {chunk_index} for job {job_id}")

def init_manifest(job_id: str, input_path: str, email: str, base_name: str):
    # Compute total rows quickly using pandas iterator
    total_rows = 0
    with storage_client.bucket(GCS_BUCKET).blob(input_path).open("rb") as f:
        for _ in pd.read_csv(f, chunksize=100_000):
            total_rows += _.shape[0]
    manifest = {
        "job_id": job_id,
        "input_path": input_path,
        "email": email,
        "base_name": base_name,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "chunk_size": CHUNK_SIZE,
        "total_rows": total_rows,
        "status": "queued",
        "processed_chunks": [],
        "error_chunks": [],
    }
    gcs_write_text(_manifest_path(job_id), json.dumps(manifest, indent=2))
    return manifest

def update_manifest(job_id: str, patch: Dict[str, Any]):
    path = _manifest_path(job_id)
    manifest = json.loads(gcs_read_text(path))
    manifest.update(patch)
    gcs_write_text(path, json.dumps(manifest, indent=2))

def read_manifest(job_id: str) -> Dict[str, Any]:
    return json.loads(gcs_read_text(_manifest_path(job_id)))

# ------------- Core chunk processor -------------
def process_chunk(job_id: str, chunk_index: int):
    manifest = read_manifest(job_id)
    input_path = manifest["input_path"]
    email = manifest["email"]
    base_name = manifest["base_name"]
    chunk_size = int(manifest["chunk_size"])
    total_rows = int(manifest["total_rows"])

    df = _read_csv_chunk_from_gcs(input_path, chunk_index, chunk_size)
    if df.empty and (chunk_index * chunk_size) < total_rows:
        logger.warning(f"No data read for chunk {chunk_index} though rows remain; marking error.")
        manifest["error_chunks"].append(chunk_index)
        update_manifest(job_id, {"error_chunks": list(set(manifest["error_chunks"]))})
        # still attempt next chunk
    else:
        # Ensure at least 15 columns (A–O = 15); Columns P will be added
        # If columns fewer than 15, pad with empty unnamed columns to index properly.
        if df.shape[1] < 15:
            for _i in range(15 - df.shape[1]):
                df[f"extra_{_i}"] = ""
        df = df.copy()

        # Column indices: A=0, B=1, D=3, J=9, P(new)=15
        # 1) Clean First, Last, Company via OpenAI in batches of 50–100
        def batched_indices(n, size_min=50, size_max=100):
            i = 0
            while i < n:
                # Choose batch size within range, but don't exceed remaining
                size = min(size_max, n - i)
                if size < size_min and i == 0:
                    size = min(size_min, n)
                yield i, i + size
                i += size

        # 2) For rows with J (website), fetch visible text and possibly overwrite D
        #    Build cache to avoid duplicate site fetch
        homepage_cache: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        homepage_texts: List[str] = [""] * len(df)

        # Clean fields
        firsts = list(df.iloc[:, 0].astype(str))
        lasts = list(df.iloc[:, 1].astype(str))
        companies = list(df.iloc[:, 3].astype(str))

        for start, end in batched_indices(len(df)):
            f_batch = firsts[start:end]
            l_batch = lasts[start:end]
            c_batch = companies[start:end]
            try:
                cf, cl, cc = clean_fields_batch(f_batch, l_batch, c_batch)
                firsts[start:end] = cf
                lasts[start:end] = cl
                companies[start:end] = cc
            except Exception as e:
                logger.error(f"Clean batch {start}-{end} failed: {e}")

        df.iloc[:, 0] = firsts
        df.iloc[:, 1] = lasts
        df.iloc[:, 3] = companies

        # Fetch website info and collect homepage texts for opener generation
        urls = list(df.iloc[:, 9].astype(str))
        extracted_company_names: List[Optional[str]] = [None] * len(df)

        for idx, u in enumerate(urls):
            u = (u or "").strip()
            if not u:
                continue
            if u not in homepage_cache:
                homepage_cache[u] = fetch_homepage_text_and_name(u)
            text, comp_name = homepage_cache[u]
            homepage_texts[idx] = text or ""
            if comp_name:
                extracted_company_names[idx] = comp_name

        # Overwrite D (company) where a name was extracted
        for i, name in enumerate(extracted_company_names):
            if name:
                df.iat[i, 3] = name

        # 3) Generate Column P (new) using OpenAI in batches of 10–20
        def batched_small(n, size_min=10, size_max=20):
            i = 0
            while i < n:
                size = min(size_max, n - i)
                if size < size_min and i == 0:
                    size = min(size_min, n)
                yield i, i + size
                i += size

        # Prepare inputs for opener gen: company name + homepage text
        final_company_names = list(df.iloc[:, 3].astype(str))
        openers = [""] * len(df)

        for start, end in batched_small(len(df)):
            pairs_c = final_company_names[start:end]
            pairs_t = homepage_texts[start:end]
            try:
                res = generate_openers_batch(pairs_c, pairs_t)
                openers[start:end] = res
            except Exception as e:
                logger.error(f"Opener batch {start}-{end} failed: {e}")
                for k in range(start, end):
                    openers[k] = (
                        "We reviewed your homepage and noted key offerings and value to customers. "
                        "Your messaging highlights specific solutions and outcomes that appear relevant to your audience."
                    )

        # Ensure Column P appended
        if df.shape[1] >= 16:
            df.iloc[:, 15] = openers  # overwrite if exists
        else:
            # pad exactly to 15 then append P
            while df.shape[1] < 15:
                df[f"pad_{df.shape[1]}"] = ""
            df["P_EmailOpener"] = openers

        # Save chunk
        out_prefix = _output_prefix(job_id)
        _write_chunk_to_gcs(df, out_prefix, chunk_index)

        # Update manifest
        manifest["processed_chunks"] = list(sorted(set(manifest.get("processed_chunks", []) + [chunk_index])))
        manifest["status"] = "running"
        update_manifest(job_id, {"processed_chunks": manifest["processed_chunks"], "status": manifest["status"]})

    # Decide next step
    last_row_index_processed = (chunk_index + 1) * CHUNK_SIZE
    has_more = last_row_index_processed < total_rows
    if has_more:
        publish_next_chunk(job_id, chunk_index + 1)
    else:
        # Merge and email
        try:
            merged_path = _merged_output_path(job_id, base_name)
            _merge_chunks(_output_prefix(job_id), merged_path)
            url = gcs_generate_signed_url(merged_path, days=SIGNED_URL_TTL_DAYS)
            body = f"Your processed file is ready:\n\n{url}\n\nThis link expires in {SIGNED_URL_TTL_DAYS} days."
            send_email(
                subject="Your processed CSV is ready",
                body=body,
                to_email=email,
                cc_email=CC_EMAIL,
            )
            update_manifest(job_id, {"status": "done", "download_url": url, "completed_at": datetime.utcnow().isoformat() + "Z"})
            logger.info(f"Job {job_id} completed; emailed {email}")
        except Exception as e:
            logger.exception(f"Merge/email failed for job {job_id}: {e}")
            update_manifest(job_id, {"status": "merge_failed", "error": str(e)})

# ------------- HTTP Endpoints -------------

@app.post("/upload", response_class=PlainTextResponse)
async def upload(file: UploadFile = File(...), email: EmailStr = Form(...)):
    # Save uploaded file to GCS
    job_id = uuid.uuid4().hex
    base_name = os.path.basename(file.filename)
    input_path = f"jobs/{job_id}/input/{base_name}"

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file.")

    gcs_blob(input_path).upload_from_string(content, content_type="text/csv")
    logger.info(f"Uploaded to gs://{GCS_BUCKET}/{input_path}")

    manifest = init_manifest(job_id, input_path, str(email), base_name)

    # Kick off first chunk
    publish_next_chunk(job_id, 0)

    return PlainTextResponse(
        f"Upload received. Job ID: {job_id}\n"
        f"We'll email {email} a signed download link when processing completes."
    )

# Pub/Sub push endpoint
@app.post("/process", response_class=PlainTextResponse)
async def process(request: Request):
    # Accept both raw (dev) and Pub/Sub push
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if "message" in payload:
        # Pub/Sub push format
        try:
            data_b64 = payload["message"].get("data", "")
            if not data_b64:
                raise ValueError("No data in message.")
            data_json = json.loads(io.BytesIO(base64.b64decode(data_b64)).read().decode("utf-8"))
        except Exception:
            # Some proxies forward already-decoded message
            data_json = payload["message"].get("json", payload["message"])
    else:
        data_json = payload

    job_id = data_json.get("job_id")
    chunk_index = int(data_json.get("chunk_index", 0))

    if not job_id:
        raise HTTPException(status_code=400, detail="Missing job_id")

    logger.info(f"Process invoked for job {job_id}, chunk {chunk_index}")
    try:
        process_chunk(job_id, chunk_index)
        return PlainTextResponse("OK")
    except Exception as e:
        logger.exception(f"Processing failed for job {job_id} chunk {chunk_index}: {e}")
        # Mark error but do not crash; partial results preserved
        try:
            manifest = read_manifest(job_id)
            manifest["error_chunks"] = list(sorted(set(manifest.get("error_chunks", []) + [chunk_index])))
            update_manifest(job_id, {"error_chunks": manifest["error_chunks"], "status": "error"})
        except Exception:
            pass
        # Continue with next chunk to avoid deadlock
        try:
            publish_next_chunk(job_id, chunk_index + 1)
        except Exception:
            pass
        return PlainTextResponse("FAILED", status_code=500)

# Health check
@app.get("/healthz", response_class=PlainTextResponse)
async def healthz():
    return PlainTextResponse("ok")

# ----------- Local dev helper -----------
# Allow manual trigger without Pub/Sub
@app.post("/dev/process_once", response_class=PlainTextResponse)
async def dev_process_once(job_id: str, chunk_index: int = 0):
    process_chunk(job_id, chunk_index)
    return PlainTextResponse("done")

# ----------- Base64 import -------------
# Defer import to bottom to avoid unused complaint if Pub/Sub not used
import base64  # noqa: E402