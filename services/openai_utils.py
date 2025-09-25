import os
import json
from typing import List, Dict
import asyncio
import httpx
import unicodedata
import re
import multiprocessing
from ftfy import fix_text
from openai import AsyncOpenAI

_client = None

# Auto-scale concurrency: 5 per CPU core unless overridden
_cpu_count = multiprocessing.cpu_count()
_default_concurrency = _cpu_count * 5
_openai_concurrency = int(os.environ.get("OPENAI_CONCURRENCY", _default_concurrency))
_semaphore = asyncio.Semaphore(_openai_concurrency)

_clean_cache: dict[str, dict] = {}
_opener_cache: dict[str, dict] = {}

# ✅ Separate failure counters
_clean_failure_count = 0
_opener_failure_count = 0
_FAILURE_LIMIT = 5

MAX_TEXT_LEN = 800


# ---------------- Pre-clean ---------------- #

def preclean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", str(text))
    text = fix_text(text)
    text = re.sub(r"[^\w\s\-&.,]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ---------------- OpenAI Client ---------------- #

def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY environment variable is not set")
        http_client = httpx.AsyncClient(timeout=25.0, follow_redirects=True)
        _client = AsyncOpenAI(api_key=api_key, http_client=http_client)
    return _client


def _extract_choice_content(resp) -> str | None:
    try:
        choice = resp.choices[0]
        if hasattr(choice, "message") and getattr(choice.message, "content", None):
            return choice.message.content
        if hasattr(choice, "text"):
            return choice.text
        return None
    except Exception as e:
        print(f"⚠️ Failed to extract choice content: {e}")
        return None


# ---------------- CLEAN ROWS ---------------- #

async def clean_rows(rows: List[Dict], job_id=None) -> List[Dict]:
    global _clean_failure_count
    if _clean_failure_count > _FAILURE_LIMIT:
        print("⚠️ Skipping clean_rows due to circuit breaker")
        return [
            {
                "first": preclean_text(r.get("first", "")),
                "last": preclean_text(r.get("last", "")),
                "company": preclean_text(r.get("company", "")),
            }
            for r in rows
        ]

    client = get_client()
    results: List[Dict] = []

    uncached, uncached_indices = [], []
    for i, r in enumerate(rows):
        key = f"{r.get('first','')}|{r.get('last','')}|{r.get('company','')}"
        if key in _clean_cache:
            results.append(_clean_cache[key])
        else:
            uncached.append({
                "first": preclean_text(r.get("first", "")),
                "last": preclean_text(r.get("last", "")),
                "company": preclean_text(r.get("company", "")),
            })
            uncached_indices.append(i)
            results.append(None)

    async def _clean_batch(batch):
        nonlocal _clean_failure_count
        prompt = (
            "You are a data cleaner. Normalize personal names and company names.\n"
            "- Remove symbols.\n"
            "- Trim whitespace.\n"
            "- Fix encoding issues (ü, ö, ä, ß).\n"
            "- Capitalize names and company names.\n"
            "Return strictly as JSON: {\"rows\": [{\"first\":...,\"last\":...,\"company\":...}]}\n\n"
            f"Rows: {json.dumps(batch, ensure_ascii=False)}"
        )
        async with _semaphore:
            try:
                resp = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                )
                content = _extract_choice_content(resp)
                parsed = json.loads(content)
                return parsed.get("rows", batch)
            except Exception as e:
                _clean_failure_count += 1
                print(f"⚠️ clean_rows batch failed: {e}")
                return batch

    tasks = [_clean_batch(uncached[i:i + 200]) for i in range(0, len(uncached), 200)]
    batched_results = await asyncio.gather(*tasks)

    idx = 0
    for batch_result in batched_results:
        for cleaned in batch_result:
            original = uncached[idx]
            key = f"{original.get('first','')}|{original.get('last','')}|{original.get('company','')}"
            _clean_cache[key] = cleaned
            results[uncached_indices[idx]] = cleaned
            idx += 1

    return results


# ---------------- GENERATE OPENERS ---------------- #

def _validate_opener(text: str) -> bool:
    if not text:
        return False
    sentences = [s.strip() for s in text.split(".") if s.strip()]
    if len(sentences) != 2:
        return False
    return all(10 <= len(s.split()) <= 18 for s in sentences)


async def generate_openers(rows: List[Dict], job_id=None) -> List[Dict]:
    global _opener_failure_count
    if _opener_failure_count > _FAILURE_LIMIT:
        print("⚠️ Skipping generate_openers due to circuit breaker")
        return [{"opener": "", "error": "ai temporarily unavailable"} for _ in rows]

    client = get_client()
    results: List[Dict] = [None] * len(rows)

    async def _gen_batch(batch, start_idx):
        nonlocal _opener_failure_count
        cleaned_batch = [
            {
                "company": preclean_text(r.get("company", "")),
                "website": preclean_text(r.get("website", "")),
                "text": preclean_text(r.get("text", ""))[:MAX_TEXT_LEN],
            }
            for r in batch
        ]
        system_prompt = (
            "You are a professional cold email assistant.\n"
            "Your job: write exactly TWO sentences per row.\n"
            "Constraints:\n"
            "- Each 10–18 words.\n"
            "- Observational, respectful, not salesy.\n"
            "- Use only provided text, never invent facts.\n"
            "- If no useful detail is found, fallback to a polite compliment.\n"
            "- Avoid buzzwords like 'leading provider', 'innovative', 'synergy'.\n"
            "- Each opener must be unique across rows.\n"
            "Output strictly as JSON: {\"rows\":[{\"opener\":\"...\",\"error\":\"\"},...]}"
        )
        async with _semaphore:
            try:
                resp = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Rows: {json.dumps(cleaned_batch, ensure_ascii=False)}"},
                    ],
                    response_format={"type": "json_object"},
                )
                content = _extract_choice_content(resp)
                parsed = json.loads(content)
                for i, r in enumerate(parsed.get("rows", [])):
                    opener = r.get("opener", "")
                    if _validate_opener(opener):
                        results[start_idx + i] = {"opener": opener, "error": ""}
                    else:
                        results[start_idx + i] = {"opener": "", "error": "invalid opener"}
            except Exception as e:
                _opener_failure_count += 1
                print(f"⚠️ opener batch failed: {e}")
                for i in range(len(batch)):
                    results[start_idx + i] = {"opener": "", "error": f"openai failed: {e}"}

    tasks = []
    for i in range(0, len(rows), 50):
        tasks.append(_gen_batch(rows[i:i + 50], i))
    await asyncio.gather(*tasks)

    for i, r in enumerate(results):
        if not r:
            results[i] = {"opener": "", "error": "missing"}

    return results
