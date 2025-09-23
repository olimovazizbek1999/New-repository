import os
import json
from typing import List, Dict
import asyncio
from openai import AsyncOpenAI

_client = None
_semaphore = asyncio.Semaphore(5)  # limit concurrent API calls


def get_client() -> AsyncOpenAI:
    """Lazy-load async OpenAI client."""
    global _client
    if _client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY environment variable is not set")
        _client = AsyncOpenAI(api_key=api_key)
    return _client


async def clean_rows(rows: List[Dict], job_id=None) -> List[Dict]:
    """
    Clean first/last/company fields using OpenAI (async).
    Batches of 100 rows.
    """
    client = get_client()
    results: List[Dict] = []

    async def _clean_batch(batch):
        prompt = (
            "You are a data cleaner. Normalize personal names and company names.\n"
            "- Remove symbols like &!@^#$.\n"
            "- Trim whitespace.\n"
            "- Preserve capitalization (Hans-Josef Schiffers).\n"
            "Return JSON list with objects {first,last,company}.\n\n"
            f"Rows: {json.dumps(batch)}"
        )
        async with _semaphore:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
        try:
            content = resp.choices[0].message.content
            parsed = json.loads(content)
            return parsed.get("rows", batch)
        except Exception as e:
            print(f"⚠️ clean_rows batch error: {e}")
            return batch

    tasks = [_clean_batch(rows[i:i+100]) for i in range(0, len(rows), 100)]
    for batch_result in await asyncio.gather(*tasks):
        results.extend(batch_result)

    return results


async def generate_openers(rows: List[Dict], job_id=None) -> List[Dict]:
    """
    Generate professional two-sentence openers (async).
    Batches of 20 rows.
    """
    client = get_client()
    results: List[Dict] = []

    async def _gen_batch(batch):
        prompt = (
            "You are a B2B sales assistant.\n"
            "For each row, generate a professional email opener from homepage text.\n"
            "- Style: professional, calm.\n"
            "- Tone: neutral-positive, no hype, no emojis, no questions.\n"
            "- Exactly 2 sentences, each 10–18 words.\n"
            "- Second sentence must follow naturally, no sales pitch.\n"
            "Return JSON list with objects {opener}.\n\n"
            f"Rows: {json.dumps(batch)}"
        )
        async with _semaphore:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
        try:
            content = resp.choices[0].message.content
            parsed = json.loads(content)
            return parsed.get("rows", [{"opener": "No opener generated"} for _ in batch])
        except Exception as e:
            print(f"⚠️ generate_openers batch error: {e}")
            return [{"opener": "No opener generated"} for _ in batch]

    tasks = [_gen_batch(rows[i:i+20]) for i in range(0, len(rows), 20)]
    for batch_result in await asyncio.gather(*tasks):
        results.extend(batch_result)

    return results
