import json
import logging
import os
import re
import time
from typing import Dict, List

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

MODEL_CLEAN = os.environ.get("OPENAI_MODEL_CLEAN", "gpt-3.5-turbo")
MODEL_OPENER = os.environ.get("OPENAI_MODEL_OPENER", "gpt-3.5-turbo")

from openai import OpenAI
_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

_LAST_TS = 0.0
def _respect_rate_limit(max_rps: float = 3.0):
    global _LAST_TS
    now = time.time()
    min_interval = 1.0 / max_rps
    if now - _LAST_TS < min_interval:
        time.sleep(min_interval - (now - _LAST_TS))
    _LAST_TS = time.time()

class OpenAITransientError(Exception):
    pass

@retry(reraise=True, wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(OpenAITransientError))
def _call_chat(model: str, messages: List[Dict], response_format: str = "json_object") -> str:
    _respect_rate_limit()
    try:
        resp = _client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.2,
            response_format={"type": response_format},
        )
        return resp.choices[0].message.content
    except Exception as e:
        logger.warning(f"OpenAI error: {e}")
        raise OpenAITransientError(str(e))

_SENTENCE_RE = re.compile(r"[^.!?]+[.!?]")

def _validate_opener(two_sent: str) -> bool:
    sentences = [s.strip() for s in _SENTENCE_RE.findall(two_sent or "")]
    if len(sentences) != 2:
        return False
    for s in sentences:
        wc = len(s.split())
        if not (10 <= wc <= 18):
            return False
    if "?" in two_sent:
        return False
    return True

def batch_clean_names(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    sys = "You sanitize person and company names. Title-case names. Remove stray symbols."
    user = {"rows": rows}
    messages = [{"role": "system", "content": sys}, {"role": "user", "content": json.dumps(user)}]
    content = _call_chat(MODEL_CLEAN, messages)
    try:
        return json.loads(content).get("rows", rows)
    except Exception:
        return rows

def batch_generate_openers(items: List[Dict[str, str]]) -> List[str]:
    sys = ("You write two-sentence email openers. Professional, calm, specific. "
           "Exactly 2 sentences, 10–18 words each. No hype, emojis, or questions.")
    payload = {"items": items}
    messages = [{"role": "system", "content": sys}, {"role": "user", "content": json.dumps(payload)}]
    content = _call_chat(MODEL_OPENER, messages)
    try:
        obj = json.loads(content)
        openers = obj.get("openers") or []
    except Exception:
        openers = []

    out = []
    for text in openers:
        if _validate_opener(text):
            out.append(text.strip())
        else:
            out.append("")
    return out
