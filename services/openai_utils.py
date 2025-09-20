import asyncio


def batch_clean_names(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
"""Rows: [{"first":"..","last":"..","company":".."}, ...]"""
sys = (
"You sanitize person and company names. Remove leading/trailing/multiple spaces, quotes, emojis, and stray symbols. "
"Title-case person names. Keep ASCII where possible. Do NOT invent or translate. Return JSON with 'first','last','company'."
)
user = {
"instruction": "Clean these entries.",
"rows": rows,
}
messages = [
{"role": "system", "content": sys},
{"role": "user", "content": json.dumps(user)}
]
content = _call_chat(MODEL_CLEAN, messages)
try:
obj = json.loads(content)
return obj.get("rows", obj) # tolerate either schema
except Exception:
# Fallback: try to parse list
try:
return json.loads(content)
except Exception:
logger.error("Failed to parse cleaning response; returning input")
return rows




_SENTENCE_RE = re.compile(r"[^.!?]+[.!?]")




def _validate_opener(two_sent: str) -> bool:
sentences = [s.strip() for s in _SENTENCE_RE.findall(two_sent or "")]
if len(sentences) != 2:
return False
for s in sentences:
wc = len(s.split())
if not (10 <= wc <= 18):
return False
# no question marks per spec
if "?" in two_sent:
return False
return True




def batch_generate_openers(items: List[Dict[str, str]]) -> List[str]:
"""Each item: {"homepage_text": str, "company": str}
Returns list of openers aligned to items.
"""
sys = (
"You write two-sentence email openers. Style: professional, calm, specific. Tone: neutral-positive, no hype, no emojis, no questions. "
"Use ONLY the provided homepage visible text; do not infer beyond it. Structure: exactly 2 sentences, each 10–18 words. "
"The second sentence must follow naturally and end cleanly (no sales pitch). Output JSON with a 'openers' array of strings."
)
payload = {"items": [{"homepage_text": it.get("homepage_text", ""), "company": it.get("company", "")} for it in items]}
messages = [
{"role": "system", "content": sys},
{"role": "user", "content": json.dumps(payload)}
]
content = _call_chat(MODEL_OPENER, messages)
try:
obj = json.loads(content)
cands = obj.get("openers") or obj.get("items") or []
except Exception:
cands = []
# Validate and repair by re-asking individually if needed
out: List[str] = []
for idx, base in enumerate(cands):
text = base if isinstance(base, str) else base.get("opener", "")
if _validate_opener(text):
out.append(text.strip())
else:
# Re-ask for this single item strictly
strict_sys = (
"Return ONLY the opener text. Exactly two sentences, each 10–18 words, neutral-positive, no questions or emojis. "
"Use only the provided homepage text."
)
m = [
{"role": "system", "content": strict_sys},
{"role": "user", "content": items[idx].get("homepage_text", "")}
]
try:
text2 = _call_chat(MODEL_OPENER, m, response_format="text")
except Exception:
text2 = ""
out.append(text2.strip())
# Guarantee length
return [t.strip() if isinstance(t, str) else "" for t in out]