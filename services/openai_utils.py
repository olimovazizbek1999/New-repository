import os
import json
import hashlib
from typing import List, Dict, Optional
from collections import OrderedDict
import asyncio
import httpx
import unicodedata
import re
import multiprocessing
from ftfy import fix_text
from openai import AsyncOpenAI
from services.cache_manager import get_cached_openai, cache_openai_result, get_prompt_hash
from services.url_utils import normalize_url
from services.opener_rules import validate_opener_rules

# JSON parsing retry configuration
JSON_PARSE_MAX_RETRIES = 2
JSON_PARSE_RETRY_DELAY = 0.5  # seconds

_client: Optional[AsyncOpenAI] = None
_http_client: Optional[httpx.AsyncClient] = None

# Adaptive concurrency management
class AdaptiveConcurrency:
    def __init__(self, initial_limit: int = 8, min_limit: int = 2, max_limit: int = 25,
                 scale_up_threshold: int = 10, scale_down_threshold: int = 3, scale_up_step: int = 2):
        self.current_limit = initial_limit
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.scale_up_step = scale_up_step
        self.success_count = 0
        self.error_count = 0
        self.last_adjustment = 0
        self.semaphore = asyncio.Semaphore(initial_limit)

    async def acquire(self):
        await self.semaphore.acquire()

    def release(self):
        self.semaphore.release()

    def record_success(self):
        self.success_count += 1
        # Scale up after configured consecutive successes
        if self.success_count >= self.scale_up_threshold and self.current_limit < self.max_limit:
            self._scale_up()

    def record_error(self, is_rate_limit: bool = False):
        self.error_count += 1
        # Immediate scale down on rate limits, gradual on other errors
        if is_rate_limit or self.error_count >= self.scale_down_threshold:
            self._scale_down(aggressive=is_rate_limit)

    def _scale_up(self):
        old_limit = self.current_limit
        self.current_limit = min(self.current_limit + self.scale_up_step, self.max_limit)
        if self.current_limit > old_limit:
            # Add permits to semaphore
            for _ in range(self.current_limit - old_limit):
                self.semaphore.release()
            print(f"🚀 Scaled UP concurrency: {old_limit} → {self.current_limit}")
        self.success_count = 0

    def _scale_down(self, aggressive: bool = False):
        old_limit = self.current_limit
        reduction = 4 if aggressive else 2
        self.current_limit = max(self.current_limit - reduction, self.min_limit)
        if self.current_limit < old_limit:
            # Rebuild semaphore with new limit to avoid orphaned holder tasks
            # This is cleaner than managing holder tasks and prevents permit accumulation
            self._rebuild_semaphore()
            print(f"🔻 Scaled DOWN concurrency: {old_limit} → {self.current_limit} {'(RATE LIMIT)' if aggressive else ''}")
        self.error_count = 0

    def _rebuild_semaphore(self):
        """
        Rebuild semaphore with current limit to prevent orphaned holder tasks.

        This approach avoids the permit accumulation problem by creating a fresh
        semaphore instead of trying to manage holder tasks that never release.
        """
        # Get current number of available permits (approximate)
        old_semaphore = self.semaphore

        # Create new semaphore with the current limit
        # Note: Any currently waiting tasks will need to re-acquire from new semaphore
        self.semaphore = asyncio.Semaphore(self.current_limit)

        # Log the rebuild for diagnostic purposes
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(
            "Rebuilt semaphore for concurrency scaling",
            extra={
                "event": "semaphore_rebuilt",
                "new_limit": self.current_limit,
                "component": "adaptive_concurrency"
            }
        )

# Simple LRU cache implementation
class LRUCache:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache = OrderedDict()

    def get(self, key):
        if key in self.cache:
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def set(self, key, value):
        if key in self.cache:
            # Update existing key
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.max_size:
            # Remove least recently used item
            self.cache.popitem(last=False)
        self.cache[key] = value

# Handle both specific concurrency settings and general OPENAI_CONCURRENCY
def _get_concurrency_config():
    # Check if OPENAI_CONCURRENCY is set (for backward compatibility)
    general_concurrency = os.getenv("OPENAI_CONCURRENCY")

    if general_concurrency:
        # Map OPENAI_CONCURRENCY to specific settings
        max_concurrency = int(general_concurrency)
        initial_concurrency = min(8, max_concurrency)  # Start conservative
        min_concurrency = max(2, max_concurrency // 10)  # 10% of max, but at least 2

        print(f"Using OPENAI_CONCURRENCY={general_concurrency} -> initial={initial_concurrency}, min={min_concurrency}, max={max_concurrency}")

        return {
            "initial_limit": initial_concurrency,
            "min_limit": min_concurrency,
            "max_limit": max_concurrency,
            "scale_up_threshold": int(os.getenv("OPENAI_SCALE_UP_THRESHOLD", "10")),
            "scale_down_threshold": int(os.getenv("OPENAI_SCALE_DOWN_THRESHOLD", "3")),
            "scale_up_step": int(os.getenv("OPENAI_SCALE_UP_STEP", "2"))
        }
    else:
        # Use specific settings if OPENAI_CONCURRENCY is not set
        config = {
            "initial_limit": int(os.getenv("OPENAI_INITIAL_CONCURRENCY", "8")),
            "min_limit": int(os.getenv("OPENAI_MIN_CONCURRENCY", "2")),
            "max_limit": int(os.getenv("OPENAI_MAX_CONCURRENCY", "25")),
            "scale_up_threshold": int(os.getenv("OPENAI_SCALE_UP_THRESHOLD", "10")),
            "scale_down_threshold": int(os.getenv("OPENAI_SCALE_DOWN_THRESHOLD", "3")),
            "scale_up_step": int(os.getenv("OPENAI_SCALE_UP_STEP", "2"))
        }
        print(f"Using specific concurrency settings -> initial={config['initial_limit']}, min={config['min_limit']}, max={config['max_limit']}")
        return config

_concurrency_config = _get_concurrency_config()
_adaptive_concurrency = AdaptiveConcurrency(**_concurrency_config)
_cpu_count = multiprocessing.cpu_count()

_clean_cache = LRUCache(max_size=int(os.getenv("OPENAI_CACHE_SIZE", "2000")))  # Bounded LRU cache

# Track failures per job instead of global
_clean_failure_counts: Dict[str, int] = {}
_opener_failure_counts: Dict[str, int] = {}
_FAILURE_LIMIT = 50  # More reasonable limit - don't give up too easily

# Optimized batch processing for maximum throughput
BATCH_SIZE = 15  # Larger batches for better API efficiency

# Optimized token limits for speed and cost
MAX_INPUT_TEXT = 400  # Reduced for faster processing
MAX_OUTPUT_TOKENS = 150  # Strict output limits

UMLAUT_MAP = {
    "Ã¤": "ae", "Ã¶": "oe", "Ã¼": "ue",
    "Ã„": "Ae", "Ã–": "Oe", "Ãœ": "Ue",
    "ÃŸ": "ss",
}

LEGAL_SUFFIXES = [
    "gmbh", "ag", "kg", "ug", "mbh",
    "inc", "llc", "ltd", "corp", "co", "sarl", "oy", "ab"
]


def normalize_umlauts(text: str) -> str:
    for k, v in UMLAUT_MAP.items():
        text = text.replace(k, v)
    return text


def clean_company_name(name: str) -> str:
    """Post-clean company names for consistency."""
    if not name:
        return "Unknown"

    name = normalize_umlauts(name)
    name = unicodedata.normalize("NFKC", str(name))
    name = fix_text(name)
    name = re.sub(r"\s+", " ", name).strip()

    # keep legal suffixes (aligned with AI prompt instructions)
    # No suffix removal - legal suffixes are important for business identification

    # deduplicate consecutive words
    deduped = []
    for word in name.split():
        if not deduped or deduped[-1].lower() != word.lower():
            deduped.append(word)
    name = " ".join(deduped)

    # title case (except all caps acronyms)
    tokens = []
    for w in name.split():
        if len(w) <= 3 and w.isupper():
            tokens.append(w)  # keep acronyms
        else:
            tokens.append(w.capitalize())
    return " ".join(tokens)


def assess_content_quality(text: str) -> tuple[str, bool]:
    """
    Assess if website content is suitable for opener generation.
    Returns (quality_level, is_usable)
    """
    if not text or len(text.strip()) < 10:
        return "empty", False

    text_lower = text.lower().strip()
    word_count = len(text.split())

    # Check for completely unusable content
    unusable_patterns = [
        "this domain is for sale",
        "under construction",
        "coming soon",
        "page not found",
        "404 error",
        "access denied"
    ]

    if any(pattern in text_lower for pattern in unusable_patterns):
        return "unusable", False

    # Check for very generic content
    generic_patterns = [
        "welcome to our website",
        "contact us for more information",
        "home page",
        "click here"
    ]

    generic_count = sum(1 for pattern in generic_patterns if pattern in text_lower)

    # Assess quality levels - more lenient thresholds
    if word_count < 3:
        return "too_short", False
    elif word_count < 8 or generic_count >= 3:
        return "sparse", False  # Only reject very poor content
    elif word_count < 20 or generic_count >= 2:
        return "minimal", True  # Usable but limited
    else:
        return "good", True


def repair_opener(opener: str) -> tuple[str, str]:
    """
    Attempt to repair an opener that fails validation rules.

    Applies the following repairs:
    1. Truncate to exactly 2 sentences if too many
    2. Fix UK spelling (organization → organisation, etc.)
    3. Inject 'your' if missing direct address
    4. Trim sentences to 10-18 words if too long

    Args:
        opener: The failed opener text

    Returns:
        Tuple of (repaired_opener, repair_notes)
        - If successfully repaired: (repaired_opener, description_of_repairs)
        - If unrepairable: ("", reason_cannot_repair)
    """
    if not opener or not opener.strip():
        return "", "empty opener cannot be repaired"

    original_opener = opener
    repairs_applied = []

    # Step 1: UK spelling corrections (fix ALL occurrences)
    uk_spelling_map = {
        r'\borganization\b': 'organisation',
        r'\borganizations\b': 'organisations',
        r'\boptimize\b': 'optimise',
        r'\boptimized\b': 'optimised',
        r'\boptimizing\b': 'optimising',
        r'\bcolor\b': 'colour',
        r'\bcolors\b': 'colours',
        r'\banalyze\b': 'analyse',
        r'\banalyzed\b': 'analysed',
        r'\banalyzing\b': 'analysing',
        r'\brealize\b': 'realise',
        r'\brealized\b': 'realised',
        r'\brealizing\b': 'realising',
        r'\bspecialize\b': 'specialise',
        r'\bspecialized\b': 'specialised',
        r'\bspecializing\b': 'specialising',
    }

    spelling_fixed = False
    for us_pattern, uk_word in uk_spelling_map.items():
        new_opener = re.sub(us_pattern, uk_word, opener, flags=re.IGNORECASE)
        if new_opener != opener:
            opener = new_opener
            spelling_fixed = True

    if spelling_fixed:
        repairs_applied.append("fixed UK spelling")

    # Step 2: Truncate to exactly 2 sentences
    sentences = [s.strip() for s in opener.replace('!', '.').split('.') if s.strip()]

    if len(sentences) > 2:
        opener = '. '.join(sentences[:2]) + '.'
        repairs_applied.append(f"truncated from {len(sentences)} to 2 sentences")
        sentences = sentences[:2]
    elif len(sentences) < 2:
        # Cannot repair - need at least 2 sentences
        return "", f"only {len(sentences)} sentence(s), cannot create 2 sentences"

    # Step 3: Trim sentences to 10-18 words if too long/short
    trimmed_sentences = []
    for i, sentence in enumerate(sentences):
        words = sentence.split()
        if len(words) > 18:
            # Trim to 18 words
            trimmed = ' '.join(words[:18])
            trimmed_sentences.append(trimmed)
            repairs_applied.append(f"trimmed sentence {i+1} from {len(words)} to 18 words")
        elif len(words) < 10:
            # Try to pad with context, but if impossible, just keep as-is
            # The final validation will reject if still invalid
            trimmed_sentences.append(sentence)
        else:
            trimmed_sentences.append(sentence)

    opener = '. '.join(trimmed_sentences) + '.'

    # Step 4: Inject 'your' if missing direct address
    opener_lower = opener.lower()
    has_you = any(word in opener_lower for word in ['you ', 'your ', ' you', ' your', 'you.', 'your.'])

    if not has_you:
        # Try to inject 'your' before common nouns
        injection_targets = [
            (r'\b(company|business|organisation|organization|service|product|team|approach)\b', r'your \1'),
            (r'\b(focus|expertise|work|offering)\b', r'your \1'),
        ]

        injected = False
        for pattern, replacement in injection_targets:
            new_opener = re.sub(pattern, replacement, opener, count=1, flags=re.IGNORECASE)
            if new_opener != opener:
                opener = new_opener
                repairs_applied.append("injected 'your' for direct address")
                injected = True
                break

        if not injected:
            # Try prepending "I see your " to first sentence
            sentences = [s.strip() for s in opener.replace('!', '.').split('.') if s.strip()]
            if sentences:
                first_sentence = sentences[0]
                # Only prepend if it won't make sentence too long
                if len(first_sentence.split()) + 3 <= 18:
                    sentences[0] = f"I see your {first_sentence.lower()}"
                    opener = '. '.join(sentences) + '.'
                    repairs_applied.append("prepended 'I see your' for direct address")
                else:
                    return "", "cannot inject 'your' without exceeding word limit"

    # Step 5: Remove greetings and questions
    greeting_patterns = [
        (r'\b(hello|hi|dear|greetings)\b[,\s]+', ''),
        (r'\?', '.'),
    ]

    for pattern, replacement in greeting_patterns:
        new_opener = re.sub(pattern, replacement, opener, flags=re.IGNORECASE)
        if new_opener != opener:
            opener = new_opener
            repairs_applied.append("removed greeting/question")

    # Final validation
    validated, error = validate_opener_rules(opener, "", seen=None)

    if validated:
        repair_summary = "; ".join(repairs_applied) if repairs_applied else "no repairs needed"
        return validated, repair_summary
    else:
        # Still failing validation after repairs
        return "", f"repairs failed: {error} (applied: {', '.join(repairs_applied) if repairs_applied else 'none'})"




def preclean_text(text: str) -> str:
    """Enhanced pre-clean input text for consistency."""
    if not text:
        return ""

    text = unicodedata.normalize("NFKC", str(text))
    text = fix_text(text)
    text = normalize_umlauts(text)

    # Remove common junk patterns - be more specific to avoid removing valid names
    text = re.sub(r"\b[A-C]\.\s+", "", text)  # Only remove "A. " "B. " "C. " with dots and spaces
    text = re.sub(r"[^\w\s\-&.,äöüÄÖÜß]", " ", text)  # Keep umlauts
    text = re.sub(r"\d{4,}", " ", text)  # Remove long number sequences
    text = re.sub(r"[_]{2,}", " ", text)  # Remove multiple underscores
    text = re.sub(r"\.{2,}", ".", text)  # Fix multiple dots

    # Clean up whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Remove very short or very long segments
    words = text.split()
    filtered_words = [w for w in words if 1 < len(w) < 50]

    return " ".join(filtered_words)


def get_client() -> AsyncOpenAI:
    global _client, _http_client
    if _client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY environment variable is not set")
        # Optimized timeout for speed - fail fast and retry
        _http_client = httpx.AsyncClient(timeout=10.0, follow_redirects=True)
        _client = AsyncOpenAI(api_key=api_key, http_client=_http_client)
    return _client


async def close_openai_client():
    """Close the global OpenAI HTTP client to prevent resource leaks."""
    global _client, _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
        _client = None  # Reset client too since it depends on the HTTP client


def clear_failure_counts():
    """Clear failure count dictionaries to avoid memory growth after job finalization."""
    global _clean_failure_counts, _opener_failure_counts
    _clean_failure_counts.clear()
    _opener_failure_counts.clear()


def _extract_choice_content(resp) -> Optional[str]:
    try:
        choice = resp.choices[0]
        if hasattr(choice, "message") and getattr(choice.message, "content", None):
            content = choice.message.content
            # Ensure we have valid content
            if content and isinstance(content, str):
                return content.strip()
        if hasattr(choice, "text"):
            text = choice.text
            if text and isinstance(text, str):
                return text.strip()
        return None
    except Exception as e:
        print(f"⚠️ Failed to extract choice content: {e}")
        print(f"⚠️ Response type: {type(resp)}")
        if hasattr(resp, 'choices') and len(resp.choices) > 0:
            print(f"⚠️ Choice type: {type(resp.choices[0])}")
        return None


def _validate_response_schema(data: dict, expected_schema: str) -> bool:
    """
    Validate OpenAI response against expected schema structure.

    Args:
        data: Parsed JSON response data
        expected_schema: Either 'cleaning_result' or 'opener_result'
    """
    try:
        if not isinstance(data, dict):
            return False

        # Both schemas require a 'rows' array
        if "rows" not in data:
            return False

        rows = data["rows"]
        if not isinstance(rows, list):
            return False

        # Validate each row based on schema type
        for row in rows:
            if not isinstance(row, dict):
                return False

            if expected_schema == "cleaning_result":
                # Cleaning schema requires first, last, company fields
                required_fields = ["first", "last", "company"]
                if not all(field in row for field in required_fields):
                    return False

            elif expected_schema == "opener_result":
                # Opener schema requires opener field
                if "opener" not in row:
                    return False

        return True

    except Exception:
        return False


def _extract_json_from_content(content: str) -> str:
    """
    Extract JSON from content using brace counting for more accurate extraction.

    Handles cases where OpenAI wraps JSON in code fences or adds prose.
    """
    # First, try to find JSON in code fences
    code_fence_patterns = [
        r'```json\s*(\{.*?\})\s*```',
        r'```\s*(\{.*?\})\s*```',
        r'`(\{.*?\})`'
    ]

    for pattern in code_fence_patterns:
        match = re.search(pattern, content, re.DOTALL)
        if match:
            return match.group(1)

    # If no code fences, use brace counting to find complete JSON
    start_idx = content.find('{')
    if start_idx == -1:
        raise ValueError("No opening brace found in content")

    brace_count = 0
    in_string = False
    escaped = False

    for i in range(start_idx, len(content)):
        char = content[i]

        if escaped:
            escaped = False
            continue

        if char == '\\' and in_string:
            escaped = True
            continue

        if char == '"' and not escaped:
            in_string = not in_string
            continue

        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    # Found complete JSON
                    return content[start_idx:i+1]

    # If we get here, braces weren't balanced - fall back to greedy match
    json_match = re.search(r'\{.*\}', content, re.DOTALL)
    if json_match:
        return json_match.group(0)

    raise ValueError("No valid JSON structure found in content")


async def _parse_openai_response_with_retry(content: str, expected_schema: str, attempt_context: str = "") -> dict:
    """
    Robustly parse OpenAI response with JSON extraction, schema validation, and retry logic.

    Args:
        content: Raw response content from OpenAI
        expected_schema: 'cleaning_result' or 'opener_result'
        attempt_context: Context string for logging (e.g., "clean_rows batch 1")

    Returns:
        Parsed and validated JSON data

    Raises:
        ValueError: If parsing fails after all retries
    """
    last_error = None

    for attempt in range(JSON_PARSE_MAX_RETRIES + 1):
        try:
            # Try to parse as-is first
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                # Extract JSON from prose using improved extraction
                extracted_json = _extract_json_from_content(content)
                parsed = json.loads(extracted_json)

            # Validate against expected schema
            if _validate_response_schema(parsed, expected_schema):
                if attempt > 0:
                    print(f"✅ JSON parsing succeeded on retry {attempt} for {attempt_context}")
                return parsed
            else:
                raise ValueError(f"Response does not match expected {expected_schema} schema")

        except (json.JSONDecodeError, ValueError) as e:
            last_error = e
            if attempt < JSON_PARSE_MAX_RETRIES:
                print(f"⚠️ JSON parse attempt {attempt + 1} failed for {attempt_context}: {e}")
                print(f"⚠️ Content preview: {content[:200]}...")
                print(f"🔄 Retrying in {JSON_PARSE_RETRY_DELAY}s...")
                await asyncio.sleep(JSON_PARSE_RETRY_DELAY)
                continue
            else:
                # Final attempt failed
                print(f"❌ All JSON parsing attempts failed for {attempt_context}")
                print(f"❌ Final error: {e}")
                print(f"❌ Content: {content[:300]}...")
                raise ValueError(f"Failed to parse JSON after {JSON_PARSE_MAX_RETRIES + 1} attempts: {e}")

    # Should never reach here
    raise ValueError(f"Unexpected error in JSON parsing for {attempt_context}: {last_error}")


# ---------------- CLEAN ROWS (Company + Names) ---------------- #

async def clean_rows(rows: List[Dict], job_id: Optional[str] = None) -> List[Dict]:
    """Clean rows with persistent caching and optimized batch processing."""
    job_id = job_id or "default"
    _clean_failure_counts.setdefault(job_id, 0)

    if _clean_failure_counts[job_id] > _FAILURE_LIMIT:
        print(f"Skipping clean_rows for job {job_id} due to circuit breaker")
        return [
            {
                "first": preclean_text(r.get("first", "")),
                "last": preclean_text(r.get("last", "")),
                "company": clean_company_name(preclean_text(r.get("company", ""))),
            }
            for r in rows
        ]

    client = get_client()
    results: List[Dict] = [{}] * len(rows)

    # Check both memory and persistent cache
    uncached, uncached_indices = [], []
    for i, r in enumerate(rows):
        # Create consistent key for caching
        row_data = {
            "first": preclean_text(r.get("first", "")),
            "last": preclean_text(r.get("last", "")),
            "company": preclean_text(r.get("company", "")),
            "text": preclean_text(r.get("text", ""))[:MAX_INPUT_TEXT],
        }

        key = f"{row_data['first']}|{row_data['last']}|{row_data['company']}|{row_data['text']}"

        # Check memory cache first
        cached_result = _clean_cache.get(key)
        if cached_result is not None:
            results[i] = cached_result
            continue

        # Row-level cache lookups disabled to prevent GCS object explosion
        # Only batch-level caching is used now

        uncached.append(row_data)
        uncached_indices.append(i)

    async def _clean_batch(batch, indices):
        # Create a hash for this specific batch for caching
        batch_str = json.dumps(batch, sort_keys=True)
        prompt_hash = get_prompt_hash("clean_rows_batch_v2", batch_str)

        # Check if this batch was already processed
        cached_result = await get_cached_openai(prompt_hash)
        if cached_result:
            return cached_result

        prompt = (
            "You are a data cleaner and company extractor. Clean and standardize ALL data thoroughly.\n\n"
            "**NAME CLEANING RULES:**\n"
            "- Remove ALL non-alphabetic characters (except hyphens in compound names)\n"
            "- Fix common abbreviations: J. → John, M. → Michael, etc.\n"
            "- Proper capitalization: first letter uppercase, rest lowercase\n"
            "- Remove titles: Dr., Mr., Mrs., CEO, etc.\n"
            "- Convert umlauts: ü→ue, ö→oe, ä→ae, ß→ss\n"
            "- If name has numbers or symbols, clean completely or set to empty\n"
            "- Examples: 'j.smith123' → 'John Smith', 'DR.MUELLER' → 'Mueller'\n\n"
            "**COMPANY NAME RULES:**\n"
            "- Use the most professional candidate from 'company' field\n"
            "- If candidates are weak/generic, analyze 'text' field for company mentions\n"
            "- Remove website domains (.com, .de, etc.) unless part of official name\n"
            "- Keep legal suffixes: GmbH, AG, Inc, LLC, Ltd, Corp\n"
            "- Proper capitalization and remove extra spaces\n"
            "- If no good company found, return 'Unknown'\n\n"
            "**QUALITY STANDARDS:**\n"
            "- Names must be 2-50 characters, alphabetic only\n"
            "- Companies must look professional and real\n"
            "- Clean thoroughly - no partial cleaning\n\n"
            "Return strictly as JSON: {\"rows\": [{\"first\":...,\"last\":...,\"company\":...}]}\n\n"
            f"Rows: {json.dumps(batch, ensure_ascii=False)}"
        )

        await _adaptive_concurrency.acquire()
        try:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "cleaning_result",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "rows": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "first": {"type": "string", "maxLength": 50},
                                            "last": {"type": "string", "maxLength": 50},
                                            "company": {"type": "string", "maxLength": 100}
                                        },
                                        "required": ["first", "last", "company"],
                                        "additionalProperties": False
                                    }
                                }
                            },
                            "required": ["rows"],
                            "additionalProperties": False
                        }
                    }
                },
                temperature=0.7,  # Higher temperature for faster processing
                max_tokens=MAX_OUTPUT_TOKENS,  # Strict output limit
            )
            content = _extract_choice_content(resp)
            if not content:
                raise ValueError("No content received from OpenAI")

            # Use robust JSON parsing with retry logic and schema validation
            parsed = await _parse_openai_response_with_retry(
                content,
                "cleaning_result",
                f"clean_rows batch {len(batch)} rows"
            )
            cleaned = parsed.get("rows", batch)

            # Record success for adaptive scaling
            _adaptive_concurrency.record_success()

            # Cache the successful result (batch-level only)
            # Note: Row-level caching disabled to prevent GCS object explosion
            await cache_openai_result(prompt_hash, cleaned)
        except Exception as e:
            # Check if it's a rate limit error
            is_rate_limit = "429" in str(e) or "rate_limit" in str(e).lower()
            _adaptive_concurrency.record_error(is_rate_limit)

            _clean_failure_counts[job_id] += 1
            print(f"⚠️ clean_rows batch failed for job {job_id}: {e}")
            # Fallback dictionaries
            cleaned = [
                {
                    "first": preclean_text(row.get("first", "")),
                    "last": preclean_text(row.get("last", "")),
                    "company": clean_company_name(preclean_text(row.get("company", ""))),
                }
                for row in batch
            ]
        finally:
            _adaptive_concurrency.release()

        # Ensure we always populate results with proper dictionaries
        for i, idx in enumerate(indices):
            if i < len(cleaned) and cleaned[i] is not None:
                row = cleaned[i]
                # Post-clean company name
                row["company"] = clean_company_name(row.get("company", ""))
                # Use original input data for cache key to match lookup
                original_row = batch[i] if i < len(batch) else {}
                key = f"{preclean_text(original_row.get('first',''))}|{preclean_text(original_row.get('last',''))}|{preclean_text(original_row.get('company',''))}|{preclean_text(original_row.get('text',''))[:MAX_INPUT_TEXT]}"
                _clean_cache.set(key, row)
                results[idx] = row
            else:
                # Fallback for missing/None results
                original_row = batch[i] if i < len(batch) else {}
                fallback_row = {
                    "first": preclean_text(original_row.get("first", "")),
                    "last": preclean_text(original_row.get("last", "")),
                    "company": clean_company_name(preclean_text(original_row.get("company", ""))),
                }
                results[idx] = fallback_row

    # Initialize tasks list before conditional
    tasks = []

    # Use optimized batch size for better throughput/cost balance
    if uncached:
        for i in range(0, len(uncached), BATCH_SIZE):
            batch = uncached[i:i + BATCH_SIZE]
            indices = uncached_indices[i:i + BATCH_SIZE]
            tasks.append(_clean_batch(batch, indices))

    if tasks:
        await asyncio.gather(*tasks)

    # Final safety check - ensure no empty values remain
    for i, result in enumerate(results):
        if not result or result is None:
            original_row = rows[i] if i < len(rows) else {}
            results[i] = {
                "first": preclean_text(original_row.get("first", "")),
                "last": preclean_text(original_row.get("last", "")),
                "company": clean_company_name(preclean_text(original_row.get("company", ""))),
            }

    return results


# ---------------- GENERATE OPENERS ---------------- #

async def generate_openers(rows: List[Dict], job_id: Optional[str] = None) -> List[Dict]:
    """Generate email openers with persistent caching and optimized batch processing."""
    job_id = job_id or "default"
    _opener_failure_counts.setdefault(job_id, 0)

    if _opener_failure_counts[job_id] > _FAILURE_LIMIT:
        print(f"CIRCUIT BREAKER: Skipping generate_openers for job {job_id} ({_opener_failure_counts[job_id]}/{_FAILURE_LIMIT} failures)")
        return [{"opener": "", "error": "openai api unavailable (too many failures)"} for _ in rows]

    client = get_client()
    results: List[Dict] = [{}] * len(rows)

    # Check cache and filter by content quality
    uncached, uncached_indices = [], []
    for i, r in enumerate(rows):
        row_data = {
            "company": preclean_text(r.get("company", "")),
            "website": normalize_url(r.get("website", "")),
            "text": preclean_text(r.get("text", ""))[:MAX_INPUT_TEXT],
        }

        # Assess content quality BEFORE sending to AI
        quality_level, is_usable = assess_content_quality(row_data["text"])

        if not is_usable:
            # Don't send poor content to AI - save API calls and avoid failures
            results[i] = {
                "opener": "",
                "error": f"content quality too low ({quality_level})"
            }
            continue

        # Row-level cache lookups disabled to prevent GCS object explosion
        # Only batch-level caching is used to minimize storage overhead

        uncached.append(row_data)
        uncached_indices.append(i)

    async def _gen_batch(batch, indices):
        # Check if this batch was already processed
        batch_str = json.dumps(batch, sort_keys=True)
        prompt_hash = get_prompt_hash("generate_opener_batch_v2", batch_str)

        cached_result = await get_cached_openai(prompt_hash)
        if cached_result:
            return cached_result

        # batch is already cleaned from above
        cleaned_batch = batch
        # UK spelling replacements map
        uk_spelling_map = {
            "organization": "organisation",
            "organizations": "organisations",
            "optimize": "optimise",
            "optimized": "optimised",
            "optimizing": "optimising",
            "color": "colour",
            "colors": "colours",
            "analyze": "analyse",
            "analyzed": "analysed",
            "analyzing": "analysing",
            "realize": "realise",
            "realized": "realised",
            "realizing": "realising",
            "specialize": "specialise",
            "specialized": "specialised",
            "specializing": "specialising",
        }

        system_prompt = (
            "Generate professional 2-sentence email openers based on website content.\n\n"
            "**REQUIREMENTS:**\n"
            "- Exactly 2 sentences, 10-18 words each\n"
            "- Use 'you/your' to address website owner\n"
            "- Reference specific content from website text\n"
            "- Professional tone, no greetings or sales pitches\n"
            "- Use UK spelling (organisation, optimise, colour, analyse, realise, specialise, etc.)\n\n"
            "**CONTENT STRATEGY:**\n"
            "- Extract key business focus, services, or industry from text\n"
            "- Reference specific offerings, expertise, or approach mentioned\n"
            "- If content mentions consulting → reference consulting expertise\n"
            "- If content mentions products → reference product offerings  \n"
            "- If content mentions services → reference service capabilities\n"
            "- Always create a relevant opener - don't leave empty\n\n"
            "**EXAMPLES:**\n"
            "Text: 'Professional consulting services for businesses' → 'I see you provide professional consulting services for businesses. Your expertise could be valuable for companies seeking strategic guidance.'\n"
            "Text: 'Software solutions and automation tools' → 'I noticed your focus on software solutions and automation tools. Your technology approach could help businesses streamline their operations.'\n\n"
            "Generate openers for all provided text. Only return empty opener with error if the text is completely invalid.\n\n"
            "Return as JSON: {\"rows\":[{\"opener\":\"...\",\"error\":\"...\"},...]}"
        )

        await _adaptive_concurrency.acquire()
        try:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Rows: {json.dumps(cleaned_batch, ensure_ascii=False)}"},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "opener_result",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "rows": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "opener": {"type": "string", "maxLength": 300},
                                            "error": {"type": "string", "maxLength": 100}
                                        },
                                        "required": ["opener", "error"],
                                        "additionalProperties": False
                                    }
                                }
                            },
                            "required": ["rows"],
                            "additionalProperties": False
                        }
                    }
                },
                temperature=0.3,  # Lower temperature for more consistent rule-following
                max_tokens=200,  # Optimized for opener generation
            )
            content = _extract_choice_content(resp)
            if not content:
                raise ValueError("No content received from OpenAI")

            # Use robust JSON parsing with retry logic and schema validation
            parsed = await _parse_openai_response_with_retry(
                content,
                "opener_result",
                f"generate_opener batch {len(cleaned_batch)} rows"
            )
            batch_results = parsed.get("rows", [])

            # Record success for adaptive scaling
            _adaptive_concurrency.record_success()

            # Cache the successful result (batch-level only)
            # Note: Row-level caching disabled to prevent GCS object explosion
            await cache_openai_result(prompt_hash, batch_results)

            # Map results to correct indices with repair attempt
            for i, (idx, result) in enumerate(zip(indices, batch_results)):
                if idx < len(results):
                    opener = result.get("opener", "")
                    error = result.get("error", "")

                    # If opener exists but might fail validation, attempt repair
                    if opener and not error:
                        validated, validation_error = validate_opener_rules(opener, "", seen=None)
                        if not validated:
                            # Validation failed - attempt repair
                            repaired, repair_note = repair_opener(opener)
                            if repaired:
                                # Repair succeeded
                                results[idx] = {"opener": repaired, "error": f"repaired: {repair_note}"}
                            else:
                                # Repair failed - use empty opener with error
                                results[idx] = {"opener": "", "error": f"validation failed: {validation_error}; {repair_note}"}
                        else:
                            # Validation passed - use as-is
                            results[idx] = {"opener": opener, "error": ""}
                    else:
                        # No opener or AI reported error
                        results[idx] = {"opener": opener, "error": error}

        except Exception as e:
            # Check if it's a rate limit error
            is_rate_limit = "429" in str(e) or "rate_limit" in str(e).lower()
            _adaptive_concurrency.record_error(is_rate_limit)

            _opener_failure_counts[job_id] += 1
            print(f"⚠️ opener batch failed for job {job_id}: {e}")
            print(f"Failure count for job {job_id}: {_opener_failure_counts[job_id]}/{_FAILURE_LIMIT}")
            # Set fallback results for failed batch
            for idx in indices:
                if idx < len(results):
                    results[idx] = {"opener": "", "error": f"openai failed: {e}"}
        finally:
            _adaptive_concurrency.release()

    # Process uncached items in optimized batches
    if uncached:
        tasks = []
        for i in range(0, len(uncached), BATCH_SIZE):
            batch = uncached[i:i + BATCH_SIZE]
            indices = uncached_indices[i:i + BATCH_SIZE]
            tasks.append(_gen_batch(batch, indices))
        await asyncio.gather(*tasks)

    # Final safety check for opener results
    for i, r in enumerate(results):
        if not r:
            results[i] = {"opener": "", "error": "missing"}

    return results