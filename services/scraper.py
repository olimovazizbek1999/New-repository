import logging
import re
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
}

def _normalize_url(url: str) -> str:
    if not url:
        return url
    if not re.match(r"^https?://", url, re.I):
        return "http://" + url.strip()
    return url.strip()

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5))
def fetch(url: str, timeout: float = 10.0) -> Optional[str]:
    url = _normalize_url(url)
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}")
    return resp.text

def extract_company_name(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml") if html else None
    if not soup:
        return None
    og = soup.find("meta", property="og:site_name")
    if og and og.get("content"):
        return og["content"].strip()
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        title = re.split(r"[-|•–—⋅·]", title)[0].strip()
        return title
    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t
    return None

def extract_visible_text(html: str, max_chars: int = 1800) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    text = soup.get_text(" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]
