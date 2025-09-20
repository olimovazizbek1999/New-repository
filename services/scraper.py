import logging
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential


logger = logging.getLogger(__name__)


HEADERS = {
"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
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




def _text_or_none(tag) -> Optional[str]:
if not tag:
return None
text = tag.get_text(" ", strip=True)
return text[:200] if text else None




def extract_company_name(html: str) -> Optional[str]:
soup = BeautifulSoup(html, "lxml") if html else None
if not soup:
return None
# Try Open Graph
og = soup.find("meta", property="og:site_name")
if og and og.get("content"):
return og["content"].strip()
# <title>
if soup.title and soup.title.string:
title = soup.title.string.strip()
# Remove common separators
title = re.split(r"[\-|•–—⋅·]", title)[0].strip()
return title
# First H1
h1 = soup.find("h1")
if h1:
t = _text_or_none(h1)
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