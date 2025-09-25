import httpx
from bs4 import BeautifulSoup
import re
import asyncio
import random
import json
from urllib.parse import urlparse
from tenacity import retry, stop_after_attempt, wait_random_exponential
from playwright.async_api import async_playwright

# ✅ Global + per-domain concurrency
_scrape_semaphore = asyncio.Semaphore(50)
_domain_locks: dict[str, asyncio.Semaphore] = {}

# Shared HTTP clients
_http_client: httpx.AsyncClient | None = None
_http_client_fallback: httpx.AsyncClient | None = None

# Cache results
_scrape_cache: dict[str, dict] = {}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


def _normalize_domain(url: str) -> str:
    if not url.startswith("http"):
        url = "http://" + url
    parsed = urlparse(url)
    return parsed.netloc.lower()


def get_http_client(timeout=10) -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
    return _http_client


def get_http_client_fallback(timeout=25) -> httpx.AsyncClient:
    global _http_client_fallback
    if _http_client_fallback is None:
        _http_client_fallback = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
    return _http_client_fallback


def _make_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
    }


@retry(stop=stop_after_attempt(2), wait=wait_random_exponential(multiplier=1, max=5))
async def _fetch_html(url: str, fallback: bool = False) -> str:
    if not url.startswith("http"):
        url = "http://" + url
    client = get_http_client_fallback() if fallback else get_http_client()
    resp = await client.get(url, headers=_make_headers())
    resp.encoding = "utf-8"
    return resp.text


async def _fetch_with_playwright(url: str) -> str:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = await context.new_page()
            await page.goto(url, timeout=30000)
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        print(f"⚠️ playwright fallback failed {url}: {e}")
        return ""


async def extract_company_info(url: str) -> dict:
    """Scrape homepage with caching, richer parsing, per-domain concurrency."""
    async with _scrape_semaphore:
        domain = _normalize_domain(url)

        if domain not in _domain_locks:
            _domain_locks[domain] = asyncio.Semaphore(2)
        lock = _domain_locks[domain]

        # ✅ Add per-domain lock safeguard
        try:
            async with asyncio.wait_for(lock.acquire(), timeout=15):
                if domain in _scrape_cache:
                    return _scrape_cache[domain]
        except asyncio.TimeoutError:
            print(f"⚠️ Lock timeout for domain {domain}")
            return {"name": domain, "text": ""}

        html = None
        try:
            html = await _fetch_html(url, fallback=False)
        except Exception as e:
            print(f"⚠️ primary scrape failed {url}: {e}")
            try:
                cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{url}"
                html = await _fetch_html(cache_url, fallback=True)
            except Exception as e2:
                print(f"⚠️ fallback scrape failed {url}: {e2}")
                try:
                    root = f"http://{domain}"
                    html = await _fetch_html(root, fallback=True)
                except Exception as e3:
                    print(f"⚠️ root fallback failed {url}: {e3}")
                    html = await _fetch_with_playwright(url)

        result = {"name": "", "text": ""}
        try:
            if html:
                soup = BeautifulSoup(html, "lxml")

                # Remove noise
                for tag in soup(["script", "style", "noscript"]):
                    tag.decompose()

                parts = []

                # Title + H1
                if soup.title and soup.title.string:
                    parts.append(soup.title.string.strip())
                if soup.find("h1"):
                    parts.append(soup.find("h1").get_text(strip=True))

                # Meta descriptions
                if soup.find("meta", attrs={"name": "description"}):
                    parts.append(soup.find("meta", attrs={"name": "description"}).get("content", "").strip())
                if soup.find("meta", attrs={"property": "og:description"}):
                    parts.append(soup.find("meta", attrs={"property": "og:description"}).get("content", "").strip())

                # First 2–3 paragraphs
                paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p", limit=3)]
                for p in paragraphs:
                    parts.append(p[:300])

                # Hero / banner
                hero = soup.find(["section", "div"], attrs={"class": re.compile(r"(hero|banner)", re.I)})
                if hero:
                    parts.append(hero.get_text(" ", strip=True)[:400])

                # Schema.org JSON-LD
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict) and data.get("@type") == "Organization":
                            if data.get("name"):
                                parts.append(data["name"])
                            if data.get("description"):
                                parts.append(data["description"])
                    except Exception:
                        continue

                text = " ".join(parts)
                text = re.sub(r"\s+", " ", text)

                # Fix encoding issues
                replacements = {
                    "âˆšÂ§": "ß", "âˆšÂº": "ü", "âˆšÂ©": "é", "âˆšÂ¶": "ö",
                    "Ã¼": "ü", "Ã¶": "ö", "Ã¤": "ä", "ÃŸ": "ß", "Ã©": "é",
                }
                for bad, good in replacements.items():
                    text = text.replace(bad, good)

                # Truncate long text (max ~800 chars)
                text = text[:800]

                name = soup.title.string.strip() if soup.title else domain
                result = {"name": name, "text": text or ""}
        except Exception as e:
            print(f"⚠️ parsing error {url}: {e}")

        _scrape_cache[domain] = result
        return result
