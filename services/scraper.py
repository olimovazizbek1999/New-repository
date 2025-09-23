import httpx
from bs4 import BeautifulSoup
import re

async def extract_company_info(url: str) -> dict:
    """
    Async scraper: fetch a company's homepage and return clean text + normalized name.
    - Uses httpx.AsyncClient (non-blocking)
    - Forces UTF-8 decoding
    - Removes scripts/styles/noscript
    - Cleans weird encodings
    """
    try:
        if not url.startswith("http"):
            url = "http://" + url

        async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            resp = await client.get(url)
            resp.encoding = "utf-8"
            html = resp.text

        soup = BeautifulSoup(html, "lxml")

        # Remove unwanted tags
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text)

        # Normalize common broken UTF-8 sequences
        replacements = {
            "âˆšÂ§": "ß",
            "âˆšÂº": "ü",
            "âˆšÂ©": "é",
            "âˆšÂ¶": "ö",
            "Ã¼": "ü",
            "Ã¶": "ö",
            "Ã¤": "ä",
            "ÃŸ": "ß",
            "Ã©": "é",
        }
        for bad, good in replacements.items():
            text = text.replace(bad, good)

        # Guess company name
        name = None
        if soup.title and soup.title.string:
            name = soup.title.string.strip()
        elif soup.find("h1"):
            name = soup.find("h1").get_text(strip=True)

        return {"name": name, "text": text}

    except Exception as e:
        print(f"⚠️ scrape error {url}: {e}")
        return {"name": None, "text": ""}
