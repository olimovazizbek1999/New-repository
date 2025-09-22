import requests
from bs4 import BeautifulSoup


def extract_company_name(url: str) -> str:
    """
    Fetch a company's homepage and extract its name.

    Priority:
      1. og:site_name meta tag
      2. <title>
      3. First <h1>
    Returns None if nothing found or request fails.
    """
    if not url:
        return None

    try:
        # Ensure URL has scheme
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url

        resp = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LeadProcessor/1.0)"}
        )
        resp.raise_for_status()

        # ✅ FIX: parse actual HTML, not the URL string
        soup = BeautifulSoup(resp.text, "lxml")

        # 1. Prefer og:site_name
        meta = soup.find("meta", property="og:site_name")
        if meta and meta.get("content"):
            return meta["content"].strip()

        # 2. Then <title>
        if soup.title and soup.title.string:
            return soup.title.string.strip()

        # 3. Then first <h1>
        h1 = soup.find("h1")
        if h1 and h1.get_text():
            return h1.get_text().strip()

    except Exception as e:
        print(f"⚠️ Scraper error for {url}: {e}")
        return None

    return None
