import requests
from bs4 import BeautifulSoup

def extract_company_name(url):
    try:
        resp = requests.get(url, timeout=5)
        soup = BeautifulSoup(resp.content, "lxml")
        # extract og:site_name, <title>, <h1>
        name = soup.find("meta", property="og:site_name")
        if name and name.get("content"):
            return name["content"]
        title = soup.find("title")
        if title:
            return title.get_text()
        h1 = soup.find("h1")
        if h1:
            return h1.get_text()
    except Exception:
        return None
