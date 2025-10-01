import httpx
from bs4 import BeautifulSoup
import re
import asyncio
import random
import os
import time
import logging
import socket
import ipaddress
from collections import OrderedDict
from urllib.parse import urlparse, urljoin
from typing import Optional
from services.url_utils import normalize_url
from urllib.robotparser import RobotFileParser
from tenacity import retry, stop_after_attempt, wait_exponential
from services.cache_manager import get_cached_scrape, cache_scrape_result
import threading

# Configure logger with environment-controlled verbosity
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Default to INFO level

# Allow debug mode via environment variable
DEBUG_SCRAPER = os.getenv("DEBUG_SCRAPER", "false").lower() == "true"
if DEBUG_SCRAPER:
    logger.setLevel(logging.DEBUG)

# Only add handler if not already configured (avoid duplicate handlers)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))
    logger.addHandler(handler)

# Private/internal network CIDR blocks to block for security
BLOCKED_NETWORKS = [
    # RFC 1918 - Private networks
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),

    # RFC 3927 - Link-local
    ipaddress.ip_network("169.254.0.0/16"),

    # RFC 5735 - Special use
    ipaddress.ip_network("127.0.0.0/8"),    # Loopback
    ipaddress.ip_network("0.0.0.0/8"),      # This network
    ipaddress.ip_network("224.0.0.0/4"),    # Multicast
    ipaddress.ip_network("240.0.0.0/4"),    # Reserved

    # Cloud metadata services (critical security issue)
    ipaddress.ip_network("169.254.169.254/32"),  # AWS/GCP/Azure metadata
    ipaddress.ip_network("100.100.100.200/32"),  # DigitalOcean metadata
    ipaddress.ip_network("192.0.0.192/32"),      # Oracle Cloud metadata

    # IPv6 equivalents
    ipaddress.ip_network("::1/128"),        # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),       # IPv6 unique local
    ipaddress.ip_network("fe80::/10"),      # IPv6 link-local
]


def _is_ip_blocked(ip_str: str) -> bool:
    """Check if an IP address is in blocked private/internal networks."""
    try:
        ip = ipaddress.ip_address(ip_str)
        for blocked_network in BLOCKED_NETWORKS:
            if ip in blocked_network:
                logger.warning(f"Blocked attempt to access private/internal IP: {ip_str} (in {blocked_network})")
                return True
        return False
    except (ipaddress.AddressValueError, ValueError):
        # Invalid IP format - let it through (will fail later in HTTP request)
        return False


async def _resolve_and_validate_hostname(hostname: str) -> bool:
    """
    Resolve hostname and check if any resolved IPs are blocked.
    Returns True if hostname is safe to access, False if blocked.
    """
    try:
        # Resolve hostname to IP addresses
        loop = asyncio.get_event_loop()
        addr_infos = await loop.getaddrinfo(
            hostname, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
        )

        resolved_ips = set()
        for addr_info in addr_infos:
            ip = addr_info[4][0]
            resolved_ips.add(ip)

        # Check each resolved IP
        for ip in resolved_ips:
            if _is_ip_blocked(ip):
                logger.warning(f"Hostname {hostname} resolves to blocked IP {ip} - denying access")
                return False

        logger.debug(f"Hostname {hostname} resolved to safe IPs: {list(resolved_ips)}")
        return True

    except (socket.gaierror, OSError) as e:
        # DNS resolution failed - allow through (will fail in HTTP request with better error)
        logger.debug(f"DNS resolution failed for {hostname}: {e} - allowing through")
        return True
    except Exception as e:
        # Unexpected error - fail safe by allowing
        logger.warning(f"Unexpected error resolving {hostname}: {e} - allowing through")
        return True

# Remove playwright import for now - it's causing issues
# from playwright.async_api import async_playwright

"""
Web-friendly scraping configuration:

Environment Variables:
- SCRAPER_CONCURRENCY: Global concurrent requests (default: 10, was 50)
- SCRAPER_PER_DOMAIN: Per-domain concurrent requests (default: 1, was 4)
- SCRAPER_BASE_DELAY: Base delay between requests in seconds (default: 1.0, was 0.01)
- SCRAPER_MAX_DELAY: Maximum exponential backoff delay (default: 60.0)
- SCRAPER_RESPECT_ROBOTS: Respect robots.txt (default: true)
- SCRAPE_CACHE_SIZE: LRU cache size (default: 1000)

Features:
- Exponential backoff on failures (429, 503, 502, 504 status codes)
- Robots.txt compliance (cached per domain)
- Per-domain failure tracking and backoff periods
- Conservative rate limiting to avoid blocks
"""

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

# Web-friendly scraping configuration - conservative defaults
_max_concurrent = int(os.getenv("SCRAPER_CONCURRENCY", "10"))  # Reduced global concurrency
_per_domain_limit = int(os.getenv("SCRAPER_PER_DOMAIN", "1"))  # Very conservative per-domain limit
_base_delay = float(os.getenv("SCRAPER_BASE_DELAY", "1.0"))  # Base delay between requests (seconds)
_max_delay = float(os.getenv("SCRAPER_MAX_DELAY", "60.0"))  # Max backoff delay
_respect_robots = os.getenv("SCRAPER_RESPECT_ROBOTS", "true").lower() == "true"  # Respect robots.txt

_scrape_semaphore = asyncio.Semaphore(_max_concurrent)
_domain_locks: dict[str, asyncio.Semaphore] = {}
_scrape_cache = LRUCache(max_size=int(os.getenv("SCRAPE_CACHE_SIZE", "1000")))  # Bounded LRU cache

# Enhanced rate limiting with exponential backoff per domain
_rate_limit_tracker: dict[str, float] = {}  # Track last request time per domain
_domain_failure_counts: dict[str, int] = {}  # Track failures for backoff
_domain_backoff_until: dict[str, float] = {}  # Track backoff periods
_robots_cache: dict[str, RobotFileParser] = {}  # Cache robots.txt parsers

# Removed proxy management - using direct connections only

# Global reusable HTTP client for connection pooling
_global_client: Optional[httpx.AsyncClient] = None
_client_lock = threading.Lock()

# Security tracking
_insecure_connections_used = 0
_total_connections_attempted = 0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# Failure logging via structured logger (serverless-friendly)
# Removed local file logging - use structured logging for cloud environments

# ScraperAPI key (optional) - set as environment variable if you have it
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")


async def _check_robots_txt(url: str, user_agent: str = "*") -> bool:
    """
    Check if URL is allowed by robots.txt with improved fail open/closed logic.

    Fail closed (deny access) for:
    - 403 Forbidden: Explicit robots.txt access denied
    - 451 Unavailable for Legal Reasons: Legal restriction on robots.txt

    Fail open (allow access) for:
    - Network errors (timeout, connection refused, DNS errors)
    - 404 Not Found: No robots.txt exists
    - 5xx Server errors: Temporary server issues
    """
    if not _respect_robots:
        return True

    try:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"

        if domain not in _robots_cache:
            robots_url = urljoin(domain, "/robots.txt")
            try:
                # Reuse the pooled HTTP client instead of creating a new one
                client = get_global_client(verify_ssl=True)
                response = await client.get(robots_url, follow_redirects=True, timeout=5.0)

                if response.status_code == 200:
                    rp = RobotFileParser()
                    rp.set_url(robots_url)
                    rp.read_from_string(response.text)
                    _robots_cache[domain] = rp
                    logger.debug(f"Robots.txt loaded for {domain}")
                elif response.status_code in [403, 451]:
                    # Explicit denial - fail closed
                    logger.warning(f"Robots.txt access denied for {domain} (HTTP {response.status_code}) - blocking crawling")
                    _robots_cache[domain] = "BLOCKED"  # Special marker for explicit denial
                elif response.status_code == 404:
                    # No robots.txt - fail open
                    logger.debug(f"No robots.txt found for {domain} (404) - allowing crawling")
                    _robots_cache[domain] = None
                else:
                    # Other errors (5xx, etc) - fail open for temporary issues
                    logger.debug(f"Robots.txt fetch returned {response.status_code} for {domain} - allowing crawling")
                    _robots_cache[domain] = None

            except httpx.RequestError as e:
                # Network errors (DNS, connection, timeout, etc.) - fail open (temporary issues)
                logger.debug(f"Network error fetching robots.txt for {domain}: {type(e).__name__} - allowing crawling")
                _robots_cache[domain] = None
            except Exception as e:
                # Other errors - fail open but log for investigation
                logger.warning(f"Unexpected error fetching robots.txt for {domain}: {type(e).__name__}: {e} - allowing crawling")
                _robots_cache[domain] = None

        cached_result = _robots_cache.get(domain)
        if cached_result == "BLOCKED":
            # Explicit denial (403/451) - deny access
            return False
        elif cached_result is None:
            # No robots.txt or error - allow access
            return True
        else:
            # Valid robots.txt - check rules
            return cached_result.can_fetch(user_agent, url)

    except Exception as e:
        # Final fallback - fail open but log
        logger.warning(f"Robots.txt check failed for {url}: {type(e).__name__}: {e} - allowing crawling")
        return True


def _get_domain_delay(domain: str) -> float:
    """Calculate appropriate delay for domain based on failure history."""
    failure_count = _domain_failure_counts.get(domain, 0)
    if failure_count == 0:
        return _base_delay

    # Exponential backoff: base_delay * (2 ^ failures), capped at max_delay
    delay = _base_delay * (2 ** min(failure_count, 6))  # Cap at 2^6 = 64x multiplier
    return min(delay, _max_delay)


def _should_wait_for_backoff(domain: str) -> float:
    """Check if domain is in backoff period. Returns seconds to wait, or 0 if ready."""
    backoff_until = _domain_backoff_until.get(domain, 0)
    current_time = time.time()

    if current_time < backoff_until:
        return backoff_until - current_time
    return 0


def _record_domain_success(domain: str):
    """Record successful request - reset failure count."""
    _domain_failure_counts[domain] = 0
    _domain_backoff_until.pop(domain, None)


def _record_domain_failure(domain: str):
    """Record failed request - increase failure count and set backoff."""
    _domain_failure_counts[domain] = _domain_failure_counts.get(domain, 0) + 1
    delay = _get_domain_delay(domain)
    _domain_backoff_until[domain] = time.time() + delay


def _log_failure(message: str, error_type: str = "scraper_failure", **kwargs):
    """Log failure message using structured logging (serverless-friendly)."""
    # Use structured logging with additional context for monitoring/alerting
    logger.error(
        f"SCRAPER_FAILURE: {message}",
        extra={
            "error_type": error_type,
            "timestamp": time.time(),
            "component": "scraper",
            **kwargs
        }
    )


# URL normalization consolidated to url_utils.normalize_url

def _normalize_domain(url: str) -> str:
    """Get domain from URL using url_utils.normalize_url."""
    try:
        parsed = urlparse(normalize_url(url))
        return parsed.netloc.lower()
    except Exception:
        return url.lower()


def _make_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(["en-GB,en;q=0.9", "de-DE,de;q=0.9", "fr-FR,fr;q=0.9"]),
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1"
    }


def normalize_ascii(text: str) -> str:
    """Fix common encoding issues including HTML entities and mangled UTF-8."""
    if not text:
        return ""

    # First fix HTML entity encoding issues
    import html
    text = html.unescape(text)

    # Fix common mangled UTF-8 patterns (like PrÃ¼fungsverband → Prüfungsverband)
    utf8_fixes = {
        "Ã¤": "ä", "Ã¶": "ö", "Ã¼": "ü", "ÃŸ": "ß",
        "Ã„": "Ä", "Ã–": "Ö", "Ãœ": "Ü",
        "Ã©": "é", "Ã¨": "è", "Ã¡": "á", "Ã ": "à",
        "Ã³": "ó", "Ã²": "ò", "Ã­": "í", "Ã¬": "ì",
        "Ãº": "ú", "Ã¹": "ù", "Ã±": "ñ", "Ã§": "ç"
    }
    for bad, good in utf8_fixes.items():
        text = text.replace(bad, good)

    # Now convert umlauts to ASCII equivalents
    umlaut_replacements = {
        "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
        "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
        "é": "e", "è": "e", "á": "a", "à": "a",
        "ó": "o", "ò": "o", "í": "i", "ì": "i",
        "ú": "u", "ù": "u", "ñ": "n", "ç": "c"
    }
    for bad, good in umlaut_replacements.items():
        text = text.replace(bad, good)

    # Handle other encoding issues and normalize
    try:
        text = text.encode('utf-8', errors='ignore').decode('utf-8')
    except UnicodeError:
        # If encoding fails, keep original text
        pass

    return text.strip()


async def _fetch_via_scraperapi(url: str, custom_logger=None) -> tuple[str, int]:
    """Use ScraperAPI as a fallback."""
    if not SCRAPERAPI_KEY:
        return "", 0
    
    try:
        from urllib.parse import quote
        api_url = f"https://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={quote(url)}&render=true"
        
        if custom_logger:
            custom_logger(f" ScraperAPI fetching: {url}")
        else:
            logger.info(f"ScraperAPI fetching: {url}")

        # Reuse the pooled HTTP client for ScraperAPI requests too
        client = get_global_client(verify_ssl=True)
        resp = await client.get(api_url, timeout=30.0)

        if resp.status_code == 200 and resp.text and len(resp.text) > 100:
            return resp.text, resp.status_code
        else:
            msg = f"ScraperAPI returned status {resp.status_code} for {url}"
            if custom_logger:
                custom_logger(msg)
            else:
                logger.warning(msg)
            _log_failure(msg, error_type="scraperapi_http_error", url=url, status_code=resp.status_code)
            return "", resp.status_code
    except Exception as e:
        msg = f"ScraperAPI failed for {url} → {type(e).__name__}: {e}"
        if custom_logger:
            custom_logger(msg)
        else:
            logger.error(msg)
        _log_failure(msg, error_type="scraperapi_exception", url=url, exception_type=type(e).__name__)
        return "", 0


def get_global_client(verify_ssl: bool = True) -> httpx.AsyncClient:
    """Get or create global HTTP client with connection pooling and proper SSL handling.

    Thread-safe using a lock to prevent multiple clients being created under concurrency.
    """
    global _global_client
    with _client_lock:
        if _global_client is None or _global_client.is_closed:
            _global_client = httpx.AsyncClient(
                timeout=httpx.Timeout(15.0, connect=8.0),
                follow_redirects=True,
                verify=verify_ssl,  # Enable SSL verification by default for security
                limits=httpx.Limits(max_keepalive_connections=100, max_connections=200)
            )
    return _global_client

def get_security_stats() -> dict:
    """Get security statistics for monitoring SSL usage."""
    global _insecure_connections_used, _total_connections_attempted
    return {
        "total_connections": _total_connections_attempted,
        "insecure_connections": _insecure_connections_used,
        "secure_connections": _total_connections_attempted - _insecure_connections_used,
        "insecure_percentage": round(_insecure_connections_used / max(1, _total_connections_attempted) * 100, 2)
    }

def get_insecure_client() -> httpx.AsyncClient:
    """
    Get HTTP client with SSL verification disabled - SECURITY WARNING!

    This client should ONLY be used as a last resort for sites with broken SSL certificates
    after the secure connection attempt has failed. Using this client makes connections
    vulnerable to man-in-the-middle attacks and should be avoided whenever possible.

    All usage of this client is logged with warnings.
    """
    return httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=8.0),
        follow_redirects=True,
        verify=False,  # WARNING: SSL verification disabled - vulnerable to MITM attacks
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
    )

async def close_global_client():
    """Close global HTTP client."""
    global _global_client
    if _global_client:
        await _global_client.aclose()
        _global_client = None

# Proxy URL formatting removed - no longer needed


async def _fetch_html(url: str, custom_logger=None) -> dict:
    """Fetch HTML using secure HTTPS with fallback for problematic sites. Direct connections only.

    Always returns a structured dict on failure instead of raising.
    Returns: {"html": str, "status_code": int, "error": str | None}

    This function is guaranteed to return a dict and never raise exceptions.
    """
    try:
        url = normalize_url(url)
        domain = _normalize_domain(url)  # Fix: derive domain at start to avoid NameError
        logger.info(f"Fetching: {url} (direct connection)")
    except Exception as e:
        # Even URL normalization could fail - catch everything
        error_msg = f"URL normalization failed: {type(e).__name__}: {e}"
        logger.error(error_msg)
        return {"html": "", "status_code": 0, "error": error_msg}

    # Try secure connection first, then insecure fallback if SSL issues
    for attempt, (verify_ssl, desc) in enumerate([(True, "secure"), (False, "insecure-fallback")], 1):
        # Manage client lifecycle properly to prevent resource leaks
        client = None
        try:
            logger.debug(f"Attempt {attempt}: {desc} direct connection")

            # Use appropriate client based on security level
            if verify_ssl:
                client = get_global_client(verify_ssl=True)
                # Global client is managed elsewhere, don't close it
                should_close_client = False
            else:
                client = get_insecure_client()
                # Insecure client is created fresh, must close it
                should_close_client = True

            resp = await client.get(url, headers=_make_headers())

            if resp.status_code != 200:
                msg = f"Non-200 status for {url} → {resp.status_code}"
                if custom_logger:
                    custom_logger(msg)
                else:
                    logger.info(msg)

                # Handle rate limiting and server errors with exponential backoff
                if resp.status_code in [429, 503, 502, 504]:
                    _record_domain_failure(domain)
                    if resp.status_code == 429:
                        msg = f"Rate limited by {domain} - will backoff exponentially"
                        if custom_logger:
                            custom_logger(msg)
                        else:
                            logger.warning(msg)

            # Try to handle encoding properly
            if resp.encoding is None:
                resp.encoding = 'utf-8'

            # Success - log security level used and track stats
            global _insecure_connections_used, _total_connections_attempted
            _total_connections_attempted += 1

            if verify_ssl:
                logger.debug(f"Secure connection successful for {url}")
            else:
                _insecure_connections_used += 1
                logger.warning(f"Insecure connection used for {url} (SSL issues)")
                logger.info(f"Security stats: {_insecure_connections_used}/{_total_connections_attempted} insecure connections")

            # Store response data before closing client
            response_text = resp.text
            response_status = resp.status_code

            # Close insecure client to prevent resource leaks
            if should_close_client and client:
                await client.aclose()

            return {"html": response_text, "status_code": response_status, "error": None}

        except Exception as e:
            error_msg = f"Attempt {attempt} ({desc}) failed for {url} → {type(e).__name__}: {e}"
            if custom_logger:
                custom_logger(error_msg)
            else:
                logger.debug(error_msg)

            # If this was the secure attempt and it's an SSL error, try insecure
            if verify_ssl and ("ssl" in str(e).lower() or "certificate" in str(e).lower() or "tls" in str(e).lower()):
                logger.debug(f"SSL error detected, will retry with insecure connection")
                continue

            # If this was the insecure attempt or non-SSL error, return failure dict
            if not verify_ssl:
                _log_failure(error_msg, error_type="fetch_connection_error", url=url, exception_type=type(e).__name__)
                # Close client before returning
                if should_close_client and client:
                    try:
                        await client.aclose()
                    except Exception:
                        pass  # Don't let cleanup errors mask the original error
                return {"html": "", "status_code": 0, "error": error_msg}

            # If this was the secure attempt with a non-SSL error, also fail
            # (don't retry insecure for non-SSL errors)
            if verify_ssl:
                _log_failure(error_msg, error_type="fetch_connection_error", url=url, exception_type=type(e).__name__)
                return {"html": "", "status_code": 0, "error": error_msg}

        finally:
            # Ensure insecure clients are always closed, even on unexpected errors
            if 'should_close_client' in locals() and should_close_client and client:
                try:
                    await client.aclose()
                except Exception:
                    pass  # Don't let cleanup errors propagate

    # Fallback: Should never reach here due to the loop logic, but if we do, return error
    fallback_error = f"All connection attempts failed for {url}"
    logger.error(f"Unexpected: reached end of _fetch_html without returning. URL: {url}")
    return {"html": "", "status_code": 0, "error": fallback_error}


async def extract_company_info(url: str, custom_logger=None) -> dict:
    """Main scraper entrypoint with simplified logic and persistent caching."""
    if not url:
        return {"domain": "", "candidates": [], "text": "No URL provided"}

    url = normalize_url(url)
    domain = _normalize_domain(url)

    # Security check: Validate hostname to prevent access to private/internal networks
    try:
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname

        if hostname:
            # Check if hostname is an IP address
            try:
                ip = ipaddress.ip_address(hostname)
                if _is_ip_blocked(str(ip)):
                    logger.warning(f"Blocked direct IP access to private/internal network: {hostname}")
                    return {"domain": domain, "candidates": [], "text": "Access to private/internal networks blocked", "scraped": False}
            except ipaddress.AddressValueError:
                # Hostname is not an IP - resolve and validate
                if not await _resolve_and_validate_hostname(hostname):
                    return {"domain": domain, "candidates": [], "text": "Access to private/internal networks blocked", "scraped": False}

    except Exception as e:
        logger.warning(f"Error during hostname validation for {url}: {e}")
        # Continue on validation errors to avoid breaking legitimate requests

    # Check persistent cache first
    cached_result = await get_cached_scrape(url)
    if cached_result:
        if custom_logger:
            custom_logger(f" Using persistent cache for: {domain}")
        else:
            logger.debug(f"Using persistent cache for: {domain}")
        return cached_result

    # Check memory cache
    cached_result = _scrape_cache.get(domain)
    if cached_result is not None:
        logger.debug(f"Using memory cache for {domain}")
        return cached_result

    if custom_logger:
        custom_logger(f" Starting fresh scrape for: {url}")
    else:
        logger.info(f"Starting fresh scrape for: {url}")

    # Check robots.txt first
    if not await _check_robots_txt(url):
        return {"domain": domain, "candidates": [], "text": "Blocked by robots.txt", "scraped": False}

    async with _scrape_semaphore:
        # Web-friendly rate limiting with exponential backoff

        # Check if domain is in backoff period
        backoff_wait = _should_wait_for_backoff(domain)
        if backoff_wait > 0:
            if custom_logger:
                custom_logger(f" Domain {domain} in backoff, waiting {backoff_wait:.1f}s")
            else:
                logger.info(f"Domain {domain} in backoff, waiting {backoff_wait:.1f}s")
            await asyncio.sleep(backoff_wait)

        # Respect minimum delay between requests to same domain
        if domain in _rate_limit_tracker:
            domain_delay = _get_domain_delay(domain)
            elapsed = time.time() - _rate_limit_tracker[domain]
            if elapsed < domain_delay:
                wait_time = domain_delay - elapsed
                if custom_logger:
                    custom_logger(f" Rate limiting {domain}, waiting {wait_time:.1f}s")
                else:
                    logger.debug(f"Rate limiting {domain}, waiting {wait_time:.1f}s")
                await asyncio.sleep(wait_time)

        # Set up per-domain concurrency limit
        if domain not in _domain_locks:
            _domain_locks[domain] = asyncio.Semaphore(_per_domain_limit)

        result = {"domain": domain, "candidates": [], "text": ""}

        async with _domain_locks[domain]:
            # Double-check cache after acquiring lock
            cached_result = _scrape_cache.get(domain)
            if cached_result is not None:
                return cached_result
            
            html = ""

            # DIRECT CONNECTION STRATEGY: Simple direct connection only
            html = ""

            logger.debug(f"Trying direct connection for {domain}")
            fetch_result = await _fetch_html(url, custom_logger=custom_logger)
            html = fetch_result.get("html", "")
            status_code = fetch_result.get("status_code", 0)
            error = fetch_result.get("error")

            if html and len(html) > 300:
                logger.debug(f"Direct connection success ({len(html)} chars)")
            elif error:
                logger.debug(f"Direct connection failed: {error}")
            else:
                logger.debug(f"Direct connection insufficient content ({len(html)} chars)")

            # Try ScraperAPI as last resort (only if we have almost nothing)
            if (not html or len(html) < 100) and SCRAPERAPI_KEY:
                logger.debug(f"Last resort: Trying ScraperAPI for {url}")
                html, _ = await _fetch_via_scraperapi(url, custom_logger=custom_logger)
                if html and len(html) > 100:
                    logger.info(f"Success via ScraperAPI for {url}")

            # If we still don't have good content, return failure (lower threshold)
            if not html or len(html) < 100:
                msg = f"All attempts failed for {url} - got {len(html)} chars"
                if custom_logger:
                    custom_logger(msg)
                else:
                    logger.warning(msg)
                result = {"domain": domain, "candidates": [domain], "text": "Failed to scrape content", "scraped": False}
                _scrape_cache.set(domain, result)
                _record_domain_failure(domain)  # Track failure for exponential backoff
                return result

            # Parse the HTML
            try:
                soup = BeautifulSoup(html, "html.parser")  # Use html.parser instead of lxml
                
                # Remove unwanted elements
                for tag in soup(["script", "style", "noscript", "nav", "footer", "aside"]):
                    tag.decompose()

                # Extract text parts
                parts = []
                
                # Title
                if soup.title and soup.title.string:
                    title = soup.title.string.strip()
                    if title:
                        parts.append(title)

                # Main heading
                h1 = soup.find("h1")
                if h1:
                    h1_text = h1.get_text(strip=True)
                    if h1_text and h1_text not in parts:
                        parts.append(h1_text)

                # Meta description
                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc and meta_desc.get("content"):
                    desc = meta_desc.get("content").strip()
                    if desc:
                        parts.append(desc)

                # Open Graph description
                og_desc = soup.find("meta", attrs={"property": "og:description"})
                if og_desc and og_desc.get("content"):
                    og_desc_text = og_desc.get("content").strip()
                    if og_desc_text and og_desc_text not in parts:
                        parts.append(og_desc_text)

                # First few paragraphs
                paragraphs = soup.find_all("p", limit=5)
                for p in paragraphs:
                    p_text = p.get_text(" ", strip=True)
                    if p_text and len(p_text) > 20:  # Only meaningful paragraphs
                        parts.append(p_text[:400])  # Limit paragraph length

                # Combine all text
                text = " ".join(parts)
                text = re.sub(r'\s+', ' ', text).strip()
                
                if not text:
                    text = "No meaningful content extracted"

                # Extract company name candidates
                candidates = []
                
                # Open Graph site name
                og_site = soup.find("meta", attrs={"property": "og:site_name"})
                if og_site and og_site.get("content"):
                    candidates.append(og_site.get("content").strip())

                # Title
                if soup.title and soup.title.string:
                    title_text = soup.title.string.strip()
                    # Clean up title (remove common suffixes)
                    title_clean = re.sub(r'\s*[-|–—]\s*(Home|Homepage|Welcome).*$', '', title_text, flags=re.IGNORECASE)
                    candidates.append(title_clean)

                # Domain as fallback
                candidates.append(domain)

                # Clean up candidates
                text = normalize_ascii(text)
                candidates = [normalize_ascii(c) for c in candidates if c and len(c.strip()) > 0]
                candidates = list(dict.fromkeys(candidates))  # Remove duplicates while preserving order

                result = {
                    "domain": domain,
                    "candidates": candidates,
                    "text": text[:2000],  # Limit text length
                    "scraped": True
                }

                logger.info(f"Successfully scraped {domain}")
                _record_domain_success(domain)  # Reset failure counter

            except Exception as e:
                msg = f"Parsing error for {url} → {type(e).__name__}: {e}"
                if custom_logger:
                    custom_logger(msg)
                else:
                    logger.error(msg)
                _log_failure(msg, error_type="html_parsing_error", url=url, domain=domain, exception_type=type(e).__name__)
                result = {"domain": domain, "candidates": [domain], "text": "Parsing failed", "scraped": False}
                _record_domain_failure(domain)  # Track parsing failure too

            # Update rate limiting tracker
            _rate_limit_tracker[domain] = time.time()

            # Cache both in memory and persistent storage
            _scrape_cache.set(domain, result)
            await cache_scrape_result(url, result)

            return result


# Add a simple test function
# Add a simple test function
def test_url_normalization():
    """Test the URL normalization function with various inputs."""
    test_cases = [
        "btrsumus.de",
        "https://btrsumus.de",
        "http://btrsumus.de",
        "https://https btrsumus.de",  # This was the problem case
        "treuhand-luebeck.de",
        "www.example.com",
        "https://www.example.com/path",
        " https://example.com ",
        "example.com/path/to/page"
    ]

    logger.info("Testing URL normalization:")
    for test_url in test_cases:
        try:
            normalized = normalize_url(test_url)
            logger.info(f"  '{test_url}' → '{normalized}'")
        except Exception as e:
            logger.error(f"  '{test_url}' → ERROR: {e}")


async def test_scraper():
    """Simple test function to verify the scraper works."""
    logger.info("Testing scraper...")

    # First test URL normalization
    test_url_normalization()

    test_urls = [
        "example.com",
        "google.com",
        "github.com"
    ]

    for url in test_urls:
        logger.info(f"Testing: {url}")
        try:
            result = await extract_company_info(url)
            logger.info(f"Result: domain='{result['domain']}', candidates={result['candidates'][:2]}")
        except Exception as e:
            logger.error(f"Error: {type(e).__name__}: {e}")

        # Small delay between requests
        await asyncio.sleep(1)


# Add main execution block
if __name__ == "__main__":
    logger.info("Starting scraper test...")
    asyncio.run(test_scraper())