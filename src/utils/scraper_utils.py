import cloudscraper
import logging
import time
from requests.exceptions import RequestException

# Configure logger
logger = logging.getLogger(__name__)

def get_scraper():
    """
    Creates a CloudScraper session configured to mimic a real Chrome browser on Windows.
    """
    try:
        return cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
    except Exception as e:
        logger.error(f"Failed to create scraper: {e}")
        return None

def fetch_url(url, retries=3, delay=2):
    """
    Smart fetcher that handles WAF blocking (403 errors).
    
    Logic:
    1. Tries a standard GET request mimicking Chrome.
    2. If blocked (403), tries a POST request (common WAF bypass).
    3. Retries on connection failures.
    
    Args:
        url (str): Target URL.
        retries (int): Number of attempts.
        delay (int): Seconds to wait between retries.
        
    Returns:
        response object or None
    """
    scraper = get_scraper()
    if not scraper:
        return None

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🌐 Fetching {url} (Attempt {attempt}/{retries})...")
            
            # 1. Try Standard GET
            response = scraper.get(url, timeout=30)
            
            # 2. WAF Bypass Strategy: If GET is blocked (403), try POST
            # Many firewalls block automated GETs but are lenient with POSTs
            if response.status_code == 403:
                logger.warning("⚠️ GET 403 Forbidden. Switching to POST method to bypass WAF...")
                response = scraper.post(url, timeout=30)
            
            # 3. Check Success
            if response.status_code == 200:
                return response
            else:
                logger.warning(f"❌ Request failed with status code: {response.status_code}")
                
        except RequestException as e:
            logger.error(f"❌ Network Error: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected Error: {e}")
        
        # Wait before retrying (exponential backoff optional, here simple delay)
        if attempt < retries:
            time.sleep(delay)

    logger.error(f"❌ Failed to fetch {url} after {retries} attempts.")
    return None