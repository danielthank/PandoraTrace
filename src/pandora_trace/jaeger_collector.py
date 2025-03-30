import time
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

JAEGER_URL = "http://localhost:16686"

def create_session_with_retries(max_retries=5, backoff_factor=0.5):
    """Creates a requests session with retry configuration"""
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def request_with_retry(url, params, max_retries=5, backoff_factor=0.5):
    """Make a GET request with retry logic"""
    session = create_session_with_retries(max_retries, backoff_factor)
    
    for attempt in range(max_retries + 1):
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < max_retries:
                sleep_time = backoff_factor * (2 ** attempt)
                print(f"Connection error on {url}, retrying in {sleep_time:.2f}s... ({attempt+1}/{max_retries})")
                time.sleep(sleep_time)
            else:
                print(f"Failed after {max_retries} retries: {e}")
                raise
        except Exception as e:
            print(f"Unexpected error for {url}: {e}")
            raise

def download_traces_from_jaeger(
    service_name: str,
    jaeger_url: str = JAEGER_URL,
    start_time: int = None
) -> List[dict]:
    try:
        params = {
            "service": service_name,
            "limit": 100000
        }
        if start_time:
            params["start"] = start_time
            
        response = request_with_retry(f"{jaeger_url}/api/traces", params=params)
        return response["data"]
        
    except Exception as e:
        print(f"Failed to download traces for service {service_name}: {e}")
        return []
