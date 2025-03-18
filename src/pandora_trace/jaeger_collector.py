import json
import os
import time
from pathlib import Path
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

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

def request_with_retry(url, max_retries=5, backoff_factor=0.5):
    """Make a GET request with retry logic"""
    session = create_session_with_retries(max_retries, backoff_factor)
    
    for attempt in range(max_retries + 1):
        try:
            response = session.get(url)
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

def download_traces_from_jaeger(service_name: str, jaeger_url: str, target_dir: Path, root_cause: List[str] = None) -> int:
    try:
        # Get trace IDs with retry
        response = request_with_retry(f"{jaeger_url}/api/traces?service={service_name}&limit=100000")
        traces = []
        
        for trace in tqdm(response["data"]):
            trace_id = trace["traceID"]
            try:
                # Get individual trace with retry
                trace_response = request_with_retry(f"{jaeger_url}/api/traces/{trace_id}")
                traces.extend(trace_response["data"])
            except Exception as e:
                print(f"Error fetching trace {trace_id} for service {service_name}: {e}")
                # Continue with other traces instead of failing completely
                continue
                
        os.makedirs(target_dir, exist_ok=True)
        target_file = target_dir / f"{service_name}.json"
        
        with open(target_file, "w") as f:
            # Add root_cause to each trace
            traces_with_root = [
                {**trace, "rootCause": root_cause or []}
                for trace in traces
            ]
            json.dump(traces_with_root, f, indent=4)
            
        return len(traces)
    except Exception as e:
        print(f"Failed to download traces for service {service_name}: {e}")
        return 0

def download_traces_from_jaeger_for_all_services(target_dir: Path, jaeger_url: str = JAEGER_URL, root_cause: List[str] = None) -> int:
    try:
        # Get services list with retry
        response = request_with_retry(f"{jaeger_url}/api/services")
        total = 0
        all_services = response["data"] or []
        
        for service in all_services:
            if "jaeger" in service:
                continue
            try:
                service_traces = download_traces_from_jaeger(service, jaeger_url, target_dir, root_cause)
                total += service_traces
                print(f"Downloaded {service_traces} traces for service {service}")
            except Exception as e:
                print(f"Failed to process service {service}: {e}")
                # Continue with other services
                continue
                
        return total
    except Exception as e:
        print(f"Failed to get services list: {e}")
        return 0
