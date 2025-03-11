import json
import os
from pathlib import Path
from typing import List

import requests

JAEGER_URL = "http://localhost:16686"

def download_traces_from_jaeger(service_name: str, jaeger_url: str, target_dir: Path, root_cause: List[str] = None) -> int:
    response = requests.get(f"{jaeger_url}/api/traces?service={service_name}&limit=10000").json()
    traces = []
    for trace in response["data"]:
        trace_id = trace["traceID"]
        response = requests.get(f"{jaeger_url}/api/traces/{trace_id}").json()
        traces.extend(response["data"])
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

def download_traces_from_jaeger_for_all_services(target_dir: Path, jaeger_url: str = JAEGER_URL, root_cause: List[str] = None) -> int:
    response = requests.get(f"{jaeger_url}/api/services")
    total = 0
    all_services = response.json()["data"] or []
    for service in all_services:
        if "jaeger" in service:
            continue
        total += download_traces_from_jaeger(service, jaeger_url, target_dir, root_cause)
    return total
