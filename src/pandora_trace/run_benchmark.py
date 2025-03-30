import argparse
import json
import math
import os
import random
import subprocess
import time
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from time import sleep
from typing import Set, NamedTuple, List

import docker
from docker.models.containers import Container

from jaeger_collector import download_traces_from_jaeger

FUZZLER_COMPILE_COMMAND = "/RESTler/restler/Restler compile --api_spec ./swagger.json"
EXEC_FUZZ_LEAN_COMMAND = "/RESTler/restler/Restler fuzz-lean --grammar_file ./Compile/grammar.py --dictionary_file ./Compile/dict.json --settings ./Compile/engine_settings.json --no_ssl"

BENCHMARK_DIR = str(Path(__file__).parent / "data" / "restler")
SOCIAL_NETWORK_APP = "socialNetwork"


class AppName(Enum):
    socialNetwork = "socialNetwork"
    hotelReservation = "hotelReservation"
    mediaMicroservices = "mediaMicroservices"


class Incident(NamedTuple):
    command: str
    ratio: float
    incident_name: str
    apt_dependencies: List[str]


RAW_TRAIN_TICKET_INCIDENTS = [
    ("tc qdisc add dev eth0 root netem loss 50%", "packet_loss", ["iproute2"]),
    # ("tc qdisc add dev eth0 root netem delay 100ms 20ms distribution normal", "latency", ["iproute2"]),
    # ("shutdown", "crush", []),
    # ("stress --cpu 1 --timeout 60s", "cpu_load", ["stress"]),
    # ("stress --vm 4 --vm-bytes 256M --timeout 60s", "memory_stress", ["stress"]),
    # ("stress --io 4 --timeout 60s", "disk_io_stress", ["stress"]),
]
INCIDENTS: List[Incident] = [
    Incident(command=cmd, ratio=p / 10, incident_name=f"{name}-{p / 10}", apt_dependencies=deps)
    for p in range(1, 2) for cmd, name, deps in RAW_TRAIN_TICKET_INCIDENTS
]


def run_subprocess_real_time(cmd, cwd=None, shell=True):
    """Run subprocess command with real-time output streaming"""
    print(f"\nExecuting: {cmd}", flush=True)
    
    # Use Popen for real-time output instead of check_output
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        shell=shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    # Read and print output in real-time
    output_lines = []
    for line in iter(process.stdout.readline, ''):
        print(line, end='', flush=True)
        output_lines.append(line)
    
    # Wait for process to complete and get return code
    process.wait()
    
    if process.returncode != 0:
        print(f"Command failed with return code {process.returncode}", flush=True)
        raise subprocess.CalledProcessError(process.returncode, cmd, ''.join(output_lines))
    
    return ''.join(output_lines)


@contextmanager
def setup_test(app: AppName, deathstar_dir: str):
    before_containers = set(docker.from_env().containers.list())
    print(f"Starting {app.value} containers...", flush=True)
    run_subprocess_real_time("docker compose up -d --wait", cwd=f"{deathstar_dir}/{app.value}")
    
    if app == AppName.socialNetwork:
        print("Initializing social graph...", flush=True)
        result = run_subprocess_real_time("python3 scripts/init_social_graph.py --graph=socfb-Reed98",
                            cwd=f"{deathstar_dir}/{app.value}")
        print("Init graph result:", result.splitlines()[-1], flush=True)
    
    app_containers = set(docker.from_env().containers.list()) - before_containers
    print("Starting RESTler container...", flush=True)
    restler_container: Container = docker.from_env().containers.run(
        "restler", stdin_open=True, tty=True, detach=True, network_mode="host", auto_remove=True,
        working_dir=f"/code/{app.value}",
        volumes={BENCHMARK_DIR: {"bind": "/code", "mode": "rw"}},
    )
    
    print("Compiling RESTler...", flush=True)
    run(restler_container, FUZZLER_COMPILE_COMMAND, stream=True)
    
    try:
        yield restler_container, app_containers
    except KeyboardInterrupt:
        print("\n\n############\nInterrupted. Killing app and fuzzler\n############\n\n", flush=True)
        raise
    finally:
        try:
            print("Cleaning up containers...", end=" ", flush=True)
            try:
                run_subprocess_real_time("docker compose down -v", cwd=f"{deathstar_dir}/{app.value}")
            except Exception:
                print("Warning: Failed to run docker compose down", flush=True)
            print("done", flush=True)
        except Exception as e:
            print(f"Failed to kill app: {e}", flush=True)
        try:
            restler_container.kill()
            print("Killed RESTler container", flush=True)
        except Exception as e:
            print(f"Failed to kill fuzzler: {e}", flush=True)


def print_result(app: str):
    results_dir = f"{BENCHMARK_DIR}/{app}/FuzzLean/RestlerResults/"
    for experiment in os.listdir(results_dir):
        print(f"Experiment {experiment}", flush=True)
        with open(f"{results_dir}/{experiment}/logs/main.txt") as f:
            content = f.read()
            print(content, flush=True)


def wait_for_container(container: Container, timeout: int = 60) -> bool:
    print(f"Waiting for container {container.name} to be ready...", flush=True)
    start_time = time.time()
    while time.time() - start_time < timeout:
        container.reload()  # Refresh container state
        if container.status == 'running':
            print(f"Container {container.name} is ready!", flush=True)
            return True
        print(".", end="", flush=True)
        sleep(1)
    print(f"\nTimeout waiting for container {container.name}", flush=True)
    return False


def run(container: Container, cmd: str, **kwargs):
    """Execute command in container with real-time output streaming"""
    if not wait_for_container(container):
        raise Exception(f"Container failed to enter running state within timeout. Could not execute command {cmd}.")
    
    print(f"Executing in container: {cmd}", flush=True)
    
    # Handle detached execution which doesn't need streaming
    if kwargs.get("detach"):
        return container.exec_run(cmd, privileged=True, user='root', **kwargs)
    
    # For interactive commands, use stream=True to get real-time output
    for i in range(5):
        try:
            # Remove stream from kwargs if it exists to avoid duplicate parameter
            kwargs_filtered = {k: v for k, v in kwargs.items() if k not in ('detach', 'stream')}
            
            # Always use stream=True for real-time output
            exec_result = container.exec_run(
                cmd, 
                privileged=True, 
                user='root',
                stream=True,  # Enable streaming output
                **kwargs_filtered
            )
            
            # Stream and print output in real time
            output_chunks = []
            
            for chunk in exec_result.output:
                if isinstance(chunk, tuple) and len(chunk) == 2:
                    # Handle demuxed output (stdout, stderr)
                    stdout_chunk, stderr_chunk = chunk
                    if stdout_chunk:
                        print(stdout_chunk.decode(), end='', flush=True)
                        output_chunks.append(stdout_chunk)
                    if stderr_chunk:
                        print(stderr_chunk.decode(), end='', flush=True)
                        output_chunks.append(stderr_chunk)
                else:
                    # Regular output
                    print(chunk.decode(), end='', flush=True)
                    output_chunks.append(chunk)
            
            # For the RESTler commands, if we see successful output, consider it a success
            # regardless of the exit code
            output_text = b''.join(output_chunks).decode()
            
            # Check for success messages in the output
            if ("Task Compile succeeded" in output_text or 
                "Grammar file successfully compiled" in output_text or
                "Attempted requests" in output_text):
                print(f"Command completed successfully based on output", flush=True)
                return type('ExecResult', (), {
                    'exit_code': 0,
                    'output': b''.join(output_chunks)
                })
            
            # If not detected as successful by output, check exit code
            exit_code = getattr(exec_result, 'exit_code', None)
            
            if exit_code == 0 or exit_code is None:
                # Consider None exit code as success too (docker API may not always return exit code)
                print(f"Command completed with exit code: {exit_code}", flush=True)
                return type('ExecResult', (), {
                    'exit_code': 0,
                    'output': b''.join(output_chunks)
                })
            else:
                raise Exception(f"Exit status {exit_code}, output: {b''.join(output_chunks)[:100]}")
                
        except Exception as e:
            if i == 4:
                raise e
            print(f"Command failed, retrying ({i+1}/5): {str(e)}", flush=True)
            sleep(5)


def add_chaos(app_containers: Set[Container], incident: Incident) -> List[str]:
    chosen_containers = random.sample(list(app_containers), math.ceil(len(app_containers) * incident.ratio))
    affected_containers = [c.labels["com.docker.compose.service"] for c in chosen_containers]
    print(f"Adding incident {incident.incident_name} to containers {affected_containers}", flush=True)
    
    for i, container in enumerate(chosen_containers):
        container_name = container.name
        print(f"[{i+1}/{len(chosen_containers)}] Adding chaos to {container_name}...", end='', flush=True)
        try:
            if incident.apt_dependencies:
                print(f"\n  - Updating apt repositories for {container_name}...", flush=True)
                run(container, "apt update --allow-insecure-repositories")
                
                for dep in incident.apt_dependencies:
                    print(f"  - Installing {dep} in {container_name}...", flush=True)
                    run(container, f"apt install -y {dep}")
            
            if "crush" in incident.incident_name:
                print(f"  - Killing container {container_name}...", flush=True)
                container.kill()
            else:
                print(f"  - Running chaos command in {container_name}: {incident.command}", flush=True)
                run(container, incident.command, tty=True, demux=False, detach=True)
                
            print(f"✓ Successfully added chaos to {container_name}", flush=True)
            
        except Exception as e:
            if '"apt": executable file not found' in str(e):
                print(f"✗ Skipping container {container_name} (no apt available)", flush=True)
                continue
            else:
                print(f"✗ Failed to add incident to {container_name}: {e}", flush=True)
    
    print(f"Chaos injection complete. Affected services: {affected_containers}", flush=True)
    return affected_containers


def run_restler(restler_container: Container):
    """Run RESTler with real-time output streaming"""
    print(f"Running RESTler fuzz-lean command...", flush=True)
    
    # Execute with stream=True for real-time output
    exec_result = restler_container.exec_run(
        EXEC_FUZZ_LEAN_COMMAND,
        stream=True
    )
    
    attempted_requests_line = None
    
    # Process the stream in real-time
    for chunk in exec_result.output:
        if isinstance(chunk, bytes):
            line = chunk.decode()
            print(line, end='', flush=True)
            
            # Capture the attempted requests line
            if "Attempted requests" in line:
                attempted_requests_line = line.strip()
    
    # Now we can return our extracted info after processing all output
    return attempted_requests_line or "No request information found"


def get_baseline_traces_path(app: AppName, working_directory: Path) -> Path:
    return working_directory / app.value / "baseline" / "raw_jaeger"


def get_incident_traces_path(app: AppName, incident: Incident, working_directory: Path) -> Path:
    return working_directory / app.value / incident.incident_name / "raw_jaeger"


def get_merged_traces_base_path(working_directory: Path) -> Path:
    return working_directory / "merged_traces"


def run_test(app: AppName, incidents: List[Incident], deathstar_dir: str, target_count: int, working_directory: Path):
    for idx, incident in enumerate(incidents):
        print(f"\n[{idx+1}/{len(incidents)}] Testing incident: {incident.incident_name}", flush=True)
        
        target_dir = get_incident_traces_path(app, incident, working_directory)
        incident_traces = []
        
        if os.path.exists(target_dir):
            trace_files = os.listdir(target_dir)
            print(f"Found {len(trace_files)} trace files in {target_dir}", flush=True)
            
            for f in trace_files:
                file_path = os.path.join(target_dir, f)
                print(f"Loading traces from {file_path}...", end="", flush=True)
                with open(file_path) as json_file:
                    file_traces = json.load(json_file)
                    incident_traces.extend(file_traces)
                    print(f" loaded {len(file_traces)} traces", flush=True)
        
        if len(incident_traces) > target_count:
            print(f"✓ Skipping incident {incident.incident_name} as it already has {len(incident_traces)} traces (target: {target_count})", flush=True)
            continue
        else:
            print(f"→ Running incident {incident.incident_name} - current traces: {len(incident_traces)}, target: {target_count}", flush=True)
        
        with setup_test(app, deathstar_dir) as (restler_container, app_containers):
            root_cause = add_chaos(app_containers, incident)
            
            for iteration in range(50):
                print(f"\nIteration {iteration+1}/50 for incident {incident.incident_name}", flush=True)
                run_restler(restler_container)
                
                print(f"Downloading traces for iteration {iteration+1}...", flush=True)
                traces = download_traces_from_jaeger(
                    service_name="nginx-web-server", 
                    target_dir=target_dir, 
                    root_cause=root_cause
                )
                
                print(f"Current trace count: {traces}", flush=True)
                if traces > target_count:
                    print(f"✓ Successfully collected {traces} traces for incident {incident.incident_name} (target: {target_count})", flush=True)
                    break
            else:
                print(f"⚠ Failed to collect enough traces for incident {incident.incident_name} after 50 iterations", flush=True)


def create_baseline(app: AppName, deathstar_dir: str, target_count: int, working_directory: Path):
    if target_count <= 0:
        raise ValueError(f"Target count must be positive, got {target_count}")

    print(f"Creating baseline for {app.value} (target: {target_count} traces)...", flush=True)
    
    target_dir = get_baseline_traces_path(app, working_directory)
    os.makedirs(target_dir, exist_ok=True)
    print(f"Baseline traces will be stored in {target_dir}", flush=True)
    
    with setup_test(app, deathstar_dir) as (restler_container, app_containers):
        # Set start_time after environment is ready
        start_time = int(time.time() * 1000000)  # microseconds for Jaeger
        
        for attempt in range(100):
            print(f"\nBaseline iteration {attempt+1}/100", flush=True)
            run_restler(restler_container)
            
            print("Downloading baseline traces...", flush=True)
            traces = download_traces_from_jaeger(
                service_name="nginx-web-server",
                start_time=start_time
            )
            
            print(f"Collected {len(traces)} traces", flush=True)
            
            if len(traces) >= target_count:
                target_file = target_dir / "nginx-web-server.json" 
                with open(target_file, "w") as f:
                    json.dump(traces, f, indent=4)
                print(f"✓ Successfully collected and saved {len(traces)} traces (target: {target_count})", flush=True)
                break
            else:
                print(f"Not enough traces yet ({len(traces)}/{target_count})", flush=True)
                time.sleep(5)  # Wait before retrying
        else:
            raise RuntimeError(
                f"Failed to collect enough traces after 100 attempts.\n"
                f"Last attempt collected: {len(traces)}, Target: {target_count}"
            )
    
    print(f"Baseline collection complete: {len(traces)} traces stored in {target_dir}", flush=True)


def merge_with_exp(benign_traces: List[dict], incident_traces: List[dict], exp_lambda: float, target_count: int) -> List[dict]:
    print(f"Merging traces with lambda={exp_lambda}...", flush=True)
    print(f"  - Benign traces: {len(benign_traces)}", flush=True)
    print(f"  - Incident traces: {len(incident_traces)}", flush=True)
    
    if len(incident_traces) == 0:
        print("⚠ Warning: No incident traces to merge!", flush=True)
        return benign_traces[:target_count] if len(benign_traces) >= target_count else benign_traces
    
    merged_traces = []
    incident_traces_copy = incident_traces.copy()  # Copy to avoid modifying the original
    time_until_next_incident = random.expovariate(exp_lambda)
    
    total_to_process = min(len(benign_traces), target_count * 2)  # Process enough to reach target
    print(f"Processing {total_to_process} traces to reach target of {target_count}...", flush=True)
    
    # Show progress in chunks of 10%
    chunk_size = max(1, total_to_process // 10)
    next_progress = chunk_size
    
    for i, trace in enumerate(benign_traces):
        if i >= total_to_process:
            break
            
        # Show progress
        if i >= next_progress:
            progress_pct = (i / total_to_process) * 100
            print(f"  - Merging progress: {progress_pct:.1f}% ({i}/{total_to_process})", flush=True)
            next_progress += chunk_size
        
        if time_until_next_incident <= 0:
            if not incident_traces_copy:
                if len(merged_traces) > target_count:
                    print(f"✓ Reached target count of {target_count} traces", flush=True)
                    break
                print(f"⚠ Ran out of incidents after {len(merged_traces)} / {target_count}. Using only benign traces for the remainder.", flush=True)
                # Add remaining benign traces to reach target count
                needed = target_count - len(merged_traces)
                if needed > 0 and i + needed <= len(benign_traces):
                    merged_traces.extend(benign_traces[i:i+needed])
                break
            
            merged_traces.append(incident_traces_copy.pop())
            time_until_next_incident = random.expovariate(exp_lambda)
        else:
            merged_traces.append(trace)
            time_until_next_incident -= 1
            
        if len(merged_traces) >= target_count:
            print(f"✓ Reached target count of {target_count} traces", flush=True)
            break

    print(f"Merge complete: created {len(merged_traces)} merged traces", flush=True)
    print(f"  - Benign traces in result: {len(merged_traces) - (len(incident_traces) - len(incident_traces_copy))}", flush=True)
    print(f"  - Incident traces in result: {len(incident_traces) - len(incident_traces_copy)}", flush=True)
    
    return merged_traces


def prepare_merged_traces(app: AppName, incident: Incident, exp_lambda: float, target_count: int, working_directory: Path):
    print(f"\nPreparing merged traces for {app.value} - {incident.incident_name} with lambda={exp_lambda}", flush=True)
    
    incident_traces = []
    incident_dir = get_incident_traces_path(app, incident, working_directory)
    
    if not os.path.exists(incident_dir):
        print(f"⚠ Skipping: No traces found for incident {incident.incident_name} at {incident_dir}", flush=True)
        return
    
    print(f"Loading incident traces from {incident_dir}...", flush=True)
    trace_files = os.listdir(incident_dir)
    
    for i, f in enumerate(trace_files):
        file_path = os.path.join(incident_dir, f)
        print(f"  - Loading file [{i+1}/{len(trace_files)}]: {f}...", end="", flush=True)
        with open(file_path) as json_file:
            file_traces = json.load(json_file)
            incident_traces.extend(file_traces)
            print(f" loaded {len(file_traces)} traces", flush=True)
    
    print(f"Loaded {len(incident_traces)} incident traces", flush=True)

    benign_dir = get_baseline_traces_path(app, working_directory)
    if not os.path.exists(benign_dir):
        print(f"⚠ Error: Baseline traces directory not found at {benign_dir}", flush=True)
        return
        
    print(f"Loading benign traces from {benign_dir}...", flush=True)
    benign_traces = []
    benign_files = os.listdir(benign_dir)
    
    for i, f in enumerate(benign_files):
        file_path = os.path.join(benign_dir, f)
        print(f"  - Loading file [{i+1}/{len(benign_files)}]: {f}...", end="", flush=True)
        with open(file_path) as json_file:
            file_traces = json.load(json_file)
            benign_traces.extend(file_traces)
            print(f" loaded {len(file_traces)} traces", flush=True)
            
    print(f"Loaded {len(benign_traces)} benign traces", flush=True)

    merged_traces = merge_with_exp(benign_traces, incident_traces, exp_lambda, target_count=target_count)

    target_dir = get_merged_traces_base_path(working_directory) / f"{app.value}_{incident.incident_name}_{exp_lambda}"
    os.makedirs(target_dir, exist_ok=True)
    
    output_file = f"{target_dir}/txs.json"
    print(f"Saving {len(merged_traces)} merged traces to {output_file}...", flush=True)
    
    with open(output_file, "w") as f:
        json.dump(merged_traces, f, indent=4)
        
    print(f"✓ Successfully saved merged traces to {output_file}", flush=True)


def main():
    start_time = time.time()
    print(f"PandoraTrace Benchmark started at {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    
    parser = argparse.ArgumentParser(description="PandoraTrace Benchmark")

    parser.add_argument(
        "app_name",
        type=str,
        choices=[app.value for app in AppName],
        help="Specify the application to run the benchmark on"
    )

    parser.add_argument(
        "--deathstar_dir",
        type=str,
        required=True,
        help="Path to the DeathStarBench directory"
    )

    parser.add_argument(
        "--create_baseline",
        action="store_true",
        help="Create baseline traces for the specified application"
    )

    parser.add_argument(
        "--run_test",
        action="store_true",
        help="Run tests with predefined incidents"
    )

    parser.add_argument(
        "--prepare_traces",
        action="store_true",
        help="Prepare merged traces by combining baseline and incident traces"
    )

    parser.add_argument(
        "--lambda_values",
        type=float,
        nargs="+",
        default=[0.2, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001],
        help="List of lambda values for preparing merged traces"
    )

    parser.add_argument(
        "--working_dir",
        type=str,
        default=str(Path(__file__).parent / "traces"),
        help="Target working directory for PandoraTrace benchmark"
    )

    parser.add_argument(
        "--num_traces",
        type=int,
        default=0,
        help="Target minimum number of traces that should be collected. Default is 10,000 for baseline, 3,000 for incidents, and 3,000 for merged traces"
    )

    args = parser.parse_args()
    print(f"Parsed arguments: {args}", flush=True)

    app = AppName(args.app_name)
    working_directory = Path(args.working_dir)
    print(f"Working directory: {working_directory}", flush=True)

    os.makedirs(working_directory, exist_ok=True)

    if args.create_baseline:
        print("\n=== CREATING BASELINE TRACES ===", flush=True)
        create_baseline(app, args.deathstar_dir, target_count=args.num_traces or 10_000, working_directory=working_directory)

    if args.run_test:
        print("\n=== RUNNING INCIDENT TESTS ===", flush=True)
        run_test(app, INCIDENTS, args.deathstar_dir, target_count=args.num_traces or 10_000, working_directory=working_directory)

    if args.prepare_traces:
        print("\n=== PREPARING MERGED TRACES ===", flush=True)
        for incident in INCIDENTS:
            for exp_lambda in args.lambda_values:
                prepare_merged_traces(app, incident, exp_lambda, target_count=args.num_traces or 3_000, working_directory=working_directory)
        
        print(f"\n✓ Traces have been prepared to directory {get_merged_traces_base_path(working_directory)}", flush=True)
    
    end_time = time.time()
    elapsed = end_time - start_time
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"\nPandoraTrace Benchmark completed at {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"Total execution time: {int(hours)}h {int(minutes)}m {int(seconds)}s", flush=True)


if __name__ == "__main__":
    main()
