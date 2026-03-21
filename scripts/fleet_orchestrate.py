#!/usr/bin/env python3
"""Orchestrate Phonetic Prior experiments across a fleet of Vast.ai instances.

Discovers running instances, sets them up, distributes jobs, launches, and collects results.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

GH_TOKEN = subprocess.check_output(["gh", "auth", "token"], text=True).strip()

# All 43 experiments
JOBS = []
# Validation: 25 pairs
for lost in ["grc", "lat", "san", "osc", "xum"]:
    for known in ["grc", "lat", "san", "ang", "osc", "xum"]:
        if lost != known:
            JOBS.append(f"--validation-only --job-start {len(JOBS)} --job-end {len(JOBS)+1}")
# Fix: rebuild job list properly
JOBS = []
idx = 0
for lost in ["grc", "lat", "san", "osc", "xum"]:
    for known in ["grc", "lat", "san", "ang", "osc", "xum"]:
        if lost != known:
            idx += 1
# Linear A: 18 targets
for _ in range(18):
    idx += 1
# Total = 43

def ssh_cmd(host, port, cmd, timeout=30):
    """Run a command on a remote host via SSH."""
    try:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
             "-p", str(port), f"root@{host}", cmd],
            capture_output=True, text=True, timeout=timeout,
        )
        return result.stdout.strip(), result.returncode
    except (subprocess.TimeoutExpired, Exception) as e:
        return str(e), -1


def get_running_instances():
    """Get list of running Vast.ai instances."""
    result = subprocess.run(
        ["vastai", "show", "instances", "--raw"],
        capture_output=True, text=True,
    )
    instances = json.loads(result.stdout)
    running = []
    for inst in instances:
        if inst["actual_status"] == "running":
            running.append({
                "id": inst["id"],
                "host": inst.get("ssh_host", ""),
                "port": inst.get("ssh_port", ""),
                "cpus": inst.get("cpu_cores_effective", 0),
            })
    return running


def setup_instance(inst):
    """Setup an instance: install deps, clone repos."""
    host, port = inst["host"], inst["port"]
    print(f"  Setting up {inst['id']} ({host}:{port})...", flush=True)

    # Check if already setup
    out, rc = ssh_cmd(host, port, "ls /workspace/phonetic-prior-v2/src 2>/dev/null && echo ALREADY_SETUP", timeout=15)
    if "ALREADY_SETUP" in out:
        # Just pull latest
        ssh_cmd(host, port,
                f"cd /workspace/ancient-scripts-datasets && git pull https://{GH_TOKEN}@github.com/Nacryos/ancient-scripts-datasets.git master 2>/dev/null",
                timeout=30)
        print(f"    {inst['id']}: already setup, pulled latest", flush=True)
        return True

    # Full setup
    setup_cmd = (
        f"cd /workspace && "
        f"pip install -q ipapy tqdm 2>/dev/null && "
        f"GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 https://{GH_TOKEN}@github.com/Project-Phaistos/phonetic-prior-v2.git 2>/dev/null && "
        f"git clone --depth 1 https://{GH_TOKEN}@github.com/Nacryos/ancient-scripts-datasets.git 2>/dev/null && "
        f"export PYTHONPATH=/workspace/phonetic-prior-v2/src:$PYTHONPATH && "
        f"python -c 'from phonetic_prior_v2.model.cognate_validator import CognateValidator; print(\"OK\")'"
    )
    out, rc = ssh_cmd(host, port, setup_cmd, timeout=180)
    if "OK" in out:
        print(f"    {inst['id']}: setup complete", flush=True)
        return True
    else:
        print(f"    {inst['id']}: setup FAILED: {out[:100]}", flush=True)
        return False


def launch_jobs(inst, job_start, job_end):
    """Launch experiments on an instance."""
    host, port = inst["host"], inst["port"]
    n_jobs = job_end - job_start
    cmd = (
        f"cd /workspace && rm -rf outputs && "
        f"export PYTHONPATH=/workspace/phonetic-prior-v2/src:$PYTHONPATH && "
        f"nohup python -u /workspace/ancient-scripts-datasets/scripts/run_proper_validation.py "
        f"--data-dir /workspace/ancient-scripts-datasets "
        f"--workers 1 --output-dir /workspace/outputs "
        f"--job-start {job_start} --job-end {job_end} "
        f"> /workspace/run.log 2>&1 &"
    )
    out, rc = ssh_cmd(host, port, cmd, timeout=15)
    print(f"  {inst['id']}: launched jobs {job_start}-{job_end-1} ({n_jobs} experiments)", flush=True)


def check_progress(inst):
    """Check how many experiments have completed on an instance."""
    host, port = inst["host"], inst["port"]
    out, rc = ssh_cmd(host, port,
                      "find /workspace/outputs -name 'result.json' 2>/dev/null | wc -l",
                      timeout=10)
    try:
        return int(out.strip())
    except:
        return 0


def collect_results(inst, local_dir):
    """Download results from an instance."""
    host, port = inst["host"], inst["port"]
    subprocess.run(
        ["scp", "-r", "-P", str(port), "-o", "StrictHostKeyChecking=no",
         f"root@{host}:/workspace/outputs/", str(local_dir / f"machine_{inst['id']}")],
        capture_output=True, timeout=60,
    )


def main():
    print("=== Fleet Orchestrator for Phonetic Prior ===\n")

    # 1. Get running instances
    print("Step 1: Finding running instances...")
    instances = get_running_instances()
    print(f"  Found {len(instances)} running instances\n")

    if not instances:
        print("ERROR: No running instances found!")
        return

    # 2. Setup all instances
    print("Step 2: Setting up instances...")
    ready = []
    for inst in instances:
        if setup_instance(inst):
            ready.append(inst)

    print(f"\n  {len(ready)} instances ready\n")

    if not ready:
        print("ERROR: No instances ready!")
        return

    # 3. Distribute 43 jobs across ready instances
    print("Step 3: Distributing jobs...")
    n_machines = len(ready)
    jobs_per_machine = 43 // n_machines
    remainder = 43 % n_machines

    job_idx = 0
    assignments = []
    for i, inst in enumerate(ready):
        n = jobs_per_machine + (1 if i < remainder else 0)
        start = job_idx
        end = job_idx + n
        assignments.append((inst, start, end))
        job_idx = end

    # 4. Launch on all machines
    print("Step 4: Launching experiments...")
    for inst, start, end in assignments:
        launch_jobs(inst, start, end)

    print(f"\n  All {job_idx} experiments launched across {len(ready)} machines")
    print(f"  ~{jobs_per_machine} experiments per machine, ~45 min each")
    print(f"  Estimated completion: {jobs_per_machine * 45 / 60:.1f} hours\n")

    # 5. Monitor progress
    print("Step 5: Monitoring (checking every 5 min)...")
    total_expected = job_idx
    while True:
        time.sleep(300)
        total_done = 0
        for inst, start, end in assignments:
            n = check_progress(inst)
            total_done += n
        print(f"  Progress: {total_done}/{total_expected} experiments complete", flush=True)
        if total_done >= total_expected:
            break

    # 6. Collect results
    print("\nStep 6: Collecting results...")
    local_dir = Path("fleet_results")
    local_dir.mkdir(exist_ok=True)
    for inst, _, _ in assignments:
        collect_results(inst, local_dir)

    print(f"\nResults collected to: {local_dir}")
    print("=== DONE ===")


if __name__ == "__main__":
    main()
