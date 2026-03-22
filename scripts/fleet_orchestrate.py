#!/usr/bin/env python3
"""Robust fleet orchestrator for Phonetic Prior validation on Vast.ai.

Fixes from failed first attempt:
- No rm -rf outputs (use mkdir -p, timestamped dirs)
- Kill existing processes before launching new ones
- Always collect results before destroying machines
- Proper job distribution with explicit experiment list
- Health checks: verify process is alive, not just file counts
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

GH_TOKEN = subprocess.check_output(["gh", "auth", "token"], text=True).strip()

# Build explicit experiment list (43 total)
EXPERIMENTS = []
# Track A: Validation — 25 pairs
for lost in ["grc", "lat", "san", "osc", "xum"]:
    for known in ["grc", "lat", "san", "ang", "osc", "xum"]:
        if lost != known:
            EXPERIMENTS.append({"idx": len(EXPERIMENTS), "lost": lost, "known": known, "type": "validation"})
# Track B: Linear A — 18 pairs
for known in ["hit", "uga", "phn", "xur", "elx", "ave", "peo",
              "xld", "xlc", "xcr", "xpg", "xle", "xrr", "cms",
              "ine-pro", "sem-pro", "ccs-pro", "dra-pro"]:
    EXPERIMENTS.append({"idx": len(EXPERIMENTS), "lost": "linear_a", "known": known, "type": "linear_a"})

assert len(EXPERIMENTS) == 43, f"Expected 43 experiments, got {len(EXPERIMENTS)}"


def ssh_cmd(host, port, cmd, timeout=30):
    """Run a command on a remote host via SSH."""
    try:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
             "-p", str(port), f"root@{host}", cmd],
            capture_output=True, text=True, timeout=timeout,
        )
        return result.stdout.strip(), result.returncode
    except subprocess.TimeoutExpired:
        return "TIMEOUT", -1
    except Exception as e:
        return str(e), -1


def get_running_instances():
    """Get list of running Vast.ai instances."""
    result = subprocess.run(
        ["vastai", "show", "instances", "--raw"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"ERROR: vastai show instances failed: {result.stderr}", flush=True)
        return []
    instances = json.loads(result.stdout)
    running = []
    for inst in instances:
        if inst.get("actual_status") == "running":
            running.append({
                "id": inst["id"],
                "host": inst.get("ssh_host", ""),
                "port": inst.get("ssh_port", ""),
                "cpus": inst.get("cpu_cores_effective", 0),
                "ram_gb": inst.get("cpu_ram", 0) / 1024,
            })
    return running


def setup_instance(inst):
    """Setup an instance: install deps, clone repos, verify imports."""
    host, port = inst["host"], inst["port"]
    print(f"  Setting up {inst['id']} ({host}:{port}, {inst['cpus']:.0f} cores, {inst['ram_gb']:.0f} GB)...", flush=True)

    # Kill any existing python processes to prevent duplicate workers
    ssh_cmd(host, port, "pkill -f run_proper_validation 2>/dev/null", timeout=10)

    # Check if already setup
    out, rc = ssh_cmd(host, port,
        "export PYTHONPATH=/workspace/phonetic-prior-v2/src:$PYTHONPATH && "
        "python -c 'from phonetic_prior_v2.model.cognate_validator import CognateValidator; print(\"READY\")' 2>&1",
        timeout=30)
    if "READY" in out:
        # Pull latest data repo (to get fixed runner script)
        ssh_cmd(host, port,
            f"cd /workspace/ancient-scripts-datasets && git pull https://{GH_TOKEN}@github.com/Nacryos/ancient-scripts-datasets.git master 2>/dev/null",
            timeout=60)
        print(f"    {inst['id']}: already setup, pulled latest", flush=True)
        return True

    # Full setup
    cmds = [
        "cd /workspace",
        "pip install -q ipapy tqdm 2>/dev/null",
        f"GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 https://{GH_TOKEN}@github.com/Project-Phaistos/phonetic-prior-v2.git 2>/dev/null",
        f"git clone --depth 1 https://{GH_TOKEN}@github.com/Nacryos/ancient-scripts-datasets.git 2>/dev/null",
        "export PYTHONPATH=/workspace/phonetic-prior-v2/src:$PYTHONPATH",
        "python -c 'from phonetic_prior_v2.model.cognate_validator import CognateValidator; print(\"READY\")'",
    ]
    setup_cmd = " && ".join(cmds)
    out, rc = ssh_cmd(host, port, setup_cmd, timeout=300)
    if "READY" in out:
        print(f"    {inst['id']}: setup complete", flush=True)
        return True
    else:
        print(f"    {inst['id']}: setup FAILED: {out[:200]}", flush=True)
        return False


def launch_jobs(inst, job_start, job_end):
    """Launch experiments on an instance. Does NOT destroy existing outputs."""
    host, port = inst["host"], inst["port"]
    n_jobs = job_end - job_start

    # Kill any lingering processes first
    ssh_cmd(host, port, "pkill -f run_proper_validation 2>/dev/null", timeout=10)
    time.sleep(1)

    # Create output dir (never rm -rf!)
    ssh_cmd(host, port, "mkdir -p /workspace/outputs", timeout=5)

    # Write PID file for health checks
    cmd = (
        f"cd /workspace && "
        f"export PYTHONPATH=/workspace/phonetic-prior-v2/src:$PYTHONPATH && "
        f"nohup python -u /workspace/ancient-scripts-datasets/scripts/run_proper_validation.py "
        f"--data-dir /workspace/ancient-scripts-datasets "
        f"--workers 1 --output-dir /workspace/outputs "
        f"--job-start {job_start} --job-end {job_end} "
        f"> /workspace/run.log 2>&1 & echo $! > /workspace/.pid"
    )
    out, rc = ssh_cmd(host, port, cmd, timeout=15)

    # Verify process started
    time.sleep(2)
    pid_out, _ = ssh_cmd(host, port, "cat /workspace/.pid 2>/dev/null && ps aux | grep run_proper | grep -v grep | wc -l", timeout=10)
    print(f"  {inst['id']}: launched jobs {job_start}-{job_end-1} ({n_jobs} experiments) [pid check: {pid_out}]", flush=True)


def check_health(inst):
    """Check instance health: process alive + completed count."""
    host, port = inst["host"], inst["port"]
    out, rc = ssh_cmd(host, port,
        "echo PROC=$(ps aux | grep run_proper | grep -v grep | wc -l) && "
        "echo DONE=$(find /workspace/outputs -name 'result.json' 2>/dev/null | wc -l) && "
        "tail -1 /workspace/run.log 2>/dev/null",
        timeout=15)
    return out


def collect_results(inst, local_dir):
    """Download ALL results from an instance."""
    host, port = inst["host"], inst["port"]
    dest = local_dir / f"machine_{inst['id']}"
    dest.mkdir(parents=True, exist_ok=True)

    # Collect outputs
    result = subprocess.run(
        ["scp", "-r", "-P", str(port), "-o", "StrictHostKeyChecking=no",
         f"root@{host}:/workspace/outputs/", str(dest)],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        print(f"    {inst['id']}: scp outputs FAILED: {result.stderr[:100]}", flush=True)

    # Also collect run log
    subprocess.run(
        ["scp", "-P", str(port), "-o", "StrictHostKeyChecking=no",
         f"root@{host}:/workspace/run.log", str(dest / "run.log")],
        capture_output=True, timeout=30,
    )
    print(f"    {inst['id']}: results collected to {dest}", flush=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["deploy", "monitor", "collect", "full"], default="full")
    parser.add_argument("--local-dir", type=Path, default=Path("data/fleet_results_v2"))
    args = parser.parse_args()

    print(f"=== Fleet Orchestrator v2 ({len(EXPERIMENTS)} experiments) ===\n")

    # 1. Get running instances
    print("Step 1: Finding running instances...")
    instances = get_running_instances()
    print(f"  Found {len(instances)} running instances\n")
    if not instances:
        print("ERROR: No running instances. Rent some first:")
        print("  vastai create instance <offer_id> --image pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime --disk 20")
        return

    if args.action in ("deploy", "full"):
        # 2. Setup
        print("Step 2: Setting up instances...")
        ready = []
        for inst in instances:
            if setup_instance(inst):
                ready.append(inst)
        print(f"\n  {len(ready)}/{len(instances)} instances ready\n")
        if not ready:
            print("ERROR: No instances ready!")
            return

        # 3. Distribute
        print("Step 3: Distributing 43 experiments...")
        n = len(ready)
        per = 43 // n
        rem = 43 % n
        job_idx = 0
        assignments = []
        for i, inst in enumerate(ready):
            count = per + (1 if i < rem else 0)
            assignments.append((inst, job_idx, job_idx + count))
            exps = EXPERIMENTS[job_idx:job_idx + count]
            names = [f"{e['lost']}_vs_{e['known']}" for e in exps]
            print(f"  {inst['id']}: jobs {job_idx}-{job_idx+count-1} ({count} exps): {', '.join(names)}")
            job_idx += count

        # 4. Launch
        print("\nStep 4: Launching experiments...")
        for inst, start, end in assignments:
            launch_jobs(inst, start, end)

        est_hours = (per + 1) * 55 / 60
        print(f"\n  Estimated wall clock: ~{est_hours:.1f} hours")
        print(f"  Estimated cost: {len(ready)} machines x {est_hours:.1f}hr x $0.06 = ${len(ready)*est_hours*0.06:.2f}\n")

    if args.action in ("monitor", "full"):
        # 5. Monitor
        print("Step 5: Monitoring (every 5 min, Ctrl+C to stop)...")
        try:
            while True:
                time.sleep(300)
                total_done = 0
                for inst in instances:
                    health = check_health(inst)
                    # Parse DONE count
                    for line in health.split("\n"):
                        if line.startswith("DONE="):
                            try:
                                total_done += int(line.split("=")[1])
                            except ValueError:
                                pass
                    print(f"  {inst['id']}: {health}", flush=True)
                print(f"\n  Total: {total_done}/43 experiments complete\n", flush=True)
                if total_done >= 43:
                    print("  ALL EXPERIMENTS COMPLETE!")
                    break
        except KeyboardInterrupt:
            print("\n  Monitoring stopped by user. Use --action collect to gather results.\n")

    if args.action in ("collect", "full"):
        # 6. Collect — ALWAYS do this before destroying machines
        print("Step 6: Collecting results...")
        args.local_dir.mkdir(parents=True, exist_ok=True)
        for inst in instances:
            collect_results(inst, args.local_dir)
        print(f"\n  All results collected to: {args.local_dir}")
        print("  SAFE to destroy instances now.\n")

    print("=== DONE ===")


if __name__ == "__main__":
    main()
