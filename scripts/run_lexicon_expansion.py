#!/usr/bin/env python3
"""Orchestrator for the full lexicon expansion pipeline.

Runs all phases sequentially:
  Phase 1: Re-run fixed existing extraction scripts (limits removed)
  Phase 2: Wiktionary category expansion (7 languages)
  Phase 3: Dedicated source scrapers (5 languages)
  Phase 4: Report results

Each phase can be run independently with --phase N.

Usage:
    python scripts/run_lexicon_expansion.py [--phase N] [--dry-run]
    python scripts/run_lexicon_expansion.py --phase 1     # Re-extract existing
    python scripts/run_lexicon_expansion.py --phase 2     # Wiktionary expansion
    python scripts/run_lexicon_expansion.py --phase 3     # Dedicated sources
    python scripts/run_lexicon_expansion.py --report       # Just show counts
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
LEXICON_DIR = ROOT / "data" / "training" / "lexicons"


def count_entries(tsv_path: Path) -> int:
    """Count data rows in a TSV file (excluding header)."""
    if not tsv_path.exists():
        return 0
    with open(tsv_path, "r", encoding="utf-8") as f:
        return sum(1 for line in f if not line.startswith("Word\t")) - 0


def report():
    """Print current entry counts for all 18 ancient language TSVs."""
    files = {
        "ine-pro": "Proto-Indo-European",
        "uga": "Ugaritic",
        "peo": "Old Persian",
        "ave": "Avestan",
        "dra-pro": "Proto-Dravidian",
        "hit": "Hittite",
        "sem-pro": "Proto-Semitic",
        "ccs-pro": "Proto-Kartvelian",
        "phn": "Phoenician",
        "elx": "Elamite",
        "xpg": "Phrygian",
        "xur": "Urartian",
        "xlc": "Lycian",
        "xld": "Lydian",
        "xcr": "Carian",
        "xle": "Lemnian",
        "cms": "Messapic",
        "xrr": "Rhaetic",
    }

    print(f"{'ISO':10s} {'Name':25s} {'Entries':>8s}")
    print("-" * 45)
    total = 0
    for iso, name in files.items():
        path = LEXICON_DIR / f"{iso}.tsv"
        n = count_entries(path)
        total += n
        print(f"{iso:10s} {name:25s} {n:8d}")
    print("-" * 45)
    print(f"{'TOTAL':36s} {total:8d}")


def run_script(script_name: str, extra_args: list[str] | None = None):
    """Run a Python script as a subprocess."""
    cmd = [sys.executable, str(SCRIPTS / script_name)]
    if extra_args:
        cmd.extend(extra_args)
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=str(ROOT))
    if result.returncode != 0:
        print(f"WARNING: {script_name} exited with code {result.returncode}")
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run lexicon expansion pipeline")
    parser.add_argument("--phase", type=int, help="Run specific phase (1, 2, or 3)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report", action="store_true", help="Just show counts")
    args = parser.parse_args()

    if args.report:
        report()
        return

    extra = ["--dry-run"] if args.dry_run else []

    print("BEFORE expansion:")
    report()

    if args.phase is None or args.phase == 1:
        print("\n\n" + "#" * 60)
        print("# PHASE 1: Re-run existing extraction scripts (limits removed)")
        print("#" * 60)
        run_script("extract_ave_peo_xpg.py")
        run_script("extract_wiktionary_lexicons.py")
        run_script("extract_phn_elx.py")
        run_script("extract_pie_urartian.py")
        run_script("extract_anatolian_lexicons.py")
        run_script("extract_rhaetic_messapic.py")

    if args.phase is None or args.phase == 2:
        print("\n\n" + "#" * 60)
        print("# PHASE 2: Wiktionary category expansion (7 languages)")
        print("#" * 60)
        run_script("expand_wiktionary_categories.py", extra)

    if args.phase is None or args.phase == 3:
        print("\n\n" + "#" * 60)
        print("# PHASE 3: Dedicated source scrapers")
        print("#" * 60)
        run_script("scrape_avesta.py", extra)
        run_script("scrape_oracc_urartian.py", extra)
        run_script("scrape_palaeolexicon.py", extra)
        run_script("expand_xpg.py", extra)
        run_script("scrape_tir_rhaetic.py", extra)

    print("\n\nAFTER expansion:")
    report()


if __name__ == "__main__":
    main()
