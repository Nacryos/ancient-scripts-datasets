#!/usr/bin/env python3
"""Download inscription / running-text corpora for the ancient inscription dataset.

Sources (all CC-BY-SA 4.0 or more permissive):
  - UD_Ancient_Greek-PTNK   (CC BY-SA 4.0)  — Pentateuch/Codex Alexandrinus
  - UD_Latin-LLCT            (CC BY-SA 4.0)  — 521 Early Medieval charters
  - UD_Latin-CIRCSE          (CC BY-SA 4.0)  — Seneca tragedies + Tacitus Germania
  - UD_Sanskrit-Vedic        (CC BY-SA 4.0)  — Vedic Sanskrit texts
  - UD_Old_English-OEDT      (CC BY-SA 4.0)  — Old English sentences
  - CEIPoM v1.3 (Zenodo)     (CC BY-SA 4.0)  — Italic inscriptions (Oscan, Umbrian)

Usage:
    python scripts/ingest_inscriptions.py [--output-dir DIR]
"""

import argparse
import os
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

# UD treebank repos to shallow-clone
UD_REPOS = {
    "UD_Ancient_Greek-PTNK": "https://github.com/UniversalDependencies/UD_Ancient_Greek-PTNK.git",
    "UD_Latin-LLCT": "https://github.com/UniversalDependencies/UD_Latin-LLCT.git",
    "UD_Latin-CIRCSE": "https://github.com/UniversalDependencies/UD_Latin-CIRCSE.git",
    "UD_Sanskrit-Vedic": "https://github.com/UniversalDependencies/UD_Sanskrit-Vedic.git",
    "UD_Old_English-OEDT": "https://github.com/UniversalDependencies/UD_Old_English-OEDT.git",
}

# CEIPoM Zenodo download
CEIPOM_ZENODO_URL = "https://zenodo.org/records/6475427/files/CEIPoM_v1.3.zip"
CEIPOM_ZIP_NAME = "CEIPoM_v1.3.zip"


def clone_ud_treebank(name: str, url: str, output_dir: Path) -> None:
    """Shallow-clone a UD treebank repo."""
    target = output_dir / name
    if target.exists():
        print(f"  [skip] {name} already exists at {target}")
        return
    print(f"  [clone] {name} ...")
    subprocess.run(
        ["git", "clone", "--depth", "1", url, str(target)],
        check=True,
        capture_output=True,
        text=True,
    )
    print(f"  [done] {name}")


def download_ceipom(output_dir: Path) -> None:
    """Download CEIPoM v1.3 from Zenodo and extract."""
    ceipom_dir = output_dir / "CEIPoM"
    if ceipom_dir.exists():
        print(f"  [skip] CEIPoM already exists at {ceipom_dir}")
        return

    zip_path = output_dir / CEIPOM_ZIP_NAME
    if not zip_path.exists():
        print(f"  [download] CEIPoM v1.3 from Zenodo ...")
        urllib.request.urlretrieve(CEIPOM_ZENODO_URL, str(zip_path))
        print(f"  [done] Downloaded {zip_path.stat().st_size / 1024:.0f} KB")

    print(f"  [extract] CEIPoM ...")
    with zipfile.ZipFile(str(zip_path), "r") as zf:
        zf.extractall(str(output_dir))

    # Zenodo ZIPs often have a nested directory; find it
    extracted = [d for d in output_dir.iterdir()
                 if d.is_dir() and "ceipom" in d.name.lower() and d.name != "CEIPoM"]
    if extracted:
        extracted[0].rename(ceipom_dir)
    elif not ceipom_dir.exists():
        # Extracted flat — just create a marker
        ceipom_dir.mkdir(exist_ok=True)
        print("  [warn] Could not find CEIPoM directory after extraction; "
              "check output_dir manually.")

    print(f"  [done] CEIPoM extracted")


def main():
    parser = argparse.ArgumentParser(description="Download inscription text corpora")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/training/raw/inscriptions"),
        help="Directory to store downloaded data (default: data/training/raw/inscriptions)",
    )
    args = parser.parse_args()

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=== Downloading UD treebanks ===")
    for name, url in UD_REPOS.items():
        try:
            clone_ud_treebank(name, url, output_dir)
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] Failed to clone {name}: {e.stderr}", file=sys.stderr)

    print("\n=== Downloading CEIPoM ===")
    try:
        download_ceipom(output_dir)
    except Exception as e:
        print(f"  [ERROR] Failed to download CEIPoM: {e}", file=sys.stderr)

    print("\n=== Summary ===")
    for item in sorted(output_dir.iterdir()):
        if item.is_dir():
            conllu_files = list(item.glob("*.conllu"))
            csv_files = list(item.glob("*.csv"))
            if conllu_files:
                print(f"  {item.name}: {len(conllu_files)} CoNLL-U files")
            elif csv_files:
                print(f"  {item.name}: {len(csv_files)} CSV files")
            else:
                print(f"  {item.name}: (directory)")


if __name__ == "__main__":
    main()
