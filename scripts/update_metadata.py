#!/usr/bin/env python3
"""Update data/training/metadata/languages.tsv from actual TSV files on disk.

Scans all .tsv files in data/training/lexicons/, counts entries, extracts
unique sources, and merges into the metadata file. Does NOT remove existing
entries — only adds missing ones or updates counts/sources for existing ones.

Iron Rule: This script does NOT add data. It only updates metadata bookkeeping.

Usage:
    python scripts/update_metadata.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
METADATA_PATH = ROOT / "data" / "training" / "metadata" / "languages.tsv"

logger = logging.getLogger(__name__)

# Known ancient language metadata (ISO -> (Family, Glottocode))
# Sourced from Glottolog and standard comparative linguistics references
ANCIENT_LANG_INFO: dict[str, tuple[str, str]] = {
    "hit": ("anatolian", "hitt1242"),
    "xlw": ("anatolian", "hier1240"),
    "xur": ("hurro_urartian", "urar1245"),
    "xhu": ("hurro_urartian", "hurr1240"),
    "uga": ("semitic", "ugar1238"),
    "phn": ("semitic", "phoe1239"),
    "elx": ("isolates", "elam1244"),
    "ave": ("indo_iranian", "aves1237"),
    "peo": ("indo_iranian", "oldp1254"),
    "ett": ("tyrsenian", "etru1241"),
    "xlc": ("anatolian", "lyci1241"),
    "xld": ("anatolian", "lydi1241"),
    "xcr": ("anatolian", "cari1275"),
    "xpg": ("paleo_balkan", "phry1239"),
    "xle": ("anatolian", "leps1239"),
    "xrr": ("tyrsenian", "raet1238"),
    "cms": ("anatolian", "cimm1244"),
    "txb": ("tocharian", "tokh1243"),
    "xto": ("tocharian", "tokh1242"),
    "ine-pro": ("indo_european", "indo1319"),
    "sem-pro": ("afroasiatic_semitic", "semi1276"),
    "ccs-pro": ("kartvelian", "kart1248"),
    "dra-pro": ("dravidian", "drav1251"),
}


def scan_lexicons() -> dict[str, dict]:
    """Scan all TSV files and return {iso: {entries, sources}}."""
    result = {}
    for tsv_path in sorted(LEXICON_DIR.glob("*.tsv")):
        iso = tsv_path.stem
        with open(tsv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) <= 1:
            continue
        entries = len(lines) - 1  # subtract header
        # Extract unique sources from column 3 (0-indexed)
        sources = set()
        for line in lines[1:]:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 4 and parts[3] and parts[3] != "-":
                sources.add(parts[3])
        result[iso] = {
            "entries": entries,
            "sources": ",".join(sorted(sources)) if sources else "unknown",
        }
    return result


def load_metadata() -> tuple[str, dict[str, dict]]:
    """Load existing metadata. Returns (header, {iso: {family, glottocode, entries, sources}})."""
    existing = {}
    header = "ISO\tFamily\tGlottocode\tEntries\tSources\n"
    if METADATA_PATH.exists():
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if lines:
            header = lines[0]
            for line in lines[1:]:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 5:
                    existing[parts[0]] = {
                        "family": parts[1],
                        "glottocode": parts[2],
                        "entries": int(parts[3]) if parts[3].isdigit() else 0,
                        "sources": parts[4],
                    }
    return header, existing


def main():
    parser = argparse.ArgumentParser(description="Update languages.tsv metadata")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    header, existing = load_metadata()
    disk = scan_lexicons()

    added = 0
    updated = 0

    for iso, info in disk.items():
        if iso in existing:
            # Update entry count and sources if they've changed
            old_entries = existing[iso]["entries"]
            if old_entries != info["entries"] or existing[iso]["sources"] != info["sources"]:
                existing[iso]["entries"] = info["entries"]
                existing[iso]["sources"] = info["sources"]
                updated += 1
                logger.info("UPDATE %s: %d -> %d entries", iso, old_entries, info["entries"])
        else:
            # New language — look up family/glottocode from our table
            if iso in ANCIENT_LANG_INFO:
                family, glottocode = ANCIENT_LANG_INFO[iso]
            else:
                family = "unknown"
                glottocode = ""
            existing[iso] = {
                "family": family,
                "glottocode": glottocode,
                "entries": info["entries"],
                "sources": info["sources"],
            }
            added += 1
            logger.info("ADD %s: %d entries (%s)", iso, info["entries"], family)

    # Sort by entry count descending (same as original)
    sorted_isos = sorted(existing.keys(), key=lambda x: existing[x]["entries"], reverse=True)

    if not args.dry_run and (added > 0 or updated > 0):
        METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(METADATA_PATH, "w", encoding="utf-8") as f:
            f.write(header)
            for iso in sorted_isos:
                e = existing[iso]
                f.write(f"{iso}\t{e['family']}\t{e['glottocode']}\t{e['entries']}\t{e['sources']}\n")

    print(f"\n{'DRY RUN: ' if args.dry_run else ''}Metadata update complete:")
    print(f"  Added:   {added}")
    print(f"  Updated: {updated}")
    print(f"  Total:   {len(existing)}")


if __name__ == "__main__":
    main()
