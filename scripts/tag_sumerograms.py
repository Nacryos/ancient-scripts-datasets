#!/usr/bin/env python3
"""Tag Sumerogram/logogram entries in cuneiform-language TSV files.

Sumerograms are uppercase Sumerian logograms used as shorthand in cuneiform
texts (e.g., LUGAL = king, URU = city, DINGIR = god). They are NOT phonemic
data in the target language. This script tags them in the Concept_ID field
with a 'sumerogram:' prefix so downstream pipelines can filter if needed.

The entries are PRESERVED (not deleted) because they are legitimate scholarly
data — they just shouldn't be used for phonetic comparison.

Iron Rule: This script does NOT add data. It only modifies Concept_ID tags.

Usage:
    python scripts/tag_sumerograms.py [--dry-run] [--language ISO]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"

logger = logging.getLogger(__name__)

# Languages written in cuneiform that may contain Sumerograms
CUNEIFORM_LANGUAGES = ["hit", "xlw", "xur", "xhu"]

# Known Sumerogram patterns
KNOWN_SUMEROGRAMS = {
    "LUGAL", "URU", "DINGIR", "LU", "MUNUS", "GIS", "KUR", "E",
    "AN", "EN", "NIN", "GAL", "TUR", "DUG", "KU6", "ITUD", "ZAG",
    "SAG", "SU", "KA", "UD", "GI", "MES", "HI.A", "KASKAL",
    "NINDA", "A", "KI", "GU4", "UDU", "ANSE", "GIR", "EDIN",
    "SILA", "NUMUN", "NAM", "ALAN", "GUB", "TUG", "SIG", "DUB",
}


def is_sumerogram(word: str) -> bool:
    """Detect cuneiform Sumerograms (uppercase sign names)."""
    stripped = word.strip("-").strip()
    if not stripped:
        return False

    # Pattern 1: All uppercase ASCII, length >= 2 (e.g., LUGAL, URU)
    if stripped.isupper() and stripped.replace(".", "").replace("-", "").isascii() and len(stripped) >= 2:
        return True

    # Pattern 2: Compound Sumerograms (e.g., MUNUS.LUGAL, ANSE.KUR.RA)
    if re.match(r"^[A-Z]+(\.[A-Z]+)+$", stripped):
        return True

    # Pattern 3: Sumerogram with number (e.g., KU6, AN2)
    if re.match(r"^[A-Z]+\d+$", stripped):
        return True

    # Pattern 4: Known Sumerogram followed by Luwian/Hittite suffix
    # e.g., LUGAL-us, URU-as, DINGIR-LIM
    parts = stripped.split("-")
    if parts and parts[0] in KNOWN_SUMEROGRAMS:
        return True

    # Pattern 5: Mixed SUMEROGRAM-phonetic (e.g., GEŠTU-, ANŠE.KUR.RA-u-)
    base = parts[0] if parts else stripped
    if base.isupper() and base.isascii() and len(base) >= 2:
        # Check if the base part is all uppercase (Sumerogram base)
        return True

    return False


def tag_tsv(iso: str, tsv_path: Path, dry_run: bool = False) -> dict:
    """Tag Sumerogram entries in a single TSV file."""
    with open(tsv_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        return {"iso": iso, "total": 0, "tagged": 0}

    header = lines[0]
    output = [header]
    tagged_entries = []

    for line in lines[1:]:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 6:
            output.append(line)
            continue

        word = parts[0]
        concept_id = parts[4]

        if is_sumerogram(word) and not concept_id.startswith("sumerogram:"):
            # Tag it
            if concept_id and concept_id != "-":
                new_concept = f"sumerogram:{concept_id}"
            else:
                new_concept = "sumerogram:unknown"
            parts[4] = new_concept
            tagged_entries.append({"word": word, "old_concept": concept_id, "new_concept": new_concept})
            output.append("\t".join(parts) + "\n")
        else:
            output.append(line)

    if tagged_entries and not dry_run:
        with open(tsv_path, "w", encoding="utf-8") as f:
            f.writelines(output)

        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"tag_sumerograms_{iso}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in tagged_entries:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "iso": iso,
        "total": len(lines) - 1,
        "tagged": len(tagged_entries),
    }


def main():
    parser = argparse.ArgumentParser(description="Tag Sumerograms in cuneiform-language TSVs")
    parser.add_argument("--language", "-l", help="ISO code (default: all cuneiform languages)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        isos = [args.language]
    else:
        isos = CUNEIFORM_LANGUAGES

    results = []
    for iso in isos:
        tsv_path = LEXICON_DIR / f"{iso}.tsv"
        if not tsv_path.exists():
            logger.info("SKIP %s: file not found", iso)
            continue
        result = tag_tsv(iso, tsv_path, dry_run=args.dry_run)
        results.append(result)

    print("\n" + "=" * 60)
    print(f"TAG SUMEROGRAMS {'(DRY RUN)' if args.dry_run else ''}")
    print("=" * 60)
    total_tagged = 0
    for r in results:
        print(f"  {r['iso']:10s} total={r['total']}, tagged={r['tagged']}")
        total_tagged += r["tagged"]
    print(f"\n  Total tagged: {total_tagged}")
    print("=" * 60)


if __name__ == "__main__":
    main()
