#!/usr/bin/env python3
"""Remove known data processing artifacts from ancient language TSV files.

Iron Rule: This script does NOT add data. It only removes entries that match
known artifact patterns (processing placeholders, wrong-language entries,
encoding errors). All removals are logged to an audit trail.

Usage:
    python scripts/clean_artifacts.py [--dry-run] [--language ISO]
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

# Known artifact patterns: (iso, word_pattern, reason)
ARTIFACT_PATTERNS: list[tuple[str | None, re.Pattern, str]] = [
    # Processing placeholders found in Avestan
    (None, re.compile(r"^inprogress$", re.IGNORECASE), "processing placeholder"),
    (None, re.compile(r"^phoneticvalue$", re.IGNORECASE), "processing placeholder"),
    (None, re.compile(r"^testentry$", re.IGNORECASE), "processing placeholder"),
    (None, re.compile(r"^placeholder$", re.IGNORECASE), "processing placeholder"),
    (None, re.compile(r"^TODO$"), "processing placeholder"),
]

# Known cross-language contamination: (iso, exact_word, reason)
CONTAMINATION: list[tuple[str, str, str]] = [
    ("hit", "xshap", "Avestan word (xshap- = night), not Hittite"),
]


def is_artifact(iso: str, word: str) -> str | None:
    """Return reason string if word is a known artifact, else None."""
    for pattern_iso, pattern, reason in ARTIFACT_PATTERNS:
        if pattern_iso is not None and pattern_iso != iso:
            continue
        if pattern.match(word):
            return reason

    for cont_iso, cont_word, reason in CONTAMINATION:
        if iso == cont_iso and word == cont_word:
            return reason

    return None


def clean_tsv(iso: str, tsv_path: Path, dry_run: bool = False) -> dict:
    """Clean a single TSV file. Returns stats dict."""
    with open(tsv_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        return {"iso": iso, "total": 0, "removed": 0, "kept": 0}

    header = lines[0]
    kept = [header]
    removed = []

    for line in lines[1:]:
        parts = line.rstrip("\n").split("\t")
        if not parts:
            continue
        word = parts[0]
        reason = is_artifact(iso, word)
        if reason:
            removed.append({"word": word, "reason": reason, "line": line.rstrip("\n")})
            logger.warning("REMOVE %s/%s: %s", iso, word, reason)
        else:
            kept.append(line)

    if removed and not dry_run:
        # Write cleaned file
        with open(tsv_path, "w", encoding="utf-8") as f:
            f.writelines(kept)

        # Write audit trail
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"clean_artifacts_{iso}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in removed:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "iso": iso,
        "total": len(lines) - 1,
        "removed": len(removed),
        "kept": len(kept) - 1,
    }


ANCIENT_ISOS = [
    "ave", "txb", "xlw", "ine-pro", "xlc", "ett", "xur", "xld",
    "xcr", "ccs-pro", "peo", "xto", "dra-pro", "sem-pro", "uga",
    "hit", "xhu", "elx", "xrr", "phn", "xpg", "cms", "xle",
]


def main():
    parser = argparse.ArgumentParser(description="Remove known artifacts from TSV files")
    parser.add_argument("--language", "-l", help="ISO code (default: all ancient)")
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
        isos = ANCIENT_ISOS

    results = []
    for iso in isos:
        tsv_path = LEXICON_DIR / f"{iso}.tsv"
        if not tsv_path.exists():
            logger.info("SKIP %s: file not found", iso)
            continue
        result = clean_tsv(iso, tsv_path, dry_run=args.dry_run)
        results.append(result)

    print("\n" + "=" * 60)
    print(f"CLEAN ARTIFACTS {'(DRY RUN)' if args.dry_run else ''}")
    print("=" * 60)
    total_removed = 0
    for r in results:
        if r["removed"] > 0:
            print(f"  {r['iso']:10s} total={r['total']}, removed={r['removed']}, kept={r['kept']}")
            total_removed += r["removed"]
    if total_removed == 0:
        print("  No artifacts found.")
    else:
        print(f"\n  Total removed: {total_removed}")
    print("=" * 60)


if __name__ == "__main__":
    main()
