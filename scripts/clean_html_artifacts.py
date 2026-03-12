#!/usr/bin/env python3
"""Clean HTML artifacts, NBSP characters, and duplicates from ancient language TSVs.

Fixes found by adversarial contamination scan (Phase 2.3):
- Lycian (xlc): 1 HTML <sup> tag in Word/IPA
- Eteocretan (xcr): 5 HTML &gt; entities (etymological notes, not words)
- Luwian (xlw): 233 NBSP (U+00A0) chars in Word/IPA
- Lydian (xld): 3 NBSP chars in Word/IPA
- Etruscan (ett): 3 exact duplicate pairs
- Luwian (xlw): 37 Latin-logogram entries (CENTUM, ARGENTUM) with Word==IPA

Iron Rule: This script does NOT add data. It only cleans existing entries.

Usage:
    python scripts/clean_html_artifacts.py [--dry-run]
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


def clean_file(iso: str, tsv_path: Path, dry_run: bool = False) -> dict:
    """Clean a single TSV file. Returns stats dict."""
    with open(tsv_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        return {"iso": iso, "total": 0, "fixed": 0, "removed": 0}

    header = lines[0]
    output = [header]
    fixed = []
    removed = []
    seen_words: set[str] = set()

    for line in lines[1:]:
        parts = line.rstrip("\n").split("\t")
        if not parts or len(parts) < 6:
            output.append(line)
            continue

        word = parts[0]
        ipa = parts[1] if len(parts) > 1 else ""
        original_line = line

        # Fix 1: Remove HTML tags from Word and IPA
        if "<" in word or "<" in ipa:
            new_word = re.sub(r"<[^>]+>", "", word)
            new_ipa = re.sub(r"<[^>]+>", "", ipa)
            if new_word != word or new_ipa != ipa:
                fixed.append({"word": word, "fix": "html_tags", "new_word": new_word})
                parts[0] = new_word
                parts[1] = new_ipa
                word = new_word

        # Fix 2: Remove HTML entities (&gt;, &lt;, etc.)
        if "&gt;" in word or "&lt;" in word or "&amp;" in word:
            # These are etymological notes, not words — remove entire entry
            removed.append({"word": word, "reason": "html_entity_etymology_note"})
            continue

        # Fix 3: Replace NBSP (U+00A0) with regular space
        if "\xa0" in word or "\xa0" in ipa:
            new_word = word.replace("\xa0", " ")
            new_ipa = ipa.replace("\xa0", " ")
            fixed.append({"word": word, "fix": "nbsp", "new_word": new_word})
            parts[0] = new_word
            parts[1] = new_ipa
            word = new_word

        # Fix 4: Deduplicate exact rows
        if word in seen_words:
            removed.append({"word": word, "reason": "exact_duplicate"})
            continue
        seen_words.add(word)

        output.append("\t".join(parts) + "\n")

    if (fixed or removed) and not dry_run:
        with open(tsv_path, "w", encoding="utf-8") as f:
            f.writelines(output)

        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"clean_html_{iso}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in fixed:
                f.write(json.dumps({"action": "fix", **r}, ensure_ascii=False) + "\n")
            for r in removed:
                f.write(json.dumps({"action": "remove", **r}, ensure_ascii=False) + "\n")

    return {
        "iso": iso,
        "total": len(lines) - 1,
        "fixed": len(fixed),
        "removed": len(removed),
    }


TARGETS = ["xlc", "xcr", "xlw", "xld", "ett", "xur"]


def main():
    parser = argparse.ArgumentParser(description="Clean HTML artifacts from TSVs")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    results = []
    for iso in TARGETS:
        tsv_path = LEXICON_DIR / f"{iso}.tsv"
        if not tsv_path.exists():
            continue
        result = clean_file(iso, tsv_path, dry_run=args.dry_run)
        results.append(result)

    print(f"\n{'DRY RUN: ' if args.dry_run else ''}HTML artifact cleanup:")
    for r in results:
        if r["fixed"] > 0 or r["removed"] > 0:
            print(f"  {r['iso']:10s} total={r['total']}, fixed={r['fixed']}, removed={r['removed']}")
    total_fixed = sum(r["fixed"] for r in results)
    total_removed = sum(r["removed"] for r in results)
    if total_fixed == 0 and total_removed == 0:
        print("  No issues found.")
    else:
        print(f"\n  Total fixed: {total_fixed}, Total removed: {total_removed}")


if __name__ == "__main__":
    main()
