#!/usr/bin/env python3
"""Scrape eDiAna for Lycian, Lydian, and Carian word lists.

Source: https://www.ediana.gwi.uni-muenchen.de/
eDiAna (Digital Philological-Etymological Dictionary of Minor Ancient
Anatolian Corpus Languages) has a POST API at /includes/api.php that
returns JSON arrays of lemma entries.

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/scrape_ediana.py [--language xlc|xld|xcr] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402
from transliteration_maps import transliterate  # noqa: E402

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"

USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets)"

# eDiAna POST API endpoint
EDIANA_API = "https://www.ediana.gwi.uni-muenchen.de/includes/api.php"

# Language configurations
# headword_sub = language name, headword_spec = sub-specification
LANGUAGE_CONFIGS = {
    "xlc": {
        "name": "Lycian",
        "specs": [
            {"headword_sub": "Lycian", "headword_spec": "Lycian A"},
            {"headword_sub": "Lycian", "headword_spec": "Lycian B"},
        ],
        "iso_for_translit": "xlc",
        "tsv_filename": "xlc.tsv",
    },
    "xld": {
        "name": "Lydian",
        "specs": [
            {"headword_sub": "Lydian", "headword_spec": "Lydian"},
        ],
        "iso_for_translit": "xld",
        "tsv_filename": "xld.tsv",
    },
    "xcr": {
        "name": "Carian",
        "specs": [
            {"headword_sub": "Carian", "headword_spec": "Carian"},
        ],
        "iso_for_translit": "xcr",
        "tsv_filename": "xcr.tsv",
    },
}


def fetch_ediana(headword_sub: str, headword_spec: str) -> list[dict] | None:
    """POST to eDiAna API and return JSON response."""
    post_data = urllib.parse.urlencode({
        "headword_sub": headword_sub,
        "headword_spec": headword_spec,
        "headword_search": "true",
    }).encode("utf-8")

    req = urllib.request.Request(
        EDIANA_API,
        data=post_data,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return json.loads(raw)
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for eDiAna (%s/%s): %s",
                               attempt + 1, headword_sub, headword_spec, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED eDiAna fetch (%s/%s): %s",
                               headword_sub, headword_spec, exc)
                return None


def fetch_language_entries(config: dict) -> list[dict]:
    """Fetch all entries for a language from eDiAna."""
    all_entries: list[dict] = []

    for spec in config["specs"]:
        sub = spec["headword_sub"]
        spec_val = spec["headword_spec"]
        logger.info("  Fetching eDiAna: %s / %s", sub, spec_val)

        data = fetch_ediana(sub, spec_val)
        if not data or not isinstance(data, list):
            logger.warning("  No data for %s / %s", sub, spec_val)
            continue

        logger.info("  Got %d entries for %s / %s", len(data), sub, spec_val)

        for item in data:
            if not isinstance(item, dict):
                continue

            lemma = item.get("L_lemma_full", "").strip()
            translit = item.get("L_trans", "").strip()
            lang = item.get("L_lang", "").strip()
            entry_id = item.get("L_id", "")

            if not lemma:
                continue

            # Clean lemma: remove HTML tags if present
            lemma_clean = re.sub(r"<[^>]+>", "", lemma).strip()

            if lemma_clean:
                all_entries.append({
                    "word": lemma_clean,
                    "transliteration": translit,
                    "language_detail": lang,
                    "entry_id": entry_id,
                    "spec": spec_val,
                })

        time.sleep(2)  # Be polite between requests

    return all_entries


def load_existing_words(tsv_path: Path) -> set[str]:
    """Load existing Word column values."""
    existing = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Word\t"):
                    continue
                existing.add(line.split("\t")[0])
    return existing


def process_language(iso: str, config: dict, dry_run: bool = False) -> dict:
    """Process a single language."""
    tsv_path = LEXICON_DIR / config["tsv_filename"]
    existing = load_existing_words(tsv_path)
    logger.info("%s: loaded %d existing entries", iso, len(existing))

    # Fetch from eDiAna
    all_entries = fetch_language_entries(config)

    # Save raw JSON for audit trail
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with open(RAW_DIR / f"ediana_{iso}.json", "w", encoding="utf-8") as f:
        json.dump(all_entries, f, ensure_ascii=False, indent=2)

    # Deduplicate
    seen: set[str] = set()
    new_entries: list[dict] = []
    for e in all_entries:
        word = e["word"].strip()
        if not word or word in seen or word in existing:
            continue
        if len(word) > 50:
            continue
        seen.add(word)
        new_entries.append(e)

    logger.info("%s: %d new unique entries", iso, len(new_entries))

    if dry_run:
        for e in new_entries[:20]:
            print(f"  {e['word']:30s} {e.get('transliteration', '')}")
        return {"iso": iso, "existing": len(existing), "new": len(new_entries)}

    # Append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    if new_entries:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                word = e["word"]

                # Use transliteration if available, else use word form
                translit = e.get("transliteration", "")
                try:
                    ipa = transliterate(
                        translit if translit else word,
                        config["iso_for_translit"],
                    )
                except Exception:
                    ipa = translit if translit else word

                try:
                    sca = ipa_to_sound_class(ipa)
                except Exception:
                    sca = ""

                concept_id = "-"

                f.write(f"{word}\t{ipa}\t{sca}\tediana\t{concept_id}\t-\n")
                new_count += 1

                audit_trail.append({
                    "word": word,
                    "transliteration": translit,
                    "ipa": ipa,
                    "entry_id": e.get("entry_id", ""),
                    "language_detail": e.get("language_detail", ""),
                    "spec": e.get("spec", ""),
                    "source": "ediana",
                })

    # Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / f"ediana_{iso}.jsonl", "w",
              encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(existing) + new_count
    return {"iso": iso, "existing": len(existing), "new": new_count, "total": total}


def main():
    parser = argparse.ArgumentParser(description="Scrape eDiAna")
    parser.add_argument("--language", "-l",
                        help="ISO code: xlc, xld, or xcr (default: all 3)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        if args.language not in LANGUAGE_CONFIGS:
            print(f"Unknown language: {args.language}", file=sys.stderr)
            sys.exit(1)
        languages = {args.language: LANGUAGE_CONFIGS[args.language]}
    else:
        languages = LANGUAGE_CONFIGS

    print("=" * 60)
    print("eDiAna Scraper")
    print(f"Languages: {', '.join(languages)}")
    print("=" * 60)

    results = []
    for iso, config in languages.items():
        print(f"\n--- {config['name']} ({iso}) ---")
        try:
            result = process_language(iso, config, dry_run=args.dry_run)
            results.append(result)
        except Exception as exc:
            logger.error("FAILED for %s: %s", iso, exc, exc_info=True)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    for r in results:
        print(f"  {r['iso']:10s} existing={r['existing']}, new={r['new']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
