#!/usr/bin/env python3
"""Scrape Palaeolexicon for Lycian, Lydian, and Carian word lists.

Source: https://www.palaeolexicon.com/
Palaeolexicon has a REST API at /api/Search/ that returns JSON with word
entries browsable by letter. Each language has a numeric ID.

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/scrape_palaeolexicon.py [--language xlc|xld|xcr] [--dry-run]
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

# Browser-like User-Agent required (urllib default gets 403)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Palaeolexicon REST API base
API_BASE = "https://www.palaeolexicon.com/api/Search/"

# Language IDs discovered from the API
LANGUAGE_CONFIGS = {
    "xlc": {
        "name": "Lycian",
        "language_id": 36,
        "iso_for_translit": "xlc",
        "tsv_filename": "xlc.tsv",
    },
    "xld": {
        "name": "Lydian",
        "language_id": 35,
        "iso_for_translit": "xld",
        "tsv_filename": "xld.tsv",
    },
    "xcr": {
        "name": "Carian",
        "language_id": 19,
        "iso_for_translit": "xcr",
        "tsv_filename": "xcr.tsv",
    },
}


def fetch_json(url: str) -> dict | list | None:
    """Fetch JSON from Palaeolexicon API with retries."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/javascript, */*",
        "Referer": "https://www.palaeolexicon.com/",
    })
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED to fetch %s: %s", url, exc)
                return None


def fetch_letter_index(language_id: int) -> list[str]:
    """Get the list of available letter clusters for a language."""
    url = f"{API_BASE}?languageId={language_id}&letter=a&pageSize=1&page=1"
    data = fetch_json(url)
    if not data or not isinstance(data, dict):
        return []

    letter_index = data.get("LetterIndex", [])
    if not letter_index:
        # Fallback: try basic Latin alphabet
        return list("abcdefghijklmnopqrstuvwxyz")

    clusters = []
    for item in letter_index:
        if isinstance(item, dict):
            cluster = item.get("Cluster", "")
            if cluster:
                clusters.append(cluster)
        elif isinstance(item, str):
            clusters.append(item)

    logger.info("  Letter index: %d clusters", len(clusters))
    return clusters


def fetch_words_for_letter(language_id: int, letter: str) -> list[dict]:
    """Fetch all words for a given letter in a language."""
    entries = []
    page = 1
    page_size = 500

    while True:
        encoded_letter = urllib.parse.quote(letter)
        url = (
            f"{API_BASE}?languageId={language_id}"
            f"&letter={encoded_letter}"
            f"&pageSize={page_size}&page={page}"
        )
        data = fetch_json(url)
        if not data or not isinstance(data, dict):
            break

        words = data.get("Words", [])
        if not words:
            break

        for w in words:
            if not isinstance(w, dict):
                continue
            word_text = (w.get("WordText") or "").strip()
            meaning = (w.get("Meaning") or "").strip()
            translit = (w.get("Transliteration") or "").strip()
            ipa_val = (w.get("IPA") or "").strip()
            word_id = w.get("Id", "")

            if word_text and meaning:
                entries.append({
                    "word": word_text,
                    "gloss": meaning,
                    "transliteration": translit,
                    "ipa_source": ipa_val,
                    "word_id": word_id,
                })

        index_size = data.get("IndexSize", 0)
        if len(words) < page_size or page * page_size >= index_size:
            break
        page += 1
        time.sleep(1)

    return entries


def fetch_language_words(language_id: int, language_name: str) -> list[dict]:
    """Fetch all words for a language using the letter index."""
    all_entries: list[dict] = []

    letters = fetch_letter_index(language_id)
    if not letters:
        logger.warning("No letter index for %s (id=%d)", language_name, language_id)
        return all_entries

    for i, letter in enumerate(letters):
        entries = fetch_words_for_letter(language_id, letter)
        all_entries.extend(entries)

        if (i + 1) % 5 == 0:
            logger.info("  %s: processed %d/%d letters, %d entries so far",
                        language_name, i + 1, len(letters), len(all_entries))

        time.sleep(1.5)

    logger.info("  %s: total %d entries from %d letters",
                language_name, len(all_entries), len(letters))
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

    # Fetch from Palaeolexicon API
    all_entries = fetch_language_words(config["language_id"], config["name"])

    # Save raw JSON for audit trail
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with open(RAW_DIR / f"palaeolexicon_{iso}.json", "w", encoding="utf-8") as f:
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
            print(f"  {e['word']:30s} {e['gloss']}")
        return {"iso": iso, "existing": len(existing), "new": len(new_entries)}

    # Append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    if new_entries:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                word = e["word"]
                gloss = e["gloss"]

                # Prefer source IPA if available, else transliterate
                ipa = e.get("ipa_source", "")
                if not ipa or ipa == word:
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

                concept = gloss.split(",")[0].split(";")[0].strip()
                concept_id = concept.replace(" ", "_").lower()[:50]

                f.write(f"{word}\t{ipa}\t{sca}\tpalaeolexicon\t{concept_id}\t-\n")
                new_count += 1

                audit_trail.append({
                    "word": word,
                    "gloss": gloss,
                    "ipa": ipa,
                    "transliteration": e.get("transliteration", ""),
                    "word_id": e.get("word_id", ""),
                    "source": "palaeolexicon",
                })

    # Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / f"palaeolexicon_{iso}.jsonl", "w",
              encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(existing) + new_count
    return {"iso": iso, "existing": len(existing), "new": new_count, "total": total}


def main():
    parser = argparse.ArgumentParser(description="Scrape Palaeolexicon")
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
    print("Palaeolexicon Scraper")
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
