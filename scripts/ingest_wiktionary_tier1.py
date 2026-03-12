#!/usr/bin/env python3
"""Fast Wiktionary category ingestion for Tier 1 ancient languages.

Fetches word lists from Wiktionary category API (fast pagination),
then applies transliteration maps for IPA. Does NOT fetch individual
pages (too slow for 4000+ entries). Glosses can be enriched later.

Iron Rule: All words come from HTTP API responses. No hardcoded data.

Languages: Old Norse (non), Gothic (got), Old Church Slavonic (chu),
           Mycenaean Greek (gmy), Akkadian (akk), Sumerian (sux)

Usage:
    python scripts/ingest_wiktionary_tier1.py [--language ISO] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import unicodedata
import urllib.request
import urllib.error
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
API_URL = "https://en.wiktionary.org/w/api.php"

# Tier 1 language configs
TIER1_CONFIGS = {
    "non": {
        "name": "Old Norse",
        "category": "Old_Norse_lemmas",
        "namespace": 0,
    },
    "got": {
        "name": "Gothic",
        "category": "Gothic_lemmas",
        "namespace": 0,
    },
    "chu": {
        "name": "Old Church Slavonic",
        "category": "Old_Church_Slavonic_lemmas",
        "namespace": 0,
    },
    "gmy": {
        "name": "Mycenaean Greek",
        "category": "Mycenaean_Greek_lemmas",
        "namespace": 0,
    },
    "akk": {
        "name": "Akkadian",
        "category": "Akkadian_lemmas",
        "namespace": 0,
    },
    "sux": {
        "name": "Sumerian",
        "category": "Sumerian_lemmas",
        "namespace": 0,
    },
}


def fetch_all_category_members(category: str, namespace: int = 0) -> list[str]:
    """Fetch ALL members of a Wiktionary category via API pagination.

    Returns list of page titles (words).
    """
    members = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": f"Category:{category}",
        "cmtype": "page",
        "cmnamespace": str(namespace),
        "cmlimit": "500",
        "format": "json",
    }

    page = 0
    while True:
        page += 1
        url = f"{API_URL}?" + "&".join(f"{k}={v}" for k, v in params.items())
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                break
            except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                else:
                    logger.error("FAILED after 3 retries: %s", exc)
                    return members

        for m in data.get("query", {}).get("categorymembers", []):
            members.append(m["title"])

        # Check for continuation
        cont = data.get("continue", {})
        if "cmcontinue" in cont:
            params["cmcontinue"] = cont["cmcontinue"]
            logger.info("  Page %d: %d members so far...", page, len(members))
            time.sleep(0.5)  # Be nice to Wiktionary
        else:
            break

    return members


def load_existing_words(tsv_path: Path) -> set[str]:
    """Load existing Word column values."""
    existing = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Word\t"):
                    continue
                word = line.split("\t")[0]
                existing.add(word)
    return existing


def clean_word(title: str, iso: str) -> str:
    """Clean a Wiktionary page title into a word."""
    word = title.strip()
    # Remove reconstruction prefix
    if "/" in word:
        word = word.split("/")[-1]
    # Remove leading asterisk (reconstruction marker)
    word = re.sub(r"^\*+", "", word)
    # NFC normalize
    word = unicodedata.normalize("NFC", word)
    return word


def is_valid_word(word: str) -> bool:
    """Validate a word for inclusion."""
    if not word or len(word) > 50:
        return False
    # Skip single chars that are just letters
    if len(word) == 1 and word.isascii():
        return False
    # Skip if looks like a suffix/prefix notation only
    if word == "-" or word == "--":
        return False
    # Skip all-caps ASCII (likely English or Sumerograms)
    if word.isascii() and word.isupper() and len(word) > 3:
        return False
    return True


def ingest_language(iso: str, config: dict, dry_run: bool = False) -> dict:
    """Ingest a single language from Wiktionary category."""
    tsv_path = LEXICON_DIR / f"{iso}.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("%s (%s): %d existing entries", iso, config["name"], len(existing))

    # Fetch all category members
    logger.info("%s: Fetching category members...", iso)
    titles = fetch_all_category_members(config["category"], config["namespace"])
    logger.info("%s: Got %d category members", iso, len(titles))

    # Process entries
    new_entries = []
    audit_trail = []
    skipped = 0

    for title in titles:
        word = clean_word(title, iso)
        if not is_valid_word(word):
            skipped += 1
            continue
        if word in existing:
            skipped += 1
            continue

        # Transliterate
        try:
            ipa = transliterate(word, iso)
        except Exception:
            ipa = word

        if not ipa:
            ipa = word

        # SCA
        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""

        new_entries.append({
            "word": word,
            "ipa": ipa,
            "sca": sca,
        })
        existing.add(word)  # Prevent intra-batch dupes

        audit_trail.append({
            "word": word,
            "page_title": title,
            "ipa": ipa,
        })

    logger.info("%s: %d new, %d skipped", iso, len(new_entries), skipped)

    if dry_run:
        return {
            "iso": iso,
            "name": config["name"],
            "existing": len(existing) - len(new_entries),
            "new": len(new_entries),
            "total": len(existing),
            "skipped": skipped,
        }

    # Write to TSV
    if new_entries:
        # Create file with header if it doesn't exist
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\twiktionary_cat\t-\t-\n")

    # Save audit trail
    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"tier1_ingest_{iso}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Save raw member list
    if titles:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        raw_path = RAW_DIR / f"wiktionary_category_{iso}.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump({"category": config["category"], "members": titles}, f, ensure_ascii=False)

    return {
        "iso": iso,
        "name": config["name"],
        "existing": len(existing) - len(new_entries),
        "new": len(new_entries),
        "total": len(existing),
        "skipped": skipped,
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest Tier 1 languages from Wiktionary")
    parser.add_argument("--language", "-l", help="Specific ISO code (default: all Tier 1)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        if args.language not in TIER1_CONFIGS:
            logger.error("Unknown language: %s. Available: %s",
                         args.language, ", ".join(TIER1_CONFIGS.keys()))
            sys.exit(1)
        configs = {args.language: TIER1_CONFIGS[args.language]}
    else:
        configs = TIER1_CONFIGS

    results = []
    for iso, config in configs.items():
        result = ingest_language(iso, config, dry_run=args.dry_run)
        results.append(result)

    print(f"\n{'DRY RUN: ' if args.dry_run else ''}Tier 1 Wiktionary Ingestion:")
    print("=" * 60)
    total_new = 0
    for r in results:
        print(f"  {r['iso']:8s} {r['name']:25s} existing={r['existing']:>5d}, "
              f"new={r['new']:>5d}, total={r['total']:>5d}")
        total_new += r["new"]
    print(f"\n  Total new entries: {total_new}")
    print("=" * 60)


if __name__ == "__main__":
    main()
