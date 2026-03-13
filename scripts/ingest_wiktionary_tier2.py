#!/usr/bin/env python3
"""Wiktionary category ingestion for Tier 2 ancient/classical languages.

Fetches word lists from Wiktionary category API (fast pagination),
then applies transliteration maps for IPA.

Iron Rule: All words come from HTTP API responses. No hardcoded data.

Languages: Coptic (cop), Pali (pli), Old Armenian (xcl), Old English (ang),
           Ge'ez (gez), Biblical Hebrew (hbo), Hattic (xht)

Usage:
    python scripts/ingest_wiktionary_tier2.py [--language ISO] [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import subprocess
import sys
import time
import unicodedata
import urllib.request
import urllib.error
from pathlib import Path

# Fix Windows encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

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

# Tier 2 language configs
TIER2_CONFIGS = {
    "cop": {
        "name": "Coptic",
        "category": "Coptic_lemmas",
        "namespace": 0,
    },
    "pli": {
        "name": "Pali",
        "category": "Pali_lemmas",
        "namespace": 0,
    },
    "xcl": {
        "name": "Old Armenian",
        "category": "Old_Armenian_lemmas",
        "namespace": 0,
    },
    "ang": {
        "name": "Old English",
        "category": "Old_English_lemmas",
        "namespace": 0,
    },
    "gez": {
        "name": "Ge'ez",
        "category": "Ge%27ez_lemmas",
        "namespace": 0,
    },
    "hbo": {
        "name": "Biblical Hebrew",
        "category": "Hebrew_lemmas",
        "namespace": 0,
    },
    "xht": {
        "name": "Hattic",
        "category": "Hattic_lemmas",
        "namespace": 0,
    },
    # Tier 3 + Proto-languages
    "gem-pro": {
        "name": "Proto-Germanic",
        "category": "Proto-Germanic_lemmas",
        "namespace": 118,  # Reconstruction namespace
    },
    "cel-pro": {
        "name": "Proto-Celtic",
        "category": "Proto-Celtic_lemmas",
        "namespace": 118,
    },
    "urj-pro": {
        "name": "Proto-Uralic",
        "category": "Proto-Uralic_lemmas",
        "namespace": 118,
    },
    "nci": {
        "name": "Classical Nahuatl",
        "category": "Classical_Nahuatl_lemmas",
        "namespace": 0,
    },
    "sga": {
        "name": "Old Irish",
        "category": "Old_Irish_lemmas",
        "namespace": 0,
    },
    # Phase 7 additions
    "pal": {
        "name": "Middle Persian",
        "category": "Middle_Persian_lemmas",
        "namespace": 0,
    },
    "bnt-pro": {
        "name": "Proto-Bantu",
        "category": "Proto-Bantu_lemmas",
        "namespace": 118,
    },
    "sit-pro": {
        "name": "Proto-Sino-Tibetan",
        "category": "Proto-Sino-Tibetan_lemmas",
        "namespace": 118,
    },
    "xtg": {
        "name": "Gaulish",
        "category": "Gaulish_lemmas",
        "namespace": 0,
    },
    "sog": {
        "name": "Sogdian",
        "category": "Sogdian_lemmas",
        "namespace": 0,
    },
    "ojp": {
        "name": "Old Japanese",
        "category": "Old_Japanese_lemmas",
        "namespace": 0,
    },
    # Phase 8 P0 additions
    "sla-pro": {
        "name": "Proto-Slavic",
        "category": "Proto-Slavic_lemmas",
        "namespace": 118,
    },
    "trk-pro": {
        "name": "Proto-Turkic",
        "category": "Proto-Turkic_lemmas",
        "namespace": 118,
    },
    "itc-pro": {
        "name": "Proto-Italic",
        "category": "Proto-Italic_lemmas",
        "namespace": 118,
    },
    "jpx-pro": {
        "name": "Proto-Japonic",
        "category": "Proto-Japonic_lemmas",
        "namespace": 118,
    },
    "ira-pro": {
        "name": "Proto-Iranian",
        "category": "Proto-Iranian_lemmas",
        "namespace": 118,
    },
    "xce": {
        "name": "Celtiberian",
        "category": "Celtiberian_lemmas",
        "namespace": 0,
    },
    # Phase 8 P1 proto-languages
    "alg-pro": {
        "name": "Proto-Algonquian",
        "category": "Proto-Algonquian_lemmas",
        "namespace": 118,
    },
    "sqj-pro": {
        "name": "Proto-Albanian",
        "category": "Proto-Albanian_lemmas",
        "namespace": 118,
    },
    "aav-pro": {
        "name": "Proto-Austroasiatic",
        "category": "Proto-Austroasiatic_lemmas",
        "namespace": 118,
    },
    "poz-pol-pro": {
        "name": "Proto-Polynesian",
        "category": "Proto-Polynesian_lemmas",
        "namespace": 118,
    },
    "tai-pro": {
        "name": "Proto-Tai",
        "category": "Proto-Tai_lemmas",
        "namespace": 118,
    },
    "xto-pro": {
        "name": "Proto-Tocharian",
        "category": "Proto-Tocharian_lemmas",
        "namespace": 118,
    },
    "poz-oce-pro": {
        "name": "Proto-Oceanic",
        "category": "Proto-Oceanic_lemmas",
        "namespace": 118,
    },
    "xgn-pro": {
        "name": "Proto-Mongolic",
        "category": "Proto-Mongolic_lemmas",
        "namespace": 118,
    },
    # Phase 8 additional ancient languages
    "obm": {
        "name": "Moabite",
        "category": "Moabite_lemmas",
        "namespace": 0,
    },
    # Batch 3: P2 proto-languages + Iberian
    "myn-pro": {
        "name": "Proto-Mayan",
        "category": "Proto-Mayan_lemmas",
        "namespace": 118,
    },
    "afa-pro": {
        "name": "Proto-Afroasiatic",
        "category": "Proto-Afroasiatic_lemmas",
        "namespace": 118,
    },
    "xib": {
        "name": "Iberian",
        "category": "Iberian_lemmas",
        "namespace": 0,
    },
}


def fetch_all_category_members(category: str, namespace: int = 0) -> list[str]:
    """Fetch ALL members of a Wiktionary category via API pagination.

    Uses curl subprocess to avoid Python urllib 429 rate limiting issues.
    Falls back to urllib if curl is not available.
    """
    members = []
    base_params = (
        f"action=query&list=categorymembers&cmtitle=Category:{category}"
        f"&cmtype=page&cmnamespace={namespace}&cmlimit=500&format=json"
    )
    extra = ""

    page = 0
    while True:
        page += 1
        url = f"{API_URL}?{base_params}{extra}"

        for attempt in range(5):
            try:
                result = subprocess.run(
                    ["curl", "-s", "-H", f"User-Agent: {USER_AGENT}", url],
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode != 0 or not result.stdout.strip():
                    raise OSError(f"curl failed: rc={result.returncode}")
                data = json.loads(result.stdout)
                break
            except (OSError, json.JSONDecodeError) as exc:
                wait = 5 * (attempt + 1)
                if attempt < 4:
                    logger.warning("Attempt %d failed: %s, retrying in %ds...",
                                   attempt + 1, exc, wait)
                    time.sleep(wait)
                else:
                    logger.error("FAILED after 5 retries: %s", exc)
                    return members

        for m in data.get("query", {}).get("categorymembers", []):
            members.append(m["title"])

        cont = data.get("continue", {})
        if "cmcontinue" in cont:
            extra = f"&cmcontinue={cont['cmcontinue']}"
            logger.info("  Page %d: %d members so far...", page, len(members))
            time.sleep(1.0)  # Be nice to Wiktionary
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
    if len(word) == 1 and word.isascii():
        return False
    if word == "-" or word == "--":
        return False
    if word.isascii() and word.isupper() and len(word) > 3:
        return False
    return True


def ingest_language(iso: str, config: dict, dry_run: bool = False,
                    from_cache: bool = False) -> dict:
    """Ingest a single language from Wiktionary category."""
    tsv_path = LEXICON_DIR / f"{iso}.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("%s (%s): %d existing entries", iso, config["name"], len(existing))

    # Check for cached raw data first
    raw_path = RAW_DIR / f"wiktionary_category_{iso}.json"
    if from_cache and raw_path.exists():
        with open(raw_path, "r", encoding="utf-8") as f:
            cached = json.load(f)
        titles = cached.get("members", [])
        logger.info("%s: Loaded %d members from cache", iso, len(titles))
    else:
        # Fetch all category members
        logger.info("%s: Fetching category members...", iso)
        titles = fetch_all_category_members(config["category"], config["namespace"])
        logger.info("%s: Got %d category members", iso, len(titles))

        # Always save raw data (even dry run) for cache
        if titles:
            RAW_DIR.mkdir(parents=True, exist_ok=True)
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump({"category": config["category"], "members": titles},
                          f, ensure_ascii=False)

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
        LEXICON_DIR.mkdir(parents=True, exist_ok=True)
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\twiktionary_cat\t-\t-\n")

    # Save audit trail
    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"tier2_ingest_{iso}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "iso": iso,
        "name": config["name"],
        "existing": len(existing) - len(new_entries),
        "new": len(new_entries),
        "total": len(existing),
        "skipped": skipped,
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest Tier 2 languages from Wiktionary")
    parser.add_argument("--language", "-l", help="Specific ISO code (default: all Tier 2)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--from-cache", action="store_true",
                        help="Use cached raw data instead of fetching (avoids rate limits)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        if args.language not in TIER2_CONFIGS:
            logger.error("Unknown language: %s. Available: %s",
                         args.language, ", ".join(TIER2_CONFIGS.keys()))
            sys.exit(1)
        configs = {args.language: TIER2_CONFIGS[args.language]}
    else:
        configs = TIER2_CONFIGS

    results = []
    for iso, config in configs.items():
        result = ingest_language(iso, config, dry_run=args.dry_run,
                                from_cache=args.from_cache)
        results.append(result)

    print(f"\n{'DRY RUN: ' if args.dry_run else ''}Tier 2 Wiktionary Ingestion:")
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
