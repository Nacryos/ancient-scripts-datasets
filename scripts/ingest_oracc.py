#!/usr/bin/env python3
"""Ingest Sumerian and Akkadian lexicon data from ORACC Neo glossaries.

ORACC (Open Richly Annotated Cuneiform Corpus) provides structured JSON
glossaries with citation forms, glosses, and POS tags. This is the richest
freely available source for Sumerian and Akkadian vocabulary.

URLs:
    Sumerian: https://build-oracc.museum.upenn.edu/neo/downloads/gloss-sux.json
    Akkadian: https://build-oracc.museum.upenn.edu/neo/downloads/gloss-akk.json

License: CC BY-SA (stated on ePSD2/ORACC metadata)

Iron Rule: All words come from HTTP API responses. No hardcoded data.

Usage:
    python scripts/ingest_oracc.py [--language sux|akk] [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
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

from transliteration_maps import transliterate  # noqa: E402
from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"

USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets)"

ORACC_CONFIGS = {
    "sux": {
        "name": "Sumerian",
        "url": "https://build-oracc.museum.upenn.edu/neo/downloads/gloss-sux.json",
    },
    "akk": {
        "name": "Akkadian",
        "url": "https://build-oracc.museum.upenn.edu/neo/downloads/gloss-akk.json",
    },
}


def download_glossary(url: str) -> dict:
    """Download an ORACC Neo glossary JSON file."""
    logger.info("Downloading %s ...", url)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read()
                logger.info("Downloaded %d bytes", len(raw))
                return json.loads(raw.decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Attempt %d failed: %s, retrying...", attempt + 1, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.error("FAILED after 3 retries: %s", exc)
                raise


def extract_headwords(glossary: dict) -> list[dict]:
    """Extract headwords from an ORACC glossary JSON.

    Each entry has: cf (citation form), gw (guide word/gloss), pos, forms.
    """
    entries_raw = glossary.get("entries", [])
    results = []

    for entry in entries_raw:
        cf = entry.get("cf", "").strip()
        if not cf:
            continue

        # NFC normalize
        cf = unicodedata.normalize("NFC", cf)

        # Skip if too long or too short
        if len(cf) > 50 or len(cf) < 2:
            continue

        # Skip all-caps ASCII (Sumerograms in Akkadian glossary)
        if cf.isascii() and cf.isupper() and len(cf) > 3:
            continue

        gw = entry.get("gw", "").strip()
        pos = entry.get("pos", "").strip()

        results.append({
            "cf": cf,
            "gw": gw,
            "pos": pos,
        })

    return results


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


def ingest_language(iso: str, config: dict, dry_run: bool = False) -> dict:
    """Ingest a single language from ORACC Neo glossary."""
    tsv_path = LEXICON_DIR / f"{iso}.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("%s (%s): %d existing entries", iso, config["name"], len(existing))

    # Download glossary
    glossary = download_glossary(config["url"])

    # Save raw glossary
    if not dry_run:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        raw_path = RAW_DIR / f"oracc_glossary_{iso}.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(glossary, f, ensure_ascii=False)

    # Extract headwords
    headwords = extract_headwords(glossary)
    logger.info("%s: %d headwords extracted from ORACC", iso, len(headwords))

    # Process entries
    new_entries = []
    audit_trail = []
    skipped = 0

    for hw in headwords:
        word = hw["cf"]
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
            "ipa": ipa,
            "gloss": hw["gw"],
            "pos": hw["pos"],
            "source": "oracc_neo",
        })

    logger.info("%s: %d new, %d skipped (existing/dupe)", iso, len(new_entries), skipped)

    if dry_run:
        return {
            "iso": iso,
            "name": config["name"],
            "existing": len(existing) - len(new_entries),
            "oracc_total": len(headwords),
            "new": len(new_entries),
            "total": len(existing),
            "skipped": skipped,
        }

    # Write to TSV
    if new_entries:
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\toracc_neo\t-\t-\n")

    # Save audit trail
    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"oracc_ingest_{iso}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "iso": iso,
        "name": config["name"],
        "existing": len(existing) - len(new_entries),
        "oracc_total": len(headwords),
        "new": len(new_entries),
        "total": len(existing),
        "skipped": skipped,
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest from ORACC Neo glossaries")
    parser.add_argument("--language", "-l", choices=["sux", "akk"],
                        help="Specific ISO code (default: both)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        configs = {args.language: ORACC_CONFIGS[args.language]}
    else:
        configs = ORACC_CONFIGS

    results = []
    for iso, config in configs.items():
        result = ingest_language(iso, config, dry_run=args.dry_run)
        results.append(result)

    print(f"\n{'DRY RUN: ' if args.dry_run else ''}ORACC Neo Ingestion:")
    print("=" * 70)
    for r in results:
        print(f"  {r['iso']:8s} {r['name']:15s} ORACC={r['oracc_total']:>6d}, "
              f"existing={r['existing']:>5d}, new={r['new']:>5d}, total={r['total']:>5d}")
    total_new = sum(r["new"] for r in results)
    print(f"\n  Total new entries: {total_new}")
    print("=" * 70)


if __name__ == "__main__":
    main()
