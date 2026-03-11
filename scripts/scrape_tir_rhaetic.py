#!/usr/bin/env python3
"""Scrape Rhaetic word forms from TIR (Thesaurus Inscriptionum Raeticarum).

Source: https://tir.univie.ac.at/
TIR is a Semantic MediaWiki with 155 catalogued words and 389 inscriptions.
The SMW Ask API at /api.php returns structured JSON.

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/scrape_tir_rhaetic.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import ssl
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

# TIR moved to tir.univie.ac.at (redirected from www.univie.ac.at/raetica/)
TIR_API = "https://tir.univie.ac.at/api.php"

# SSL context — TIR has certificate chain issues
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def fetch_json(url: str) -> dict | None:
    """Fetch JSON from TIR SMW API."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60, context=_SSL_CTX) as resp:
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED to fetch %s: %s", url, exc)
                return None


def strategy_smw_words() -> list[dict]:
    """Fetch all 155 words from Category:Word via SMW Ask API."""
    entries = []

    # Query for all words with their properties
    query = (
        "[[Category:Word]]"
        "|?language"
        "|?type_word"
        "|?meaning"
        "|?case"
        "|?number"
        "|?lemma"
        "|?gender"
        "|limit=500"
    )
    url = f"{TIR_API}?action=ask&query={urllib.parse.quote(query)}&format=json"
    logger.info("SMW ask query: %s", url)
    data = fetch_json(url)
    if not data:
        return entries

    results = data.get("query", {}).get("results", {})
    logger.info("  Got %d word results", len(results))

    for page_name, page_data in results.items():
        if not isinstance(page_data, dict):
            continue

        printouts = page_data.get("printouts", {})

        # Extract word form (page name is the word)
        word = page_name.strip()

        # Extract meaning
        meaning_list = printouts.get("meaning", [])
        meaning = ""
        if meaning_list:
            m = meaning_list[0]
            if isinstance(m, dict):
                meaning = m.get("fulltext", str(m))
            else:
                meaning = str(m)
        meaning = meaning.strip().strip("'\"")

        # Extract language
        lang_list = printouts.get("language", [])
        lang = ""
        if lang_list:
            l = lang_list[0]
            if isinstance(l, dict):
                lang = l.get("fulltext", str(l))
            else:
                lang = str(l)

        # Extract word type
        type_list = printouts.get("type_word", [])
        word_type = ""
        if type_list:
            t = type_list[0]
            if isinstance(t, dict):
                word_type = t.get("fulltext", str(t))
            else:
                word_type = str(t)

        # Extract lemma (base form)
        lemma_list = printouts.get("lemma", [])
        lemma = ""
        if lemma_list:
            lm = lemma_list[0]
            if isinstance(lm, dict):
                lemma = lm.get("fulltext", str(lm))
            else:
                lemma = str(lm)

        if word and len(word) >= 1:
            entries.append({
                "word": word,
                "gloss": meaning,
                "language": lang,
                "word_type": word_type,
                "lemma": lemma if lemma else word,
            })

    return entries


def strategy_smw_morphemes() -> list[dict]:
    """Fetch morphemes from Category:Morpheme via SMW Ask API."""
    entries = []

    query = (
        "[[Category:Morpheme]]"
        "|?language"
        "|?type_morpheme"
        "|?meaning"
        "|limit=500"
    )
    url = f"{TIR_API}?action=ask&query={urllib.parse.quote(query)}&format=json"
    logger.info("SMW morpheme query: %s", url)
    data = fetch_json(url)
    if not data:
        return entries

    results = data.get("query", {}).get("results", {})
    logger.info("  Got %d morpheme results", len(results))

    for page_name, page_data in results.items():
        if not isinstance(page_data, dict):
            continue

        printouts = page_data.get("printouts", {})
        meaning_list = printouts.get("meaning", [])
        meaning = ""
        if meaning_list:
            m = meaning_list[0]
            meaning = m.get("fulltext", str(m)) if isinstance(m, dict) else str(m)

        if page_name.strip():
            entries.append({
                "word": page_name.strip(),
                "gloss": meaning.strip().strip("'\""),
                "language": "Raetic",
                "word_type": "morpheme",
                "lemma": page_name.strip(),
            })

    return entries


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


def main():
    parser = argparse.ArgumentParser(description="Scrape TIR for Rhaetic")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    tsv_path = LEXICON_DIR / "xrr.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Loaded %d existing Rhaetic entries", len(existing))

    # Fetch words and morphemes
    all_entries: list[dict] = []

    logger.info("=== Strategy 1: SMW Words ===")
    words = strategy_smw_words()
    all_entries.extend(words)
    logger.info("Strategy 1: %d word entries", len(words))

    logger.info("=== Strategy 2: SMW Morphemes ===")
    morphemes = strategy_smw_morphemes()
    all_entries.extend(morphemes)
    logger.info("Strategy 2: %d morpheme entries", len(morphemes))

    # Save raw JSON for audit trail
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with open(RAW_DIR / "tir_raetica_raw.json", "w", encoding="utf-8") as f:
        json.dump(all_entries, f, ensure_ascii=False, indent=2)

    # Filter: only Raetic words (not Etruscan/Celtic intrusions)
    raetic_entries = []
    for e in all_entries:
        lang = e.get("language", "").lower()
        # Keep entries that are Raetic, unknown language, or empty
        if lang in ("raetic", "rhaetic", "") or not lang:
            raetic_entries.append(e)
        else:
            logger.debug("Skipping non-Raetic entry: %s (lang=%s)", e["word"], lang)

    logger.info("Raetic-only entries: %d (filtered %d non-Raetic)",
                len(raetic_entries), len(all_entries) - len(raetic_entries))

    # Deduplicate
    seen: set[str] = set()
    new_entries: list[dict] = []
    for e in raetic_entries:
        word = e["word"].strip()
        word_lower = word.lower()
        if not word or word_lower in seen or word in existing:
            continue
        if len(word) > 30:
            continue
        seen.add(word_lower)
        new_entries.append(e)

    logger.info("Total new unique entries: %d", len(new_entries))

    if args.dry_run:
        for e in new_entries[:30]:
            print(f"  {e['word']:25s} {e.get('gloss', ''):30s} {e.get('word_type', '')}")
        return

    # Append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    if new_entries:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                word = e["word"]
                gloss = e.get("gloss", "")

                try:
                    ipa = transliterate(word, "xrr")
                except Exception:
                    ipa = word

                try:
                    sca = ipa_to_sound_class(ipa)
                except Exception:
                    sca = ""

                concept = gloss.split(",")[0].strip() if gloss else ""
                concept_id = concept.replace(" ", "_").lower()[:50] if concept else "-"

                f.write(f"{word}\t{ipa}\t{sca}\ttir_raetica\t{concept_id}\t-\n")
                new_count += 1

                audit_trail.append({
                    "word": word,
                    "gloss": gloss,
                    "ipa": ipa,
                    "word_type": e.get("word_type", ""),
                    "language": e.get("language", ""),
                    "source": "tir_raetica",
                })

    # Audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / "tir_raetica_xrr.jsonl", "w", encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(existing) + new_count
    print(f"\nRhaetic (xrr): {len(existing)} existing + {new_count} new = {total} total")


if __name__ == "__main__":
    main()
