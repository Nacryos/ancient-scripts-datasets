#!/usr/bin/env python3
"""Scrape Urartian lexical data from Oracc eCUT (Electronic Corpus of Urartian Texts).

Source: http://oracc.museum.upenn.edu/ecut/
Oracc provides JSON APIs for accessing cuneiform text corpora.

Tries 3 strategies to extract lemmas:
1. Glossary JSON: /ecut/json/gloss-xur.json (Urartian glossary)
2. Index/lemmatization: /ecut/json/index/lem/xur.json
3. Catalogue + CDL: /ecut/json/cat.json -> individual text CDL

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/scrape_oracc_urartian.py [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import time
import urllib.request
import urllib.error
import zipfile
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

# Oracc eCUT URLs
ORACC_BASE = "http://oracc.museum.upenn.edu"
ECUT_BASE = f"{ORACC_BASE}/ecut"


def fetch_json(url: str) -> dict | None:
    """Fetch JSON from URL with retries."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                # Oracc may serve gzipped content
                try:
                    import gzip
                    raw = gzip.decompress(raw)
                except Exception:
                    pass
                return json.loads(raw.decode("utf-8", errors="replace"))
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED to fetch %s: %s", url, exc)
                return None


def strategy_zip_glossary(project: str = "ecut") -> list[dict]:
    """Strategy 0: Download the project zip and extract gloss-xur.json.

    Oracc serves full project data as zip at /json/{project}.zip.
    The zip contains ecut/gloss-xur.json with structured glossary entries.
    """
    entries = []
    zip_url = f"{ORACC_BASE}/json/{project}.zip"
    logger.info("Downloading zip: %s", zip_url)

    req = urllib.request.Request(zip_url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read()
            break
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, zip_url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED to fetch %s: %s", zip_url, exc)
                return entries

    if len(raw) < 100:
        logger.info("Zip too small (%d bytes), likely empty", len(raw))
        return entries

    logger.info("Downloaded %d bytes", len(raw))

    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            # Try main glossary first, then numbered variants
            gloss_files = [n for n in zf.namelist()
                          if "gloss-xur" in n and n.endswith(".json")]
            logger.info("Found glossary files: %s", gloss_files)

            for gf in sorted(gloss_files):
                try:
                    with zf.open(gf) as f:
                        content = f.read()
                    if not content or not content.strip():
                        logger.info("  Skipping empty file: %s", gf)
                        continue
                    data = json.loads(content)
                except (json.JSONDecodeError, ValueError) as exc:
                    logger.warning("  Failed to parse %s: %s", gf, exc)
                    continue

                if not isinstance(data, dict):
                    continue

                for ge in data.get("entries", []):
                    if not isinstance(ge, dict):
                        continue
                    cf = ge.get("cf", "")
                    gw = ge.get("gw", "")
                    if cf and gw:
                        entries.append({"word": cf, "gloss": gw})
    except (zipfile.BadZipFile, KeyError) as exc:
        logger.warning("Failed to read zip: %s", exc)

    logger.info("Strategy zip glossary: %d entries", len(entries))
    return entries


def strategy_glossary(project: str = "ecut") -> list[dict]:
    """Strategy 1: Fetch the Urartian glossary JSON."""
    entries = []

    # Try multiple possible glossary URLs
    urls = [
        f"{ORACC_BASE}/{project}/json/gloss-xur.json",
        f"{ORACC_BASE}/{project}/json/glossary/xur.json",
        f"{ORACC_BASE}/{project}/json/xur/glossary.json",
    ]

    for url in urls:
        logger.info("Trying glossary URL: %s", url)
        data = fetch_json(url)
        if not data:
            continue

        if not isinstance(data, dict):
            logger.info("Glossary returned non-dict type: %s", type(data).__name__)
            continue
        logger.info("Glossary response keys: %s", list(data.keys())[:10])

        # Parse glossary entries
        # Oracc glossary format: {"entries": [{"cf": "citform", "gw": "guideword", ...}]}
        glossary_entries = data.get("entries", [])
        if not glossary_entries and isinstance(data, dict):
            # Try alternate structure
            for key in ("instances", "items", "lemmas", "glossary"):
                if key in data:
                    glossary_entries = data[key]
                    break

        for ge in glossary_entries:
            if isinstance(ge, dict):
                cf = ge.get("cf", ge.get("citform", ge.get("form", "")))
                gw = ge.get("gw", ge.get("guideword", ge.get("meaning", "")))
                if cf and gw:
                    entries.append({"word": cf, "gloss": gw})

        if entries:
            logger.info("Strategy glossary: found %d entries from %s", len(entries), url)
            return entries

    logger.info("Strategy glossary: no entries found")
    return entries


def strategy_index_lem(project: str = "ecut") -> list[dict]:
    """Strategy 2: Fetch the lemmatization index."""
    entries = []

    urls = [
        f"{ORACC_BASE}/{project}/json/index/lem/xur.json",
        f"{ORACC_BASE}/{project}/json/index/lem.json",
    ]

    for url in urls:
        logger.info("Trying index URL: %s", url)
        data = fetch_json(url)
        if not data:
            continue

        if not isinstance(data, dict):
            logger.info("Index returned non-dict type: %s", type(data).__name__)
            continue
        logger.info("Index response keys: %s", list(data.keys())[:10])

        # Parse index: {"lemma": {"cf": "...", "gw": "...", "instances": [...]}}
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    cf = value.get("cf", value.get("citform", key))
                    gw = value.get("gw", value.get("guideword", ""))
                    if cf and gw:
                        entries.append({"word": cf, "gloss": gw})
                elif isinstance(value, list):
                    # Array of lemma objects
                    for item in value:
                        if isinstance(item, dict):
                            cf = item.get("cf", item.get("citform", ""))
                            gw = item.get("gw", item.get("guideword", ""))
                            if cf and gw:
                                entries.append({"word": cf, "gloss": gw})

        if entries:
            logger.info("Strategy index: found %d entries from %s", len(entries), url)
            return entries

    logger.info("Strategy index: no entries found")
    return entries


def strategy_catalogue_cdl(project: str = "ecut") -> list[dict]:
    """Strategy 3: Fetch catalogue, then CDL for each text to extract lemmas."""
    entries = []

    cat_url = f"{ORACC_BASE}/{project}/json/cat.json"
    logger.info("Trying catalogue URL: %s", cat_url)
    cat_data = fetch_json(cat_url)
    if not cat_data or not isinstance(cat_data, dict):
        logger.info("Catalogue returned non-dict type: %s", type(cat_data).__name__ if cat_data else "None")
        return entries

    # Get text IDs from catalogue
    members = cat_data.get("members", {})
    if not members:
        members = cat_data.get("catalogue", {})
    if not members:
        # Try top-level keys that look like text IDs
        members = {k: v for k, v in cat_data.items()
                   if isinstance(v, dict) and "designation" in v}

    logger.info("Catalogue has %d texts", len(members))

    # Fetch CDL for each text (limit to avoid overwhelming server)
    texts_fetched = 0
    lemma_set: set[tuple[str, str]] = set()

    for text_id in list(members.keys())[:200]:  # Process up to 200 texts
        cdl_url = f"{ORACC_BASE}/{project}/json/{text_id}/cdl.json"
        cdl_data = fetch_json(cdl_url)
        if not cdl_data:
            continue

        # Extract lemmas from CDL tree
        _extract_lemmas_from_cdl(cdl_data, lemma_set)
        texts_fetched += 1

        if texts_fetched % 20 == 0:
            logger.info("  Processed %d texts, %d unique lemmas so far",
                        texts_fetched, len(lemma_set))
            time.sleep(3)
        else:
            time.sleep(1)

    for cf, gw in lemma_set:
        entries.append({"word": cf, "gloss": gw})

    logger.info("Strategy CDL: %d unique lemmas from %d texts",
                len(entries), texts_fetched)
    return entries


def _extract_lemmas_from_cdl(data: dict | list, results: set[tuple[str, str]]) -> None:
    """Recursively extract lemmas from Oracc CDL JSON structure."""
    if isinstance(data, list):
        for item in data:
            _extract_lemmas_from_cdl(item, results)
    elif isinstance(data, dict):
        # Check if this node is a lemma
        if "f" in data:
            f = data["f"]
            if isinstance(f, dict):
                cf = f.get("cf", "")
                gw = f.get("gw", "")
                lang = f.get("lang", "")
                # Only Urartian lemmas
                if cf and gw and (lang in ("xur", "qur", "") or "urart" in lang.lower()):
                    results.add((cf, gw))

        # Recurse into children
        for key in ("cdl", "c", "children"):
            if key in data:
                _extract_lemmas_from_cdl(data[key], results)


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
    parser = argparse.ArgumentParser(description="Scrape Oracc eCUT for Urartian")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    tsv_path = LEXICON_DIR / "xur.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Loaded %d existing Urartian entries", len(existing))

    # Try strategies in order (zip is most reliable)
    all_entries: list[dict] = []

    logger.info("=== Strategy 0: Zip Glossary ===")
    entries = strategy_zip_glossary()
    all_entries.extend(entries)

    if not entries:
        logger.info("=== Strategy 1: Glossary ===")
        entries = strategy_glossary()
        all_entries.extend(entries)

    if not all_entries:
        logger.info("=== Strategy 2: Index/Lem ===")
        entries = strategy_index_lem()
        all_entries.extend(entries)

    if not all_entries:
        logger.info("=== Strategy 3: Catalogue + CDL ===")
        entries = strategy_catalogue_cdl()
        all_entries.extend(entries)

    # Deduplicate and filter
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

    logger.info("Total new unique entries: %d", len(new_entries))

    if args.dry_run:
        for e in new_entries[:30]:
            print(f"  {e['word']:30s} {e['gloss']}")
        return

    # Append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    with open(tsv_path, "a", encoding="utf-8") as f:
        for e in new_entries:
            word = e["word"]
            gloss = e["gloss"]

            try:
                ipa = transliterate(word, "xur")
            except Exception:
                ipa = word

            try:
                sca = ipa_to_sound_class(ipa)
            except Exception:
                sca = ""

            concept = gloss.split(",")[0].split(";")[0].strip()
            concept_id = concept.replace(" ", "_").lower()[:50]

            f.write(f"{word}\t{ipa}\t{sca}\toracc_ecut\t{concept_id}\t-\n")
            new_count += 1

            audit_trail.append({
                "word": word,
                "gloss": gloss,
                "ipa": ipa,
                "source": "oracc_ecut",
            })

    # Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / "oracc_ecut_xur.jsonl", "w", encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(existing) + new_count
    print(f"\nUrartian (xur): {len(existing)} existing + {new_count} new = {total} total")


if __name__ == "__main__":
    main()
