#!/usr/bin/env python3
"""Ingest Old Japanese headwords from ONCOJ lexicon.xml (GitHub repository).

Source: Oxford-NINJAL Corpus of Old Japanese (ONCOJ)
URL: https://github.com/ONCOJ/data
License: CC-BY 4.0 (annotation)
Citation: Frellesvig, Bjarke et al., ONCOJ (Oxford-NINJAL Corpus of Old Japanese)

ONCOJ provides a TEI-compatible lexicon.xml with ~5,871 lemma entries including
ONCOJ romanization, POS, inflection class, and English gloss.

Iron Rule: Data comes from downloaded XML files. No hardcoded word lists.

Usage:
    python scripts/ingest_oncoj.py [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import unicodedata
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

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

ONCOJ_LEXICON_URL = (
    "https://raw.githubusercontent.com/ONCOJ/data/master/lexicon.xml"
)
ONCOJ_LOCAL = RAW_DIR / "oncoj_lexicon.xml"


def download_if_needed():
    """Download ONCOJ lexicon.xml if not cached."""
    ONCOJ_LOCAL.parent.mkdir(parents=True, exist_ok=True)
    if ONCOJ_LOCAL.exists():
        logger.info("Cached: oncoj_lexicon.xml (%d bytes)", ONCOJ_LOCAL.stat().st_size)
        return
    logger.info("Downloading ONCOJ lexicon.xml ...")
    req = urllib.request.Request(ONCOJ_LEXICON_URL, headers={
        "User-Agent": "PhaiPhon/1.0 (ancient-scripts-datasets)"
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    with open(ONCOJ_LOCAL, "wb") as f:
        f.write(data)
    logger.info("Downloaded oncoj_lexicon.xml (%d bytes)", len(data))


def extract_headwords(xml_path: Path) -> list[dict]:
    """Extract headwords from ONCOJ lexicon.xml."""
    entries = []

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Handle TEI namespace if present
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    for entry in root.iter(f"{ns}entry"):
        # Get <orth> element
        orth = entry.find(f".//{ns}orth")
        if orth is None or not orth.text:
            continue

        word = orth.text.strip()

        # Get POS
        pos = ""
        pos_elem = entry.find(f".//{ns}pos")
        if pos_elem is not None and pos_elem.text:
            pos = pos_elem.text.strip()

        # Get English gloss
        gloss = ""
        def_elem = entry.find(f".//{ns}def")
        if def_elem is not None and def_elem.text:
            gloss = def_elem.text.strip()

        # Clean word
        word = unicodedata.normalize("NFC", word)
        word = word.strip()

        # Skip empty or single-char
        if not word or len(word) < 2:
            continue
        # Skip very long
        if len(word) > 50:
            continue
        # Skip entries that are just punctuation or numbers
        if re.match(r'^[\d\s\-\.\,]+$', word):
            continue

        entries.append({
            "word": word,
            "pos": pos,
            "gloss": gloss,
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
                word = line.split("\t")[0]
                existing.add(word)
    return existing


def main():
    parser = argparse.ArgumentParser(description="Ingest Old Japanese from ONCOJ")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    download_if_needed()

    tsv_path = LEXICON_DIR / "ojp.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Existing Old Japanese entries: %d", len(existing))

    # Extract from lexicon.xml
    entries = extract_headwords(ONCOJ_LOCAL)
    logger.info("ONCOJ headwords: %d", len(entries))

    # Process
    new_entries = []
    audit_trail = []
    skipped = 0
    seen = set(existing)

    for entry in entries:
        word = entry["word"]
        if word in seen:
            skipped += 1
            continue

        try:
            ipa = transliterate(word, "ojp")
        except Exception:
            ipa = word

        if not ipa:
            ipa = word

        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""

        new_entries.append({
            "word": word,
            "ipa": ipa,
            "sca": sca,
        })
        seen.add(word)

        audit_trail.append({
            "word": word,
            "ipa": ipa,
            "pos": entry["pos"],
            "gloss": entry["gloss"],
            "source": "oncoj",
        })

    logger.info("New: %d, Skipped: %d", len(new_entries), skipped)

    if args.dry_run:
        print(f"\nDRY RUN: ONCOJ Old Japanese Ingestion:")
        print(f"  ONCOJ headwords:    {len(entries)}")
        print(f"  Existing:           {len(existing)}")
        print(f"  New:                {len(new_entries)}")
        print(f"  Total:              {len(seen)}")
        return

    if new_entries:
        LEXICON_DIR.mkdir(parents=True, exist_ok=True)
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\toncoj\t-\t-\n")

    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / "oncoj_ingest_ojp.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"\nONCOJ Old Japanese Ingestion:")
    print(f"  ONCOJ headwords:    {len(entries)}")
    print(f"  Existing:           {len(existing)}")
    print(f"  New:                {len(new_entries)}")
    print(f"  Total:              {len(seen)}")


if __name__ == "__main__":
    main()
