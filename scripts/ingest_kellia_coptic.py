#!/usr/bin/env python3
"""Ingest Coptic lexicon data from KELLIA Comprehensive Coptic Lexicon.

Source: KELLIA/dictionary GitHub repo (Georgetown University + BBAW)
URL: https://github.com/KELLIA/dictionary/blob/master/xml/Comprehensive_Coptic_Lexicon-v1.2-2020.xml
License: CC BY-SA 4.0
Citation: Feder, Kupreyev, Manning, Schroeder, Zeldes (2018)

TEI XML with 11,000+ entries in Coptic script. Each <entry> has
<orth> (headword) and <gramGrp>/<pos> tags.

Iron Rule: Data comes from downloaded XML file. No hardcoded word lists.

Usage:
    python scripts/ingest_kellia_coptic.py [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import unicodedata
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

TEI_NS = "{http://www.tei-c.org/ns/1.0}"
XML_PATH = RAW_DIR / "coptic_kellia_lexicon.xml"
DOWNLOAD_URL = ("https://raw.githubusercontent.com/KELLIA/dictionary/master/"
                "xml/Comprehensive_Coptic_Lexicon-v1.2-2020.xml")


def download_if_needed():
    """Download the KELLIA XML if not cached."""
    if XML_PATH.exists():
        logger.info("Using cached XML: %s (%d bytes)", XML_PATH, XML_PATH.stat().st_size)
        return
    import urllib.request
    logger.info("Downloading KELLIA Coptic lexicon...")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(DOWNLOAD_URL,
                                headers={"User-Agent": "PhaiPhon/1.0 (ancient-scripts-datasets)"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    with open(XML_PATH, "wb") as f:
        f.write(data)
    logger.info("Downloaded %d bytes", len(data))


def extract_entries(xml_path: Path) -> list[dict]:
    """Extract headwords from KELLIA TEI XML."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    entries = []
    for entry in root.iter(f"{TEI_NS}entry"):
        orth = entry.find(f".//{TEI_NS}orth")
        if orth is None or not orth.text:
            continue

        word = orth.text.strip()
        word = unicodedata.normalize("NFC", word)

        # Skip prefixes/suffixes (ending with -)
        if word.endswith("-") or word.startswith("-"):
            continue
        # Skip single-character entries
        if len(word) < 2:
            continue
        # Skip overly long
        if len(word) > 50:
            continue

        # Get POS
        pos_elem = entry.find(f".//{TEI_NS}pos")
        pos = pos_elem.text.strip() if pos_elem is not None and pos_elem.text else ""

        # Get sense/gloss
        sense_elem = entry.find(f".//{TEI_NS}sense")
        gloss = ""
        if sense_elem is not None:
            def_elem = sense_elem.find(f".//{TEI_NS}def")
            if def_elem is not None and def_elem.text:
                gloss = def_elem.text.strip()

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
    parser = argparse.ArgumentParser(description="Ingest KELLIA Coptic lexicon")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    download_if_needed()

    tsv_path = LEXICON_DIR / "cop.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Existing Coptic entries: %d", len(existing))

    # Extract
    raw_entries = extract_entries(XML_PATH)
    logger.info("KELLIA XML entries: %d", len(raw_entries))

    # Process
    new_entries = []
    audit_trail = []
    skipped = 0

    for entry in raw_entries:
        word = entry["word"]
        if word in existing:
            skipped += 1
            continue

        try:
            ipa = transliterate(word, "cop")
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
        existing.add(word)

        audit_trail.append({
            "word": word,
            "ipa": ipa,
            "pos": entry["pos"],
            "gloss": entry["gloss"],
            "source": "kellia_coptic",
        })

    logger.info("New: %d, Skipped: %d", len(new_entries), skipped)

    if args.dry_run:
        print(f"\nDRY RUN: KELLIA Coptic Ingestion:")
        print(f"  KELLIA XML entries: {len(raw_entries)}")
        print(f"  Existing:           {len(existing) - len(new_entries)}")
        print(f"  New:                {len(new_entries)}")
        print(f"  Total:              {len(existing)}")
        return

    if new_entries:
        LEXICON_DIR.mkdir(parents=True, exist_ok=True)
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\tkellia_coptic\t-\t-\n")

    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / "kellia_coptic_ingest.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"\nKELLIA Coptic Ingestion:")
    print(f"  KELLIA XML entries: {len(raw_entries)}")
    print(f"  Existing:           {len(existing) - len(new_entries)}")
    print(f"  New:                {len(new_entries)}")
    print(f"  Total:              {len(existing)}")


if __name__ == "__main__":
    main()
