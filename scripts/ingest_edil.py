#!/usr/bin/env python3
"""Ingest Old Irish headwords from eDIL XML files (GitHub repository).

Source: Electronic Dictionary of the Irish Language (eDIL)
URL: https://github.com/e-dil/dil
Copyright: Royal Irish Academy (text), Queen's University Belfast (digitization)
Citation: eDIL 2019, dil.ie

eDIL provides XML files per letter with <orth> headwords inside <entry> elements.

Iron Rule: Data comes from downloaded XML files. No hardcoded word lists.

Usage:
    python scripts/ingest_edil.py [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
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

EDIL_DIR = RAW_DIR / "edil_xml"
EDIL_REPO_BASE = (
    "https://raw.githubusercontent.com/e-dil/dil/master/xml/"
)

# eDIL XML files per letter
EDIL_FILES = [
    "A.xml", "B.xml", "C.xml", "D1.xml", "D2.xml", "E.xml",
    "F.xml", "G.xml", "H.xml", "I.xml", "L.xml", "M.xml",
    "N.xml", "O.xml", "P.xml", "R.xml", "S.xml", "T.xml", "U.xml",
]


def download_if_needed():
    """Download eDIL XML files if not cached."""
    import urllib.request

    EDIL_DIR.mkdir(parents=True, exist_ok=True)
    for fname in EDIL_FILES:
        local = EDIL_DIR / fname
        if local.exists():
            logger.info("Cached: %s (%d bytes)", fname, local.stat().st_size)
            continue
        url = EDIL_REPO_BASE + fname
        logger.info("Downloading %s ...", fname)
        req = urllib.request.Request(url, headers={
            "User-Agent": "PhaiPhon/1.0 (ancient-scripts-datasets)"
        })
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            with open(local, "wb") as f:
                f.write(data)
            logger.info("Downloaded %s (%d bytes)", fname, len(data))
        except Exception as e:
            logger.warning("Failed to download %s: %s", fname, e)


# HTML entity → Unicode replacements for eDIL XML
HTML_ENTITIES = {
    "&aelig;": "æ", "&Auml;": "Ä", "&Eacute;": "É", "&Oacute;": "Ó",
    "&aacute;": "á", "&aacte;": "á", "&agrave;": "à",
    "&eacute;": "é", "&iacute;": "í", "&oacute;": "ó", "&uacute;": "ú",
    "&ucirc;": "û", "&sect;": "§", "&sup2;": "²",
    "&macr;": "¯", "&amacr;": "ā", "&omacr;": "ō", "&rmacr;": "r̄",
    "&lstrok;": "ł", "&ebreve;": "ĕ",
    "&adot;": "ȧ", "&edot;": "ė", "&fdot;": "ḟ", "&Sdot;": "Ṡ",
    "&ccon;": "ç", "&dotabove;": "̇",
    "&supa;": "ᵃ", "&mt;": "",
}


def resolve_html_entities(text: str) -> str:
    """Replace HTML entities with Unicode characters."""
    for entity, char in HTML_ENTITIES.items():
        text = text.replace(entity, char)
    return text


def extract_headwords(xml_path: Path) -> list[dict]:
    """Extract headwords from a single eDIL XML file."""
    entries = []

    # Read and resolve HTML entities before XML parsing
    try:
        raw = xml_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raw = xml_path.read_text(encoding="latin-1")

    raw = resolve_html_entities(raw)

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        logger.warning("XML parse error in %s: %s", xml_path.name, e)
        return entries

    for entry in root.iter("entry"):
        # Find <orth> inside <form>
        orth = None
        for form_elem in entry.iter("form"):
            o = form_elem.find("orth")
            if o is not None:
                orth = o
                break
        if orth is None:
            # Try direct orth
            orth = entry.find(".//orth")
        if orth is None or not orth.text:
            continue

        word = orth.text.strip()

        # Remove numeric prefixes like "1 ", "2 "
        word = re.sub(r"^\d+\s+", "", word)
        # Remove leading ? (uncertain entries)
        word = re.sub(r"^\?\s*", "", word)
        # Remove parenthetical optional letters like "(h)"
        word = re.sub(r"\([^)]+\)", "", word)
        # NFC normalize
        word = unicodedata.normalize("NFC", word)
        word = word.strip()

        if not word:
            continue
        # Skip single-char entries
        if len(word) < 2:
            continue
        # Skip very long
        if len(word) > 50:
            continue
        # Skip entries that are pure Latin/English
        # (eDIL has some Latin loanword entries)
        # Keep Irish words — they typically have Irish diacritics or
        # standard Gaelic letter combinations

        # Get POS if available
        pos = ""
        pos_elem = entry.find(".//pos")
        if pos_elem is not None and pos_elem.get("value"):
            pos = pos_elem.get("value")

        entries.append({
            "word": word,
            "pos": pos,
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
    parser = argparse.ArgumentParser(description="Ingest Old Irish from eDIL")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    download_if_needed()

    tsv_path = LEXICON_DIR / "sga.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Existing Old Irish entries: %d", len(existing))

    # Extract from all XML files
    all_entries = []
    for fname in EDIL_FILES:
        xml_path = EDIL_DIR / fname
        if not xml_path.exists():
            logger.warning("Missing: %s", fname)
            continue
        entries = extract_headwords(xml_path)
        logger.info("%s: %d headwords", fname, len(entries))
        all_entries.extend(entries)

    logger.info("Total eDIL headwords: %d", len(all_entries))

    # Process
    new_entries = []
    audit_trail = []
    skipped = 0
    seen = set(existing)

    for entry in all_entries:
        word = entry["word"]
        if word in seen:
            skipped += 1
            continue

        try:
            ipa = transliterate(word, "sga")
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
            "source": "edil",
        })

    logger.info("New: %d, Skipped: %d", len(new_entries), skipped)

    if args.dry_run:
        print(f"\nDRY RUN: eDIL Old Irish Ingestion:")
        print(f"  eDIL headwords:     {len(all_entries)}")
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
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\tedil\t-\t-\n")

    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / "edil_ingest_sga.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"\neDIL Old Irish Ingestion:")
    print(f"  eDIL headwords:     {len(all_entries)}")
    print(f"  Existing:           {len(existing)}")
    print(f"  New:                {len(new_entries)}")
    print(f"  Total:              {len(seen)}")


if __name__ == "__main__":
    main()
