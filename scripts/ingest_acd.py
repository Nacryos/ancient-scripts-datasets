#!/usr/bin/env python3
"""Ingest Proto-Austronesian reconstructed forms from the ACD CLDF dataset.

Source: Austronesian Comparative Dictionary (ACD) — CLDF on GitHub
URL: https://github.com/lexibank/acd
License: CC BY 4.0
Citation: Blust, Trussel & Smith (2023), DOI: 10.5281/zenodo.7737547

The CLDF forms.csv contains reconstructed forms for 42 proto-languages.
Forms use Blust notation (not IPA) — requires transliteration.

Iron Rule: Data comes from downloaded CSV files. No hardcoded word lists.

Usage:
    python scripts/ingest_acd.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"

ACD_DIR = RAW_DIR / "acd_cldf"
ACD_BASE = "https://raw.githubusercontent.com/lexibank/acd/main/cldf/"

# Blust notation → IPA mapping
# Reference: Blust (2009) The Austronesian Languages, Chapter 2
BLUST_TO_IPA = {
    # Capital letters = special proto-phonemes
    "C": "ts",      # *C — voiceless dental/alveolar affricate
    "N": "ŋ",       # *N — velar nasal (sometimes ñ)
    "R": "ʀ",       # *R — uvular trill or retroflex
    "S": "s",       # *S — voiceless sibilant
    "Z": "z",       # *Z — voiced sibilant
    "H": "h",       # *H — laryngeal
    "L": "ɬ",       # *L — lateral fricative
    "T": "t",       # *T — voiceless dental stop
    "D": "d",       # *D — voiced dental stop
    # Digraphs
    "ng": "ŋ",
    "ny": "ɲ",
    "nj": "ɲ",
    # Glottal
    "q": "ʔ",
    # Vowels with special values
    "e": "ə",       # Blust *e = schwa in PAN
    # Subscript digits (used for homonyms) — remove
    "₁": "", "₂": "", "₃": "", "₄": "", "₅": "",
    "₆": "", "₇": "", "₈": "", "₉": "", "₀": "",
}


def blust_to_ipa(form: str) -> str:
    """Convert Blust notation to approximate IPA."""
    # Remove reconstruction asterisk
    form = form.lstrip("*")
    # Remove parenthetical optional segments
    form = re.sub(r"\([^)]+\)", "", form)

    # Greedy longest-match transliteration
    keys = sorted(BLUST_TO_IPA.keys(), key=len, reverse=True)
    result = []
    i = 0
    while i < len(form):
        matched = False
        for key in keys:
            if form[i:i + len(key)] == key:
                result.append(BLUST_TO_IPA[key])
                i += len(key)
                matched = True
                break
        if not matched:
            if form[i] not in "- ":  # skip hyphens and spaces
                result.append(form[i])
            i += 1
    return "".join(result)


def download_if_needed():
    """Download ACD CLDF files if not cached."""
    import urllib.request

    ACD_DIR.mkdir(parents=True, exist_ok=True)
    for fname in ("forms.csv", "languages.csv", "cognatesets.csv"):
        local = ACD_DIR / fname
        if local.exists():
            logger.info("Cached: %s (%d bytes)", fname, local.stat().st_size)
            continue
        url = ACD_BASE + fname
        logger.info("Downloading %s ...", url)
        req = urllib.request.Request(url, headers={
            "User-Agent": "PhaiPhon/1.0 (ancient-scripts-datasets)"
        })
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        with open(local, "wb") as f:
            f.write(data)
        logger.info("Downloaded %s (%d bytes)", fname, len(data))


def load_proto_languages():
    """Load language metadata to identify proto-languages."""
    lang_path = ACD_DIR / "languages.csv"
    protos = {}
    with open(lang_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("Name", "")
            lid = row.get("ID", "")
            # Proto-languages have names starting with "Proto-"
            if name.startswith("Proto-"):
                protos[lid] = name
    return protos


def extract_proto_forms():
    """Extract reconstructed forms from ACD CLDF."""
    protos = load_proto_languages()
    logger.info("Found %d proto-languages in ACD", len(protos))

    forms_path = ACD_DIR / "forms.csv"
    entries = {}  # (proto_lang, form) -> {gloss, ...}

    with open(forms_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lang_id = row.get("Language_ID", "")
            if lang_id not in protos:
                continue

            form = row.get("Form", "").strip()
            value = row.get("Value", "").strip()
            gloss = row.get("Description", "").strip()

            if not form:
                continue
            # Use Form (cleaned) rather than Value (has optional segments)
            word = form

            # Strip infix angle brackets: C<in>aliS → CinaliS
            word = re.sub(r"<([^>]+)>", r"\1", word)
            # Strip parenthetical optional segments: (q)uNah → uNah
            word = re.sub(r"\([^)]+\)", "", word)
            # Remove leading asterisk
            word = word.lstrip("*")
            # Remove subscript digits (homonym markers)
            word = re.sub(r"[₀₁₂₃₄₅₆₇₈₉]", "", word)
            # Remove tilde variants: keep only first form
            if " ~ " in word:
                word = word.split(" ~ ")[0]
            # Remove hyphens (prefix/suffix markers)
            word = word.strip("-").strip()

            # NFC normalize
            word = unicodedata.normalize("NFC", word)

            key = (lang_id, word)
            if key not in entries:
                entries[key] = {
                    "word": word,
                    "gloss": gloss,
                    "proto_lang": protos[lang_id],
                    "proto_lang_id": lang_id,
                }

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
    parser = argparse.ArgumentParser(description="Ingest ACD Proto-Austronesian")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    download_if_needed()

    # We ingest all proto-forms into a single map.tsv (Proto-Austronesian family)
    tsv_path = LEXICON_DIR / "map.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Existing Proto-Austronesian entries: %d", len(existing))

    entries = extract_proto_forms()
    logger.info("ACD proto-forms: %d", len(entries))

    # Count by proto-language
    by_lang = {}
    for (lid, _), info in entries.items():
        name = info["proto_lang"]
        by_lang[name] = by_lang.get(name, 0) + 1
    for name, count in sorted(by_lang.items(), key=lambda x: -x[1])[:10]:
        logger.info("  %s: %d", name, count)

    # Process
    new_entries = []
    audit_trail = []
    skipped = 0

    for (lid, word), info in sorted(entries.items()):
        clean_word = word.strip()
        if not clean_word or len(clean_word) < 2 or len(clean_word) > 50:
            skipped += 1
            continue

        if clean_word in existing:
            skipped += 1
            continue

        # Convert Blust notation to IPA
        ipa = blust_to_ipa(word)
        if not ipa:
            ipa = clean_word

        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""

        new_entries.append({
            "word": clean_word,
            "ipa": ipa,
            "sca": sca,
        })
        existing.add(clean_word)

        audit_trail.append({
            "word": clean_word,
            "raw_form": word,
            "ipa": ipa,
            "gloss": info["gloss"],
            "proto_lang": info["proto_lang"],
            "source": "acd",
        })

    logger.info("New: %d, Skipped: %d", len(new_entries), skipped)

    if args.dry_run:
        print(f"\nDRY RUN: ACD Proto-Austronesian Ingestion:")
        print(f"  ACD proto-forms:    {len(entries)}")
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
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\tacd\t-\t-\n")

    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / "acd_ingest_map.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"\nACD Proto-Austronesian Ingestion:")
    print(f"  ACD proto-forms:    {len(entries)}")
    print(f"  Existing:           {len(existing) - len(new_entries)}")
    print(f"  New:                {len(new_entries)}")
    print(f"  Total:              {len(existing)}")


if __name__ == "__main__":
    main()
