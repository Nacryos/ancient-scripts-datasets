#!/usr/bin/env python3
"""Ingest Oscan, Umbrian, and Venetic word data from CEIPoM.

Source: Corpus of the Epigraphy of the Italian Peninsula in the 1st Millennium BCE
URL: https://github.com/ReubenJPitts/Corpus-of-the-Epigraphy-of-the-Italian-Peninsula-in-the-1st-Millennium-BCE
License: CC BY-SA 4.0
Citation: Pitts (2022), DOI: 10.5281/zenodo.6475427

CEIPoM provides analysis.csv (UTF-16) with linguistic annotations and
tokens.csv (UTF-16) with attested word forms. For Oscan/Umbrian, the
Standard_aligned field gives standardized phonological forms.

Iron Rule: Data comes from downloaded CSV files. No hardcoded word lists.

Usage:
    python scripts/ingest_ceipom.py [--dry-run] [--language ISO]
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import sys
import unicodedata
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

CEIPOM_DIR = RAW_DIR / "ceipom"
CEIPOM_REPO = (
    "https://raw.githubusercontent.com/ReubenJPitts/"
    "Corpus-of-the-Epigraphy-of-the-Italian-Peninsula-in-the-1st-Millennium-BCE"
    "/master/"
)

LANGUAGE_MAP = {
    "osc": "Oscan",
    "xum": "Umbrian",
    "xve": "Venetic",
}


def download_if_needed():
    """Download CEIPoM CSV files if not cached."""
    import urllib.request

    CEIPOM_DIR.mkdir(parents=True, exist_ok=True)
    for fname in ("analysis.csv", "tokens.csv"):
        local = CEIPOM_DIR / fname
        if local.exists():
            logger.info("Using cached: %s (%d bytes)", local, local.stat().st_size)
            continue
        url = CEIPOM_REPO + fname
        logger.info("Downloading %s ...", url)
        req = urllib.request.Request(url, headers={
            "User-Agent": "PhaiPhon/1.0 (ancient-scripts-datasets)"
        })
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        with open(local, "wb") as f:
            f.write(data)
        logger.info("Downloaded %d bytes", len(data))


def load_ceipom_data():
    """Load analysis and token data from CEIPoM CSVs."""
    analysis_path = CEIPOM_DIR / "analysis.csv"
    tokens_path = CEIPOM_DIR / "tokens.csv"

    # Try UTF-16 first (CEIPoM standard), fall back to UTF-8
    for enc in ("utf-16", "utf-8-sig", "utf-8"):
        try:
            with open(analysis_path, "r", encoding=enc) as f:
                analysis_rows = list(csv.DictReader(f))
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    else:
        raise RuntimeError(f"Cannot read {analysis_path}")

    for enc in ("utf-16", "utf-8-sig", "utf-8"):
        try:
            with open(tokens_path, "r", encoding=enc) as f:
                token_rows = list(csv.DictReader(f))
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    else:
        raise RuntimeError(f"Cannot read {tokens_path}")

    # Build Token_ID -> analysis mapping
    tok_to_analysis = {}
    for row in analysis_rows:
        tid = row["Token_ID"]
        if tid not in tok_to_analysis:
            tok_to_analysis[tid] = row

    return token_rows, tok_to_analysis


def extract_words(token_rows, tok_to_analysis, lang_name: str, iso: str):
    """Extract unique word forms for a language."""
    words = {}  # word_form -> {meaning, pos, standard}

    for tok in token_rows:
        tid = tok["Token_ID"]
        a = tok_to_analysis.get(tid)
        if not a or a["Language"] != lang_name:
            continue

        word = tok.get("Token_clean", "").strip()
        if not word or word == "-":
            continue

        # NFC normalize
        word = unicodedata.normalize("NFC", word)

        # Filter single characters (abbreviations in inscriptions)
        if len(word) < 2:
            continue
        # Filter very long entries
        if len(word) > 50:
            continue
        # Filter entries with brackets (fragmentary)
        if "[" in word or "]" in word:
            continue
        # Filter numeric-only
        if word.isdigit():
            continue

        meaning = a.get("Meaning", "")
        pos = a.get("Part_of_speech", "")
        standard = a.get("Standard_aligned", "")

        if word not in words:
            words[word] = {
                "meaning": meaning,
                "pos": pos,
                "standard": standard if standard != "-" else "",
            }

    return words


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
    parser = argparse.ArgumentParser(description="Ingest CEIPoM Italic languages")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--language", "-l",
                        help="Specific ISO code (osc, xum, xve)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    download_if_needed()

    logger.info("Loading CEIPoM data...")
    token_rows, tok_to_analysis = load_ceipom_data()
    logger.info("Loaded %d tokens, %d analyses", len(token_rows), len(tok_to_analysis))

    if args.language:
        if args.language not in LANGUAGE_MAP:
            logger.error("Unknown: %s. Available: %s",
                         args.language, ", ".join(LANGUAGE_MAP))
            sys.exit(1)
        langs = {args.language: LANGUAGE_MAP[args.language]}
    else:
        langs = LANGUAGE_MAP

    results = []
    for iso, lang_name in langs.items():
        tsv_path = LEXICON_DIR / f"{iso}.tsv"
        existing = load_existing_words(tsv_path)
        logger.info("%s (%s): %d existing entries", iso, lang_name, len(existing))

        # Extract words
        words = extract_words(token_rows, tok_to_analysis, lang_name, iso)
        logger.info("%s: %d unique word forms from CEIPoM", iso, len(words))

        # Process new entries
        new_entries = []
        audit_trail = []
        skipped = 0

        for word, info in sorted(words.items()):
            if word in existing:
                skipped += 1
                continue

            # For Oscan/Umbrian, prefer Standard_aligned for transliteration
            # For Venetic (no Standard_aligned), use Token_clean
            source_form = info["standard"] if info["standard"] else word

            try:
                ipa = transliterate(source_form, iso)
            except Exception:
                ipa = source_form

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
                "standard_aligned": info["standard"],
                "ipa": ipa,
                "pos": info["pos"],
                "meaning": info["meaning"],
                "source": "ceipom",
            })

        logger.info("%s: %d new, %d skipped", iso, len(new_entries), skipped)

        if args.dry_run:
            results.append({
                "iso": iso, "name": lang_name,
                "existing": len(existing) - len(new_entries),
                "new": len(new_entries), "total": len(existing),
            })
            continue

        # Write TSV
        if new_entries:
            LEXICON_DIR.mkdir(parents=True, exist_ok=True)
            if not tsv_path.exists():
                with open(tsv_path, "w", encoding="utf-8") as f:
                    f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

            with open(tsv_path, "a", encoding="utf-8") as f:
                for e in new_entries:
                    f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\tceipom\t-\t-\n")

        # Save audit trail
        if audit_trail:
            AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
            audit_path = AUDIT_TRAIL_DIR / f"ceipom_ingest_{iso}.jsonl"
            with open(audit_path, "w", encoding="utf-8") as f:
                for r in audit_trail:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")

        results.append({
            "iso": iso, "name": lang_name,
            "existing": len(existing) - len(new_entries),
            "new": len(new_entries), "total": len(existing),
        })

    mode = "DRY RUN: " if args.dry_run else ""
    print(f"\n{mode}CEIPoM Italic Language Ingestion:")
    print("=" * 60)
    total_new = 0
    for r in results:
        print(f"  {r['iso']:8s} {r['name']:15s} existing={r['existing']:>5d}, "
              f"new={r['new']:>5d}, total={r['total']:>5d}")
        total_new += r["new"]
    print(f"\n  Total new entries: {total_new}")
    print("=" * 60)


if __name__ == "__main__":
    main()
