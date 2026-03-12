#!/usr/bin/env python3
"""Expand lexicons for 7 languages by paginating through ALL Wiktionary category members.

This script uses the MediaWiki API with cmcontinue pagination to fetch every
lemma in each language's Wiktionary category, extracts glosses from individual
pages, deduplicates against existing TSV entries, and appends new entries.

Languages handled:
  - Proto-Indo-European (ine-pro) — Category:Proto-Indo-European_lemmas (ns=118)
  - Ugaritic (uga)               — Category:Ugaritic_lemmas
  - Old Persian (peo)            — Category:Old_Persian_lemmas
  - Avestan (ave)                — Category:Avestan_lemmas
  - Proto-Dravidian (dra-pro)    — Category:Proto-Dravidian_lemmas (ns=118)
  - Proto-Semitic (sem-pro)      — Category:Proto-Semitic_lemmas (ns=118)
  - Proto-Kartvelian (ccs-pro)   — Category:Proto-Kartvelian_lemmas (ns=118)

Data source: Wiktionary MediaWiki API (en.wiktionary.org)
Iron Rule: NO hardcoded lexical data. Every entry traced to API responses.

Usage:
    python scripts/expand_wiktionary_categories.py [--language ISO] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402
from transliteration_maps import transliterate  # noqa: E402

# Also import the enhanced wiktionary parser with category pagination
sys.path.insert(0, str(ROOT / "scripts" / "parsers"))
from parse_wiktionary import (  # noqa: E402
    fetch_category_members,
    fetch_page_html,
    extract_gloss_from_html,
    extract_romanization_from_html,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"

LANGUAGE_CONFIGS = {
    "ine-pro": {
        "name": "Proto-Indo-European",
        "category": "Category:Proto-Indo-European_lemmas",
        "namespace": 118,  # Reconstruction namespace
        "wikt_lang_name": "Proto-Indo-European",
        "iso_for_translit": "ine",
        "tsv_filename": "ine-pro.tsv",
        "source_label": "wiktionary_cat",
    },
    "uga": {
        "name": "Ugaritic",
        "category": "Category:Ugaritic_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Ugaritic",
        "iso_for_translit": "uga",
        "tsv_filename": "uga.tsv",
        "source_label": "wiktionary_cat",
    },
    "peo": {
        "name": "Old Persian",
        "category": "Category:Old_Persian_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Old Persian",
        "iso_for_translit": "peo",
        "tsv_filename": "peo.tsv",
        "source_label": "wiktionary_cat",
        # Also check Reconstruction namespace
        "extra_namespace": 118,
        "extra_category": "Category:Old_Persian_lemmas",
    },
    "ave": {
        "name": "Avestan",
        "category": "Category:Avestan_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Avestan",
        "iso_for_translit": "ave",
        "tsv_filename": "ave.tsv",
        "source_label": "wiktionary_cat",
    },
    "dra-pro": {
        "name": "Proto-Dravidian",
        "category": "Category:Proto-Dravidian_lemmas",
        "namespace": 118,
        "wikt_lang_name": "Proto-Dravidian",
        "iso_for_translit": "dra",
        "tsv_filename": "dra-pro.tsv",
        "source_label": "wiktionary_cat",
    },
    "sem-pro": {
        "name": "Proto-Semitic",
        "category": "Category:Proto-Semitic_lemmas",
        "namespace": 118,
        "wikt_lang_name": "Proto-Semitic",
        "iso_for_translit": "sem",
        "tsv_filename": "sem-pro.tsv",
        "source_label": "wiktionary_cat",
    },
    "ccs-pro": {
        "name": "Proto-Kartvelian",
        "category": "Category:Proto-Kartvelian_lemmas",
        "namespace": 118,
        "wikt_lang_name": "Proto-Kartvelian",
        "iso_for_translit": "ccs",
        "tsv_filename": "ccs-pro.tsv",
        "source_label": "wiktionary_cat",
    },
    # --- New languages added for Phase B expansion ---
    "xlw": {
        "name": "Luwian",
        "category": "Category:Luwian_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Luwian",
        "iso_for_translit": "xlw",  # dedicated Luwian transliteration map
        "tsv_filename": "xlw.tsv",
        "source_label": "wiktionary_cat",
        "use_word_for_ipa": True,  # Don't trust romanization field
    },
    "xhu": {
        "name": "Hurrian",
        "category": "Category:Hurrian_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Hurrian",
        "iso_for_translit": "xhu",  # dedicated Hurrian transliteration map
        "tsv_filename": "xhu.tsv",
        "source_label": "wiktionary_cat",
        "use_word_for_ipa": True,
    },
    "ett": {
        "name": "Etruscan",
        "category": "Category:Etruscan_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Etruscan",
        "iso_for_translit": "ett",  # dedicated Etruscan transliteration map
        "tsv_filename": "ett.tsv",
        "source_label": "wiktionary_cat",
        "use_word_for_ipa": True,
    },
    "txb": {
        "name": "Tocharian B",
        "category": "Category:Tocharian_B_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Tocharian B",
        "iso_for_translit": "txb",  # Tocharian transliteration
        "tsv_filename": "txb.tsv",
        "source_label": "wiktionary_cat",
        "use_word_for_ipa": True,
    },
    "xto": {
        "name": "Tocharian A",
        "category": "Category:Tocharian_A_lemmas",
        "namespace": 0,
        "wikt_lang_name": "Tocharian A",
        "iso_for_translit": "xto",  # Tocharian transliteration
        "tsv_filename": "xto.tsv",
        "source_label": "wiktionary_cat",
        "use_word_for_ipa": True,
    },
}


# ---------------------------------------------------------------------------
# Pre-processing (reused from extract_wiktionary_lexicons.py)
# ---------------------------------------------------------------------------

def preprocess_kartvelian(word: str) -> str:
    """Convert Klimov (1998) Wiktionary notation to transliteration_maps input."""
    word = unicodedata.normalize("NFC", word)
    replacements = [
        ("č̣", "č'"), ("c̣", "c'"), ("ṭ", "t'"), ("ḳ", "k'"),
        ("p̣", "p'"), ("q̣", "q'"), ("ʓ̌", "ǯ"), ("ʓ₁", "ʒ"),
        ("ʓ", "ʒ"), ("s₁", "s"), ("c₁", "c"), ("z₁", "z"),
        ("//", ""),
    ]
    for old, new in replacements:
        word = word.replace(old, new)
    return word


def preprocess_semitic(word: str) -> str:
    """Convert Kogan (2012) Wiktionary notation to transliteration_maps input."""
    word = unicodedata.normalize("NFC", word)
    replacements = [
        ("ṣ́", "ṣ"), ("ṣ̂", "ṣ"), ("ŝ", "ś"), ("ṯ̣", "ṱ"), ("ā̆", "ā"),
    ]
    for old, new in replacements:
        word = word.replace(old, new)
    return word


def custom_transliterate(word: str, iso: str) -> str:
    """Apply language-specific preprocessing before transliteration."""
    if iso == "ccs":
        word = preprocess_kartvelian(word)
    elif iso == "sem":
        word = preprocess_semitic(word)
        # Handle extra Semitic chars not in standard map
        result = transliterate(word, iso)
        extra = {"ḫ": "x", "V": "a", "ṣ́": "sˤ", "ŝ": "ɬ", "ṱ": "θˤ"}
        return "".join(extra.get(c, c) for c in result)

    return transliterate(word, iso)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def load_existing_words(tsv_path: Path) -> set[str]:
    """Load existing Word column values from a TSV (case-sensitive)."""
    existing = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Word\t"):
                    continue
                word = line.split("\t")[0]
                existing.add(word)
    return existing


def clean_word(word: str, namespace: int) -> str:
    """Clean a word extracted from Wiktionary title."""
    # For Reconstruction namespace, strip prefix
    if namespace == 118:
        parts = word.split("/")
        word = parts[-1].strip() if len(parts) >= 2 else word

    # Remove reconstruction asterisk
    word = re.sub(r"^\*+", "", word)
    # Remove trailing/leading hyphens (keep internal ones)
    word = word.strip()
    # Normalize unicode
    word = unicodedata.normalize("NFC", word)
    return word


def is_valid_word(word: str) -> bool:
    """Check if a word looks like a valid lexical entry."""
    if not word or len(word) > 50:
        return False
    if " " in word and len(word) > 25:
        return False
    # Skip if all uppercase ASCII (likely English leak)
    if word.isascii() and word.isupper() and len(word) > 3:
        return False
    return True


def expand_language(
    iso: str,
    config: dict,
    dry_run: bool = False,
) -> dict:
    """Expand a single language's lexicon from Wiktionary category.

    Returns a summary dict with counts.
    """
    tsv_path = LEXICON_DIR / config["tsv_filename"]
    existing_words = load_existing_words(tsv_path)
    logger.info("%s: loaded %d existing entries from %s",
                iso, len(existing_words), tsv_path)

    # Fetch all category members
    all_members = fetch_category_members(
        config["category"],
        namespace=config["namespace"],
    )

    # Also fetch from extra namespace if configured
    if "extra_namespace" in config:
        extra = fetch_category_members(
            config.get("extra_category", config["category"]),
            namespace=config["extra_namespace"],
        )
        all_members.extend(extra)
        logger.info("%s: total members including extra namespace: %d",
                    iso, len(all_members))

    # Process each member
    new_entries: list[dict] = []
    audit_trail: list[dict] = []
    skipped = 0

    for i, m in enumerate(all_members):
        title = m.get("title", "")
        ns = m.get("ns", 0)
        word = clean_word(title, ns)

        if not is_valid_word(word):
            skipped += 1
            continue

        # Check if already in existing TSV
        if word in existing_words:
            skipped += 1
            continue

        # Fetch gloss from individual page
        gloss = ""
        romanization = ""
        try:
            html = fetch_page_html(title)
            if html:
                gloss = extract_gloss_from_html(html, config["wikt_lang_name"])
                romanization = extract_romanization_from_html(
                    html, config["wikt_lang_name"]
                )
        except Exception as exc:
            logger.warning("Failed to fetch '%s': %s", title, exc)

        # Use romanization if found, otherwise the word itself
        translit_input = romanization if romanization else word

        # Apply transliteration -> IPA
        try:
            ipa = custom_transliterate(translit_input, config["iso_for_translit"])
        except Exception:
            ipa = translit_input

        if not ipa:
            ipa = translit_input

        # Generate SCA
        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""

        # Determine concept ID from gloss
        if gloss:
            concept = gloss.split(",")[0].split(";")[0].strip()
            concept_id = concept.replace(" ", "_").lower()[:50]
        else:
            concept_id = "-"

        entry = {
            "word": word,
            "ipa": ipa,
            "sca": sca,
            "source": config["source_label"],
            "concept_id": concept_id,
            "cognate_set_id": "-",
        }
        new_entries.append(entry)
        existing_words.add(word)  # Prevent intra-batch dupes

        # Audit trail
        audit_trail.append({
            "word": word,
            "page_title": title,
            "namespace": ns,
            "gloss": gloss,
            "romanization": romanization,
            "ipa": ipa,
        })

        # Rate limiting
        if (i + 1) % 50 == 0:
            logger.info("  %s: processed %d/%d members (%d new)",
                        iso, i + 1, len(all_members), len(new_entries))
            time.sleep(3)
        else:
            time.sleep(1.5)

    logger.info("%s: %d new entries, %d skipped (existing/invalid)",
                iso, len(new_entries), skipped)

    if dry_run:
        logger.info("%s: DRY RUN — not writing to disk", iso)
        return {
            "iso": iso,
            "existing": len(existing_words) - len(new_entries),
            "new": len(new_entries),
            "total": len(existing_words),
            "skipped": skipped,
        }

    # Append new entries to TSV
    if new_entries:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(
                    f"{e['word']}\t{e['ipa']}\t{e['sca']}\t"
                    f"{e['source']}\t{e['concept_id']}\t{e['cognate_set_id']}\n"
                )

    # Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = AUDIT_TRAIL_DIR / f"wiktionary_expansion_{iso}.jsonl"
    with open(audit_path, "w", encoding="utf-8") as f:
        for record in audit_trail:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info("%s: wrote %d new entries to %s", iso, len(new_entries), tsv_path)
    logger.info("%s: audit trail at %s", iso, audit_path)

    return {
        "iso": iso,
        "existing": len(existing_words) - len(new_entries),
        "new": len(new_entries),
        "total": len(existing_words),
        "skipped": skipped,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Expand lexicons from Wiktionary categories"
    )
    parser.add_argument(
        "--language", "-l",
        help="ISO code of specific language to expand (default: all 7)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch data but don't write to TSV files",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        if args.language not in LANGUAGE_CONFIGS:
            print(f"Unknown language: {args.language}", file=sys.stderr)
            print(f"Valid options: {', '.join(LANGUAGE_CONFIGS)}", file=sys.stderr)
            sys.exit(1)
        languages = {args.language: LANGUAGE_CONFIGS[args.language]}
    else:
        languages = LANGUAGE_CONFIGS

    print("=" * 60)
    print("Wiktionary Category Expansion")
    print(f"Languages: {', '.join(languages)}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 60)

    results = []
    for iso, config in languages.items():
        print(f"\n--- {config['name']} ({iso}) ---")
        try:
            result = expand_language(iso, config, dry_run=args.dry_run)
            results.append(result)
        except Exception as exc:
            logger.error("FAILED for %s: %s", iso, exc, exc_info=True)
            results.append({"iso": iso, "error": str(exc)})

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for r in results:
        if "error" in r:
            print(f"  {r['iso']:10s} FAILED: {r['error']}")
        else:
            print(f"  {r['iso']:10s} existing={r['existing']}, "
                  f"new={r['new']}, total={r['total']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
