#!/usr/bin/env python3
"""Expand Phrygian (xpg) lexicon from Wiktionary category + appendix pages.

Sources:
- Category:Phrygian_lemmas (full pagination)
- Appendix:Phrygian_Swadesh_list
- Reconstruction:Phrygian/ pages (if any)

Deduplicates against existing xpg.tsv and appends new entries.

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/expand_xpg.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "parsers"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402
from transliteration_maps import transliterate  # noqa: E402
from parse_wiktionary import (  # noqa: E402
    fetch_category_members,
    fetch_page_html,
    extract_gloss_from_html,
    parse,
)

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"


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
    parser = argparse.ArgumentParser(description="Expand Phrygian lexicon")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    tsv_path = LEXICON_DIR / "xpg.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Loaded %d existing Phrygian entries", len(existing))

    all_new: list[dict] = []

    # Source 1: Wiktionary Category:Phrygian_lemmas (main namespace)
    logger.info("Fetching Category:Phrygian_lemmas...")
    members = fetch_category_members("Category:Phrygian_lemmas", namespace=0)
    logger.info("Found %d category members", len(members))

    for i, m in enumerate(members):
        title = m.get("title", "")
        if not title or title in existing:
            continue

        word = title.strip()
        # Skip non-Latin script entries (Phrygian in Greek script)
        # but keep transliterated forms
        if not word or len(word) > 50:
            continue

        gloss = ""
        try:
            html = fetch_page_html(title)
            if html:
                gloss = extract_gloss_from_html(html, "Phrygian")
        except Exception as exc:
            logger.warning("Failed to fetch '%s': %s", title, exc)

        if word not in existing:
            all_new.append({"word": word, "gloss": gloss, "source_detail": "wikt_cat"})
            existing.add(word)

        if (i + 1) % 20 == 0:
            logger.info("  Processed %d/%d", i + 1, len(members))
            time.sleep(3)
        else:
            time.sleep(1.5)

    # Source 2: Phrygian Swadesh list appendix
    logger.info("Fetching Phrygian Swadesh list...")
    swadesh_url = "https://en.wiktionary.org/wiki/Appendix:Phrygian_Swadesh_list"
    swadesh_entries = parse(swadesh_url)
    logger.info("Swadesh list: %d entries", len(swadesh_entries))

    for e in swadesh_entries:
        word = e.get("word", "").strip()
        word = re.sub(r"^\*+", "", word)
        if word and word not in existing:
            all_new.append({
                "word": word,
                "gloss": e.get("gloss", ""),
                "source_detail": "wikt_swadesh",
            })
            existing.add(word)

    logger.info("Total new entries: %d", len(all_new))

    if args.dry_run:
        for e in all_new[:30]:
            print(f"  {e['word']:25s} {e['gloss'][:40]}")
        return

    # Append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    if all_new:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in all_new:
                word = e["word"]
                gloss = e["gloss"]

                try:
                    ipa = transliterate(word, "xpg")
                except Exception:
                    ipa = word

                try:
                    sca = ipa_to_sound_class(ipa)
                except Exception:
                    sca = ""

                concept = gloss.split(",")[0].split(";")[0].strip() if gloss else ""
                concept_id = concept.replace(" ", "_").lower()[:50] if concept else "-"

                f.write(f"{word}\t{ipa}\t{sca}\twiktionary\t{concept_id}\t-\n")
                new_count += 1

                audit_trail.append({
                    "word": word,
                    "gloss": gloss,
                    "ipa": ipa,
                    "source_detail": e.get("source_detail", ""),
                })

    # Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / "wiktionary_expansion_xpg.jsonl", "w",
              encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(existing)
    print(f"\nPhrygian (xpg): +{new_count} new, {total} total")


if __name__ == "__main__":
    main()
