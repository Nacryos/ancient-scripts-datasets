#!/usr/bin/env python3
"""Re-process IPA and SCA columns for ancient language TSV files.

Uses the updated transliteration maps (with NFC normalization, dedicated
Tocharian/Luwian/Hurrian/Etruscan maps, Avestan script chars, etc.) to
regenerate the IPA column from the Word column, then recomputes SCA.

Preserves: Word, Source, Concept_ID, Cognate_Set_ID columns unchanged.
Updates:   IPA, SCA columns using current transliteration_maps.transliterate()
           and cognate_pipeline.normalise.sound_class.ipa_to_sound_class().

Usage:
    python scripts/reprocess_ipa.py [--dry-run] [--language ISO]
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
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

# Ancient languages with transliteration maps
ANCIENT_LANGUAGES = [
    "hit", "uga", "phn", "xur", "elx", "xlc", "xld", "xcr",
    "ave", "peo", "ine-pro", "sem-pro", "ccs-pro", "dra-pro",
    "xpg", "xle", "xrr", "cms", "xlw", "xhu", "ett", "txb", "xto",
]

# Map from TSV filename ISO to transliteration map ISO
# (some TSV files use e.g. "ine-pro" but ALL_MAPS uses "ine")
ISO_TO_MAP_ISO = {
    "ine-pro": "ine",
    "sem-pro": "sem",
    "ccs-pro": "ccs",
    "dra-pro": "dra",
}

HEADER = "Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n"


def reprocess_file(iso: str, dry_run: bool = False) -> dict:
    """Re-process a single TSV file."""
    tsv_path = LEXICON_DIR / f"{iso}.tsv"
    if not tsv_path.exists():
        logger.warning("File not found: %s", tsv_path)
        return {"iso": iso, "status": "not_found"}

    map_iso = ISO_TO_MAP_ISO.get(iso, iso)

    with open(tsv_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Parse header
    has_header = lines and lines[0].startswith("Word\t")
    data_lines = lines[1:] if has_header else lines

    entries = []
    total = 0
    old_identity = 0
    new_identity = 0
    changed = 0
    errors = 0

    for line in data_lines:
        line = line.rstrip("\n\r")
        if not line.strip():
            continue

        parts = line.split("\t")
        if len(parts) < 6:
            # Pad with defaults if needed
            while len(parts) < 6:
                parts.append("-")

        word = parts[0]
        old_ipa = parts[1]
        old_sca = parts[2]
        source = parts[3]
        concept_id = parts[4]
        cognate_set_id = parts[5]

        total += 1

        if word == old_ipa:
            old_identity += 1

        # Re-transliterate using updated maps
        try:
            candidate_ipa = transliterate(word, map_iso)
        except Exception as e:
            logger.warning("Transliteration error for %s '%s': %s", iso, word, e)
            candidate_ipa = word  # treat as identity (no conversion)
            errors += 1

        # NEVER-REGRESS RULE:
        # - If candidate converts the word (candidate != word): use it
        #   (the updated map handled these characters)
        # - If candidate is identity (candidate == word) but old IPA wasn't:
        #   KEEP old IPA (the old extraction had a conversion we'd lose)
        # - If both are identity: nothing we can do, keep identity
        if candidate_ipa != word:
            # New map successfully converts — use it
            final_ipa = candidate_ipa
        elif old_ipa != word:
            # New map can't convert, but old IPA was non-identity — keep old
            final_ipa = old_ipa
        else:
            # Both identity — no improvement possible
            final_ipa = word

        # Re-compute SCA from the chosen IPA
        try:
            new_sca = ipa_to_sound_class(final_ipa)
        except Exception:
            new_sca = old_sca

        if final_ipa == word:
            new_identity += 1

        if final_ipa != old_ipa:
            changed += 1

        entries.append({
            "word": word,
            "ipa": final_ipa,
            "sca": new_sca,
            "source": source,
            "concept_id": concept_id,
            "cognate_set_id": cognate_set_id,
        })

    old_pct = (old_identity / total * 100) if total > 0 else 0
    new_pct = (new_identity / total * 100) if total > 0 else 0

    result = {
        "iso": iso,
        "total": total,
        "old_identity": old_identity,
        "new_identity": new_identity,
        "old_identity_pct": old_pct,
        "new_identity_pct": new_pct,
        "changed": changed,
        "errors": errors,
        "status": "dry_run" if dry_run else "written",
    }

    if not dry_run and entries:
        with open(tsv_path, "w", encoding="utf-8") as f:
            f.write(HEADER)
            for e in entries:
                f.write(
                    f"{e['word']}\t{e['ipa']}\t{e['sca']}\t"
                    f"{e['source']}\t{e['concept_id']}\t{e['cognate_set_id']}\n"
                )

    return result


def main():
    parser = argparse.ArgumentParser(description="Re-process IPA/SCA for ancient languages")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing files")
    parser.add_argument("--language", "-l",
                        help="Process only this ISO code (default: all ancient)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        if args.language not in ANCIENT_LANGUAGES:
            print(f"Unknown language: {args.language}", file=sys.stderr)
            sys.exit(1)
        languages = [args.language]
    else:
        languages = ANCIENT_LANGUAGES

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"{'=' * 70}")
    print(f"IPA Re-processing ({mode})")
    print(f"Languages: {len(languages)}")
    print(f"{'=' * 70}")
    print()
    print(f"{'ISO':10s} {'Total':>6s} {'OldId%':>7s} {'NewId%':>7s} {'Changed':>8s} {'Errors':>7s}")
    print("-" * 50)

    results = []
    for iso in languages:
        result = reprocess_file(iso, dry_run=args.dry_run)
        results.append(result)
        if result["status"] == "not_found":
            print(f"{iso:10s} NOT FOUND")
        else:
            print(
                f"{iso:10s} {result['total']:6d} "
                f"{result['old_identity_pct']:6.1f}% "
                f"{result['new_identity_pct']:6.1f}% "
                f"{result['changed']:8d} "
                f"{result['errors']:7d}"
            )

    print()
    print(f"{'=' * 70}")
    print("SUMMARY")
    total_entries = sum(r.get("total", 0) for r in results)
    total_changed = sum(r.get("changed", 0) for r in results)
    total_errors = sum(r.get("errors", 0) for r in results)
    improved = [r for r in results if r.get("new_identity_pct", 100) < r.get("old_identity_pct", 0)]
    print(f"  Total entries:  {total_entries}")
    print(f"  Total changed:  {total_changed}")
    print(f"  Total errors:   {total_errors}")
    print(f"  Languages improved: {len(improved)}/{len(results)}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
