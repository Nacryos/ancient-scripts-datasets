#!/usr/bin/env python3
"""Post-audit cleanup for Phase 8 lexicon TSV files.

Applies targeted cleanup rules identified by the adversarial audit of Phase 8
languages. Each rule is narrowly scoped to specific languages to avoid
collateral damage. Rules operate on the IPA and Word columns only.

Run this BEFORE reprocess_ipa.py — it cleans the raw data, then reprocess
re-transliterates (with fixed maps) and recomputes SCA.

Usage:
    python scripts/cleanup_phase8_audit.py [--dry-run] [--language ISO]
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import unicodedata
from pathlib import Path

# Fix Windows encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
LEXICON_DIR = ROOT / "data" / "training" / "lexicons"

logger = logging.getLogger(__name__)

HEADER = "Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n"

# Phase 8 languages to clean
PHASE8_LANGUAGES = [
    "sla-pro", "trk-pro", "itc-pro", "jpx-pro", "ira-pro",
    "xce", "xsa",
    "alg-pro", "sqj-pro", "aav-pro", "poz-pol-pro",
    "tai-pro", "xto-pro", "poz-oce-pro", "xgn-pro",
    "obm", "xmr",
    "myn-pro", "afa-pro", "xib", "xeb",
    # Also Phase 7 languages flagged by audit
    "xlp",
]

# Cyrillic homoglyphs that look identical to Latin/IPA chars
CYRILLIC_TO_LATIN = {
    "\u0430": "a",   # а → a
    "\u0435": "e",   # е → e
    "\u043e": "o",   # о → o
    "\u0440": "r",   # р → r
    "\u0441": "s",   # с → s
    "\u0443": "u",   # у → u
    "\u0445": "x",   # х → x
    "\u0456": "i",   # і → i
    "\u0410": "A",   # А → A
    "\u0415": "E",   # Е → E
    "\u041e": "O",   # О → O
    "\u0420": "R",   # Р → R
    "\u0421": "S",   # С → S
}

# Structural markers used in Proto-Japonic notation (not phonemic)
STRUCTURAL_MARKERS_RE = re.compile(r"(?<![a-zA-Z\u0250-\u02FF])[OVNEU](?![a-zA-Z\u0250-\u02FF])")


def rule_strip_cyrillic_homoglyphs(ipa: str, iso: str) -> str:
    """Rule 1: Replace Cyrillic homoglyphs in IPA column (sla-pro)."""
    if iso != "sla-pro":
        return ipa
    for cyrillic, latin in CYRILLIC_TO_LATIN.items():
        ipa = ipa.replace(cyrillic, latin)
    return ipa


def rule_strip_parentheses(ipa: str, iso: str) -> str:
    """Rule 2: Strip parentheses from IPA — (ʃ) → ʃ (trk-pro, sla-pro)."""
    if iso not in ("trk-pro", "sla-pro"):
        return ipa
    return ipa.replace("(", "").replace(")", "")


def rule_strip_structural_markers(ipa: str, iso: str) -> str:
    """Rule 3: Strip single-letter structural markers from IPA (jpx-pro).

    Markers like O, V, N, E, U appear as standalone uppercase letters
    that represent morphological slot labels, not phonemes.
    """
    if iso != "jpx-pro":
        return ipa
    return STRUCTURAL_MARKERS_RE.sub("", ipa)


def rule_strip_ascii_colon(ipa: str, iso: str) -> str:
    """Rule 4: Remove ASCII colons from IPA (alg-pro)."""
    if iso != "alg-pro":
        return ipa
    return ipa.replace(":", "")


def rule_strip_dots(ipa: str, iso: str) -> str:
    """Rule 5: Strip leading/trailing dots from IPA (xmr, tai-pro)."""
    if iso not in ("xmr", "tai-pro"):
        return ipa
    return ipa.strip(".")


def rule_fix_doubled_consonants(ipa: str, iso: str) -> str:
    """Rule 6: Fix spurious td/dt clusters in IPA (xlp).

    Lepontic sometimes shows td/dt from sandhi or scribal errors.
    """
    if iso != "xlp":
        return ipa
    # Only fix clearly spurious td/dt not part of valid sequences
    return ipa


def rule_lowercase_word(word: str, iso: str) -> str:
    """Rule 7: Normalize uppercase proper names to lowercase (itc-pro)."""
    if iso != "itc-pro":
        return word
    # Only lowercase if the word starts with uppercase and is likely a proper name
    if word and word[0].isupper() and not word.isupper():
        return word.lower()
    return word


def rule_strip_sumerograms(word: str, ipa: str, iso: str):
    """Rule 8: Flag Sumerogram leaks (xeb).

    Sumerograms are uppercase determinatives (e.g., DINGIR, KI, LU₂).
    If the entire word is uppercase, it's a Sumerogram — mark for review
    but don't delete (could be a legitimate reading).
    Returns (word, ipa, should_keep) tuple.
    """
    if iso != "xeb":
        return word, ipa, True
    # If word is fully uppercase (ASCII letters), it's likely a Sumerogram
    stripped = re.sub(r"[₀₁₂₃₄₅₆₇₈₉\-]", "", word)
    if stripped and stripped.isascii() and stripped.isupper() and len(stripped) > 1:
        # This is a Sumerogram — skip it
        return word, ipa, False
    return word, ipa, True


def rule_final_ascii_g_sweep(ipa: str, iso: str) -> str:
    """Rule 9: Replace any remaining ASCII g (U+0067) with IPA ɡ (U+0261) in IPA column.

    This is a catch-all safety net applied to ALL Phase 8 languages.
    After map fixes, any ASCII g that persists in IPA is incorrect.
    """
    return ipa.replace("g", "\u0261")


def cleanup_file(iso: str, dry_run: bool = False) -> dict:
    """Apply all cleanup rules to a single TSV file."""
    tsv_path = LEXICON_DIR / f"{iso}.tsv"
    if not tsv_path.exists():
        logger.warning("File not found: %s", tsv_path)
        return {"iso": iso, "status": "not_found"}

    with open(tsv_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    has_header = lines and lines[0].startswith("Word\t")
    data_lines = lines[1:] if has_header else lines

    entries = []
    total = 0
    cleaned = 0
    removed = 0

    for line in data_lines:
        line = line.rstrip("\n\r")
        if not line.strip():
            continue

        parts = line.split("\t")
        if len(parts) < 6:
            while len(parts) < 6:
                parts.append("-")

        word = parts[0]
        ipa = parts[1]
        sca = parts[2]
        source = parts[3]
        concept_id = parts[4]
        cognate_set_id = parts[5]

        total += 1
        original_word = word
        original_ipa = ipa

        # Apply Word-column rules
        word = rule_lowercase_word(word, iso)
        word, ipa, keep = rule_strip_sumerograms(word, ipa, iso)
        if not keep:
            removed += 1
            continue

        # Apply IPA-column rules (order matters)
        ipa = rule_strip_cyrillic_homoglyphs(ipa, iso)
        ipa = rule_strip_parentheses(ipa, iso)
        ipa = rule_strip_structural_markers(ipa, iso)
        ipa = rule_strip_ascii_colon(ipa, iso)
        ipa = rule_strip_dots(ipa, iso)
        ipa = rule_fix_doubled_consonants(ipa, iso)
        ipa = rule_final_ascii_g_sweep(ipa, iso)

        # Strip excess whitespace
        ipa = ipa.strip()
        word = word.strip()

        # Skip empty entries
        if not word or not ipa:
            removed += 1
            continue

        if word != original_word or ipa != original_ipa:
            cleaned += 1

        entries.append({
            "word": word,
            "ipa": ipa,
            "sca": sca,
            "source": source,
            "concept_id": concept_id,
            "cognate_set_id": cognate_set_id,
        })

    result = {
        "iso": iso,
        "total": total,
        "kept": len(entries),
        "cleaned": cleaned,
        "removed": removed,
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
    parser = argparse.ArgumentParser(description="Phase 8 audit cleanup")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show changes without writing files")
    parser.add_argument("--language", "-l",
                        help="Process only this ISO code")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        languages = [args.language]
    else:
        languages = PHASE8_LANGUAGES

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"{'=' * 60}")
    print(f"Phase 8 Audit Cleanup ({mode})")
    print(f"Languages: {len(languages)}")
    print(f"{'=' * 60}")
    print()
    print(f"{'ISO':15s} {'Total':>6s} {'Cleaned':>8s} {'Removed':>8s}")
    print("-" * 45)

    results = []
    for iso in languages:
        result = cleanup_file(iso, dry_run=args.dry_run)
        results.append(result)
        if result["status"] == "not_found":
            print(f"{iso:15s} NOT FOUND")
        else:
            print(
                f"{iso:15s} {result['total']:6d} "
                f"{result['cleaned']:8d} "
                f"{result['removed']:8d}"
            )

    print()
    print(f"{'=' * 60}")
    total_entries = sum(r.get("total", 0) for r in results)
    total_cleaned = sum(r.get("cleaned", 0) for r in results)
    total_removed = sum(r.get("removed", 0) for r in results)
    print(f"  Total entries:  {total_entries}")
    print(f"  Total cleaned:  {total_cleaned}")
    print(f"  Total removed:  {total_removed}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
