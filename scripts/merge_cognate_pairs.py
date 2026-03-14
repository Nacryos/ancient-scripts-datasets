#!/usr/bin/env python3
"""Merge all staging cognate pair files into final output.

Deduplicates pairs (priority: expert > borrowing > concept_aligned > similarity).
Writes 3 output files with 14-column schema.

Output: data/training/cognate_pairs/cognate_pairs_{inherited,borrowing,similarity}.tsv
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = ROOT / "staging" / "cognate_pairs"
OUTPUT_DIR = ROOT / "data" / "training" / "cognate_pairs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADER = (
    "Lang_A\tWord_A\tIPA_A\tLang_B\tWord_B\tIPA_B\tConcept_ID\t"
    "Relationship\tScore\tSource\tRelation_Detail\tDonor_Language\t"
    "Confidence\tSource_Record_ID\n"
)

# Priority order (lower = higher priority)
PRIORITY = {
    "expert_cognate": 0,
    "borrowing": 1,
    "concept_aligned": 2,
    "similarity_only": 3,
}


def pair_key(lang_a: str, word_a: str, lang_b: str, word_b: str, concept: str) -> str:
    """Canonical pair key for deduplication (order-independent)."""
    side_a = f"{lang_a}|{word_a}"
    side_b = f"{lang_b}|{word_b}"
    if side_a > side_b:
        side_a, side_b = side_b, side_a
    return f"{side_a}||{side_b}||{concept}"


def main():
    print("=" * 60)
    print("Cognate Pairs Merge")
    print("=" * 60)

    # Collect all staging files
    staging_files = sorted(STAGING_DIR.glob("*.tsv"))
    print(f"  Staging files found: {len(staging_files)}")
    for sf in staging_files:
        print(f"    {sf.name}")

    if not staging_files:
        print("ERROR: No staging files found.")
        sys.exit(1)

    # Phase 1: Read all staging files, deduplicate by pair key
    # For memory efficiency with large files, we stream-process
    seen_pairs: dict[str, int] = {}  # pair_key → priority
    inherited_path = OUTPUT_DIR / "cognate_pairs_inherited.tsv"
    borrowing_path = OUTPUT_DIR / "cognate_pairs_borrowing.tsv"
    similarity_path = OUTPUT_DIR / "cognate_pairs_similarity.tsv"

    # First pass: collect all pair keys and their best priority
    print("\n  Pass 1: Scanning for duplicates...")
    total_input = 0
    for sf in staging_files:
        with open(sf, "r", encoding="utf-8") as f:
            header = f.readline()
            for line in f:
                total_input += 1
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 8:
                    continue
                lang_a, word_a = parts[0], parts[1]
                lang_b, word_b = parts[3], parts[4]
                concept = parts[6]
                relationship = parts[7]
                key = pair_key(lang_a, word_a, lang_b, word_b, concept)
                prio = PRIORITY.get(relationship, 99)
                if key not in seen_pairs or prio < seen_pairs[key]:
                    seen_pairs[key] = prio
    print(f"  Total input rows: {total_input:,}")
    print(f"  Unique pairs: {len(seen_pairs):,}")

    # Second pass: write output files, keeping only best-priority entries
    print("\n  Pass 2: Writing output files...")
    written_keys: set[str] = set()
    # Track (lang_pair, concept) combos that appear in inherited/borrowing
    # to prevent the same language-concept pair from also appearing in similarity
    inherited_lang_concepts: set[str] = set()
    counts = {"inherited": 0, "borrowing": 0, "similarity": 0}
    self_pair_skips = 0

    with open(inherited_path, "w", encoding="utf-8") as f_inh, \
         open(borrowing_path, "w", encoding="utf-8") as f_bor, \
         open(similarity_path, "w", encoding="utf-8") as f_sim:
        f_inh.write(HEADER)
        f_bor.write(HEADER)
        f_sim.write(HEADER)

        for sf in staging_files:
            with open(sf, "r", encoding="utf-8") as f:
                header = f.readline()
                for line in f:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 8:
                        continue
                    lang_a, word_a = parts[0], parts[1]
                    lang_b, word_b = parts[3], parts[4]
                    concept = parts[6]
                    relationship = parts[7]

                    # Skip self-pairs (same language)
                    if lang_a == lang_b:
                        self_pair_skips += 1
                        continue

                    key = pair_key(lang_a, word_a, lang_b, word_b, concept)

                    # Skip if already written (dedup)
                    if key in written_keys:
                        continue

                    # Only write if this is the best-priority entry
                    prio = PRIORITY.get(relationship, 99)
                    if seen_pairs.get(key, 99) != prio:
                        continue

                    # Language-concept key for cross-file dedup
                    lc_a, lc_b = sorted([lang_a, lang_b])
                    lang_concept_key = f"{lc_a}||{lc_b}||{concept}"

                    written_keys.add(key)

                    # Route to correct output file
                    if relationship == "expert_cognate":
                        f_inh.write(line)
                        counts["inherited"] += 1
                        inherited_lang_concepts.add(lang_concept_key)
                    elif relationship == "borrowing":
                        f_bor.write(line)
                        counts["borrowing"] += 1
                    elif relationship == "concept_aligned":
                        f_inh.write(line)
                        counts["inherited"] += 1
                        inherited_lang_concepts.add(lang_concept_key)
                    elif relationship == "similarity_only":
                        # Skip if this language-concept combo already
                        # has an inherited/expert pair (prevents cross-file
                        # contamination)
                        if lang_concept_key in inherited_lang_concepts:
                            continue
                        f_sim.write(line)
                        counts["similarity"] += 1

                    total_written = sum(counts.values())
                    if total_written % 1000000 == 0:
                        print(f"    ... {total_written:,} pairs written")

    print(f"\n  Output files:")
    print(f"    inherited:  {counts['inherited']:,}")
    print(f"    borrowing:  {counts['borrowing']:,}")
    print(f"    similarity: {counts['similarity']:,}")
    print(f"    TOTAL:      {sum(counts.values()):,}")
    print(f"\n  Deduplicated: {total_input - sum(counts.values()):,} pairs removed")
    if self_pair_skips:
        print(f"  Self-pairs skipped (Lang_A == Lang_B): {self_pair_skips:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
