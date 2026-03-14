#!/usr/bin/env python3
"""Rebuild concept-aligned pairs from internal lexicons.

Fixes: labels as 'concept_aligned' (not 'cognate_inherited'),
uses random sampling instead of file-sort truncation.

Output: staging/cognate_pairs/concept_aligned_pairs.tsv (14-column schema)
        staging/cognate_pairs/similarity_only_pairs.tsv (14-column schema)
"""

from __future__ import annotations

import io
import json
import random
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
FAMILY_MAP_PATH = ROOT / "data" / "training" / "family_map.json"
STAGING_DIR = ROOT / "staging" / "cognate_pairs"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

HEADER = (
    "Lang_A\tWord_A\tIPA_A\tLang_B\tWord_B\tIPA_B\tConcept_ID\t"
    "Relationship\tScore\tSource\tRelation_Detail\tDonor_Language\t"
    "Confidence\tSource_Record_ID\n"
)

MAX_GROUP_SIZE = 50
SEED = 42


def sca_similarity(sca_a: str, sca_b: str) -> float:
    """Compute normalised Levenshtein similarity on pre-computed SCA strings."""
    if not sca_a or not sca_b:
        return 0.0
    m, n = len(sca_a), len(sca_b)
    if m == 0 or n == 0:
        return 0.0
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, n + 1):
            temp = dp[j]
            if sca_a[i - 1] == sca_b[j - 1]:
                dp[j] = prev
            else:
                dp[j] = 1 + min(prev, dp[j], dp[j - 1])
            prev = temp
    dist = dp[n]
    return round(1.0 - dist / max(m, n), 4)


def main():
    print("=" * 60)
    print("Concept-Aligned Pairs Rebuild")
    print("=" * 60)

    # Load family map
    if not FAMILY_MAP_PATH.exists():
        print(f"ERROR: family_map.json not found at {FAMILY_MAP_PATH}")
        sys.exit(1)

    with open(FAMILY_MAP_PATH, "r", encoding="utf-8") as f:
        family_map = json.load(f)
    print(f"  Family map: {len(family_map)} languages")

    # Collect all lexicon entries grouped by (family, concept)
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    total_entries = 0
    files_read = 0

    for tsv_path in sorted(LEXICON_DIR.glob("*.tsv")):
        iso = tsv_path.stem
        family = family_map.get(iso, "unknown")
        if family == "unknown":
            continue

        with open(tsv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        if not lines or not lines[0].startswith("Word\t"):
            continue

        files_read += 1
        for line in lines[1:]:
            line = line.rstrip("\n\r")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 6:
                continue
            word, ipa, sca, source, concept_id, cog_set = parts[:6]
            if not sca or sca == "-" or not concept_id or concept_id == "-":
                continue
            groups[(family, concept_id)].append({
                "iso": iso,
                "word": word,
                "ipa": ipa,
                "sca": sca,
                "concept": concept_id,
                "family": family,
            })
            total_entries += 1

    print(f"  Files read: {files_read}")
    print(f"  Total entries: {total_entries:,}")
    print(f"  Groups (family, concept): {len(groups):,}")

    # Generate pairs
    rng = random.Random(SEED)
    aligned_path = STAGING_DIR / "concept_aligned_pairs.tsv"
    similarity_path = STAGING_DIR / "similarity_only_pairs.tsv"
    aligned_count = 0
    similarity_count = 0

    with open(aligned_path, "w", encoding="utf-8") as f_aligned, \
         open(similarity_path, "w", encoding="utf-8") as f_sim:
        f_aligned.write(HEADER)
        f_sim.write(HEADER)

        for (family, concept), members in groups.items():
            # Random sampling for large groups (fixes file-sort bias)
            if len(members) > MAX_GROUP_SIZE:
                members = rng.sample(members, MAX_GROUP_SIZE)

            for a, b in combinations(members, 2):
                if a["iso"] == b["iso"]:
                    continue
                score = sca_similarity(a["sca"], b["sca"])
                source = f"concept_align_{family}"

                if score >= 0.5:
                    f_aligned.write(
                        f"{a['iso']}\t{a['word']}\t{a['ipa']}\t"
                        f"{b['iso']}\t{b['word']}\t{b['ipa']}\t"
                        f"{concept}\tconcept_aligned\t{score}\t{source}\t"
                        f"-\t-\t-\t-\n"
                    )
                    aligned_count += 1
                elif score >= 0.3:
                    f_sim.write(
                        f"{a['iso']}\t{a['word']}\t{a['ipa']}\t"
                        f"{b['iso']}\t{b['word']}\t{b['ipa']}\t"
                        f"{concept}\tsimilarity_only\t{score}\t{source}\t"
                        f"-\t-\t-\t-\n"
                    )
                    similarity_count += 1

                if (aligned_count + similarity_count) % 1000000 == 0:
                    print(f"    ... {aligned_count + similarity_count:,} pairs")

    print(f"\n  Concept-aligned pairs (score >= 0.5): {aligned_count:,}")
    print(f"  Similarity-only pairs (0.3-0.5): {similarity_count:,}")
    print(f"  Output: {aligned_path}")
    print(f"  Output: {similarity_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
