#!/usr/bin/env python3
"""Extract Sino-Tibetan cognate pairs from sinotibetan_dump.tsv.

Fixes: filters out entries with BORROWING flag, uses IPA column (not concept).

Output: staging/cognate_pairs/sinotibetan_cognate_pairs.tsv (14-column schema)
"""

from __future__ import annotations

import csv
import io
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

SOURCE_FILE = ROOT / "sources" / "sinotibetan" / "sinotibetan_dump.tsv"
STAGING_DIR = ROOT / "staging" / "cognate_pairs"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

HEADER = (
    "Lang_A\tWord_A\tIPA_A\tLang_B\tWord_B\tIPA_B\tConcept_ID\t"
    "Relationship\tScore\tSource\tRelation_Detail\tDonor_Language\t"
    "Confidence\tSource_Record_ID\n"
)

# Map doculect names to ISO 639-3 codes
DOCULECT_MAP = {
    "Old_Chinese": "och",
    "Japhug": "jya",
    "Tibetan_Written": "bod",
    "Old_Burmese": "obr",
    "Jingpho": "kac",
    "Lisu": "lis",
    "Naxi": "nxq",
    "Khaling": "klr",
    "Limbu": "lif",
    "Pumi_Lanping": "pmi",
    "Qiang_Mawo": "qxs",
    "Tujia": "tji",
    "Dulong": "duu",
    "Hakha": "cnh",
    "Bai_Jianchuan": "bca",
}


def sca_similarity(ipa_a: str, ipa_b: str) -> float:
    """Compute normalised Levenshtein similarity on SCA strings."""
    try:
        sca_a = ipa_to_sound_class(ipa_a)
        sca_b = ipa_to_sound_class(ipa_b)
    except Exception:
        return 0.0
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
    print("Sino-Tibetan Cognate Extraction v2")
    print("=" * 60)

    if not SOURCE_FILE.exists():
        print(f"ERROR: Source file not found: {SOURCE_FILE}")
        sys.exit(1)

    # Read source TSV
    cogsets: dict[str, list[dict]] = defaultdict(list)
    total_rows = 0
    skipped_borrowing = 0
    skipped_no_cogid = 0
    skipped_unknown_doculect = 0

    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            total_rows += 1
            doculect = row.get("DOCULECT", "").strip()
            iso = DOCULECT_MAP.get(doculect, "")
            if not iso:
                skipped_unknown_doculect += 1
                continue

            # Filter borrowings
            borrowing = row.get("BORROWING", "").strip()
            if borrowing:
                skipped_borrowing += 1
                continue

            cogid = row.get("COGID", "").strip()
            if not cogid:
                skipped_no_cogid += 1
                continue

            ipa = row.get("IPA", "").strip()
            concept = row.get("CONCEPT", "").strip()
            if not ipa:
                continue

            cogsets[f"st_{cogid}"].append({
                "iso": iso,
                "word": ipa,  # Use IPA as word (no orthographic form available)
                "ipa": ipa,
                "concept": concept,
            })

    print(f"  Total rows: {total_rows}")
    print(f"  Skipped (borrowing): {skipped_borrowing}")
    print(f"  Skipped (no COGID): {skipped_no_cogid}")
    print(f"  Skipped (unknown doculect): {skipped_unknown_doculect}")
    print(f"  Cognate sets: {len(cogsets)}")

    # Generate cross-language pairs
    output_path = STAGING_DIR / "sinotibetan_cognate_pairs.tsv"
    pair_count = 0
    with open(output_path, "w", encoding="utf-8") as out:
        out.write(HEADER)
        for cogset_id, members in cogsets.items():
            for a, b in combinations(members, 2):
                if a["iso"] == b["iso"]:
                    continue
                score = sca_similarity(a["ipa"], b["ipa"])
                out.write(
                    f"{a['iso']}\t{a['word']}\t{a['ipa']}\t"
                    f"{b['iso']}\t{b['word']}\t{b['ipa']}\t"
                    f"{a['concept']}\texpert_cognate\t{score}\tsinotibetan\t"
                    f"inherited\t-\tcertain\t{cogset_id}\n"
                )
                pair_count += 1

    print(f"\n  Total pairs: {pair_count:,}")
    print(f"  Output: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
