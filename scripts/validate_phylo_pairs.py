#!/usr/bin/env python3
"""Validate phylo_pairs.tsv with known-answer checks and coverage tests.

Usage:
    python scripts/validate_phylo_pairs.py
"""

from __future__ import annotations

import csv
import io
import json
import logging
import random
import sys
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)

PHYLO_FILE = ROOT / "data" / "training" / "metadata" / "phylo_pairs.tsv"
TREE_FILE = ROOT / "data" / "training" / "raw" / "glottolog_cldf" / "glottolog_tree.json"
COGNATE_DIR = ROOT / "data" / "training" / "cognate_pairs"

# Known-answer test cases: (Lang_A, Lang_B, expected_relation, description)
# Lang_A, Lang_B are alphabetically ordered.
KNOWN_ANSWER_TESTS = [
    ("deu", "eng", "close_sister", "both West Germanic"),
    ("eng", "fra", "distant_sister", "Germanic vs Italic under IE"),
    ("fra", "lat", "near_ancestral", "Latin is ancestor of French via Romance"),
    ("lat", "spa", "near_ancestral", "Latin is ancestor of Spanish via Romance"),
    # Latin and Oscan are in different branches of Italic:
    # Latin in Latino-Faliscan, Oscan in Sabellic. MRCA=ital1284 at depth 2.
    ("lat", "osc", "distant_sister", "Latino-Faliscan vs Sabellic under Italic"),
    ("ang", "eng", "near_ancestral", "Old English is ancestor of English"),
    ("got", "swe", "distant_sister", "East Germanic vs North Germanic"),
    ("hin", "san", "near_ancestral", "Sanskrit is ancestor of Hindi via Indo-Aryan"),
    ("eng", "jpn", "cross_family", "Indo-European vs Japonic"),
    ("ceb", "tgl", "close_sister", "both Central Philippine under Austronesian"),
    ("dan", "swe", "close_sister", "both North Germanic"),
    ("ita", "spa", "close_sister", "both Italo-Western Romance"),
    ("rus", "ukr", "close_sister", "both East Slavic"),
]


def load_phylo_pairs() -> dict[tuple[str, str], dict]:
    """Load phylo_pairs.tsv into a lookup dict."""
    lookup = {}
    with open(PHYLO_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            key = (row["Lang_A"], row["Lang_B"])
            lookup[key] = row
    return lookup


def test_known_answers(lookup: dict) -> tuple[int, int]:
    """Run known-answer tests. Returns (passed, total)."""
    passed = 0
    total = len(KNOWN_ANSWER_TESTS)

    for lang_a, lang_b, expected, description in KNOWN_ANSWER_TESTS:
        key = (min(lang_a, lang_b), max(lang_a, lang_b))
        if key not in lookup:
            logger.error(
                "FAIL %s-%s: pair not found in phylo_pairs.tsv (%s)",
                lang_a, lang_b, description,
            )
            continue

        row = lookup[key]
        actual = row["Phylo_Relation"]
        if actual == expected:
            logger.info(
                "PASS %s-%s: %s (%s)",
                lang_a, lang_b, actual, description,
            )
            passed += 1
        else:
            logger.error(
                "FAIL %s-%s: expected %s, got %s (%s)",
                lang_a, lang_b, expected, actual, description,
            )

    return passed, total


def test_near_ancestral_integrity(lookup: dict) -> tuple[int, int]:
    """Verify near_ancestral pairs have valid Ancestor_Lang values."""
    passed = 0
    total = 0
    for key, row in lookup.items():
        if row["Phylo_Relation"] == "near_ancestral":
            total += 1
            anc = row["Ancestor_Lang"]
            if anc != "-" and anc in (row["Lang_A"], row["Lang_B"]):
                passed += 1
            else:
                logger.error(
                    "FAIL near_ancestral %s-%s: Ancestor_Lang=%s not in pair",
                    row["Lang_A"], row["Lang_B"], anc,
                )
    return passed, total


def test_cross_family_integrity(lookup: dict) -> tuple[int, int]:
    """Verify cross_family pairs have different families."""
    passed = 0
    total = 0
    for key, row in lookup.items():
        if row["Phylo_Relation"] == "cross_family":
            total += 1
            if row["Family_A"] != row["Family_B"]:
                passed += 1
            else:
                logger.error(
                    "FAIL cross_family %s-%s: same family %s",
                    row["Lang_A"], row["Lang_B"], row["Family_A"],
                )
    return passed, total


def test_coverage(lookup: dict) -> tuple[int, int]:
    """Verify coverage of cognate pair ISO codes in tree."""
    tree_file = TREE_FILE
    with open(tree_file, "r", encoding="utf-8") as f:
        tree = json.load(f)
    langs = tree["languages"]

    # Get all ISO codes from cognate pairs
    cognate_isos = set()
    for fname in ["cognate_pairs_inherited.tsv", "cognate_pairs_borrowing.tsv",
                   "cognate_pairs_similarity.tsv"]:
        fpath = COGNATE_DIR / fname
        if not fpath.exists():
            continue
        with open(fpath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                cognate_isos.add(row["Lang_A"])
                cognate_isos.add(row["Lang_B"])

    in_tree = sum(1 for iso in cognate_isos if iso in langs)
    pct = 100 * in_tree / len(cognate_isos)
    logger.info("Coverage: %d/%d ISO codes (%.1f%%)", in_tree, len(cognate_isos), pct)

    if pct >= 95.0:
        return (1, 1)
    else:
        logger.error("Coverage below 95%%: %.1f%%", pct)
        return (0, 1)


def test_random_audit(lookup: dict, n: int = 20) -> tuple[int, int]:
    """Audit N random pairs by tracing back to tree index."""
    with open(TREE_FILE, "r", encoding="utf-8") as f:
        tree = json.load(f)
    langs = tree["languages"]

    # Select random pairs that are not unclassified
    classifiable = [
        (k, v) for k, v in lookup.items()
        if v["Phylo_Relation"] != "unclassified"
    ]
    random.seed(42)  # reproducible
    samples = random.sample(classifiable, min(n, len(classifiable)))

    passed = 0
    for (a, b), row in samples:
        info_a = langs.get(a)
        info_b = langs.get(b)

        if not info_a or not info_b:
            logger.error("AUDIT FAIL %s-%s: missing from tree index", a, b)
            continue

        # Verify families match
        fam_a_match = info_a.get("family_name", "-") == row["Family_A"]
        fam_b_match = info_b.get("family_name", "-") == row["Family_B"]

        # Verify same/different family classification
        same_family = info_a["family"] == info_b["family"]
        relation = row["Phylo_Relation"]
        family_consistent = (
            (relation == "cross_family" and not same_family) or
            (relation in ("close_sister", "distant_sister", "near_ancestral") and same_family)
        )

        if fam_a_match and fam_b_match and family_consistent:
            passed += 1
        else:
            logger.error(
                "AUDIT FAIL %s-%s: fam_a=%s/%s fam_b=%s/%s family_consistent=%s",
                a, b,
                info_a.get("family_name"), row["Family_A"],
                info_b.get("family_name"), row["Family_B"],
                family_consistent,
            )

    return passed, n


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not PHYLO_FILE.exists():
        logger.error("Missing: %s — run build_phylo_pairs.py first", PHYLO_FILE)
        sys.exit(1)

    logger.info("Loading phylo_pairs.tsv...")
    lookup = load_phylo_pairs()
    logger.info("Loaded %d pairs", len(lookup))

    all_passed = 0
    all_total = 0

    # Test 1: Known-answer checks
    logger.info("")
    logger.info("=== Test 1: Known-Answer Checks ===")
    p, t = test_known_answers(lookup)
    all_passed += p
    all_total += t
    logger.info("Known answers: %d/%d passed", p, t)

    # Test 2: Near-ancestral integrity
    logger.info("")
    logger.info("=== Test 2: Near-Ancestral Integrity ===")
    p, t = test_near_ancestral_integrity(lookup)
    all_passed += p
    all_total += t
    logger.info("Near-ancestral integrity: %d/%d passed", p, t)

    # Test 3: Cross-family integrity
    logger.info("")
    logger.info("=== Test 3: Cross-Family Integrity ===")
    p, t = test_cross_family_integrity(lookup)
    all_passed += p
    all_total += t
    logger.info("Cross-family integrity: %d/%d passed", p, t)

    # Test 4: Coverage
    logger.info("")
    logger.info("=== Test 4: Coverage Check ===")
    p, t = test_coverage(lookup)
    all_passed += p
    all_total += t

    # Test 5: Random audit
    logger.info("")
    logger.info("=== Test 5: Random Audit (20 pairs) ===")
    p, t = test_random_audit(lookup)
    all_passed += p
    all_total += t
    logger.info("Random audit: %d/%d passed", p, t)

    # Summary
    logger.info("")
    logger.info("=" * 50)
    logger.info("TOTAL: %d/%d tests passed", all_passed, all_total)

    # Distribution summary
    relation_counts = Counter(v["Phylo_Relation"] for v in lookup.values())
    logger.info("")
    logger.info("=== Distribution ===")
    for relation, count in sorted(relation_counts.items(), key=lambda x: -x[1]):
        pct = 100 * count / len(lookup)
        logger.info("  %-18s %7d (%5.1f%%)", relation, count, pct)

    if all_passed == all_total:
        logger.info("")
        logger.info("ALL TESTS PASSED")
        sys.exit(0)
    else:
        logger.info("")
        logger.info("SOME TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
