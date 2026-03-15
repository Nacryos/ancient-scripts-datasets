#!/usr/bin/env python3
"""Build phylogenetic relationship metadata for cognate pairs.

Cross-references cognate pair language pairs against the Glottolog
tree index to classify each unique (Lang_A, Lang_B) pair by its
phylogenetic relationship.

Usage:
    python scripts/build_phylo_pairs.py
"""

from __future__ import annotations

import csv
import io
import json
import logging
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)

GLOTTOLOG_DIR = ROOT / "data" / "training" / "raw" / "glottolog_cldf"
COGNATE_DIR = ROOT / "data" / "training" / "cognate_pairs"
METADATA_DIR = ROOT / "data" / "training" / "metadata"
OUTPUT_FILE = METADATA_DIR / "phylo_pairs.tsv"

COGNATE_FILES = [
    "cognate_pairs_inherited.tsv",
    "cognate_pairs_borrowing.tsv",
    "cognate_pairs_similarity.tsv",
]

# Curated map of attested ancient/medieval languages to the Glottolog clades
# they are historically ancestral to. Glottocodes verified against Glottolog
# CLDF v5.x (2026-03-14).
#
# Logic: If language A is in this map AND language B's ancestry path passes
# through one of the listed clades AND B is not itself an ancient language
# in this map, then the pair is "near_ancestral".
#
# Limitation: "Near-ancestral" is approximate — Latin is not literally the
# ancestor of French (Vulgar Latin, unattested, is). We use it to mean
# "the attested language is historically ancestral to the clade."
NEAR_ANCESTOR_MAP: dict[str, list[str]] = {
    # Latin → Romance
    "lat": ["roma1334"],
    # Ancient Greek → Koineic Greek (descendants of Koine)
    "grc": ["koin1234"],
    # Sanskrit → Indo-Aryan
    "san": ["indo1321"],
    # Old English → Anglic
    "ang": ["angl1265"],
    # Middle English → Anglic (more recent ancestor)
    "enm": ["angl1265"],
    # Old French → Oil (includes modern French, Picard, etc.)
    "fro": ["oila1234"],
    # Old Spanish → Castilic
    "osp": ["cast1243"],
    # Old Norse → North Germanic
    "non": ["nort3160"],
    # Old High German → High German
    "goh": ["high1289"],
    # Middle Dutch → Modern Dutch group
    "dum": ["mode1257"],
    # Old Irish → Goidelic
    "sga": ["goid1240"],
    # Middle Irish → Goidelic
    "mga": ["goid1240"],
    # Old Church Slavonic → South Slavic
    "chu": ["sout3147"],
    # Old East Slavic → East Slavic
    "orv": ["east1426"],
    # Old Chinese → Classical Chinese (modern Sinitic descends from this)
    "och": ["clas1255"],
    # Ottoman Turkish → Oghuz
    "ota": ["oghu1243"],
}

# Threshold: MRCA depth >= this value → close_sister, else distant_sister
CLOSE_SISTER_DEPTH_THRESHOLD = 3


def load_tree_index() -> dict:
    """Load the Glottolog tree index JSON."""
    tree_file = GLOTTOLOG_DIR / "glottolog_tree.json"
    with open(tree_file, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_unique_pairs() -> set[tuple[str, str]]:
    """Extract unique canonically-ordered (Lang_A, Lang_B) pairs from cognate files."""
    pairs: set[tuple[str, str]] = set()
    for fname in COGNATE_FILES:
        fpath = COGNATE_DIR / fname
        if not fpath.exists():
            logger.warning("Missing cognate file: %s", fpath)
            continue
        with open(fpath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                a, b = row["Lang_A"], row["Lang_B"]
                key = (min(a, b), max(a, b))
                pairs.add(key)
        logger.info("After %s: %d unique pairs", fname, len(pairs))
    return pairs


def find_mrca(path_a: list[str], path_b: list[str]) -> tuple[str, int, int]:
    """Find Most Recent Common Ancestor of two ancestry paths.

    Returns (mrca_glottocode, mrca_depth, tree_distance).
    Tree distance = edges from A to MRCA + edges from MRCA to B.
    """
    # Find longest common prefix
    mrca_idx = -1
    min_len = min(len(path_a), len(path_b))
    for i in range(min_len):
        if path_a[i] == path_b[i]:
            mrca_idx = i
        else:
            break

    if mrca_idx < 0:
        # No common ancestor (different top-level families)
        return ("", 0, 99)

    mrca = path_a[mrca_idx]
    mrca_depth = mrca_idx  # depth from root (root = 0)
    dist_a = len(path_a) - 1 - mrca_idx
    dist_b = len(path_b) - 1 - mrca_idx
    tree_distance = dist_a + dist_b

    return (mrca, mrca_depth, tree_distance)


def check_near_ancestral(
    iso_a: str,
    iso_b: str,
    path_a: list[str],
    path_b: list[str],
) -> tuple[bool, str]:
    """Check if either language is a near-ancestor of the other.

    Returns (is_near_ancestral, ancestor_iso).
    """
    # Check if A is in the near-ancestor map
    if iso_a in NEAR_ANCESTOR_MAP:
        target_clades = NEAR_ANCESTOR_MAP[iso_a]
        # Check if B's path passes through any target clade
        if any(clade in path_b for clade in target_clades):
            # B must NOT be an ancient language itself
            if iso_b not in NEAR_ANCESTOR_MAP:
                return (True, iso_a)

    # Check if B is in the near-ancestor map
    if iso_b in NEAR_ANCESTOR_MAP:
        target_clades = NEAR_ANCESTOR_MAP[iso_b]
        if any(clade in path_a for clade in target_clades):
            if iso_a not in NEAR_ANCESTOR_MAP:
                return (True, iso_b)

    return (False, "-")


def classify_pair(
    iso_a: str,
    iso_b: str,
    langs: dict,
    family_names: dict,
) -> dict:
    """Classify a single language pair.

    Returns a dict with the TSV row fields.
    """
    info_a = langs.get(iso_a)
    info_b = langs.get(iso_b)

    # Default values
    result = {
        "Lang_A": iso_a,
        "Lang_B": iso_b,
        "Phylo_Relation": "unclassified",
        "Tree_Distance": 99,
        "MRCA_Clade": "-",
        "MRCA_Depth": 0,
        "Ancestor_Lang": "-",
        "Family_A": "-",
        "Family_B": "-",
    }

    if not info_a or not info_b:
        # One or both not in tree
        if info_a:
            result["Family_A"] = info_a.get("family_name", "-")
        if info_b:
            result["Family_B"] = info_b.get("family_name", "-")
        return result

    family_a = info_a["family"]
    family_b = info_b["family"]
    result["Family_A"] = info_a.get("family_name", family_a)
    result["Family_B"] = info_b.get("family_name", family_b)

    # Cross-family check
    if family_a != family_b:
        result["Phylo_Relation"] = "cross_family"
        return result

    # Same family — find MRCA
    path_a = info_a["path"]
    path_b = info_b["path"]

    mrca, mrca_depth, tree_distance = find_mrca(path_a, path_b)
    result["Tree_Distance"] = tree_distance
    result["MRCA_Clade"] = mrca
    result["MRCA_Depth"] = mrca_depth

    # Check near-ancestral
    is_near_anc, ancestor = check_near_ancestral(iso_a, iso_b, path_a, path_b)
    if is_near_anc:
        result["Phylo_Relation"] = "near_ancestral"
        result["Ancestor_Lang"] = ancestor
        return result

    # Sister classification based on MRCA depth
    if mrca_depth >= CLOSE_SISTER_DEPTH_THRESHOLD:
        result["Phylo_Relation"] = "close_sister"
    else:
        result["Phylo_Relation"] = "distant_sister"

    return result


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Check prerequisites
    tree_file = GLOTTOLOG_DIR / "glottolog_tree.json"
    if not tree_file.exists():
        logger.error("Missing: %s — run build_glottolog_tree.py first", tree_file)
        sys.exit(1)

    logger.info("Loading tree index...")
    tree_index = load_tree_index()
    langs = tree_index["languages"]
    family_names = tree_index.get("family_names", {})
    logger.info("Loaded %d languages from tree index", len(langs))

    logger.info("Extracting unique pairs from cognate files...")
    pairs = extract_unique_pairs()
    logger.info("Total unique pairs: %d", len(pairs))

    logger.info("Classifying pairs...")
    results = []
    for i, (a, b) in enumerate(sorted(pairs)):
        row = classify_pair(a, b, langs, family_names)
        results.append(row)
        if (i + 1) % 100000 == 0:
            logger.info("  Classified %d/%d pairs...", i + 1, len(pairs))

    logger.info("Classification complete. Writing output...")

    # Write TSV
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    columns = [
        "Lang_A", "Lang_B", "Phylo_Relation", "Tree_Distance",
        "MRCA_Clade", "MRCA_Depth", "Ancestor_Lang", "Family_A", "Family_B",
    ]
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter="\t")
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    logger.info("Wrote %d rows to %s", len(results), OUTPUT_FILE)

    # Print statistics
    from collections import Counter
    relation_counts = Counter(r["Phylo_Relation"] for r in results)
    logger.info("=== Distribution by Phylo_Relation ===")
    for relation, count in sorted(relation_counts.items(), key=lambda x: -x[1]):
        pct = 100 * count / len(results)
        logger.info("  %-18s %7d (%5.1f%%)", relation, count, pct)

    # Count unique families
    families_a = set(r["Family_A"] for r in results if r["Family_A"] != "-")
    families_b = set(r["Family_B"] for r in results if r["Family_B"] != "-")
    all_families = families_a | families_b
    logger.info("Unique families: %d", len(all_families))

    # Near-ancestral breakdown
    near_anc = [r for r in results if r["Phylo_Relation"] == "near_ancestral"]
    if near_anc:
        anc_counts = Counter(r["Ancestor_Lang"] for r in near_anc)
        logger.info("=== Near-Ancestral by Ancestor Language ===")
        for anc, count in sorted(anc_counts.items(), key=lambda x: -x[1]):
            logger.info("  %s: %d pairs", anc, count)


if __name__ == "__main__":
    main()
