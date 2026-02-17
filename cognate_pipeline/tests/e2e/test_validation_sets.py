"""End-to-end tests for the stratified validation datasets.

Validates the output of scripts/build_validation_sets.py: file existence,
schema correctness, label/phylo_dist/timespan validity, and structural
constraints (L1 pairs share a sub-branch, L4/false positives cross families,
religious files contain only religious concepts, etc.).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

# Paths relative to the ancient-scripts-datasets repo
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "ancient-scripts-datasets"
VALIDATION_DIR = REPO_ROOT / "data" / "training" / "validation"
FAMILY_MAP_PATH = (
    REPO_ROOT
    / "cognate_pipeline"
    / "src"
    / "cognate_pipeline"
    / "cognate"
    / "family_map.json"
)

# Expected column header
EXPECTED_FIELDS = [
    "Lang_A", "Word_A", "IPA_A", "SCA_A",
    "Lang_B", "Word_B", "IPA_B", "SCA_B",
    "Concept_ID", "Label", "Phylo_Dist", "Timespan", "Score", "Source",
]

VALID_LABELS = {"true_cognate", "false_positive", "true_negative", "borrowing"}
VALID_PHYLO_DIST = {"L1", "L2", "L3", "L4"}
VALID_TIMESPANS = {"ancient_ancient", "ancient_modern", "medieval_modern", "modern_modern"}

TOP_FAMILIES = [
    "germanic", "italic", "balto_slavic", "indo_iranian", "hellenic",
    "celtic", "uralic", "turkic", "sino_tibetan", "austronesian",
    "semitic", "dravidian", "japonic", "koreanic", "kartvelian",
]

RELIGIOUS_CONCEPTS = {
    "3231", "53", "911", "853", "1103", "257", "24", "852", "1702", "304",
    "391", "8", "303", "1565", "878", "1973", "1945", "392", "2137", "1175",
    "107", "1349", "1603", "811", "2971", "661", "1944",
    "DEITY/GOD", "SPIRIT", "TEMPLE", "ALTAR", "SACRIFICE", "WORSHIP", "PRAY",
    "PRIEST", "HOLY", "PREACH", "BLESS", "CURSE", "FAST", "HEAVEN", "HELL",
    "DEMON", "IDOL", "MAGIC", "SORCERER", "GHOST", "OMEN", "CHURCH", "MOSQUE",
    "SOUL", "SIN", "RELIGION", "GOD",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_tsv(path: Path) -> list[dict[str, str]]:
    """Read a TSV file and return a list of row dicts."""
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows.append(dict(row))
    return rows


def _read_tsv_header(path: Path) -> list[str]:
    """Read only the header of a TSV file."""
    with path.open(encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        return next(reader)


def _is_religious(concept_id: str) -> bool:
    """Check if a concept ID is religious (case-insensitive)."""
    if concept_id in RELIGIOUS_CONCEPTS:
        return True
    return concept_id.upper() in {c.upper() for c in RELIGIOUS_CONCEPTS}


@pytest.fixture(scope="module")
def family_map() -> dict[str, str]:
    """Load family_map.json."""
    with FAMILY_MAP_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def phylo_tree() -> dict:
    """Load phylo_tree.json."""
    tree_path = VALIDATION_DIR / "phylo_tree.json"
    with tree_path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _collect_isos_from_tree(node) -> set[str]:
    """Recursively collect all ISO codes from a tree node."""
    if isinstance(node, list):
        return set(node)
    if isinstance(node, str):
        return {node}
    if isinstance(node, dict):
        isos: set[str] = set()
        for v in node.values():
            isos |= _collect_isos_from_tree(v)
        return isos
    return set()


def _find_iso_in_tree(tree: dict, iso: str) -> list[str] | None:
    """Find the path to an ISO code in the tree. Returns list of keys."""
    def _walk(node, prefix):
        if isinstance(node, list):
            if iso in node:
                return prefix
        elif isinstance(node, str):
            if node == iso:
                return prefix
        elif isinstance(node, dict):
            for key, child in node.items():
                result = _walk(child, prefix + [key])
                if result is not None:
                    return result
        return None
    return _walk(tree, [])


# ---------------------------------------------------------------------------
# Core file existence tests
# ---------------------------------------------------------------------------

CORE_FILES = [
    "phylo_tree.json",
    "true_cognates_L1.tsv",
    "true_cognates_L2.tsv",
    "true_cognates_L3.tsv",
    "false_positives.tsv",
    "true_negatives.tsv",
    "borrowings.tsv",
    "religious_pairs.tsv",
    "timespan_ancient_ancient.tsv",
    "timespan_ancient_modern.tsv",
    "timespan_medieval_modern.tsv",
    "timespan_modern_modern.tsv",
    "validation_stats.tsv",
]


@pytest.mark.parametrize("filename", CORE_FILES)
def test_core_file_exists_and_nonempty(filename):
    """All core output files exist and are non-empty."""
    path = VALIDATION_DIR / filename
    assert path.exists(), f"Missing: {path}"
    assert path.stat().st_size > 0, f"Empty: {path}"


@pytest.mark.parametrize("family", TOP_FAMILIES)
def test_per_family_file_exists(family):
    """Per-family validation files exist."""
    path = VALIDATION_DIR / "per_family" / f"{family}.tsv"
    assert path.exists(), f"Missing: {path}"
    assert path.stat().st_size > 0, f"Empty: {path}"


# ---------------------------------------------------------------------------
# Header format tests
# ---------------------------------------------------------------------------

PAIR_FILES = [
    "true_cognates_L1.tsv",
    "true_cognates_L2.tsv",
    "true_cognates_L3.tsv",
    "false_positives.tsv",
    "true_negatives.tsv",
    "borrowings.tsv",
    "religious_pairs.tsv",
]


@pytest.mark.parametrize("filename", PAIR_FILES)
def test_header_format(filename):
    """Header matches expected 14-column schema."""
    path = VALIDATION_DIR / filename
    header = _read_tsv_header(path)
    assert header == EXPECTED_FIELDS, f"Header mismatch in {filename}: {header}"


# ---------------------------------------------------------------------------
# Label, Phylo_Dist, Timespan validity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("filename", PAIR_FILES)
def test_labels_valid(filename):
    """All Label values are one of the valid set."""
    path = VALIDATION_DIR / filename
    rows = _read_tsv(path)
    labels = {r["Label"] for r in rows}
    invalid = labels - VALID_LABELS
    assert not invalid, f"Invalid labels in {filename}: {invalid}"


@pytest.mark.parametrize("filename", PAIR_FILES)
def test_phylo_dist_valid(filename):
    """All Phylo_Dist values are L1-L4."""
    path = VALIDATION_DIR / filename
    rows = _read_tsv(path)
    dists = {r["Phylo_Dist"] for r in rows}
    invalid = dists - VALID_PHYLO_DIST
    assert not invalid, f"Invalid phylo_dist in {filename}: {invalid}"


@pytest.mark.parametrize("filename", PAIR_FILES)
def test_timespan_valid(filename):
    """All Timespan values are valid."""
    path = VALIDATION_DIR / filename
    rows = _read_tsv(path)
    timespans = {r["Timespan"] for r in rows}
    invalid = timespans - VALID_TIMESPANS
    assert not invalid, f"Invalid timespans in {filename}: {invalid}"


# ---------------------------------------------------------------------------
# Structural constraint tests
# ---------------------------------------------------------------------------


def test_l1_same_subbranch(phylo_tree):
    """L1 pairs: both languages should be in the same sub-branch of the tree."""
    rows = _read_tsv(VALIDATION_DIR / "true_cognates_L1.tsv")
    # Sample 200 pairs to keep the test fast
    sample = rows[:200]
    violations = 0
    for row in sample:
        path_a = _find_iso_in_tree(phylo_tree, row["Lang_A"])
        path_b = _find_iso_in_tree(phylo_tree, row["Lang_B"])
        if path_a is not None and path_b is not None:
            if path_a != path_b:
                violations += 1
    # Allow some violations (languages resolved from family_map may not be in tree)
    assert violations < len(sample) * 0.1, (
        f"Too many L1 violations: {violations}/{len(sample)}"
    )


def test_false_positives_cross_family(family_map):
    """False positive pairs should be from different families."""
    rows = _read_tsv(VALIDATION_DIR / "false_positives.tsv")
    sample = rows[:200]
    cross_family = 0
    for row in sample:
        fam_a = family_map.get(row["Lang_A"], "unknown_a")
        fam_b = family_map.get(row["Lang_B"], "unknown_b")
        if fam_a != fam_b:
            cross_family += 1
    # At least 80% should be cross-family
    assert cross_family >= len(sample) * 0.8, (
        f"Only {cross_family}/{len(sample)} false positives are cross-family"
    )


def test_religious_pairs_contain_religious_concepts():
    """Religious pairs file should only contain religious concept IDs."""
    rows = _read_tsv(VALIDATION_DIR / "religious_pairs.tsv")
    non_religious = 0
    for row in rows:
        cid = row["Concept_ID"]
        # Handle compound concept IDs from true negatives
        cids = [c.strip() for c in cid.split("/")]
        if not any(_is_religious(c) for c in cids):
            non_religious += 1
    assert non_religious == 0, (
        f"{non_religious} pairs in religious_pairs.tsv have non-religious concepts"
    )


def test_per_family_languages_belong(family_map):
    """Per-family files should primarily contain languages from that family."""
    for fam in TOP_FAMILIES[:5]:  # Test a sample of families
        path = VALIDATION_DIR / "per_family" / f"{fam}.tsv"
        if not path.exists():
            continue
        rows = _read_tsv(path)
        sample = rows[:200]
        matching = 0
        for row in sample:
            fam_a = family_map.get(row["Lang_A"], "")
            fam_b = family_map.get(row["Lang_B"], "")
            # At least one language should belong to this family
            # (cross-family false positives will have one language from another family)
            if fam_a == fam or fam_b == fam:
                matching += 1
            elif fam == "balto_slavic" and (fam_a == "slavic" or fam_b == "slavic"):
                matching += 1
        # At least 70% should have at least one language from the family
        assert matching >= len(sample) * 0.7, (
            f"Only {matching}/{len(sample)} pairs in {fam}.tsv "
            f"have a language from {fam}"
        )


# ---------------------------------------------------------------------------
# Duplicate check
# ---------------------------------------------------------------------------


def test_no_duplicates_l1():
    """No duplicate pairs in L1 file."""
    rows = _read_tsv(VALIDATION_DIR / "true_cognates_L1.tsv")
    keys = set()
    dups = 0
    for row in rows:
        key = (row["Lang_A"], row["Word_A"], row["Lang_B"], row["Word_B"], row["Concept_ID"])
        if key in keys:
            dups += 1
        keys.add(key)
    # Allow very small number of duplicates (from different sources)
    assert dups < len(rows) * 0.01, f"Too many duplicates in L1: {dups}/{len(rows)}"


# ---------------------------------------------------------------------------
# Minimum pair count tests
# ---------------------------------------------------------------------------


def test_minimum_pairs_l1():
    """L1 file should have at least 1,000 pairs."""
    rows = _read_tsv(VALIDATION_DIR / "true_cognates_L1.tsv")
    assert len(rows) >= 1_000, f"L1 only has {len(rows)} pairs"


def test_minimum_pairs_l2():
    """L2 file should have at least 1,000 pairs."""
    rows = _read_tsv(VALIDATION_DIR / "true_cognates_L2.tsv")
    assert len(rows) >= 1_000, f"L2 only has {len(rows)} pairs"


def test_minimum_pairs_l3():
    """L3 file should have at least 1,000 pairs."""
    rows = _read_tsv(VALIDATION_DIR / "true_cognates_L3.tsv")
    assert len(rows) >= 1_000, f"L3 only has {len(rows)} pairs"


def test_minimum_pairs_false_positives():
    """False positives file should have at least 1,000 pairs."""
    rows = _read_tsv(VALIDATION_DIR / "false_positives.tsv")
    assert len(rows) >= 1_000, f"False positives only has {len(rows)} pairs"


def test_minimum_pairs_true_negatives():
    """True negatives file should have at least 1,000 pairs."""
    rows = _read_tsv(VALIDATION_DIR / "true_negatives.tsv")
    assert len(rows) >= 1_000, f"True negatives only has {len(rows)} pairs"


def test_minimum_pairs_borrowings():
    """Borrowings file should have at least 1,000 pairs."""
    rows = _read_tsv(VALIDATION_DIR / "borrowings.tsv")
    assert len(rows) >= 1_000, f"Borrowings only has {len(rows)} pairs"


# ---------------------------------------------------------------------------
# Religious subset tests
# ---------------------------------------------------------------------------


def test_religious_subset_concept_diversity():
    """Religious subset should have at least 10 distinct concept types."""
    rows = _read_tsv(VALIDATION_DIR / "religious_pairs.tsv")
    concepts = set()
    for row in rows:
        cid = row["Concept_ID"]
        for c in cid.split("/"):
            c = c.strip()
            if _is_religious(c):
                concepts.add(c.upper())
    assert len(concepts) >= 10, (
        f"Religious subset has only {len(concepts)} concept types: {concepts}"
    )


# ---------------------------------------------------------------------------
# Timespan tests
# ---------------------------------------------------------------------------


def test_at_least_3_timespans_populated():
    """At least 3 of the 4 timespan categories should have data."""
    populated = 0
    for ts in ["ancient_ancient", "ancient_modern", "medieval_modern", "modern_modern"]:
        path = VALIDATION_DIR / f"timespan_{ts}.tsv"
        if path.exists():
            rows = _read_tsv(path)
            if len(rows) > 0:
                populated += 1
    assert populated >= 3, f"Only {populated} timespan categories populated"


# ---------------------------------------------------------------------------
# Phylo tree structure
# ---------------------------------------------------------------------------


def test_phylo_tree_valid_json():
    """phylo_tree.json is valid JSON with expected top-level families."""
    tree_path = VALIDATION_DIR / "phylo_tree.json"
    with tree_path.open(encoding="utf-8") as fh:
        tree = json.load(fh)
    assert isinstance(tree, dict)
    # Should have at least a few top-level families
    expected_families = {"indo_european", "uralic", "turkic", "sino_tibetan"}
    assert expected_families.issubset(set(tree.keys())), (
        f"Missing families. Have: {set(tree.keys())}"
    )


def test_phylo_tree_contains_languages(phylo_tree):
    """Phylo tree should contain at least 100 unique ISO codes."""
    isos = _collect_isos_from_tree(phylo_tree)
    assert len(isos) >= 100, f"Tree only has {len(isos)} languages"


# ---------------------------------------------------------------------------
# Stats file
# ---------------------------------------------------------------------------


def test_stats_file_has_content():
    """validation_stats.tsv should have meaningful content."""
    path = VALIDATION_DIR / "validation_stats.tsv"
    rows: list[list[str]] = []
    with path.open(encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        for row in reader:
            rows.append(row)
    assert len(rows) >= 10, f"Stats file only has {len(rows)} rows"
    # Check header
    assert rows[0] == ["Category", "Subset", "Count"]
