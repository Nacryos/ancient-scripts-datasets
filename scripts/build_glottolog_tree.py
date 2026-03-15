#!/usr/bin/env python3
"""Parse Glottolog NEXUS classification into a JSON tree index.

Reads the Glottolog CLDF languages.csv and classification.nex files,
builds ancestry paths for every ISO-bearing languoid, and writes
a JSON index mapping ISO codes to their tree positions.

Usage:
    python scripts/build_glottolog_tree.py
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)

GLOTTOLOG_DIR = ROOT / "data" / "training" / "raw" / "glottolog_cldf"
OUTPUT_FILE = GLOTTOLOG_DIR / "glottolog_tree.json"


def parse_newick(newick: str) -> dict:
    """Parse a Newick tree string into a parent->children dict.

    Returns a dict mapping each node label to its list of child labels,
    plus a special key '_root' with the root label.

    The Glottolog NEXUS uses format: (child1:1,child2:1)parent:1
    """
    # Remove trailing semicolon
    newick = newick.strip().rstrip(";")

    children_of: dict[str, list[str]] = {}
    parent_of: dict[str, str] = {}

    # Parse by walking the string character by character
    stack: list[list[str]] = []  # stack of child-lists being built
    i = 0
    n = len(newick)

    while i < n:
        c = newick[i]
        if c == "(":
            stack.append([])
            i += 1
        elif c == ")":
            # Read the label after the closing paren
            i += 1
            label, i = _read_label(newick, i)
            child_list = stack.pop()
            children_of[label] = child_list
            for child in child_list:
                parent_of[child] = label
            # Add this node to parent's child list (if any)
            if stack:
                stack[-1].append(label)
            else:
                # This is the root
                children_of["_root"] = label
        elif c == ",":
            i += 1
        else:
            # Leaf node
            label, i = _read_label(newick, i)
            if stack:
                stack[-1].append(label)
            else:
                children_of["_root"] = label

    return children_of


def _read_label(s: str, i: int) -> tuple[str, int]:
    """Read a node label (glottocode) and skip any :branchlength suffix."""
    start = i
    while i < len(s) and s[i] not in "(),;":
        i += 1
    raw = s[start:i]
    # Strip branch length (e.g., "abkh1242:1" -> "abkh1242")
    label = raw.split(":")[0]
    return label, i


def build_ancestry_paths(children_of: dict) -> dict[str, list[str]]:
    """Build leaf-to-root ancestry paths from a parsed tree.

    Returns dict mapping each leaf label to its full path [root, ..., leaf].
    """
    root = children_of["_root"]

    # Build parent lookup
    parent_of: dict[str, str] = {}
    for node, kids in children_of.items():
        if node == "_root":
            continue
        if isinstance(kids, list):
            for kid in kids:
                parent_of[kid] = node

    # Find all leaves (nodes not in children_of, or with no children)
    all_nodes = set()
    all_nodes.add(root)
    for node, kids in children_of.items():
        if node == "_root":
            continue
        all_nodes.add(node)
        if isinstance(kids, list):
            for kid in kids:
                all_nodes.add(kid)

    internal = {n for n in children_of if n != "_root" and isinstance(children_of.get(n), list)}
    leaves = all_nodes - internal

    # Build paths
    paths: dict[str, list[str]] = {}
    for leaf in leaves:
        path = [leaf]
        node = leaf
        while node in parent_of:
            node = parent_of[node]
            path.append(node)
        path.reverse()  # root -> ... -> leaf
        paths[leaf] = path

    # Also store paths for internal nodes (needed for languoids that are families)
    for node in internal:
        path = [node]
        n = node
        while n in parent_of:
            n = parent_of[n]
            path.append(n)
        path.reverse()
        paths[node] = path

    return paths


def load_iso_mapping() -> dict[str, tuple[str, str]]:
    """Load ISO 639-3 -> (glottocode, family_id) mapping from languages.csv.

    Returns dict mapping ISO code to (glottocode, family_id).
    """
    lang_file = GLOTTOLOG_DIR / "languages.csv"
    mapping: dict[str, tuple[str, str]] = {}
    with open(lang_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            iso = row.get("ISO639P3code", "").strip()
            if iso:
                gc = row["Glottocode"]
                fam = row.get("Family_ID", "").strip()
                mapping[iso] = (gc, fam)
    return mapping


def load_family_names() -> dict[str, str]:
    """Load glottocode -> name mapping for family-level nodes."""
    lang_file = GLOTTOLOG_DIR / "languages.csv"
    names: dict[str, str] = {}
    with open(lang_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gc = row["Glottocode"]
            names[gc] = row["Name"]
    return names


def parse_nexus_file() -> dict[str, dict]:
    """Parse the full NEXUS file, return {family_glottocode: children_of_dict}."""
    nex_file = GLOTTOLOG_DIR / "classification.nex"
    trees: dict[str, dict] = {}

    with open(nex_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("tree "):
                continue
            # Parse: tree <name> = [&R] <newick>;
            m = re.match(r"tree\s+(\S+)\s*=\s*(?:\[&R\]\s*)?(.+)", line)
            if not m:
                continue
            tree_name = m.group(1)
            newick = m.group(2)
            try:
                children_of = parse_newick(newick)
                trees[tree_name] = children_of
            except Exception as e:
                logger.warning("Failed to parse tree %s: %s", tree_name, e)

    return trees


def build_tree_index() -> dict:
    """Build the complete tree index: ISO -> tree position info.

    Returns a dict with:
      - "languages": {iso: {glottocode, path, family, depth}}
      - "family_names": {glottocode: name}
    """
    logger.info("Loading ISO mapping...")
    iso_map = load_iso_mapping()
    logger.info("Loaded %d ISO codes", len(iso_map))

    logger.info("Loading family names...")
    gc_names = load_family_names()

    logger.info("Parsing NEXUS trees...")
    trees = parse_nexus_file()
    logger.info("Parsed %d family trees", len(trees))

    # Build ancestry paths for all trees
    all_paths: dict[str, list[str]] = {}  # glottocode -> path
    for family_gc, children_of in trees.items():
        paths = build_ancestry_paths(children_of)
        all_paths.update(paths)

    logger.info("Built %d ancestry paths", len(all_paths))

    # Now map ISO codes to their tree positions
    index: dict[str, dict] = {}
    found = 0
    missing = 0
    isolate = 0

    for iso, (gc, family_id) in iso_map.items():
        if gc in all_paths:
            path = all_paths[gc]
            family = path[0]  # root of the tree = top-level family
            depth = len(path) - 1  # distance from root
            index[iso] = {
                "glottocode": gc,
                "path": path,
                "family": family,
                "family_name": gc_names.get(family, family),
                "depth": depth,
            }
            found += 1
        elif family_id:
            # Language has a family but isn't in any tree
            # This can happen for isolates or unclassified languages
            index[iso] = {
                "glottocode": gc,
                "path": [family_id, gc] if family_id != gc else [gc],
                "family": family_id,
                "family_name": gc_names.get(family_id, family_id),
                "depth": 1 if family_id != gc else 0,
            }
            found += 1
            isolate += 1
        else:
            # True isolate or unclassified - no family
            # Check if the glottocode IS a top-level family (isolate families)
            if gc in trees:
                index[iso] = {
                    "glottocode": gc,
                    "path": [gc],
                    "family": gc,
                    "family_name": gc_names.get(gc, gc),
                    "depth": 0,
                }
                found += 1
                isolate += 1
            else:
                missing += 1

    logger.info(
        "ISO mapping: %d found (%d isolate/fallback), %d missing",
        found, isolate, missing,
    )

    return {
        "languages": index,
        "family_names": {gc: gc_names.get(gc, gc) for gc in set(
            info["family"] for info in index.values()
        )},
        "stats": {
            "total_iso_codes": len(iso_map),
            "mapped": found,
            "isolate_fallback": isolate,
            "unmapped": missing,
            "families_in_nexus": len(trees),
        },
    }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Check input files exist
    for fname in ("languages.csv", "classification.nex"):
        fpath = GLOTTOLOG_DIR / fname
        if not fpath.exists():
            logger.error("Missing: %s — run ingest_glottolog.py first", fpath)
            sys.exit(1)

    tree_index = build_tree_index()

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(tree_index, f, ensure_ascii=False, indent=1)

    logger.info("Wrote tree index to %s", OUTPUT_FILE)
    logger.info("Stats: %s", json.dumps(tree_index["stats"]))

    # Print some sample entries for verification
    samples = ["eng", "deu", "fra", "spa", "lat", "grc", "hin", "san", "jpn", "tgl"]
    langs = tree_index["languages"]
    for iso in samples:
        if iso in langs:
            info = langs[iso]
            path_str = " > ".join(info["path"][-4:])  # last 4 nodes
            logger.info(
                "  %s: family=%s, depth=%d, path=...%s",
                iso, info["family_name"], info["depth"], path_str,
            )


if __name__ == "__main__":
    main()
