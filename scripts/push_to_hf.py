#!/usr/bin/env python3
"""Push updated files to HuggingFace dataset repo.

Usage:
    python scripts/push_to_hf.py [--dry-run]
"""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path

from huggingface_hub import HfApi

ROOT = Path(__file__).resolve().parent.parent
REPO_ID = "Nacryos/ancient-scripts-datasets"

# Phase 6-8 lexicons (ancient + proto languages)
LEXICON_ISOS = [
    # Phase 6-7
    "cop", "pli", "xcl", "ang", "gez", "hbo", "xht",
    "osc", "xum", "xve", "nci", "sga",
    "ojp", "pal", "sog", "xtg",
    "map", "gem-pro", "cel-pro", "urj-pro", "bnt-pro", "sit-pro",
    # Phase 8 Batch 1
    "sla-pro", "trk-pro", "itc-pro", "jpx-pro", "ira-pro",
    "xfa", "xlp", "xce", "xsa",
    # Phase 8 Batch 2
    "alg-pro", "sqj-pro", "aav-pro", "poz-pol-pro", "tai-pro",
    "xto-pro", "poz-oce-pro", "xgn-pro", "xmr", "obm",
    # Phase 8 Batch 3
    "myn-pro", "afa-pro", "xib",
    # Phase 8 Eblaite
    "xeb",
]


def collect_files() -> list[str]:
    """Collect all files that need uploading."""
    files = []

    # Lexicons
    for iso in LEXICON_ISOS:
        p = f"data/training/lexicons/{iso}.tsv"
        if (ROOT / p).exists():
            files.append(p)

    # Metadata
    files.append("data/training/metadata/languages.tsv")

    # Scripts
    for s in glob.glob(str(ROOT / "scripts" / "ingest_*.py")):
        files.append(os.path.relpath(s, ROOT).replace("\\", "/"))
    for s in [
        "scripts/transliteration_maps.py",
        "scripts/fetch_wiktionary_raw.py",
        "scripts/ingest_wiktionary_tier2.py",
        "scripts/reprocess_ipa.py",
        "scripts/validate_all.py",
        "scripts/update_metadata.py",
    ]:
        if (ROOT / s).exists():
            files.append(s)

    # Cognate pairs
    for cp in [
        "data/training/cognate_pairs/cognate_pairs_inherited.tsv",
        "data/training/cognate_pairs/cognate_pairs_borrowing.tsv",
        "data/training/cognate_pairs/cognate_pairs_similarity.tsv",
    ]:
        if (ROOT / cp).exists():
            files.append(cp)

    # Cognate pair extraction scripts
    for s in [
        "scripts/extract_abvd_cognates_v2.py",
        "scripts/extract_wold_borrowings_v2.py",
        "scripts/extract_iecor_cognates.py",
        "scripts/extract_sinotibetan_cognates_v2.py",
        "scripts/rebuild_concept_aligned_pairs.py",
        "scripts/merge_cognate_pairs.py",
        "scripts/cleanup_phase8_audit.py",
    ]:
        if (ROOT / s).exists():
            files.append(s)

    # Docs
    for d in [
        "docs/DATABASE_REFERENCE.md",
        "docs/prd/PRD_DATABASE_RECTIFICATION.md",
        "docs/prd/PRD_COGNATE_PAIRS_V2.md",
    ]:
        if (ROOT / d).exists():
            files.append(d)

    return sorted(set(files))


def main():
    parser = argparse.ArgumentParser(description="Push files to HuggingFace")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    files = collect_files()
    print(f"Files to upload: {len(files)}")
    for f in files:
        print(f"  {f}")

    if args.dry_run:
        print("\nDRY RUN — no files uploaded.")
        return

    api = HfApi()

    # Use CommitOperationAdd for a single atomic commit
    from huggingface_hub import CommitOperationAdd

    operations = []
    for f in files:
        local_path = str(ROOT / f)
        operations.append(CommitOperationAdd(
            path_in_repo=f,
            path_or_fileobj=local_path,
        ))

    print(f"\nUploading {len(operations)} files in a single commit...")
    try:
        api.create_commit(
            repo_id=REPO_ID,
            repo_type="dataset",
            operations=operations,
            commit_message="Add cognate pairs v2 (21.5M pairs) + Phase 8 audit fixes",
        )
        print(f"Done: {len(operations)} files uploaded to {REPO_ID}")
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    main()
