#!/usr/bin/env python3
"""Comprehensive validation suite for ancient language TSV datasets.

Checks every lexicon TSV in data/training/lexicons/ against header format,
data-quality invariants, SCA consistency, metadata entry counts, and
artifact patterns.  Designed to run on Windows (handles encoding) and in CI.

Usage:
    python scripts/validate_all.py              # validate all files
    python scripts/validate_all.py --verbose    # show individual failing entries
    python scripts/validate_all.py --file hittite.tsv   # single file
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
METADATA_PATH = ROOT / "data" / "training" / "metadata" / "languages.tsv"

# ---------------------------------------------------------------------------
# Ensure cognate_pipeline importable
# ---------------------------------------------------------------------------
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))

try:
    from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # type: ignore

    HAS_SCA = True
except ImportError:
    HAS_SCA = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EXPECTED_HEADER = "Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID"
EXPECTED_COLUMNS = 6

ARTIFACT_RE = re.compile(
    r"inprogress|phoneticvalue|<[^>]+>|&[a-z]+;|\xa0",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------
class FileResult:
    """Accumulates pass / warn / fail status for a single TSV file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    @property
    def status(self) -> str:
        if self.errors:
            return "FAIL"
        if self.warnings:
            return "WARN"
        return "PASS"


# ---------------------------------------------------------------------------
# Metadata loader
# ---------------------------------------------------------------------------
def load_metadata() -> dict[str, int]:
    """Return {iso_code: expected_entry_count} from languages.tsv."""
    counts: dict[str, int] = {}
    if not METADATA_PATH.exists():
        return counts
    with open(METADATA_PATH, encoding="utf-8") as fh:
        header = fh.readline().strip()
        cols = header.split("\t")
        try:
            iso_idx = cols.index("ISO")
            entries_idx = cols.index("Entries")
        except ValueError:
            return counts
        for line in fh:
            parts = line.strip().split("\t")
            if len(parts) > max(iso_idx, entries_idx):
                iso = parts[iso_idx].strip()
                try:
                    counts[iso] = int(parts[entries_idx].strip())
                except ValueError:
                    pass
    return counts


# ---------------------------------------------------------------------------
# Core validation
# ---------------------------------------------------------------------------
def validate_file(tsv_path: Path, metadata: dict[str, int], verbose: bool) -> FileResult:
    """Run all checks on a single lexicon TSV and return a FileResult."""
    result = FileResult(tsv_path)

    # --- Read file ----------------------------------------------------------
    try:
        with open(tsv_path, encoding="utf-8") as fh:
            raw_lines = fh.readlines()
    except Exception as exc:
        result.error(f"Cannot read file: {exc}")
        return result

    if not raw_lines:
        result.error("File is empty")
        return result

    # --- 1. Header validation -----------------------------------------------
    header_line = raw_lines[0].rstrip("\n\r")
    if header_line != EXPECTED_HEADER:
        result.error(f"Bad header: got {header_line!r}")

    # --- Parse rows ---------------------------------------------------------
    rows: list[dict[str, str]] = []
    for lineno, raw in enumerate(raw_lines[1:], start=2):
        parts = raw.rstrip("\n\r").split("\t")
        if len(parts) != EXPECTED_COLUMNS:
            result.error(
                f"Line {lineno}: expected {EXPECTED_COLUMNS} columns, got {len(parts)}"
            )
            continue
        rows.append(
            {
                "Word": parts[0],
                "IPA": parts[1],
                "SCA": parts[2],
                "Source": parts[3],
                "Concept_ID": parts[4],
                "Cognate_Set_ID": parts[5],
                "_lineno": str(lineno),
            }
        )

    if not rows:
        result.error("No data rows found")
        return result

    # --- 2. No empty IPA ----------------------------------------------------
    empty_ipa = [r for r in rows if not r["IPA"].strip()]
    if empty_ipa:
        result.error(f"Empty IPA: {len(empty_ipa)} entries")
        if verbose:
            for r in empty_ipa[:20]:
                result.error(f"  line {r['_lineno']}: Word={r['Word']!r}")

    # --- 3. No duplicate words ----------------------------------------------
    seen_words: dict[str, int] = {}
    duplicates: list[tuple[str, int, int]] = []
    for r in rows:
        w = r["Word"]
        if w in seen_words:
            duplicates.append((w, seen_words[w], int(r["_lineno"])))
        else:
            seen_words[w] = int(r["_lineno"])
    if duplicates:
        result.error(f"Duplicate words: {len(duplicates)} duplicates")
        if verbose:
            for word, first, second in duplicates[:20]:
                result.error(f"  {word!r}: lines {first} and {second}")

    # --- 4. SCA consistency -------------------------------------------------
    if HAS_SCA:
        sca_mismatches: list[tuple[str, str, str, str]] = []
        for r in rows:
            sca_val = r["SCA"].strip()
            ipa_val = r["IPA"].strip()
            if not sca_val or not ipa_val:
                continue
            try:
                expected_sca = ipa_to_sound_class(ipa_val)
            except Exception:
                continue
            if expected_sca and expected_sca != sca_val:
                sca_mismatches.append(
                    (r["_lineno"], r["Word"], sca_val, expected_sca)
                )
        if sca_mismatches:
            result.error(f"SCA mismatches: {len(sca_mismatches)} entries")
            if verbose:
                for lineno, word, got, expected in sca_mismatches[:20]:
                    result.error(
                        f"  line {lineno}: {word!r} SCA={got!r} expected={expected!r}"
                    )

    # --- 5. Spurious "0" in SCA (warning) -----------------------------------
    zero_sca = [r for r in rows if r["SCA"].strip() == "0"]
    if zero_sca:
        result.warn(f'SCA == "0": {len(zero_sca)} entries')
        if verbose:
            for r in zero_sca[:20]:
                result.warn(f"  line {r['_lineno']}: Word={r['Word']!r}")

    # --- 6. Non-empty Source ------------------------------------------------
    empty_source = [r for r in rows if not r["Source"].strip()]
    if empty_source:
        result.error(f"Empty Source: {len(empty_source)} entries")
        if verbose:
            for r in empty_source[:20]:
                result.error(
                    f"  line {r['_lineno']}: Word={r['Word']!r}"
                )

    # --- 7. Entry count accuracy --------------------------------------------
    stem = tsv_path.stem  # e.g. "hittite" from hittite.tsv
    # Try matching by ISO code (stem) against metadata
    actual_count = len(rows)
    matched_iso: str | None = None
    for iso, expected_count in metadata.items():
        if iso.lower() == stem.lower() or stem.lower().startswith(iso.lower()):
            matched_iso = iso
            if actual_count != expected_count:
                result.error(
                    f"Entry count mismatch: TSV has {actual_count} rows, "
                    f"metadata ({iso}) says {expected_count}"
                )
            break
    # Also try matching by filename without extension
    if matched_iso is None and stem in metadata:
        expected_count = metadata[stem]
        if actual_count != expected_count:
            result.error(
                f"Entry count mismatch: TSV has {actual_count} rows, "
                f"metadata ({stem}) says {expected_count}"
            )

    # --- 8. Artifact patterns -----------------------------------------------
    artifact_hits: list[tuple[str, str, str]] = []
    for r in rows:
        for field in ("Word", "IPA", "SCA", "Source"):
            if ARTIFACT_RE.search(r[field]):
                artifact_hits.append((r["_lineno"], field, r[field]))
    if artifact_hits:
        result.warn(f"Artifact patterns detected: {len(artifact_hits)} hits")
        if verbose:
            for lineno, field, val in artifact_hits[:20]:
                result.warn(f"  line {lineno}: {field}={val!r}")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    # Handle Windows encoding for stdout / stderr
    if sys.platform == "win32":
        if hasattr(sys.stdout, "buffer"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "buffer"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Validate ancient language TSV datasets."
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show individual failing entries"
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        default=None,
        help="Validate a single file (name or path relative to lexicons/)",
    )
    args = parser.parse_args()

    # Discover files
    if args.file:
        target = Path(args.file)
        if not target.is_absolute():
            target = LEXICON_DIR / target
        if not target.suffix:
            target = target.with_suffix(".tsv")
        if not target.exists():
            print(f"ERROR: File not found: {target}", file=sys.stderr)
            return 1
        tsv_files = [target]
    else:
        if not LEXICON_DIR.is_dir():
            print(f"ERROR: Lexicon directory not found: {LEXICON_DIR}", file=sys.stderr)
            return 1
        tsv_files = sorted(LEXICON_DIR.glob("*.tsv"))
        if not tsv_files:
            print(f"ERROR: No TSV files found in {LEXICON_DIR}", file=sys.stderr)
            return 1

    # Load metadata
    metadata = load_metadata()
    if not metadata:
        print(
            f"WARNING: Could not load metadata from {METADATA_PATH}; "
            "skipping entry-count checks.",
            file=sys.stderr,
        )

    if not HAS_SCA:
        print(
            "WARNING: Could not import ipa_to_sound_class; "
            "skipping SCA consistency checks.",
            file=sys.stderr,
        )

    # Run validation
    results: list[FileResult] = []
    for tsv in tsv_files:
        res = validate_file(tsv, metadata, args.verbose)
        results.append(res)

    # ---------------------------------------------------------------------------
    # Output
    # ---------------------------------------------------------------------------
    total_pass = 0
    total_warn = 0
    total_fail = 0

    for res in results:
        status = res.status
        name = res.path.name

        if status == "PASS":
            total_pass += 1
            print(f"  PASS  {name}")
        elif status == "WARN":
            total_warn += 1
            print(f"  WARN  {name}  ({len(res.warnings)} warning(s))")
            for w in res.warnings:
                print(f"        {w}")
        else:
            total_fail += 1
            print(f"  FAIL  {name}  ({len(res.errors)} error(s), {len(res.warnings)} warning(s))")
            for e in res.errors:
                print(f"    [E] {e}")
            for w in res.warnings:
                print(f"    [W] {w}")

    # Summary
    total = len(results)
    print()
    print("=" * 60)
    print(f"SUMMARY: {total} file(s) validated")
    print(f"  PASS: {total_pass}    WARN: {total_warn}    FAIL: {total_fail}")
    print("=" * 60)

    if total_fail > 0:
        print("\nResult: FAILED (errors found)")
        return 1
    elif total_warn > 0:
        print("\nResult: PASSED with warnings")
        return 0
    else:
        print("\nResult: PASSED")
        return 0


if __name__ == "__main__":
    sys.exit(main())
