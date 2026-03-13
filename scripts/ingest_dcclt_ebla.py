#!/usr/bin/env python3
"""Ingest Eblaite lexical data from the ORACC DCCLT/Ebla corpus.

Source: https://oracc.museum.upenn.edu/json/dcclt-ebla.zip
License: CC0 (public domain)
Language code: akk-x-earakk (Early Archaic Akkadian = Eblaite, ~2400 BCE)
Academic editors: Marco Bonechi (Rome), Niek Veldhuis (Berkeley)

Iron Rule: All data comes from the downloaded ZIP. No hardcoded word forms.

Data sources inside the ZIP:
  - dcclt/ebla/gloss-akk-x-earakk.json  : 45 lemmatized glossary entries
  - dcclt/ebla/corpusjson/P*.json        : Per-text corpus files with tokens

Filtering rules:
  - Only syllabic (lowercase Assyriology transliteration) forms — no pure logograms
  - No broken/unreadable forms (containing 'x' as damage indicator)
  - No pure number strings
  - No mixed forms where ANY hyphen-delimited syllable is uppercase (= Sumerogram)
  - Minimum 2 characters after stripping hyphens and subscripts

Usage:
    python scripts/ingest_dcclt_ebla.py [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import unicodedata
import zipfile
from pathlib import Path

# Fix Windows encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from transliteration_maps import transliterate  # noqa: E402
from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"

ZIP_PATH = RAW_DIR / "dcclt-ebla.zip"
ISO = "xeb"
SOURCE = "dcclt-ebla"

# Subscript digit characters to strip (they indicate sign variant readings)
SUBSCRIPTS = str.maketrans("", "", "₀₁₂₃₄₅₆₇₈₉")


# ---------------------------------------------------------------------------
# Form filtering helpers
# ---------------------------------------------------------------------------

def strip_subscripts(s: str) -> str:
    """Remove subscript digit characters (₀₁₂₃₄₅₆₇₈₉) from a string."""
    return s.translate(SUBSCRIPTS)


def stripped_len(form: str) -> int:
    """Return the length of a form after stripping hyphens and subscripts."""
    return len(strip_subscripts(form).replace("-", ""))


def has_damage(form: str) -> bool:
    """Return True if any syllable is exactly 'x' (damage/lacuna indicator)."""
    for part in form.split("-"):
        if strip_subscripts(part).lower() == "x":
            return True
    return False


def is_pure_number(form: str) -> bool:
    """Return True if the form is purely numeric (line numbers, counts)."""
    return form.replace("-", "").isdigit()


def is_syllable_uppercase(syllable: str) -> bool:
    """Return True if a syllable contains uppercase ASCII letters (= logogram)."""
    clean = strip_subscripts(syllable)
    return any(c.isupper() for c in clean)


def is_pure_syllabic(form: str) -> bool:
    """Return True if ALL syllables are lowercase (syllabic readings only).

    Rejects forms where any hyphen-delimited part is a Sumerogram (uppercase).
    Also rejects parenthesized complex signs like idₓ(NI), which embed logograms.
    """
    if is_pure_number(form) or has_damage(form):
        return False
    # Reject forms with parenthesized sign names (e.g. idₓ(NI))
    if "(" in form or ")" in form:
        return False
    # Reject forms with pipe-enclosed complex signs (|GUD×DIŠ|)
    if "|" in form:
        return False
    # Reject forms with determinative markers ({d}, {x})
    if "{" in form:
        return False
    # Reject forms with degree sign (°) — marks uncertain/dubious readings in ORACC
    if "\u00b0" in form:
        return False
    parts = form.split("-")
    if not parts:
        return False
    return all(not is_syllable_uppercase(p) for p in parts if p)


def keep_form(form: str) -> bool:
    """Combined filter: True if the form should be retained."""
    if not form:
        return False
    form_nfc = unicodedata.normalize("NFC", form)
    return (
        is_pure_syllabic(form_nfc)
        and stripped_len(form_nfc) >= 2
    )


# ---------------------------------------------------------------------------
# CDL walker (ORACC JSON corpus format)
# ---------------------------------------------------------------------------

def walk_cdl(node: object, tokens: list) -> None:
    """Recursively walk an ORACC CDL node tree, collecting token dicts."""
    if isinstance(node, dict):
        if node.get("node") == "l":
            f = node.get("f", {})
            if f.get("lang") == "akk-x-earakk":
                tokens.append({
                    "form": f.get("form", ""),
                    "norm": f.get("norm", ""),
                    "cf": f.get("cf", ""),
                    "gw": f.get("gw", ""),
                })
        for child in node.get("cdl", []):
            walk_cdl(child, tokens)
    elif isinstance(node, list):
        for item in node:
            walk_cdl(item, tokens)


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def extract_glossary_entries(gloss: dict) -> list[dict]:
    """Extract all entries from the glossary JSON.

    Returns list of dicts with: form, cf, gw, pos
    Only syllabic forms are kept (logograms like 'A', 'MUŠ' are filtered).
    """
    entries = gloss.get("entries", [])
    results = []
    for entry in entries:
        cf = entry.get("cf", "").strip()
        gw = entry.get("gw", "").strip()
        pos = entry.get("pos", "").strip()
        for frm in entry.get("forms", []):
            n = frm.get("n", "").strip()
            if n and keep_form(n):
                results.append({
                    "form": unicodedata.normalize("NFC", n),
                    "cf": cf,
                    "gw": gw,
                    "pos": pos,
                })
    return results


def extract_corpus_tokens(z: zipfile.ZipFile) -> list[dict]:
    """Walk all corpus JSON files and collect Eblaite tokens."""
    corpus_files = [n for n in z.namelist() if "corpusjson/P" in n]
    tokens = []
    for nm in corpus_files:
        raw = z.read(nm)
        if not raw.strip():
            continue
        try:
            doc = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("Skipping %s: JSON error: %s", nm, exc)
            continue
        walk_cdl(doc.get("cdl", []), tokens)
    return tokens


def build_glossary_lookup(gloss: dict) -> dict[str, str]:
    """Build form-string -> guide-word lookup from glossary entries."""
    lookup = {}
    for entry in gloss.get("entries", []):
        gw = entry.get("gw", "").strip()
        for frm in entry.get("forms", []):
            n = frm.get("n", "").strip()
            if n:
                lookup[unicodedata.normalize("NFC", n)] = gw
    return lookup


def convert_to_ipa(form: str) -> str:
    """Convert an Eblaite syllabic form to approximate IPA.

    Steps:
    1. NFC normalize
    2. Apply EBLAITE_MAP (inherits AKKADIAN_MAP) via transliterate()
    3. Strip subscript digits from the result
    4. Remove hyphens (syllable boundaries are not phonemic in the output)
    """
    form_nfc = unicodedata.normalize("NFC", form)
    ipa_with_hyphens = transliterate(form_nfc, ISO)
    # Strip subscript digits that survive the map (sign variant markers)
    ipa_no_subscripts = strip_subscripts(ipa_with_hyphens)
    # Remove hyphens — syllable boundaries are orthographic, not phonemic
    ipa = ipa_no_subscripts.replace("-", "")
    return ipa


def ingest(dry_run: bool = False) -> dict:
    """Run the full ingestion pipeline."""
    if not ZIP_PATH.exists():
        raise FileNotFoundError(f"ZIP not found: {ZIP_PATH}")

    tsv_path = LEXICON_DIR / f"{ISO}.tsv"
    audit_path = AUDIT_TRAIL_DIR / f"dcclt_ingest_{ISO}.jsonl"

    # Load existing words to avoid duplicates
    existing: set[str] = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.startswith("Word\t"):
                    word = line.split("\t")[0]
                    if word:
                        existing.add(word)
    logger.info("Existing %s entries: %d", ISO, len(existing))

    with zipfile.ZipFile(ZIP_PATH) as z:
        # Step 1: Parse glossary
        with z.open("dcclt/ebla/gloss-akk-x-earakk.json") as f:
            gloss = json.load(f)

        gloss_entries = extract_glossary_entries(gloss)
        logger.info("Glossary entries (syllabic only): %d", len(gloss_entries))

        # Build gloss lookup
        form_to_gw = build_glossary_lookup(gloss)

        # Step 2: Parse corpus tokens
        all_corpus_tokens = extract_corpus_tokens(z)
        logger.info("Total corpus tokens (all Eblaite): %d", len(all_corpus_tokens))

    # Step 3: Collect all unique syllabic corpus forms
    corpus_forms_raw = [t["form"] for t in all_corpus_tokens]
    total_corpus_forms = len(corpus_forms_raw)
    unique_corpus_forms_raw = set(corpus_forms_raw)
    logger.info("Unique raw corpus forms: %d", len(unique_corpus_forms_raw))

    # Filter to syllabic only
    syllabic_corpus_forms = {
        unicodedata.normalize("NFC", f)
        for f in unique_corpus_forms_raw
        if keep_form(f)
    }
    logger.info("Unique syllabic corpus forms (after filtering): %d", len(syllabic_corpus_forms))

    # Step 4: Merge glossary forms + corpus forms
    # Glossary forms come first (they have gloss metadata)
    all_entries = []

    # From glossary
    for e in gloss_entries:
        form = e["form"]
        if form not in existing:
            gw = e["gw"]
            concept_id = gw if gw else "-"
            ipa = convert_to_ipa(form)
            try:
                sca = ipa_to_sound_class(ipa)
            except Exception:
                sca = ""
            all_entries.append({
                "word": form,
                "ipa": ipa,
                "sca": sca,
                "source": SOURCE,
                "concept_id": concept_id,
                "cognate_set_id": "-",
                "gw": gw,
            })
            existing.add(form)

    # From corpus (forms not already in glossary)
    for form in sorted(syllabic_corpus_forms):
        if form in existing:
            continue
        gw = form_to_gw.get(form, "")
        concept_id = gw if gw else "-"
        ipa = convert_to_ipa(form)
        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""
        all_entries.append({
            "word": form,
            "ipa": ipa,
            "sca": sca,
            "source": SOURCE,
            "concept_id": concept_id,
            "cognate_set_id": "-",
            "gw": gw,
        })
        existing.add(form)

    logger.info("New entries to write: %d", len(all_entries))

    # Step 5: Identity rate calculation
    n_identity = sum(1 for e in all_entries if e["ipa"] == e["word"])
    identity_pct = n_identity / len(all_entries) * 100 if all_entries else 0

    stats = {
        "iso": ISO,
        "total_corpus_tokens": total_corpus_forms,
        "unique_corpus_forms_raw": len(unique_corpus_forms_raw),
        "unique_syllabic": len(syllabic_corpus_forms),
        "glossary_entries_syllabic": len(gloss_entries),
        "new_entries": len(all_entries),
        "identity_count": n_identity,
        "identity_pct": round(identity_pct, 1),
    }

    if dry_run:
        stats["status"] = "dry_run"
        return stats

    # Step 6: Write TSV
    LEXICON_DIR.mkdir(parents=True, exist_ok=True)
    if not tsv_path.exists():
        with open(tsv_path, "w", encoding="utf-8") as f:
            f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

    if all_entries:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in all_entries:
                f.write(
                    f"{e['word']}\t{e['ipa']}\t{e['sca']}\t"
                    f"{e['source']}\t{e['concept_id']}\t{e['cognate_set_id']}\n"
                )

    # Step 7: Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(audit_path, "w", encoding="utf-8") as f:
        for e in all_entries:
            record = {
                "word": e["word"],
                "ipa": e["ipa"],
                "source": e["source"],
                "concept_id": e["concept_id"],
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    stats["status"] = "written"
    logger.info("TSV written: %s", tsv_path)
    logger.info("Audit trail written: %s", audit_path)

    return stats


def main():
    parser = argparse.ArgumentParser(description="Ingest Eblaite data from DCCLT/Ebla ORACC corpus")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without writing files")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    stats = ingest(dry_run=args.dry_run)

    print()
    print("=" * 70)
    print(f"DCCLT/Ebla Ingestion  [{'DRY RUN' if args.dry_run else 'LIVE'}]")
    print("=" * 70)
    print(f"  ISO code:               {stats['iso']}")
    print(f"  Total corpus tokens:    {stats['total_corpus_tokens']}")
    print(f"  Unique raw forms:       {stats['unique_corpus_forms_raw']}")
    print(f"  Unique syllabic forms:  {stats['unique_syllabic']}")
    print(f"  Glossary entries (syl): {stats['glossary_entries_syllabic']}")
    print(f"  New entries written:    {stats['new_entries']}")
    print(f"  Identity rate:          {stats['identity_pct']:.1f}%  "
          f"({stats['identity_count']}/{stats['new_entries']} forms unchanged)")
    print(f"  Status:                 {stats['status']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
