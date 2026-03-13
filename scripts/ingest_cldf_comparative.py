#!/usr/bin/env python3
"""Ingest word forms from CLDF comparative datasets (lexibank).

Supports multiple CLDF Wordlist datasets from lexibank GitHub repos.
Each dataset has forms.csv with standard columns: Language_ID, Form, etc.

Sources:
  - diacl (Carling 2017): Gaulish (xtg), and others
  - iecor (IE-CoR): Sogdian (sog), and others

Iron Rule: Data comes from downloaded CSV files. No hardcoded word lists.

Usage:
    python scripts/ingest_cldf_comparative.py [--dataset NAME] [--language ISO] [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sys
import unicodedata
import urllib.request
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402
from transliteration_maps import transliterate  # noqa: E402

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"

# Dataset configurations
DATASETS = {
    "diacl": {
        "repo": "lexibank/diacl",
        "branch": "master",
        "forms_path": "cldf/forms.csv",
        "languages_path": "cldf/languages.csv",
        "language_id_field": "Language_ID",
        "form_field": "Form",
        "targets": {
            "xtg": {"language_ids": ["39200"], "source_tag": "diacl"},
        },
    },
    "iecor": {
        "repo": "lexibank/iecor",
        "branch": "master",
        "forms_path": "cldf/forms.csv",
        "languages_path": "cldf/languages.csv",
        "language_id_field": "Language_ID",
        "form_field": "Form",
        "targets": {
            "sog": {"language_ids": ["271"], "source_tag": "iecor"},
        },
    },
}

USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets)"


def download_csv(repo: str, branch: str, path: str, local_path: Path) -> None:
    """Download a CSV file from GitHub raw."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists():
        logger.info("Cached: %s (%d bytes)", local_path.name, local_path.stat().st_size)
        return
    url = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
    logger.info("Downloading %s ...", url)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    with open(local_path, "wb") as f:
        f.write(data)
    logger.info("Downloaded %s (%d bytes)", local_path.name, len(data))


def load_existing_words(tsv_path: Path) -> set[str]:
    """Load existing Word column values."""
    existing = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Word\t"):
                    continue
                word = line.split("\t")[0]
                existing.add(word)
    return existing


def extract_forms(forms_csv: Path, language_ids: list[str],
                  lang_id_field: str, form_field: str) -> list[dict]:
    """Extract word forms for specific language IDs from a CLDF forms.csv."""
    entries = []
    lang_ids_lower = {lid.lower() for lid in language_ids}

    with open(forms_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = row.get(lang_id_field, "").lower()
            if lid not in lang_ids_lower:
                continue

            form = row.get(form_field, "").strip()
            if not form:
                continue

            # Clean form
            form = re.sub(r"^\*+", "", form)  # Remove reconstruction asterisk
            form = re.sub(r"\(.+?\)", "", form)  # Remove parenthetical
            form = unicodedata.normalize("NFC", form.strip())

            if not form or len(form) < 2 or len(form) > 50:
                continue

            entries.append({
                "word": form,
                "parameter_id": row.get("Parameter_ID", ""),
            })

    return entries


def ingest_language(iso: str, dataset_name: str, config: dict,
                    target: dict, dry_run: bool = False) -> dict:
    """Ingest a single language from a CLDF dataset."""
    # Download forms.csv
    cache_dir = RAW_DIR / f"cldf_{dataset_name}"
    forms_local = cache_dir / "forms.csv"
    download_csv(config["repo"], config["branch"],
                 config["forms_path"], forms_local)

    tsv_path = LEXICON_DIR / f"{iso}.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("%s: %d existing entries", iso, len(existing))

    # Extract forms
    entries = extract_forms(
        forms_local, target["language_ids"],
        config["language_id_field"], config["form_field"],
    )
    logger.info("%s: %d forms in CLDF", iso, len(entries))

    # Process
    new_entries = []
    audit_trail = []
    skipped = 0
    seen = set(existing)

    for entry in entries:
        word = entry["word"]
        if word in seen:
            skipped += 1
            continue

        try:
            ipa = transliterate(word, iso)
        except Exception:
            ipa = word

        if not ipa:
            ipa = word

        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""

        new_entries.append({
            "word": word,
            "ipa": ipa,
            "sca": sca,
        })
        seen.add(word)

        audit_trail.append({
            "word": word,
            "ipa": ipa,
            "concept": entry["parameter_id"],
            "source": target["source_tag"],
        })

    logger.info("%s: %d new, %d skipped", iso, len(new_entries), skipped)

    if dry_run:
        return {
            "iso": iso, "dataset": dataset_name,
            "cldf_forms": len(entries), "existing": len(existing),
            "new": len(new_entries), "total": len(seen),
        }

    # Write to TSV
    if new_entries:
        LEXICON_DIR.mkdir(parents=True, exist_ok=True)
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\t{target['source_tag']}\t-\t-\n")

    # Save audit trail
    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"cldf_{dataset_name}_ingest_{iso}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "iso": iso, "dataset": dataset_name,
        "cldf_forms": len(entries), "existing": len(existing),
        "new": len(new_entries), "total": len(seen),
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest from CLDF comparative datasets")
    parser.add_argument("--dataset", "-d", help="Specific dataset (default: all)")
    parser.add_argument("--language", "-l", help="Specific ISO code (default: all targets)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    results = []
    for ds_name, config in DATASETS.items():
        if args.dataset and args.dataset != ds_name:
            continue
        for iso, target in config["targets"].items():
            if args.language and args.language != iso:
                continue
            result = ingest_language(iso, ds_name, config, target, dry_run=args.dry_run)
            results.append(result)

    print(f"\n{'DRY RUN: ' if args.dry_run else ''}CLDF Comparative Ingestion:")
    print("=" * 60)
    for r in results:
        print(f"  {r['iso']:8s} ({r['dataset']:8s}) cldf={r['cldf_forms']:>5d}, "
              f"existing={r.get('existing', 0):>5d}, new={r['new']:>5d}, total={r['total']:>5d}")
    print("=" * 60)


if __name__ == "__main__":
    main()
