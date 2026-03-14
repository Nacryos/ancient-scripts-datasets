#!/usr/bin/env python3
"""Extract cognate pairs from the ACD (Austronesian Comparative Dictionary) CLDF dataset.

Source: https://github.com/lexibank/acd (CC BY 4.0)
Citation: Blust, Trussel & Smith (2023), DOI: 10.5281/zenodo.7737547
Data files: data/training/raw/acd_cldf/{forms,languages,cognatesets}.csv
  — Downloaded by scripts/ingest_acd.py via urllib from GitHub raw content

The ACD provides expert cognacy assignments for 146K+ Austronesian forms.
This script reads the Cognacy column from forms.csv and generates
cross-language pairwise cognate pairs within each cognacy group.

IPA handling:
  - ACD provides NO IPA/Segments data. Forms are in Blust notation
    (proto-forms) or orthographic form (modern languages).
  - Proto-forms: converted via Blust (2009) notation → IPA mapping.
  - Modern forms: lowercased orthography used as pseudo-IPA.
  - Score = -1 sentinel for ALL pairs (no reliable IPA for SCA computation).

Iron Rule: All data read from external CSV files. No hardcoded word lists.

Output: staging/cognate_pairs/acd_cognate_pairs.tsv (14-column schema)
"""

from __future__ import annotations

import csv
import io
import re
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
ACD_DIR = ROOT / "data" / "training" / "raw" / "acd_cldf"
STAGING_DIR = ROOT / "staging" / "cognate_pairs"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

HEADER = (
    "Lang_A\tWord_A\tIPA_A\tLang_B\tWord_B\tIPA_B\tConcept_ID\t"
    "Relationship\tScore\tSource\tRelation_Detail\tDonor_Language\t"
    "Confidence\tSource_Record_ID\n"
)

# Blust notation → IPA mapping
# Reference: Blust (2009) "The Austronesian Languages", Chapter 2
BLUST_TO_IPA = {
    "C": "ts", "N": "ŋ", "R": "ʀ", "S": "s", "Z": "z",
    "H": "h", "L": "ɬ", "T": "t", "D": "d",
    "ng": "ŋ", "ny": "ɲ", "nj": "ɲ",
    "q": "ʔ", "e": "ə",
    "₁": "", "₂": "", "₃": "", "₄": "", "₅": "",
    "₆": "", "₇": "", "₈": "", "₉": "", "₀": "",
}


def blust_to_ipa(form: str) -> str:
    """Convert Blust notation to approximate IPA.

    Reference: Blust (2009) The Austronesian Languages, Chapter 2.
    """
    form = form.lstrip("*")
    form = re.sub(r"\([^)]+\)", "", form)
    keys = sorted(BLUST_TO_IPA.keys(), key=len, reverse=True)
    result = []
    i = 0
    while i < len(form):
        matched = False
        for key in keys:
            if form[i:i + len(key)] == key:
                result.append(BLUST_TO_IPA[key])
                i += len(key)
                matched = True
                break
        if not matched:
            if form[i] not in "- ":
                result.append(form[i])
            i += 1
    return "".join(result)


def clean_form(value: str) -> str:
    """Clean a form value for use as Word column."""
    if not value:
        return "-"
    result = re.sub(r"\([^)]*\)", "", value)
    result = re.sub(r"\[[^\]]*\]", "", result)
    result = re.sub(r"^\*+", "", result)
    result = result.strip().strip("-").strip()
    return result if result else "-"


def form_to_ipa(value: str, is_proto: bool) -> str:
    """Convert form value to IPA representation.

    Proto-forms: Blust notation → IPA via cited mapping.
    Modern forms: lowercased cleaned form (pseudo-IPA).
    """
    cleaned = clean_form(value)
    if cleaned == "-":
        return "-"
    if is_proto:
        return blust_to_ipa(value)
    return cleaned.lower()


def main():
    print("=" * 60)
    print("ACD Cognate Extraction")
    print("=" * 60)
    print(f"  Source: {ACD_DIR}")

    # Step 1: Load languages (for ISO codes and proto status)
    lang_map: dict[str, dict] = {}
    lang_path = ACD_DIR / "languages.csv"
    with open(lang_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lid = row["ID"]
            iso = row.get("ISO639P3code", "").strip()
            name = row.get("Name", "")
            is_proto = row.get("Is_Proto", "false").lower() == "true"
            lang_map[lid] = {
                "iso": iso if iso else "-",
                "name": name,
                "is_proto": is_proto,
            }
    print(f"  Languages loaded: {len(lang_map)}")
    proto_count = sum(1 for v in lang_map.values() if v["is_proto"])
    modern_iso = sum(1 for v in lang_map.values()
                     if not v["is_proto"] and v["iso"] != "-")
    print(f"    Proto-languages: {proto_count}")
    print(f"    Modern with ISO: {modern_iso}")

    # Step 2: Load forms grouped by cognacy
    cognacy_groups: dict[str, list[dict]] = defaultdict(list)
    forms_path = ACD_DIR / "forms.csv"
    total_forms = 0
    skipped_no_cognacy = 0
    skipped_no_lang = 0
    loan_flagged_forms = 0

    with open(forms_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total_forms += 1
            cognacy = row.get("Cognacy", "").strip()
            if not cognacy:
                skipped_no_cognacy += 1
                continue

            lang_id = row.get("Language_ID", "").strip()
            if lang_id not in lang_map:
                skipped_no_lang += 1
                continue

            lang_info = lang_map[lang_id]
            iso = lang_info["iso"]
            is_proto = lang_info["is_proto"]

            # Skip proto-languages (no ISO code, not real attestations)
            if is_proto:
                continue

            # Skip languages without ISO codes (unidentifiable)
            if iso == "-":
                continue

            value = row.get("Value", "").strip()
            if not value:
                continue

            loan = row.get("Loan", "").strip()
            doubt = row.get("Doubt", "").strip()
            is_loan = loan.lower() == "true" if loan else False
            is_doubtful = doubt.lower() == "true" if doubt else False

            if is_loan:
                loan_flagged_forms += 1

            # Concept from Description or Parameter_ID
            description = row.get("Description", "").strip()
            param_id = row.get("Parameter_ID", "").strip()
            concept = description if description else param_id
            # Normalize concept: lowercase, replace spaces with underscore
            if concept:
                concept = concept.lower().replace(" ", "_")
            else:
                concept = "-"

            word = clean_form(value)
            ipa = form_to_ipa(value, is_proto=False)

            cognacy_groups[cognacy].append({
                "iso": iso,
                "word": word,
                "ipa": ipa,
                "concept": concept,
                "is_loan": is_loan,
                "is_doubtful": is_doubtful,
                "form_id": row.get("ID", ""),
            })

    print(f"\n  Total forms: {total_forms:,}")
    print(f"  Skipped (no cognacy): {skipped_no_cognacy:,}")
    print(f"  Skipped (unknown language): {skipped_no_lang:,}")
    print(f"  Loan-flagged forms: {loan_flagged_forms:,}")
    print(f"  Cognacy groups with modern forms: {len(cognacy_groups):,}")

    # Step 3: Deduplicate members within each cognacy group by (iso, word)
    dedup_removed = 0
    for cog, members in cognacy_groups.items():
        seen: set[str] = set()
        unique: list[dict] = []
        for m in members:
            key = f"{m['iso']}|{m['word']}"
            if key not in seen:
                seen.add(key)
                unique.append(m)
            else:
                dedup_removed += 1
        cognacy_groups[cog] = unique
    print(f"  Dedup removed: {dedup_removed:,}")

    # Step 4: Generate cross-language pairs within each cognacy group
    output_path = STAGING_DIR / "acd_cognate_pairs.tsv"
    total_pairs = 0
    loan_flagged_pairs = 0
    doubtful_pairs = 0

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(HEADER)

        for cognacy_id, members in sorted(cognacy_groups.items()):
            if len(members) < 2:
                continue

            for a, b in combinations(members, 2):
                # Skip same-language pairs
                if a["iso"] == b["iso"]:
                    continue

                # Determine relation detail and confidence
                if a["is_loan"] or b["is_loan"]:
                    relation_detail = "loan_flagged"
                    loan_flagged_pairs += 1
                else:
                    relation_detail = "inherited"

                if a["is_doubtful"] or b["is_doubtful"]:
                    confidence = "doubtful"
                    doubtful_pairs += 1
                else:
                    confidence = "certain"

                # Use the concept from form A (both should share concept
                # within cognacy group, but take A's)
                concept = a["concept"] if a["concept"] != "-" else b["concept"]

                # Score = -1: ACD has no IPA, all forms are pseudo-IPA
                score = -1

                # Source_Record_ID: cognacy group from ACD forms.csv
                source_record_id = f"acd_{cognacy_id}"

                row = (
                    f"{a['iso']}\t{a['word']}\t{a['ipa']}\t"
                    f"{b['iso']}\t{b['word']}\t{b['ipa']}\t"
                    f"{concept}\texpert_cognate\t{score}\tacd\t"
                    f"{relation_detail}\t-\t{confidence}\t{source_record_id}\n"
                )
                f.write(row)
                total_pairs += 1

                if total_pairs % 500000 == 0:
                    print(f"    ... {total_pairs:,} pairs written")

    print(f"\n  Total pairs: {total_pairs:,}")
    print(f"  Loan-flagged pairs: {loan_flagged_pairs:,}")
    print(f"  Doubtful pairs: {doubtful_pairs:,}")
    print(f"  Output: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
