#!/usr/bin/env python3
"""Extract ABVD cognate pairs from the authoritative CognateTable.

Reads sources/abvd/cldf/cognates.csv (291K expert entries) instead of the
forms.csv Cognacy column. Fixes: doubt flag leakage, multi-set truncation.

Output: staging/cognate_pairs/abvd_cognate_pairs.tsv (14-column schema)
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

SOURCES_DIR = ROOT / "sources" / "abvd" / "cldf"
STAGING_DIR = ROOT / "staging" / "cognate_pairs"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

HEADER = (
    "Lang_A\tWord_A\tIPA_A\tLang_B\tWord_B\tIPA_B\tConcept_ID\t"
    "Relationship\tScore\tSource\tRelation_Detail\tDonor_Language\t"
    "Confidence\tSource_Record_ID\n"
)


def sca_similarity(ipa_a: str, ipa_b: str) -> float:
    """Compute normalised Levenshtein similarity on SCA strings."""
    try:
        sca_a = ipa_to_sound_class(ipa_a)
        sca_b = ipa_to_sound_class(ipa_b)
    except Exception:
        return 0.0
    if not sca_a or not sca_b:
        return 0.0
    # Levenshtein distance
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


def form_to_pseudo_ipa(form: str) -> str:
    """Convert ABVD orthographic form to pseudo-IPA (lowercase, strip parens)."""
    # ABVD forms are orthographic — no true IPA. Basic normalisation only.
    result = form.lower().strip()
    result = result.replace("(", "").replace(")", "")
    return result


def main():
    print("=" * 60)
    print("ABVD Cognate Extraction v2")
    print("=" * 60)

    # Step 1: Read languages.csv → Language_ID → ISO code
    lang_path = SOURCES_DIR / "languages.csv"
    lang_iso = {}
    with open(lang_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = row["ID"]
            iso = row.get("ISO639P3code", "").strip()
            if iso:
                lang_iso[lid] = iso
    print(f"  Languages with ISO codes: {len(lang_iso)}")

    # Step 2: Read forms.csv → Form_ID → {language, word, ipa, concept, loan}
    forms_path = SOURCES_DIR / "forms.csv"
    forms = {}
    with open(forms_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fid = row["ID"]
            lid = str(row["Language_ID"])
            iso = lang_iso.get(lid, "")
            if not iso:
                continue
            form = row.get("Form", row.get("Value", "")).strip()
            if not form:
                continue
            param_id = row.get("Parameter_ID", "").strip()
            # Extract concept from Parameter_ID (e.g., "1_hand" → "hand")
            concept = param_id.split("_", 1)[1] if "_" in param_id else param_id
            ipa = form_to_pseudo_ipa(form)
            loan = row.get("Loan", "").strip()
            forms[fid] = {
                "iso": iso,
                "word": form,
                "ipa": ipa,
                "concept": concept,
                "loan": loan,
            }
    print(f"  Forms loaded: {len(forms)}")

    # Step 3: Read cognates.csv → group by Cognateset_ID
    cognates_path = SOURCES_DIR / "cognates.csv"
    cogsets: dict[str, list[dict]] = defaultdict(list)
    doubt_count = 0
    total_cognate_rows = 0
    with open(cognates_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_cognate_rows += 1
            form_id = row["Form_ID"]
            cogset_id = row["Cognateset_ID"]
            doubt = row.get("Doubt", "false").strip().lower() == "true"
            if doubt:
                doubt_count += 1
            form_data = forms.get(form_id)
            if form_data is None:
                continue
            cogsets[cogset_id].append({
                **form_data,
                "doubt": doubt,
                "cogset_id": cogset_id,
                "form_id": form_id,
            })
    print(f"  Cognate rows read: {total_cognate_rows}")
    print(f"  Doubtful entries: {doubt_count}")
    print(f"  Cognate sets: {len(cogsets)}")

    # Step 4: Generate cross-language pairs within each cognate set
    output_path = STAGING_DIR / "abvd_cognate_pairs.tsv"
    pair_count = 0
    loan_flagged_count = 0
    with open(output_path, "w", encoding="utf-8") as out:
        out.write(HEADER)
        for cogset_id, members in cogsets.items():
            # Deduplicate members by (iso, word) — ABVD maps multiple
            # Language_IDs to the same ISO code with identical forms
            seen_members: set[tuple[str, str]] = set()
            deduped: list[dict] = []
            for m in members:
                key = (m["iso"], m["word"])
                if key not in seen_members:
                    seen_members.add(key)
                    deduped.append(m)
            members = deduped
            # Filter to cross-language pairs only
            for a, b in combinations(members, 2):
                if a["iso"] == b["iso"]:
                    continue
                score = sca_similarity(a["ipa"], b["ipa"])
                confidence = "doubtful" if (a["doubt"] or b["doubt"]) else "certain"
                # Check if either form is flagged as a loan
                a_loan = a.get("loan", "")
                b_loan = b.get("loan", "")
                if (a_loan and a_loan.lower() != "false") or (b_loan and b_loan.lower() != "false"):
                    relation_detail = "loan_flagged"
                    loan_flagged_count += 1
                else:
                    relation_detail = "inherited"
                out.write(
                    f"{a['iso']}\t{a['word']}\t{a['ipa']}\t"
                    f"{b['iso']}\t{b['word']}\t{b['ipa']}\t"
                    f"{a['concept']}\texpert_cognate\t{score}\tabvd\t"
                    f"{relation_detail}\t-\t{confidence}\t{cogset_id}\n"
                )
                pair_count += 1
                if pair_count % 500000 == 0:
                    print(f"    ... {pair_count:,} pairs written")

    print(f"\n  Total pairs: {pair_count:,}")
    print(f"  Loan-flagged pairs: {loan_flagged_count:,}")
    print(f"  Output: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
