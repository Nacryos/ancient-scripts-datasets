#!/usr/bin/env python3
"""Extract WOLD borrowing pairs from the authoritative BorrowingTable.

Reads sources/wold/cldf/borrowings.csv (21K explicit donor-recipient events)
instead of fabricating pairs from forms.csv Borrowed column.

Output: staging/cognate_pairs/wold_borrowing_pairs.tsv (14-column schema)
"""

from __future__ import annotations

import csv
import io
import re
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

SOURCES_DIR = ROOT / "sources" / "wold" / "cldf"
STAGING_DIR = ROOT / "staging" / "cognate_pairs"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

HEADER = (
    "Lang_A\tWord_A\tIPA_A\tLang_B\tWord_B\tIPA_B\tConcept_ID\t"
    "Relationship\tScore\tSource\tRelation_Detail\tDonor_Language\t"
    "Confidence\tSource_Record_ID\n"
)


def segments_to_ipa(segments: str) -> str:
    """Convert CLDF Segments column to IPA string."""
    if not segments:
        return ""
    # Strip boundary markers
    tokens = segments.replace("^", "").replace("$", "").replace("+", " ").replace("#", " ").replace("_", "")
    # Join phoneme tokens
    return re.sub(r"\s+", "", tokens).strip()


def sca_similarity(ipa_a: str, ipa_b: str) -> float:
    """Compute normalised Levenshtein similarity on SCA strings."""
    try:
        sca_a = ipa_to_sound_class(ipa_a)
        sca_b = ipa_to_sound_class(ipa_b)
    except Exception:
        return 0.0
    if not sca_a or not sca_b:
        return 0.0
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


def main():
    print("=" * 60)
    print("WOLD Borrowing Extraction v2")
    print("=" * 60)

    # Step 1: Read languages.csv → Language name → ISO code
    lang_path = SOURCES_DIR / "languages.csv"
    lang_iso = {}
    lang_name_to_iso = {}
    with open(lang_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = row["ID"]
            iso = row.get("ISO639P3code", "").strip()
            name = row.get("Name", "").strip()
            if iso:
                lang_iso[lid] = iso
                lang_name_to_iso[name] = iso
    print(f"  Languages with ISO codes: {len(lang_iso)}")

    # Step 2: Read parameters.csv → Parameter_ID → concept gloss
    params_path = SOURCES_DIR / "parameters.csv"
    param_concept = {}
    if params_path.exists():
        with open(params_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = row["ID"]
                concept = row.get("Concepticon_Gloss", row.get("Name", pid)).strip()
                param_concept[pid] = concept

    # Step 3: Read forms.csv → Form_ID → {language, word, ipa, concept}
    forms_path = SOURCES_DIR / "forms.csv"
    forms = {}
    with open(forms_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fid = row["ID"]
            lid = row["Language_ID"]
            iso = lang_iso.get(lid, "")
            if not iso:
                continue
            form = row.get("Form", row.get("Value", "")).strip()
            segments = row.get("Segments", "").strip()
            ipa = segments_to_ipa(segments) if segments else form.lower()
            param_id = row.get("Parameter_ID", "").strip()
            concept = param_concept.get(param_id, param_id)
            forms[fid] = {
                "iso": iso,
                "word": form,
                "ipa": ipa,
                "concept": concept,
            }
    print(f"  Forms loaded: {len(forms)}")

    # Step 4: Read borrowings.csv → generate pairs
    borrowings_path = SOURCES_DIR / "borrowings.csv"
    output_path = STAGING_DIR / "wold_borrowing_pairs.tsv"
    pair_count = 0
    skipped_no_target = 0
    skipped_no_source = 0

    with open(output_path, "w", encoding="utf-8") as out:
        out.write(HEADER)
        with open(borrowings_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                borrowing_id = row["ID"]
                target_fid = row.get("Target_Form_ID", "").strip()
                source_fid = row.get("Source_Form_ID", "").strip()
                source_word = row.get("Source_word", "").strip()
                source_lang = row.get("Source_languoid", "").strip()
                source_certain = row.get("Source_certain", "").strip()
                source_relation = row.get("Source_relation", "").strip()

                # Target form is required
                target = forms.get(target_fid)
                if target is None:
                    skipped_no_target += 1
                    continue

                # Source can come from Source_Form_ID or Source_word
                if source_fid and source_fid in forms:
                    source = forms[source_fid]
                    source_iso = source["iso"]
                    source_word_str = source["word"]
                    source_ipa = source["ipa"]
                elif source_word:
                    # Source form not in database — use Source_word + Source_languoid
                    source_iso = lang_name_to_iso.get(source_lang, "-")
                    source_word_str = source_word
                    source_ipa = source_word.lower()  # best-effort pseudo-IPA
                else:
                    skipped_no_source += 1
                    continue

                # Donor language
                donor_lang = source_iso if source_iso != "-" else source_lang

                # Confidence
                confidence = "certain" if source_certain == "yes" else (
                    "uncertain" if source_certain == "no" else source_certain if source_certain else "-"
                )

                # Score
                score = sca_similarity(target["ipa"], source_ipa)

                # Filter self-loans (same language borrowing from itself)
                if target["iso"] == source_iso:
                    continue

                # Lang_A = target (borrower), Lang_B = source (donor)
                out.write(
                    f"{target['iso']}\t{target['word']}\t{target['ipa']}\t"
                    f"{source_iso}\t{source_word_str}\t{source_ipa}\t"
                    f"{target['concept']}\tborrowing\t{score}\twold\t"
                    f"borrowed\t{donor_lang}\t{confidence}\twold_{borrowing_id}\n"
                )
                pair_count += 1

    print(f"\n  Total borrowing pairs: {pair_count:,}")
    print(f"  Skipped (no target form): {skipped_no_target}")
    print(f"  Skipped (no source info): {skipped_no_source}")
    print(f"  Output: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
