#!/usr/bin/env python3
"""Post-process Linear A cognate lists for human readability.

Takes raw cognate_list.tsv files and produces cleaned versions:
- Filters out short noise spans (< 5 IPA chars)
- Filters out spans where top-10 scores are all identical (undifferentiated)
- Reverse-maps IPA spans back to Linear A sign sequences
- Produces a cleaner TSV per language pair
"""

import csv
import json
import os
import shutil
import sys
from pathlib import Path


def load_ipa_to_sign_map(sign_map_path: Path) -> dict:
    """Build reverse map: IPA string -> Linear A sign."""
    with open(sign_map_path, "r", encoding="utf-8") as f:
        sign_to_ipa = json.load(f)
    # Reverse: IPA -> sign. Prefer longest IPA keys first for greedy matching.
    ipa_to_sign = {}
    for sign, ipa in sign_to_ipa.items():
        ipa_to_sign[ipa] = sign
    return ipa_to_sign


def ipa_to_signs(ipa_span: str, ipa_to_sign: dict) -> str:
    """Greedily convert an IPA string back to Linear A sign sequence.

    Tries longest IPA keys first at each position.
    Returns sign sequence like 'a-da-ki-te' with hyphens.
    """
    # Sort IPA keys by length descending for greedy matching
    keys_by_len = sorted(ipa_to_sign.keys(), key=len, reverse=True)

    signs = []
    pos = 0
    while pos < len(ipa_span):
        matched = False
        for key in keys_by_len:
            if ipa_span[pos:pos+len(key)] == key and len(key) > 0:
                signs.append(ipa_to_sign[key])
                pos += len(key)
                matched = True
                break
        if not matched:
            # Unknown character — keep as-is
            signs.append(ipa_span[pos])
            pos += 1

    return "-".join(signs)


def score_is_differentiated(top10_json: str, threshold: float = 0.01) -> bool:
    """Check if the top-10 scores show real differentiation (not all identical)."""
    try:
        top10 = json.loads(top10_json)
        if len(top10) < 2:
            return True
        scores = [s for _, s in top10]
        spread = max(scores) - min(scores)
        return spread > threshold
    except (json.JSONDecodeError, TypeError, ValueError):
        return False


def process_cognate_file(input_path: Path, output_path: Path, ipa_to_sign: dict,
                         min_ipa_len: int = 5, require_differentiated: bool = True):
    """Process one cognate_list.tsv into a cleaned version."""
    rows_in = 0
    rows_out = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8", newline="\n") as fout:

        reader = csv.DictReader(fin, delimiter="\t")
        writer = csv.writer(fout, delimiter="\t", lineterminator="\n")
        writer.writerow([
            "linear_a_signs", "linear_a_ipa", "matched_word", "score",
            "top3_matches", "ipa_length"
        ])

        for row in reader:
            rows_in += 1
            lost_ipa = row.get("lost", "")
            known = row.get("top1_known", "")
            score = row.get("score", "")
            top10_raw = row.get("top10", "[]")

            # Filter: minimum IPA length
            if len(lost_ipa) < min_ipa_len:
                continue

            # Filter: require differentiated top-10 scores
            if require_differentiated and not score_is_differentiated(top10_raw):
                continue

            # Reverse-map IPA to Linear A signs
            la_signs = ipa_to_signs(lost_ipa, ipa_to_sign)

            # Format top 3 for readability
            try:
                top10 = json.loads(top10_raw)
                top3_str = "; ".join(f"{w} ({s:.2f})" for w, s in top10[:3])
            except (json.JSONDecodeError, TypeError):
                top3_str = known

            writer.writerow([
                la_signs, lost_ipa, known, score, top3_str, len(lost_ipa)
            ])
            rows_out += 1

    return rows_in, rows_out


def main():
    data_dir = Path(__file__).parent.parent / "data"
    fleet_dir = data_dir / "fleet_results_v2"
    output_dir = data_dir / "linear_a_cognates_clean"
    sign_map_path = data_dir / "linear_a" / "sign_to_ipa.json"

    if not sign_map_path.exists():
        print(f"ERROR: sign map not found at {sign_map_path}")
        sys.exit(1)

    ipa_to_sign = load_ipa_to_sign_map(sign_map_path)
    print(f"Loaded {len(ipa_to_sign)} IPA-to-sign mappings")

    # Find all Linear A cognate lists
    cognate_files = sorted(fleet_dir.glob("**/linear_a/linear_a_vs_*/cognate_list.tsv"))
    if not cognate_files:
        print("ERROR: No Linear A cognate files found")
        sys.exit(1)

    print(f"Found {len(cognate_files)} cognate list files")
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = []
    for path in cognate_files:
        lang = path.parent.name.replace("linear_a_vs_", "")
        out_path = output_dir / f"linear_a_vs_{lang}_clean.tsv"

        rows_in, rows_out = process_cognate_file(path, out_path, ipa_to_sign)
        pct = (rows_out / rows_in * 100) if rows_in > 0 else 0
        summary.append((lang, rows_in, rows_out, pct))
        print(f"  {lang:12s}: {rows_in:5d} -> {rows_out:5d} rows ({pct:.0f}% kept) -> {out_path.name}")

    # Also create a combined file with all languages side by side
    combined_path = output_dir / "all_languages_combined.tsv"
    with open(combined_path, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow([
            "candidate_language", "linear_a_signs", "linear_a_ipa",
            "matched_word", "score", "top3_matches", "ipa_length"
        ])
        for path in cognate_files:
            lang = path.parent.name.replace("linear_a_vs_", "")
            clean_path = output_dir / f"linear_a_vs_{lang}_clean.tsv"
            with open(clean_path, "r", encoding="utf-8") as fin:
                reader = csv.DictReader(fin, delimiter="\t")
                for row in reader:
                    writer.writerow([
                        lang, row["linear_a_signs"], row["linear_a_ipa"],
                        row["matched_word"], row["score"],
                        row["top3_matches"], row["ipa_length"]
                    ])

    print(f"\nCombined file: {combined_path}")
    print(f"\nSummary:")
    print(f"{'Language':12s} {'Raw':>6s} {'Clean':>6s} {'Kept':>5s}")
    print("-" * 35)
    for lang, ri, ro, pct in summary:
        print(f"{lang:12s} {ri:6d} {ro:6d} {pct:4.0f}%")


if __name__ == "__main__":
    main()
