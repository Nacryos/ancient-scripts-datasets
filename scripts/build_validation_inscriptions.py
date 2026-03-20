#!/usr/bin/env python3
"""Build unsegmented inscription validation dataset for Phonetic Prior algorithm.

Takes the segmented inscription TSVs and creates:
  1. Unsegmented IPA inscription files (spaces removed, randomly chunked)
  2. Known vocabulary files (from lexicons)
  3. Ground truth cognate pair files
  4. Validation config with expected orderings

These are designed to validate the Phonetic Prior algorithm's ability to:
  - Segment unsegmented text
  - Detect cognates from fragmented text
  - Discriminate between related and unrelated languages

Output format matches phonetic-prior-v2 and repro_decipher_phonetic_prior expectations.

Usage:
    python scripts/build_validation_inscriptions.py [--out-dir DIR]
"""

import argparse
import csv
import json
import random
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Syllable length targets matching Linear A inscription characteristics:
# Linear A: median=6, P75=11, P90=21 syllables per inscription
# We target 1-14 syllables with some up to ~20
MIN_SYLLABLES = 1
MAX_SYLLABLES = 14
LONG_TAIL_MAX = 20  # ~10% of inscriptions can be this long
MID_WORD_CUT_RATE = 0.15  # 15% of fragments cut mid-word

# Languages and their expected phylogenetic relationships
# Used for validation orderings (closer language should score higher)
LANGUAGE_FAMILIES = {
    "grc": {"family": "indo_european", "branch": "hellenic"},
    "lat": {"family": "indo_european", "branch": "italic"},
    "san": {"family": "indo_european", "branch": "indo_iranian"},
    "ang": {"family": "indo_european", "branch": "germanic"},
    "osc": {"family": "indo_european", "branch": "italic"},
    "xum": {"family": "indo_european", "branch": "italic"},
}

# Expected closeness orderings for validation
# Format: (anchor_as_lost, closer_known, farther_known, notes)
EXPECTED_ORDERINGS = [
    # Oscan should be closer to Latin/Umbrian (Italic) than to Sanskrit/Greek
    ("osc", "lat", "san", "Italic > Indo-Iranian"),
    ("osc", "lat", "grc", "Italic > Hellenic"),
    ("osc", "xum", "san", "Sabellic > Indo-Iranian"),
    ("osc", "xum", "ang", "Sabellic > Germanic"),
    # Umbrian should be closer to Oscan/Latin (Italic) than to others
    ("xum", "osc", "san", "Sabellic > Indo-Iranian"),
    ("xum", "lat", "ang", "Italic > Germanic"),
    ("xum", "lat", "grc", "Italic > Hellenic"),
    # Latin should be closer to Oscan/Umbrian (Italic) than to Sanskrit
    ("lat", "osc", "san", "Italic > Indo-Iranian"),
    ("lat", "osc", "ang", "Italic > Germanic"),
    # Greek should be closer to Latin (both IE, centum) than to Sanskrit (satem)
    ("grc", "lat", "ang", "Hellenic > Germanic (debatable)"),
    # Sanskrit should be closer to Greek/Latin (IE) than unrelated
    ("san", "grc", "ang", "Hellenic > Germanic for Indo-Iranian"),
]

SEED = 42


# ---------------------------------------------------------------------------
# Syllable counting (approximate: count vowel nuclei in IPA)
# ---------------------------------------------------------------------------

_VOWEL_RE = re.compile(r'[aeiouɑæɐɛəɘɪɨɔɵʊʉɯøœyãẽĩõũ]', re.IGNORECASE)


def count_syllables(ipa: str) -> int:
    """Estimate syllable count from IPA string by counting vowel nuclei."""
    return max(1, len(_VOWEL_RE.findall(ipa)))


# ---------------------------------------------------------------------------
# Fragment generator
# ---------------------------------------------------------------------------

def fragment_inscriptions(
    ipa_texts: List[str],
    rng: random.Random,
    min_syl: int = MIN_SYLLABLES,
    max_syl: int = MAX_SYLLABLES,
    long_max: int = LONG_TAIL_MAX,
    mid_word_rate: float = MID_WORD_CUT_RATE,
) -> List[str]:
    """Convert segmented IPA texts into unsegmented fragments.

    1. Concatenate all IPA text into a continuous stream (per inscription)
    2. Remove word boundaries (spaces)
    3. Randomly chunk into fragments of target syllable length
    4. Some fragments cut mid-word (simulates real inscription breaks)

    Returns list of unsegmented IPA strings.
    """
    fragments: List[str] = []

    for text in ipa_texts:
        words = text.split()
        if not words:
            continue

        # Build a list of (word_ipa, syllable_count) for chunking
        word_syls = [(w, count_syllables(w)) for w in words]

        # Generate fragments from this inscription
        i = 0
        while i < len(word_syls):
            # Pick target syllable count for this fragment
            if rng.random() < 0.1:  # 10% long tail
                target_syl = rng.randint(max_syl + 1, long_max)
            else:
                target_syl = rng.randint(min_syl, max_syl)

            # Accumulate words until we hit target syllables
            chunk_words: List[str] = []
            chunk_syl = 0
            while i < len(word_syls) and chunk_syl < target_syl:
                w, s = word_syls[i]
                chunk_words.append(w)
                chunk_syl += s
                i += 1

            if not chunk_words:
                continue

            # Join without spaces (unsegmented)
            unseg = "".join(chunk_words)

            # Optionally cut mid-word for a minority of fragments
            if rng.random() < mid_word_rate and len(unseg) > 4:
                cut_point = rng.randint(2, len(unseg) - 2)
                # Keep the first part as one fragment
                fragments.append(unseg[:cut_point])
                # The remainder becomes part of the next fragment
                # (we just discard it — simulates inscription damage)
            else:
                fragments.append(unseg)

    return fragments


# ---------------------------------------------------------------------------
# Load inscription TSV
# ---------------------------------------------------------------------------

def load_inscription_tsv(path: Path) -> List[Dict]:
    """Load inscription TSV, return list of dicts with IPA field."""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row.get("IPA", "").strip():
                records.append(row)
    return records


# ---------------------------------------------------------------------------
# Load cognate pairs
# ---------------------------------------------------------------------------

def load_cognate_pairs_for_language(
    cognate_file: Path,
    target_iso: str,
    max_pairs: int = 5000,
) -> List[Dict]:
    """Load cognate pairs involving target language from inherited pairs TSV.

    Returns list of dicts with: lang_a, ipa_a, lang_b, ipa_b, concept.
    """
    pairs = []
    with open(cognate_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            la = row.get("Lang_A", "")
            lb = row.get("Lang_B", "")
            if la == target_iso or lb == target_iso:
                pairs.append({
                    "lang_a": la,
                    "ipa_a": row.get("IPA_A", ""),
                    "lang_b": lb,
                    "ipa_b": row.get("IPA_B", ""),
                    "concept": row.get("Concept_ID", ""),
                })
                if len(pairs) >= max_pairs:
                    break
    return pairs


# ---------------------------------------------------------------------------
# Load known vocabulary
# ---------------------------------------------------------------------------

def load_known_vocab(lexicon_path: Path, max_items: int = 2000) -> List[str]:
    """Load known vocabulary (IPA words) from a lexicon TSV."""
    vocab = set()
    with open(lexicon_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ipa = row.get("IPA", "").strip()
            if ipa and len(ipa) >= 2:
                vocab.add(ipa)
    items = sorted(vocab)
    if len(items) > max_items:
        rng = random.Random(SEED)
        items = rng.sample(items, max_items)
        items.sort()
    return items


# ---------------------------------------------------------------------------
# Write output files
# ---------------------------------------------------------------------------

def write_lost_text(path: Path, fragments: List[str]) -> None:
    """Write unsegmented inscription fragments (one per line)."""
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for frag in fragments:
            f.write(frag + "\n")


def write_known_vocab(path: Path, vocab: List[str]) -> None:
    """Write known vocabulary (one IPA word per line)."""
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for word in vocab:
            f.write(word + "\n")


def write_ground_truth(path: Path, pairs: List[Dict]) -> None:
    """Write ground truth cognate pairs TSV."""
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(
            f, fieldnames=["lost", "known", "concept_id"],
            delimiter="\t", lineterminator="\n",
        )
        writer.writeheader()
        for p in pairs:
            writer.writerow(p)


def write_validation_config(path: Path, orderings: List[Tuple]) -> None:
    """Write validation config YAML with expected orderings."""
    lines = [
        "# Validation orderings for Phonetic Prior algorithm",
        "# Each ordering asserts: anchor is closer to 'closer' than 'farther'",
        "",
        "orderings:",
    ]
    for anchor, closer, farther, notes in orderings:
        lines.append(f"  - anchor: {anchor}")
        lines.append(f"    closer: {closer}")
        lines.append(f"    farther: {farther}")
        lines.append(f"    notes: \"{notes}\"")
    lines.append("")

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))


def write_metadata(path: Path, stats: Dict) -> None:
    """Write metadata JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build unsegmented inscription validation set for Phonetic Prior"
    )
    parser.add_argument(
        "--inscription-dir",
        type=Path,
        default=Path("data/inscriptions"),
        help="Directory containing segmented inscription TSVs",
    )
    parser.add_argument(
        "--lexicon-dir",
        type=Path,
        default=Path("data/training/lexicons"),
        help="Directory containing language lexicon TSVs",
    )
    parser.add_argument(
        "--cognate-file",
        type=Path,
        default=Path("data/training/cognate_pairs/cognate_pairs_inherited.tsv"),
        help="Inherited cognate pairs TSV",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/validation_phonetic_prior"),
        help="Output directory",
    )
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    target_langs = ["grc", "lat", "san", "ang", "osc", "xum"]
    stats = {"languages": {}, "seed": args.seed}

    for iso in target_langs:
        print(f"\n=== {iso} ===")
        lang_dir = out_dir / iso
        lang_dir.mkdir(exist_ok=True)

        # 1. Load segmented inscriptions
        insc_path = args.inscription_dir / f"{iso}_inscriptions.tsv"
        if not insc_path.exists():
            print(f"  [skip] No inscription file: {insc_path}")
            continue

        records = load_inscription_tsv(insc_path)
        ipa_texts = [r["IPA"] for r in records]
        print(f"  Loaded {len(records)} inscriptions")

        # 2. Generate unsegmented fragments
        fragments = fragment_inscriptions(ipa_texts, rng)
        write_lost_text(lang_dir / "lost.txt", fragments)
        print(f"  Generated {len(fragments)} unsegmented fragments")

        # Syllable distribution of fragments
        syl_counts = [count_syllables(f) for f in fragments]
        avg_syl = sum(syl_counts) / len(syl_counts) if syl_counts else 0
        print(f"  Avg syllables per fragment: {avg_syl:.1f}")

        # 3. Load known vocabulary for ALL other languages
        for other_iso in target_langs:
            if other_iso == iso:
                continue
            lex_path = args.lexicon_dir / f"{other_iso}.tsv"
            if not lex_path.exists():
                continue
            vocab = load_known_vocab(lex_path)
            write_known_vocab(lang_dir / f"known_{other_iso}.txt", vocab)

        # 4. Load cognate pairs (this takes a while for the big file)
        print(f"  Loading cognate pairs (this may take a moment)...")
        pairs_raw = load_cognate_pairs_for_language(
            args.cognate_file, iso, max_pairs=5000
        )

        # Group by partner language
        pairs_by_lang: Dict[str, List[Dict]] = {}
        for p in pairs_raw:
            partner = p["lang_b"] if p["lang_a"] == iso else p["lang_a"]
            if partner not in target_langs:
                continue
            lost_ipa = p["ipa_a"] if p["lang_a"] == iso else p["ipa_b"]
            known_ipa = p["ipa_b"] if p["lang_a"] == iso else p["ipa_a"]
            if partner not in pairs_by_lang:
                pairs_by_lang[partner] = []
            pairs_by_lang[partner].append({
                "lost": lost_ipa,
                "known": known_ipa,
                "concept_id": p["concept"],
            })

        for partner, plist in pairs_by_lang.items():
            gt_path = lang_dir / f"ground_truth_{partner}.tsv"
            write_ground_truth(gt_path, plist)
            print(f"  Ground truth {iso}->{partner}: {len(plist)} pairs")

        # Stats
        stats["languages"][iso] = {
            "inscriptions": len(records),
            "fragments": len(fragments),
            "avg_syllables": round(avg_syl, 1),
            "cognate_pairs": {k: len(v) for k, v in pairs_by_lang.items()},
        }

    # 5. Write validation orderings
    write_validation_config(out_dir / "validation_orderings.yaml", EXPECTED_ORDERINGS)
    print(f"\nWrote validation orderings: {len(EXPECTED_ORDERINGS)} assertions")

    # 6. Write metadata
    write_metadata(out_dir / "metadata.json", stats)

    # 7. Summary
    print("\n" + "=" * 60)
    print("SUMMARY — Phonetic Prior Validation Dataset")
    print("=" * 60)
    total_frags = 0
    for iso in target_langs:
        s = stats["languages"].get(iso, {})
        n = s.get("fragments", 0)
        total_frags += n
        pairs_str = ", ".join(f"{k}:{v}" for k, v in s.get("cognate_pairs", {}).items())
        print(f"  {iso}: {n:>6} fragments, pairs: {pairs_str}")
    print(f"  TOTAL: {total_frags:>6} fragments")
    print(f"\nOutput: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
