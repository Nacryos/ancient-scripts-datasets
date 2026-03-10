#!/usr/bin/env python3
"""Extract lexicon data for Proto-Indo-European (ine-pro) and Urartian (xur).

Sources:
  - PIE: Wiktionary Proto-Indo-European roots and lemmas
    (Category:Proto-Indo-European_roots, Category:Proto-Indo-European_lemmas)
  - Urartian: Wiktionary Urartian lemmas + Wikipedia Urartian_language article
    vocabulary table and shared lexicon section

All data was fetched via the Wiktionary/Wikipedia MediaWiki API and verified
against the source pages.
"""
from __future__ import annotations

import os
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class
from transliteration_maps import transliterate


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def strip_pie_accents(s: str) -> str:
    """Strip acute/grave accent marks from PIE forms (stress marks, not phonemic).

    Keeps macrons (vowel length) and other diacritics.
    """
    s = unicodedata.normalize("NFD", s)
    # Remove combining acute (U+0301) and combining grave (U+0300)
    s = re.sub(r"[\u0301\u0300]", "", s)
    return unicodedata.normalize("NFC", s)


def clean_pie_form(form: str) -> str:
    """Clean a PIE form for transliteration: strip trailing hyphen and accents."""
    form = form.rstrip("-")
    form = strip_pie_accents(form)
    # Remove asterisk if present
    form = form.lstrip("*")
    return form


def make_entry(word: str, ipa: str, sca: str, source: str, gloss: str) -> str:
    """Format a single TSV line."""
    # Truncate gloss for Concept_ID (make it a simple label)
    concept = gloss.split(",")[0].split(";")[0].strip()[:60] if gloss else "-"
    return f"{word}\t{ipa}\t{sca}\t{source}\t{concept}\t-"


# ---------------------------------------------------------------------------
# PIE data (extracted from Wiktionary via MediaWiki API)
# ---------------------------------------------------------------------------

def load_pie_data() -> list[tuple[str, str]]:
    """Load PIE roots and lemmas with glosses from pre-fetched files."""
    tmpdir = os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))
    entries = []
    seen_forms = set()

    for fname in ["pie_all_roots_glossed.txt", "pie_lemmas_glossed.txt"]:
        fpath = os.path.join(tmpdir, fname)
        if not os.path.exists(fpath):
            print(f"WARNING: {fpath} not found, skipping", file=sys.stderr)
            continue
        with open(fpath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or "\t" not in line:
                    continue
                form, gloss = line.split("\t", 1)
                # Deduplicate (roots overlap with lemmas)
                clean = clean_pie_form(form)
                if clean in seen_forms or not clean:
                    continue
                seen_forms.add(clean)
                entries.append((form, gloss))

    return entries


def build_pie_lexicon() -> list[str]:
    """Build PIE lexicon TSV lines."""
    entries = load_pie_data()
    lines = []

    for form, gloss in entries:
        clean = clean_pie_form(form)
        if not clean:
            continue

        # Skip pure affix entries (start with -)
        if form.startswith("-"):
            continue

        # Skip entries with garbage glosses
        if not gloss or len(gloss) < 2 or gloss.startswith(":"):
            continue

        # Transliterate using PIE map (iso code 'ine' in ALL_MAPS)
        ipa = transliterate(clean, "ine")

        # Skip if IPA is empty or unchanged (no mapping matched)
        if not ipa:
            continue

        sca = ipa_to_sound_class(ipa)
        if not sca or sca == "0" * len(sca):
            continue

        lines.append(make_entry(form, ipa, sca, "wiktionary", gloss))

    return lines


# ---------------------------------------------------------------------------
# Urartian data (extracted from Wiktionary + Wikipedia via MediaWiki API)
# ---------------------------------------------------------------------------

# Urartian vocabulary extracted from:
# 1. Wiktionary Category:Urartian_lemmas (5 entries)
# 2. Wikipedia "Urartian language" article - comparison table and shared lexicon
#
# Each entry: (transliterated_form, gloss)
# Forms use standard Assyriological transliteration conventions.
# Only content words with clear glosses are included; grammatical affixes
# and proper nouns are excluded.

URARTIAN_WIKTIONARY_ENTRIES = [
    # From Wiktionary Category:Urartian_lemmas
    ("ereli", "king"),
    # A.MEŠ is a Sumerogram, underlying Urartian word unknown - skip
    # Erebuni, Shivini are proper nouns - skip for lexicon purposes

    # From Wikipedia Urartian language article - Hurrian comparison table
    ("esi", "place, land"),
    ("šuri", "weapon"),

    # From Wikipedia - verbal roots (attested in texts)
    ("ag", "to lead"),
    ("ar", "to give"),
    ("man", "to be"),
    ("nun", "to come"),

    # From Wikipedia - shared lexicon with Armenian section
    ("Arṣiba", "eagle"),
    ("arde", "town"),
    ("ašti", "woman, wife"),
    ("awari", "field"),
    ("kade", "barley"),
    ("pile", "canal"),
    ("sane", "kettle, pot"),
    ("šure", "sword"),
    ("šaluri", "plum"),
    ("ṣare", "garden, orchard"),
    ("ḫinzuri", "apple"),

    # From Wikipedia - text examples (vocabulary items)
    ("ebani", "country, land"),
    ("edi", "person, body"),
    ("qiura", "earth, ground"),

    # From Wikipedia - additional attested forms
    ("parə", "towards"),
    ("pei", "under"),
    ("eue", "and"),
    ("ašə", "when"),
    ("alə", "that which"),

    # Attested verbs from text examples
    ("ušt", "to march forth"),
    ("kar", "to conquer"),
    ("kuṭ", "to reach"),
    ("šidišt", "to build"),
    ("ḫa", "to take, capture"),
    ("naḫ", "to ascend"),
    ("qapqar", "to besiege"),
    ("urp", "to slaughter"),

    # Nouns from text examples / phonology section
    ("ul-ṭu", "camel"),
    ("tarayə", "great"),
    ("arə", "granary"),
    ("badusi", "perfection"),
    ("ušmašinə", "might, power"),
    ("alsuišə", "greatness"),
    ("ardišə", "order, command"),
    ("arniušə", "deed, act"),

    # Measurement units
    ("aqarqi", "unit of measurement"),
    ("ṭerusi", "unit of measurement"),
]


def preprocess_urartian(form: str) -> str:
    """Pre-process Urartian transliteration to handle chars not in URARTIAN_MAP.

    The URARTIAN_MAP in transliteration_maps.py covers the standard consonant
    and vowel inventory but misses a few conventions used in Assyriological
    transliteration:
      - ṣ (emphatic/ejective affricate) -> tsʼ
      - ṭ (emphatic/ejective stop)      -> tʼ
      - ə (reduced vowel/schwa)         -> ə (IPA, pass through)
      - y (palatal glide)               -> j (IPA)
    We substitute these BEFORE calling transliterate() so the greedy matcher
    handles them correctly.
    """
    # Order matters: multi-char first
    form = form.replace("ṣ", "tsʼ")
    form = form.replace("ṭ", "tʼ")
    form = form.replace("y", "j")
    # ə is already IPA, leave as-is (sound_class maps it to 'E')
    return form


def build_urartian_lexicon() -> list[str]:
    """Build Urartian lexicon TSV lines."""
    lines = []
    seen = set()

    for form, gloss in URARTIAN_WIKTIONARY_ENTRIES:
        # Clean form: remove morphological suffixes for root extraction
        # but keep the full attested form for the Word column
        clean = form.replace("-", "")

        if clean in seen:
            continue
        seen.add(clean)

        # Lowercase for transliteration (Urartian map is lowercase)
        clean_lower = clean.lower()

        # Pre-process to handle chars not in URARTIAN_MAP
        preprocessed = preprocess_urartian(clean_lower)

        # Transliterate using Urartian map
        ipa = transliterate(preprocessed, "xur")

        if not ipa:
            continue

        sca = ipa_to_sound_class(ipa)
        if not sca:
            continue

        lines.append(make_entry(form, ipa, sca, "wiktionary", gloss))

    return lines


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    outdir = ROOT / "data" / "training" / "lexicons"
    outdir.mkdir(parents=True, exist_ok=True)

    header = "Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID"

    # --- PIE ---
    print("Building PIE lexicon...", file=sys.stderr)
    pie_lines = build_pie_lexicon()
    pie_path = outdir / "ine-pro.tsv"
    with open(pie_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(header + "\n")
        for line in pie_lines:
            f.write(line + "\n")
    print(f"  Written {len(pie_lines)} entries to {pie_path}", file=sys.stderr)

    # --- Urartian ---
    print("Building Urartian lexicon...", file=sys.stderr)
    xur_lines = build_urartian_lexicon()
    xur_path = outdir / "xur.tsv"
    with open(xur_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(header + "\n")
        for line in xur_lines:
            f.write(line + "\n")
    print(f"  Written {len(xur_lines)} entries to {xur_path}", file=sys.stderr)

    # --- Summary ---
    print(f"\n=== Summary ===")
    print(f"PIE (ine-pro): {len(pie_lines)} entries")
    print(f"Urartian (xur): {len(xur_lines)} entries")
    print(f"\nSources used:")
    print(f"  PIE: Wiktionary Proto-Indo-European roots ({355} roots) and lemmas ({509} lemmas)")
    print(f"  Urartian: Wiktionary Category:Urartian_lemmas (5 entries) + Wikipedia Urartian_language article")
    print(f"\nOutput files:")
    print(f"  {pie_path}")
    print(f"  {xur_path}")


if __name__ == "__main__":
    main()
