#!/usr/bin/env python3
"""Build the inscription/running-text TSV dataset from downloaded corpora.

Reads:
  - UD CoNLL-U files  (grc, lat, san, ang)
  - CEIPoM CSV files  (osc, xum)

Writes:
  - data/inscriptions/{iso}_inscriptions.tsv   (one per language)

Each TSV row = one sentence / inscription text with IPA and SCA.

Usage:
    python scripts/build_inscriptions.py [--raw-dir DIR] [--out-dir DIR]
"""

import argparse
import csv
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Resolve imports for transliteration_maps and sound_class
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(REPO_ROOT / "cognate_pipeline" / "src"))

from transliteration_maps import transliterate, ALL_MAPS  # noqa: E402
from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

# ---------------------------------------------------------------------------
# Devanagari → IAST transliteration (for Sanskrit-Vedic treebank)
# ---------------------------------------------------------------------------
# Devanagari is an abugida: each consonant carries inherent /a/ unless
# followed by a vowel sign (matra) or virama (halant).

_DEVA_VOWELS_INDEP = {
    "अ": "a", "आ": "ā", "इ": "i", "ई": "ī", "उ": "u", "ऊ": "ū",
    "ऋ": "ṛ", "ॠ": "ṝ", "ऌ": "ḷ", "ॡ": "ḹ",
    "ए": "e", "ऐ": "ai", "ओ": "o", "औ": "au",
}

_DEVA_VOWEL_SIGNS = {
    "ा": "ā", "ि": "i", "ी": "ī", "ु": "u", "ू": "ū",
    "ृ": "ṛ", "ॄ": "ṝ", "ॢ": "ḷ", "ॣ": "ḹ",
    "े": "e", "ै": "ai", "ो": "o", "ौ": "au",
}

_DEVA_CONSONANTS = {
    "क": "k", "ख": "kh", "ग": "g", "घ": "gh", "ङ": "ṅ",
    "च": "c", "छ": "ch", "ज": "j", "झ": "jh", "ञ": "ñ",
    "ट": "ṭ", "ठ": "ṭh", "ड": "ḍ", "ढ": "ḍh", "ण": "ṇ",
    "त": "t", "थ": "th", "द": "d", "ध": "dh", "न": "n",
    "प": "p", "फ": "ph", "ब": "b", "भ": "bh", "म": "m",
    "य": "y", "र": "r", "ल": "l", "व": "v",
    "श": "ś", "ष": "ṣ", "स": "s", "ह": "h",
    # Vedic extras
    "ळ": "ḷ",  # retroflex lateral (rare)
}

_DEVA_VIRAMA = "\u094D"  # halant
_DEVA_ANUSVARA = "ं"
_DEVA_VISARGA = "ः"
_DEVA_AVAGRAHA = "ऽ"
_DEVA_NUKTA = "\u093C"

# Build sets for quick lookup
_DEVA_CONS_SET = set(_DEVA_CONSONANTS.keys())
_DEVA_VOWEL_SIGN_SET = set(_DEVA_VOWEL_SIGNS.keys())


def devanagari_to_iast(text: str) -> str:
    """Convert Devanagari text to IAST romanization."""
    text = unicodedata.normalize("NFC", text)
    result: list[str] = []
    i = 0
    pending_consonant: Optional[str] = None  # IAST consonant waiting for vowel/virama

    while i < len(text):
        ch = text[i]

        # Handle nukta (modifies previous consonant — skip for simplicity)
        if ch == _DEVA_NUKTA:
            i += 1
            continue

        # Virama: suppress inherent /a/ of pending consonant
        if ch == _DEVA_VIRAMA:
            if pending_consonant is not None:
                result.append(pending_consonant)
                pending_consonant = None
            i += 1
            continue

        # Vowel sign (matra): replaces inherent /a/ of pending consonant
        if ch in _DEVA_VOWEL_SIGN_SET:
            if pending_consonant is not None:
                result.append(pending_consonant)
                pending_consonant = None
            result.append(_DEVA_VOWEL_SIGNS[ch])
            i += 1
            continue

        # Consonant: flush previous consonant with inherent /a/
        if ch in _DEVA_CONS_SET:
            if pending_consonant is not None:
                result.append(pending_consonant + "a")
            pending_consonant = _DEVA_CONSONANTS[ch]
            i += 1
            continue

        # Independent vowel
        if ch in _DEVA_VOWELS_INDEP:
            if pending_consonant is not None:
                result.append(pending_consonant + "a")
                pending_consonant = None
            result.append(_DEVA_VOWELS_INDEP[ch])
            i += 1
            continue

        # Anusvara
        if ch == _DEVA_ANUSVARA:
            if pending_consonant is not None:
                result.append(pending_consonant + "a")
                pending_consonant = None
            result.append("ṃ")
            i += 1
            continue

        # Visarga
        if ch == _DEVA_VISARGA:
            if pending_consonant is not None:
                result.append(pending_consonant + "a")
                pending_consonant = None
            result.append("ḥ")
            i += 1
            continue

        # Avagraha
        if ch == _DEVA_AVAGRAHA:
            if pending_consonant is not None:
                result.append(pending_consonant + "a")
                pending_consonant = None
            result.append("'")
            i += 1
            continue

        # Devanagari digits (U+0966-U+096F) → skip
        if "\u0966" <= ch <= "\u096F":
            if pending_consonant is not None:
                result.append(pending_consonant + "a")
                pending_consonant = None
            i += 1
            continue

        # Anything else (spaces, punctuation, Latin chars, etc.)
        if pending_consonant is not None:
            result.append(pending_consonant + "a")
            pending_consonant = None
        result.append(ch)
        i += 1

    # Flush final pending consonant
    if pending_consonant is not None:
        result.append(pending_consonant + "a")

    return "".join(result)


# ---------------------------------------------------------------------------
# Greek diacritic stripping (breathings, accents, iota subscript)
# ---------------------------------------------------------------------------
# After NFC normalization, Greek vowels may carry combining accents and
# breathing marks.  We strip them so the ANCIENT_GREEK_MAP can match
# the bare letters.

def strip_greek_diacritics(text: str) -> str:
    """Strip accents, breathings, and iota subscript from Greek text."""
    text = unicodedata.normalize("NFD", text)
    # Remove combining marks: accents (0300-0345), breathing (0313, 0314),
    # iota subscript (0345), and other modifiers
    text = re.sub(r"[\u0300-\u0345]", "", text)
    return unicodedata.normalize("NFC", text)


# ---------------------------------------------------------------------------
# CoNLL-U parser
# ---------------------------------------------------------------------------

def parse_conllu(path: Path) -> List[Dict]:
    """Parse a CoNLL-U file, returning a list of sentence dicts.

    Each dict has:
      - 'text': the reassembled sentence text (from # text = ... or FORM column)
      - 'sent_id': sentence ID (from # sent_id = ...)
      - 'tokens': list of FORM values
    """
    sentences: List[Dict] = []
    current_text: Optional[str] = None
    current_id: Optional[str] = None
    current_tokens: List[str] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("# text = "):
                current_text = line[len("# text = "):]
            elif line.startswith("# sent_id = "):
                current_id = line[len("# sent_id = "):]
            elif line.startswith("#"):
                continue
            elif line == "":
                # End of sentence
                if current_tokens:
                    text = current_text or " ".join(current_tokens)
                    sentences.append({
                        "text": text,
                        "sent_id": current_id or f"s{len(sentences)+1}",
                        "tokens": current_tokens,
                    })
                current_text = None
                current_id = None
                current_tokens = []
            else:
                fields = line.split("\t")
                if len(fields) >= 2:
                    token_id = fields[0]
                    # Skip multi-word tokens (e.g., "1-2") and empty nodes (e.g., "1.1")
                    if "-" in token_id or "." in token_id:
                        continue
                    form = fields[1]
                    if form != "_":
                        current_tokens.append(form)

    # Handle last sentence if file doesn't end with blank line
    if current_tokens:
        text = current_text or " ".join(current_tokens)
        sentences.append({
            "text": text,
            "sent_id": current_id or f"s{len(sentences)+1}",
            "tokens": current_tokens,
        })

    return sentences


# ---------------------------------------------------------------------------
# CEIPoM parser
# ---------------------------------------------------------------------------

def parse_ceipom_sentences(ceipom_dir: Path) -> Dict[str, List[Dict]]:
    """Parse CEIPoM sentences.csv, grouping by language.

    Returns dict: language_code → list of inscription dicts.
    CEIPoM CSV is UTF-16 LE encoded.
    """
    # First load texts.csv to get language per Text_ID
    texts_csv = ceipom_dir / "texts.csv"
    sentences_csv = ceipom_dir / "sentences.csv"

    if not texts_csv.exists() or not sentences_csv.exists():
        # Try subdirectories
        for sub in ceipom_dir.iterdir():
            if sub.is_dir():
                if (sub / "texts.csv").exists():
                    texts_csv = sub / "texts.csv"
                    sentences_csv = sub / "sentences.csv"
                    break

    # Load language mapping from texts.csv
    text_lang: Dict[str, str] = {}
    text_date: Dict[str, str] = {}
    with open(texts_csv, "r", encoding="utf-16") as f:
        reader = csv.DictReader(f, delimiter=",")
        for row in reader:
            tid = row.get("Text_ID", "").strip()
            lang = row.get("Language", "").strip()
            date_after = row.get("Date_after", "").strip()
            date_before = row.get("Date_before", "").strip()
            text_lang[tid] = lang
            if date_after and date_before:
                text_date[tid] = f"{date_after} to {date_before} BCE"
            elif date_after:
                text_date[tid] = f"after {date_after} BCE"
            else:
                text_date[tid] = ""

    # Parse sentences.csv
    results: Dict[str, List[Dict]] = {}
    with open(sentences_csv, "r", encoding="utf-16") as f:
        reader = csv.DictReader(f, delimiter=",")
        for row in reader:
            tid = row.get("Text_ID", "").strip()
            sent_text = row.get("Sentence", "").strip()
            sent_pos = row.get("Sentence_position", "").strip()
            lang = text_lang.get(tid, "Unknown")

            if not sent_text or sent_text == "-":
                continue

            # Map CEIPoM language names to ISO codes
            iso = _ceipom_lang_to_iso(lang)
            if iso is None:
                continue

            # Clean editorial apparatus
            sent_text = _clean_ceipom_text(sent_text)
            if not sent_text or len(sent_text) < 2:
                continue

            # Skip Greek-script inscriptions (need different transliteration)
            if _is_mostly_greek_script(sent_text):
                continue

            # Skip letter inventories (isolated letters, not real text)
            if _is_letter_inventory(sent_text):
                continue

            if iso not in results:
                results[iso] = []

            results[iso].append({
                "text": sent_text,
                "sent_id": f"{tid}:{sent_pos}",
                "date": text_date.get(tid, ""),
                "source_text_id": tid,
            })

    return results


def _ceipom_lang_to_iso(lang: str) -> Optional[str]:
    """Map CEIPoM language name to ISO 639-3 code (only Oscan and Umbrian)."""
    lang_lower = lang.lower().strip()
    mapping = {
        "oscan": "osc",
        "umbrian": "xum",
    }
    return mapping.get(lang_lower)


# ---------------------------------------------------------------------------
# IPA conversion per language
# ---------------------------------------------------------------------------

def text_to_ipa(text: str, iso: str) -> str:
    """Convert a text string to IPA for the given language.

    Returns space-separated IPA words.
    """
    words = text.split()
    ipa_words: List[str] = []

    for word in words:
        # Clean: remove punctuation at edges but keep internal hyphens
        clean = _clean_word(word)
        if not clean:
            continue

        if iso == "grc":
            # Strip Greek diacritics, then transliterate
            stripped = strip_greek_diacritics(clean)
            ipa = transliterate(stripped, "grc")
        elif iso == "lat":
            # Skip cross/symbol markers common in medieval charters
            if clean in ("+", "†", "‡", "§"):
                continue
            ipa = transliterate(clean.lower(), "lat")
        elif iso == "san":
            # UD_Sanskrit-Vedic uses IAST romanization directly
            # If Devanagari detected, convert first; otherwise use IAST
            if any("\u0900" <= c <= "\u097F" for c in clean):
                iast = devanagari_to_iast(clean)
                ipa = transliterate(iast, "san-iast")
            else:
                ipa = transliterate(clean, "san-iast")
        elif iso in ("ang", "osc", "xum"):
            ipa = transliterate(clean, iso)
        else:
            ipa = clean  # passthrough

        if ipa:
            ipa_words.append(ipa)

    return " ".join(ipa_words)


def _clean_word(word: str) -> str:
    """Remove leading/trailing punctuation from a word."""
    # Strip common punctuation; keep letters, combining marks, hyphens
    word = re.sub(r'^[^\w\u0370-\u03FF\u0900-\u097F]+', '', word)
    word = re.sub(r'[^\w\u0370-\u03FF\u0900-\u097F]+$', '', word)
    return word


def _clean_ceipom_text(text: str) -> str:
    """Clean CEIPoM inscription text: remove editorial apparatus."""
    # Remove lacunae markers: [---], [..], [-?-]
    text = re.sub(r'\[[-\.?]+\]', '', text)
    # Remove uncertain readings in brackets: [r] -> r
    text = re.sub(r'\[([^\]]{1,10})\]', r'\1', text)
    # Remove parenthesized uncertain chars: (pi servus) -> pi servus
    text = re.sub(r'\(([^\)]{1,15})\)', r'\1', text)
    # Remove line-break markers: /
    text = re.sub(r'\s*/\s*', ' ', text)
    # Remove section markers: :-
    text = text.replace(':-', '')
    # Remove interpuncts (middle dot, colon used as word separator)
    text = text.replace('·', ' ').replace(':', ' ')
    # Remove leading numbers (editorial line numbers, esp. in Iguvine Tablets)
    text = re.sub(r'^\d+\s+', '', text)
    # Remove commas between isolated letters (letter inventories, not text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _is_letter_inventory(text: str) -> bool:
    """Check if text is just a list of isolated letters, not real text."""
    words = text.split()
    if len(words) < 2:
        return False
    single_char_count = sum(1 for w in words if len(w) <= 2)
    return single_char_count / len(words) > 0.7 and len(words) >= 3


def _is_mostly_greek_script(text: str) -> bool:
    """Check if a text is predominantly in Greek script (not Latin)."""
    greek_chars = sum(1 for c in text if '\u0370' <= c <= '\u03FF' or '\u1F00' <= c <= '\u1FFF')
    latin_chars = sum(1 for c in text if 'a' <= c.lower() <= 'z')
    total = greek_chars + latin_chars
    if total == 0:
        return False
    return greek_chars / total > 0.5


# ---------------------------------------------------------------------------
# Source metadata
# ---------------------------------------------------------------------------

SOURCE_META = {
    "grc": {
        "source": "ud_ancient_greek_ptnk",
        "date": "4th-5th c. CE (Codex Alexandrinus)",
        "genre": "religious",
        "ipa_source": "translit_map",
    },
    "lat_llct": {
        "source": "ud_latin_llct",
        "date": "774-897 CE",
        "genre": "legal",
        "ipa_source": "translit_map",
    },
    "lat_circse": {
        "source": "ud_latin_circse",
        "date": "1st c. BCE - 2nd c. CE",
        "genre": "literary",
        "ipa_source": "translit_map",
    },
    "san": {
        "source": "ud_sanskrit_vedic",
        "date": "c. 1500-500 BCE",
        "genre": "religious",
        "ipa_source": "iast_translit_map",
    },
    "ang": {
        "source": "ud_old_english_cairo",
        "date": "c. 700-1100 CE",
        "genre": "mixed",
        "ipa_source": "translit_map",
    },
    "osc": {
        "source": "ceipom",
        "date": "",
        "genre": "inscription",
        "ipa_source": "translit_map",
    },
    "xum": {
        "source": "ceipom",
        "date": "",
        "genre": "inscription",
        "ipa_source": "translit_map",
    },
}


# ---------------------------------------------------------------------------
# TSV writer
# ---------------------------------------------------------------------------

TSV_HEADER = [
    "Inscription_ID", "Text", "IPA", "SCA", "Source",
    "Date_Approx", "Genre", "IPA_Source",
]


def write_inscription_tsv(
    path: Path,
    records: List[Dict],
    iso: str,
    meta_key: str,
) -> int:
    """Write inscription records to a TSV file. Returns count written."""
    meta = SOURCE_META[meta_key]
    count = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(TSV_HEADER)
        for rec in records:
            text = rec["text"]
            sent_id = rec["sent_id"]
            ipa = text_to_ipa(text, iso)
            if not ipa.strip():
                continue
            sca = " ".join(ipa_to_sound_class(w) for w in ipa.split())
            date = rec.get("date", "") or meta["date"]
            inscription_id = f"{meta['source']}_{sent_id}"

            writer.writerow([
                inscription_id,
                text,
                ipa,
                sca,
                meta["source"],
                date,
                meta["genre"],
                meta["ipa_source"],
            ])
            count += 1
    return count


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def build_ud_language(
    raw_dir: Path,
    out_dir: Path,
    repo_name: str,
    iso: str,
    meta_key: str,
) -> int:
    """Build TSV for a UD treebank language."""
    repo_dir = raw_dir / repo_name
    if not repo_dir.exists():
        print(f"  [skip] {repo_name} not found at {repo_dir}")
        return 0

    conllu_files = sorted(repo_dir.glob("*.conllu"))
    if not conllu_files:
        print(f"  [skip] No CoNLL-U files in {repo_dir}")
        return 0

    all_sentences: List[Dict] = []
    for cf in conllu_files:
        sentences = parse_conllu(cf)
        all_sentences.extend(sentences)
        print(f"    {cf.name}: {len(sentences)} sentences")

    out_path = out_dir / f"{iso}_inscriptions.tsv"
    count = write_inscription_tsv(out_path, all_sentences, iso, meta_key)
    print(f"  -> {out_path.name}: {count} entries")
    return count


def build_ceipom(
    raw_dir: Path,
    out_dir: Path,
) -> Dict[str, int]:
    """Build TSVs for CEIPoM languages (Oscan, Umbrian)."""
    ceipom_dir = raw_dir / "CEIPoM"
    if not ceipom_dir.exists():
        print(f"  [skip] CEIPoM not found at {ceipom_dir}")
        return {}

    lang_data = parse_ceipom_sentences(ceipom_dir)
    counts: Dict[str, int] = {}

    for iso, records in lang_data.items():
        out_path = out_dir / f"{iso}_inscriptions.tsv"
        count = write_inscription_tsv(out_path, records, iso, iso)
        print(f"  -> {out_path.name}: {count} entries")
        counts[iso] = count

    return counts


def build_merged_latin(
    raw_dir: Path,
    out_dir: Path,
) -> int:
    """Build a merged Latin TSV from LLCT + CIRCSE."""
    all_records: List[Tuple[Dict, str]] = []  # (record, meta_key)

    for repo_name, meta_key in [
        ("UD_Latin-LLCT", "lat_llct"),
        ("UD_Latin-CIRCSE", "lat_circse"),
    ]:
        repo_dir = raw_dir / repo_name
        if not repo_dir.exists():
            print(f"  [skip] {repo_name} not found")
            continue
        conllu_files = sorted(repo_dir.glob("*.conllu"))
        for cf in conllu_files:
            sentences = parse_conllu(cf)
            for s in sentences:
                all_records.append((s, meta_key))
            print(f"    {cf.name}: {len(sentences)} sentences")

    if not all_records:
        return 0

    # Write merged TSV
    out_path = out_dir / "lat_inscriptions.tsv"
    count = 0
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(TSV_HEADER)
        for rec, meta_key in all_records:
            meta = SOURCE_META[meta_key]
            text = rec["text"]
            ipa = text_to_ipa(text, "lat")
            if not ipa.strip():
                continue
            sca = " ".join(ipa_to_sound_class(w) for w in ipa.split())
            inscription_id = f"{meta['source']}_{rec['sent_id']}"
            writer.writerow([
                inscription_id,
                text,
                ipa,
                sca,
                meta["source"],
                meta["date"],
                meta["genre"],
                meta["ipa_source"],
            ])
            count += 1

    print(f"  -> {out_path.name}: {count} entries")
    return count


def main():
    parser = argparse.ArgumentParser(description="Build inscription TSV dataset")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/training/raw/inscriptions"),
        help="Directory containing downloaded raw data",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/inscriptions"),
        help="Output directory for inscription TSVs",
    )
    args = parser.parse_args()

    raw_dir = args.raw_dir
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    totals: Dict[str, int] = {}

    print("=== Ancient Greek (grc) — UD_Ancient_Greek-PTNK ===")
    n = build_ud_language(raw_dir, out_dir, "UD_Ancient_Greek-PTNK", "grc", "grc")
    totals["grc"] = n

    print("\n=== Latin (lat) — UD_Latin-LLCT + UD_Latin-CIRCSE ===")
    n = build_merged_latin(raw_dir, out_dir)
    totals["lat"] = n

    print("\n=== Sanskrit (san) — UD_Sanskrit-Vedic ===")
    n = build_ud_language(raw_dir, out_dir, "UD_Sanskrit-Vedic", "san", "san")
    totals["san"] = n

    print("\n=== Old English (ang) — UD_Old_English-Cairo ===")
    # OEDT has no data files yet; Cairo is small but available
    n = build_ud_language(raw_dir, out_dir, "UD_Old_English-Cairo", "ang", "ang")
    totals["ang"] = n

    print("\n=== Oscan (osc) + Umbrian (xum) — CEIPoM ===")
    ceipom_counts = build_ceipom(raw_dir, out_dir)
    totals.update(ceipom_counts)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = 0
    for iso in ["grc", "lat", "san", "ang", "osc", "xum"]:
        n = totals.get(iso, 0)
        total += n
        print(f"  {iso}: {n:>6} entries")
    print(f"  {'TOTAL':>3}: {total:>6} entries")
    print(f"\nOutput: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
