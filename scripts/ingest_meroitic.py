#!/usr/bin/env python3
"""Ingest Meroitic (xmr) word data from Joshua Otten's Meroitic-Corpus on GitHub.

Source: https://github.com/Joshua-Otten/Meroitic-Corpus
Author: Joshua Otten (computational linguistics)
Description: First machine-readable Meroitic corpus
License: Public repository (academic)

Data files:
  - Data/LobbanVocabList.txt   -- structured word:gloss pairs (best quality)
  - Data/MilletExamples.txt    -- translated Meroitic words/phrases (Millet 1973)
  - Data/RillyExamples.txt     -- translated examples (Rilly 2007)
  - Data/mero-corpus.txt       -- running text from inscriptions

The Meroitic alphasyllabary was deciphered by Griffith (1911).  All 23 signs
have known phonetic values (see Rilly 2007, Rilly & de Voogt 2012).

The Otten corpus uses an ASCII transcription convention for scholarly
diacritics (documented in each file's header):
  S -> s-hat (IPA sh)     N -> n-tilde (IPA ny)
  X -> 4th H / h-underline (IPA x, velar fricative)
  x -> 3rd H (IPA x)      E -> e-hat (IPA e)

Iron Rule: All data comes from the downloaded GitHub files.
No hardcoded word lists.

Usage:
    python scripts/ingest_meroitic.py [--dry-run]
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import unicodedata
import urllib.error
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

ISO = "xmr"
LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"
CACHE_DIR = RAW_DIR / "meroitic"

GITHUB_RAW = "https://raw.githubusercontent.com/Joshua-Otten/Meroitic-Corpus/main/Data"
USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets; Meroitic ingestion)"

# Files to download from the corpus
DATA_FILES = [
    "LobbanVocabList.txt",
    "MilletExamples.txt",
    "RillyExamples.txt",
    "mero-corpus.txt",
]


# ---------------------------------------------------------------------------
# ASCII convention -> scholarly transliteration
# ---------------------------------------------------------------------------
# The Otten corpus uses capitals and ASCII for diacritics.
# We convert to standard Meroitic scholarly transliteration (lowercase + diacritics)
# before applying the MEROITIC_MAP.
#
# IMPORTANT: The convention differs slightly between files but the core mappings are:
#   S -> sh (s-hat)  -> scholarly: s with caron
#   N -> ny (n-tilde) -> scholarly: n with tilde
#   X -> velar fric  -> scholarly: h with breve below (or x in IPA directly)
#   E -> e (no special value in standard Meroitic -- just a vowel)
#   x -> velar fric  -> scholarly: same as X
#
# We handle these in the ingestion step, converting the corpus's ASCII convention
# into forms that the MEROITIC_MAP can handle.

def corpus_ascii_to_scholarly(text: str) -> str:
    """Convert Otten corpus ASCII convention to standard scholarly transliteration.

    The corpus uses uppercase for special characters. We convert to
    lowercase scholarly forms that the MEROITIC_MAP can process.

    Special diacritic capitals (Griffith/Rilly notation):
      S -> s-hat (sh)     N -> n-tilde (ny)
      X -> 4th H / velar  E -> e-hat (= plain e)

    Dotted-letter capitals (Millet notation for uncertain readings):
      B, M, Y, A, Q, W, I, R, K, T -> lowercase equivalents
      H -> h (h-tilde)    G -> g (variant)
    """
    result = []
    for ch in text:
        if ch == "S":
            result.append("\u0161")  # s with caron = sh
        elif ch == "N":
            result.append("\u00f1")  # n with tilde = ny
        elif ch == "X":
            result.append("\u1e2b")  # h with breve below = velar fricative
        elif ch == "E":
            result.append("e")       # e-hat = /e/
        elif ch == "H":
            result.append("h")       # h-tilde variant = h
        elif ch == "A":
            result.append("a")       # a-dot = a
        elif ch == "G":
            result.append("g")       # variant g
        # Dotted consonants (Millet uncertain readings) -> same phoneme
        elif ch == "B":
            result.append("b")
        elif ch == "M":
            result.append("m")
        elif ch == "Y":
            result.append("y")
        elif ch == "Q":
            result.append("q")
        elif ch == "W":
            result.append("w")
        elif ch == "I":
            result.append("i")
        elif ch == "R":
            result.append("r")
        elif ch == "K":
            result.append("k")
        elif ch == "T":
            result.append("t")
        elif ch in ("D", "F", "J", "L", "O", "P", "U", "V", "Z"):
            # Any remaining uppercase -> lowercase (safety catch)
            result.append(ch.lower())
        else:
            result.append(ch)
    return "".join(result)


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_file(filename: str) -> str:
    """Download a file from the GitHub repo, with local caching."""
    cache_path = CACHE_DIR / filename
    if cache_path.exists():
        logger.info("Using cached: %s", cache_path)
        with open(cache_path, "r", encoding="utf-8") as f:
            return f.read()

    url = f"{GITHUB_RAW}/{filename}"
    logger.info("Downloading: %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = resp.read().decode("utf-8")
                # Cache locally
                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(data)
                return data
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            logger.warning("Download attempt %d failed: %s", attempt + 1, e)
            import time
            if attempt < 2:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"Download failed after 3 attempts: {url}")


# ---------------------------------------------------------------------------
# Parsing functions for each source
# ---------------------------------------------------------------------------

def parse_lobban_vocab(text: str) -> list[dict]:
    """Parse LobbanVocabList.txt -- tab-separated word:gloss pairs.

    Format: word_form[;variants]  TAB  gloss[;glosses]  [TAB  POS/tags]
    Lines starting with # are comments.
    """
    entries = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) < 2:
            continue

        word_forms_raw = parts[0].strip()
        gloss = parts[1].strip() if len(parts) > 1 else ""
        pos_tags = parts[2].strip() if len(parts) > 2 else ""

        # Split on semicolons for variant forms
        for wf in word_forms_raw.split(";"):
            wf = wf.strip()
            if not wf:
                continue
            entries.append({
                "word": wf,
                "gloss": gloss,
                "pos": pos_tags,
                "source": "lobban",
            })

    logger.info("Lobban vocab: parsed %d entries", len(entries))
    return entries


def parse_millet_examples(text: str) -> list[dict]:
    """Parse MilletExamples.txt -- translated words/phrases from Millet 1973.

    Format: word_form  TAB  gloss  [TAB  POS/tags]
    Lines starting with # are comments.  Some lines have semicolons for variants.
    """
    entries = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) < 1:
            continue

        word_forms_raw = parts[0].strip()
        gloss = parts[1].strip() if len(parts) > 1 else ""
        pos_tags = parts[2].strip() if len(parts) > 2 else ""

        # Skip lines that are just page references or annotations
        if word_forms_raw.startswith("(") and word_forms_raw.endswith(")"):
            continue

        # Split on semicolons for variant forms
        for wf in word_forms_raw.split(";"):
            wf = wf.strip()
            if not wf:
                continue
            entries.append({
                "word": wf,
                "gloss": gloss,
                "pos": pos_tags,
                "source": "millet",
            })

    logger.info("Millet examples: parsed %d entries", len(entries))
    return entries


def parse_rilly_examples(text: str) -> list[dict]:
    """Parse RillyExamples.txt -- translated examples from Rilly 2007.

    Format: word_form  TAB  gloss  [TAB  POS/tags]
    Lines starting with # are comments.
    """
    entries = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Skip separator lines
        if line.startswith("---") or line.startswith("==="):
            continue

        parts = line.split("\t")
        if len(parts) < 1:
            continue

        word_forms_raw = parts[0].strip()
        gloss = parts[1].strip() if len(parts) > 1 else ""
        pos_tags = parts[2].strip() if len(parts) > 2 else ""

        # Split on semicolons for variant forms
        for wf in word_forms_raw.split(";"):
            wf = wf.strip()
            if not wf:
                continue
            entries.append({
                "word": wf,
                "gloss": gloss,
                "pos": pos_tags,
                "source": "rilly",
            })

    logger.info("Rilly examples: parsed %d entries", len(entries))
    return entries


def parse_corpus_tokens(text: str) -> list[dict]:
    """Parse mero-corpus.txt -- running text, extract unique word tokens.

    The corpus uses spaces as token boundaries.
    Words may contain Meroitic : (word dividers) which split words further.
    """
    # Tokenize: split on whitespace
    tokens = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Split on whitespace
        for token in line.split():
            # Further split on : (Meroitic word divider)
            for subtoken in token.split(":"):
                subtoken = subtoken.strip()
                if subtoken:
                    tokens.add(subtoken)

    entries = []
    for tok in sorted(tokens):
        entries.append({
            "word": tok,
            "gloss": "",
            "pos": "",
            "source": "corpus",
        })

    logger.info("Corpus tokens: extracted %d unique tokens", len(entries))
    return entries


# ---------------------------------------------------------------------------
# Word form cleaning and validation
# ---------------------------------------------------------------------------

def clean_word(word: str) -> str:
    """Clean a Meroitic word form for inclusion in the lexicon.

    - Strips whitespace
    - Removes surrounding parentheses
    - NFC normalizes
    """
    word = word.strip()
    # Remove surrounding parentheses like (a)tbE -> leave as is (partial reading)
    # But remove whole-word parens
    if word.startswith("(") and word.endswith(")"):
        word = word[1:-1]
    # NFC normalize
    word = unicodedata.normalize("NFC", word)
    return word


def is_valid_meroitic(word: str) -> bool:
    """Check if a word form is valid for inclusion.

    Meroitic uses only lowercase letters (a-z) in the Otten ASCII convention
    plus occasional uppercase for special chars (S, N, X, E, H, A, G).
    """
    if len(word) < 2:
        return False
    if len(word) > 60:
        return False
    # Must not be purely numeric
    if word.replace(".", "").replace("-", "").isdigit():
        return False
    # Reject entries with brackets (fragmentary/uncertain)
    if "[" in word or "]" in word:
        return False
    # Reject entries with question marks (uncertain readings)
    if "?" in word:
        return False
    # Reject entries with Kleene stars (damaged text markers)
    if "*" in word:
        return False
    # Reject entries that are purely annotation-like
    # (e.g. "EITXER-LBR-OR-TBR" type annotations from the corpus)
    if word.isupper() and len(word) > 3:
        return False
    # Must contain at least one alphabetic char
    if not any(c.isalpha() for c in word):
        return False
    # Reject if it contains characters outside the Meroitic repertoire
    # Valid chars: a-z, A-Z (for special chars), hyphen, period
    if not re.match(r'^[a-zA-Z\-\.]+$', word):
        return False
    return True


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest Meroitic from GitHub corpus")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and report without writing TSV")
    parser.add_argument("--no-cache", action="store_true",
                        help="Force re-download (ignore cache)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Clear cache if requested
    if args.no_cache:
        if CACHE_DIR.exists():
            import shutil
            shutil.rmtree(CACHE_DIR)
            logger.info("Cleared cache: %s", CACHE_DIR)

    # Step 1: Download all data files
    logger.info("Downloading Meroitic corpus files...")
    raw_texts = {}
    for fname in DATA_FILES:
        try:
            raw_texts[fname] = download_file(fname)
        except RuntimeError as e:
            logger.error("Failed to download %s: %s", fname, e)
            if fname == "mero-corpus.txt":
                # The main corpus is large -- continue without it if needed
                logger.warning("Continuing without main corpus text")
            else:
                raise

    # Step 2: Parse all sources
    all_entries = []

    if "LobbanVocabList.txt" in raw_texts:
        all_entries.extend(parse_lobban_vocab(raw_texts["LobbanVocabList.txt"]))

    if "MilletExamples.txt" in raw_texts:
        all_entries.extend(parse_millet_examples(raw_texts["MilletExamples.txt"]))

    if "RillyExamples.txt" in raw_texts:
        all_entries.extend(parse_rilly_examples(raw_texts["RillyExamples.txt"]))

    if "mero-corpus.txt" in raw_texts:
        all_entries.extend(parse_corpus_tokens(raw_texts["mero-corpus.txt"]))

    logger.info("Total raw entries across all sources: %d", len(all_entries))

    # Step 3: Clean, validate, and deduplicate
    seen_words = set()
    valid_entries = []
    skipped_invalid = 0
    skipped_dup = 0

    # Process structured sources first (higher quality), then corpus tokens
    source_priority = {"lobban": 0, "rilly": 1, "millet": 2, "corpus": 3}
    all_entries.sort(key=lambda e: (source_priority.get(e["source"], 99), e["word"]))

    for entry in all_entries:
        word = clean_word(entry["word"])

        if not is_valid_meroitic(word):
            skipped_invalid += 1
            continue

        # Normalize to lowercase for dedup (the ASCII convention uses case for diacritics)
        # But we need to keep the original case for transliteration
        dedup_key = word.lower()
        if dedup_key in seen_words:
            skipped_dup += 1
            continue

        seen_words.add(dedup_key)
        entry["word_clean"] = word
        valid_entries.append(entry)

    logger.info("Valid entries: %d (skipped: %d invalid, %d duplicates)",
                len(valid_entries), skipped_invalid, skipped_dup)

    # Step 4: Transliterate and generate IPA
    tsv_entries = []
    audit_trail = []
    identity_count = 0

    for entry in valid_entries:
        word = entry["word_clean"]

        # Convert ASCII convention to scholarly transliteration
        scholarly = corpus_ascii_to_scholarly(word)

        # Apply transliteration map to get IPA
        ipa = transliterate(scholarly, ISO)

        if not ipa:
            ipa = scholarly  # fallback: use scholarly form directly

        # Check identity rate
        if ipa == word:
            identity_count += 1

        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""

        tsv_entries.append({
            "word": word,
            "ipa": ipa,
            "sca": sca,
            "source": entry["source"],
        })

        audit_trail.append({
            "word_original": entry["word"],
            "word_clean": word,
            "scholarly": scholarly,
            "ipa": ipa,
            "sca": sca,
            "gloss": entry.get("gloss", ""),
            "pos": entry.get("pos", ""),
            "source": entry["source"],
        })

    identity_rate = identity_count / len(tsv_entries) * 100 if tsv_entries else 0.0

    # Step 5: Report
    mode = "DRY RUN: " if args.dry_run else ""
    print(f"\n{mode}Meroitic Corpus Ingestion (xmr):")
    print("=" * 60)
    print(f"  Source:        https://github.com/Joshua-Otten/Meroitic-Corpus")
    print(f"  Method:        GitHub raw download + parse")
    print(f"  Sources used:")
    source_counts = {}
    for e in tsv_entries:
        source_counts[e["source"]] = source_counts.get(e["source"], 0) + 1
    for src, count in sorted(source_counts.items()):
        print(f"    {src:12s}: {count:5d} entries")
    print(f"  Total entries: {len(tsv_entries)}")
    print(f"  Identity rate: {identity_rate:.1f}% ({identity_count}/{len(tsv_entries)})")

    # Sample entries
    if tsv_entries:
        print(f"\n  Sample entries (from structured sources):")
        structured = [e for e in tsv_entries if e["source"] != "corpus"]
        samples = structured[:5] if len(structured) >= 5 else tsv_entries[:5]
        for e in samples:
            print(f"    {e['word']:25s} -> {e['ipa']:25s}  [{e['source']}]")

    print("=" * 60)

    if args.dry_run:
        return

    # Step 6: Write TSV
    if tsv_entries:
        LEXICON_DIR.mkdir(parents=True, exist_ok=True)
        tsv_path = LEXICON_DIR / f"{ISO}.tsv"

        with open(tsv_path, "w", encoding="utf-8") as f:
            f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")
            for e in tsv_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\tmeroitic-corpus:{e['source']}\t-\t-\n")

        logger.info("Wrote %d entries to %s", len(tsv_entries), tsv_path)
        print(f"\n  Written: {tsv_path}")

    # Step 7: Save audit trail
    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"meroitic_ingest_{ISO}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        logger.info("Wrote audit trail: %s", audit_path)


if __name__ == "__main__":
    main()
