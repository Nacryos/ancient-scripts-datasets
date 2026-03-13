#!/usr/bin/env python3
"""Ingest Lepontic (xlp) word data from Lexicon Leponticum (University of Vienna).

Source: Lexicon Leponticum — A digital edition of Cisalpine Celtic inscriptions
URL: https://lexlep.univie.ac.at/
Institution: University of Vienna (Department of Linguistics)
PIs: David Stifter, Corinna Salomon
License: Creative Commons (academic project)

Method: MediaWiki API — query Category:Word members, then fetch each page's
wikitext to extract the {{word}} template's analysis_phonemic field.

Iron Rule: All data comes from the downloaded MediaWiki API responses.
No hardcoded word lists.

Usage:
    python scripts/ingest_lexlep.py [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
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

ISO = "xlp"
LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"
CACHE_DIR = RAW_DIR / "lexlep"

API_BASE = "https://lexlep.univie.ac.at/api.php"
USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets; Lepontic ingestion)"


# ---------------------------------------------------------------------------
# MediaWiki API helpers
# ---------------------------------------------------------------------------

def api_get(params: dict) -> dict:
    """Make a GET request to the LexLep MediaWiki API."""
    params["format"] = "json"
    url = API_BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            logger.warning("API attempt %d failed: %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"API request failed after 3 attempts: {url}")


def get_all_word_titles() -> list[str]:
    """Fetch all page titles in Category:Word via the MediaWiki API."""
    titles = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": "Category:Word",
        "cmlimit": "500",
    }
    while True:
        data = api_get(params)
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            titles.append(m["title"])
        # Handle pagination
        cont = data.get("continue")
        if cont and "cmcontinue" in cont:
            params["cmcontinue"] = cont["cmcontinue"]
        else:
            break
    return titles


def fetch_page_wikitext(title: str) -> str | None:
    """Fetch the wikitext of a single page."""
    data = api_get({
        "action": "parse",
        "page": title,
        "prop": "wikitext",
    })
    parse = data.get("parse", {})
    wikitext_data = parse.get("wikitext", {})
    if isinstance(wikitext_data, dict):
        return wikitext_data.get("*", "")
    return str(wikitext_data) if wikitext_data else None


# ---------------------------------------------------------------------------
# Wikitext parsing
# ---------------------------------------------------------------------------

def _find_template_body(wikitext: str, template_name: str) -> str | None:
    """Find the body of a top-level {{template_name ...}} by counting brace depth.

    This handles nested templates like {{m|...}} and {{p|...}} inside the body.
    Returns the content between {{template_name\\n and the matching }}.
    """
    start_marker = "{{" + template_name
    idx = wikitext.find(start_marker)
    if idx < 0:
        return None

    # Skip past "{{word" to find the body start
    body_start = idx + len(start_marker)
    # Skip any whitespace/newline after template name
    while body_start < len(wikitext) and wikitext[body_start] in (" ", "\t", "\n", "\r"):
        body_start += 1

    # Now scan forward counting {{ and }} to find the matching close
    depth = 1  # We've consumed one opening {{
    pos = body_start
    while pos < len(wikitext) - 1 and depth > 0:
        if wikitext[pos] == "{" and wikitext[pos + 1] == "{":
            depth += 1
            pos += 2
        elif wikitext[pos] == "}" and wikitext[pos + 1] == "}":
            depth -= 1
            if depth == 0:
                return wikitext[body_start:pos]
            pos += 2
        else:
            pos += 1

    return None


def parse_word_template(wikitext: str) -> dict | None:
    """Parse the {{word}} template from page wikitext.

    Returns a dict with keys: language, type_word, meaning, field_semantic,
    phonemic, morphemic, case, number, gender, stem_class.
    """
    if not wikitext:
        return None

    body = _find_template_body(wikitext, "word")
    if not body:
        return None

    result = {}

    # Extract named parameters: |key=value
    # We split on top-level pipes (not inside nested templates)
    # Simple approach: split on \n| which is safe since values don't contain \n|
    params = re.split(r"\n\|", "\n" + body)
    for param in params:
        param = param.strip()
        if "=" not in param:
            continue
        key, _, val = param.partition("=")
        key = key.strip()
        val = val.strip()
        if key and re.match(r"^\w+$", key):
            result[key] = val

    return result


def extract_phonemic(phonemic_str: str) -> str:
    """Extract phoneme sequence from analysis_phonemic field.

    The field uses {{p|X}} templates for each phoneme, e.g.:
      /{{p|k}}{{p|o}}{{p|m}}{{p|o}}{{p|n}}{{p|o}}{{p|s}}/

    Templates can have multiple arguments: {{p|n|<sup>n</sup>}} -- we take the first.
    Also handles optional segments like ({{p|i}}) and alternatives with ' or '.
    """
    if not phonemic_str:
        return ""

    # Strip values like "unknown", "-", etc.
    stripped = phonemic_str.strip().strip("/").strip()
    if stripped in ("unknown", "-", "—", ""):
        return ""

    # If the field starts with "?" it's uncertain/partial -- skip phonemic
    if stripped.startswith("?") or stripped.startswith("-"):
        return ""

    # If there are alternatives (e.g. "... or ..."), take the first one
    if " or " in phonemic_str:
        phonemic_str = phonemic_str.split(" or ")[0].strip()

    # Extract all {{p|X}} or {{p|X|display}} values -- take first argument only
    phonemes = re.findall(r"\{\{p\|([^|}]+)(?:\|[^}]*)?\}\}", phonemic_str)
    if not phonemes:
        return ""

    return "".join(phonemes)


def clean_word_form(title: str) -> str:
    """Clean a word form from the page title.

    Removes trailing parenthetical disambiguation and normalizes Unicode.
    """
    # Remove disambiguation like "word (2)" or "word (noun)"
    cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", title)
    # NFC normalize
    cleaned = unicodedata.normalize("NFC", cleaned.strip())
    return cleaned


def is_valid_word(word: str, language: str | None, word_type: str | None) -> bool:
    """Check if a word entry is valid for inclusion."""
    # Must have at least 2 characters
    if len(word) < 2:
        return False
    # Filter very long entries (likely fragments or errors)
    if len(word) > 50:
        return False
    # Filter entries with brackets (fragmentary text)
    if "[" in word or "]" in word:
        return False
    # Filter entries with parentheses (fragmentary/uncertain)
    if "(" in word or ")" in word:
        return False
    # Filter entries that are purely numeric
    if word.replace(".", "").isdigit():
        return False
    # Filter entries with question marks (uncertain readings)
    if "?" in word:
        return False
    # Filter non-Celtic entries (Latin, Etruscan, etc.)
    # The LexLep includes words from multiple languages found in inscriptions
    if language and language.lower() not in ("celtic", "lepontic", "cisalpine celtic",
                                               "cisalpine gaulish", ""):
        return False
    return True


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------

def save_cache(data: list[dict], cache_path: Path) -> None:
    """Save fetched word data to cache."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Cached %d entries to %s", len(data), cache_path)


def load_cache(cache_path: Path) -> list[dict] | None:
    """Load cached word data if available."""
    if cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("Loaded %d entries from cache: %s", len(data), cache_path)
        return data
    return None


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------

def fetch_raw_wikitext_cache() -> dict[str, str]:
    """Fetch raw wikitext for all word pages, using a persistent cache.

    Returns a dict mapping page title -> raw wikitext.
    """
    raw_cache_path = CACHE_DIR / "lexlep_wikitext.json"

    # Load existing raw cache
    if raw_cache_path.exists():
        with open(raw_cache_path, "r", encoding="utf-8") as f:
            raw_cache = json.load(f)
        logger.info("Loaded raw wikitext cache: %d pages", len(raw_cache))
        return raw_cache

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Get all word page titles
    logger.info("Fetching word list from Category:Word...")
    titles = get_all_word_titles()
    logger.info("Found %d word pages", len(titles))

    # Step 2: Fetch raw wikitext for each page
    raw_cache = {}
    for i, title in enumerate(titles):
        if (i + 1) % 50 == 0:
            logger.info("Fetching page %d/%d: %s", i + 1, len(titles), title)

        try:
            wikitext = fetch_page_wikitext(title)
        except RuntimeError as e:
            logger.warning("Failed to fetch '%s': %s", title, e)
            continue

        if wikitext:
            raw_cache[title] = wikitext

        # Be polite: small delay between requests
        time.sleep(0.1)

    logger.info("Fetched wikitext for %d pages", len(raw_cache))

    # Save raw cache
    with open(raw_cache_path, "w", encoding="utf-8") as f:
        json.dump(raw_cache, f, ensure_ascii=False)
    logger.info("Saved raw wikitext cache: %s", raw_cache_path)

    return raw_cache


def parse_all_word_data(raw_cache: dict[str, str]) -> list[dict]:
    """Parse word entries from cached wikitext."""
    entries = []

    for title, wikitext in raw_cache.items():
        parsed = parse_word_template(wikitext)
        if not parsed:
            logger.debug("No {{word}} template in: %s", title)
            continue

        word_form = clean_word_form(title)
        language = parsed.get("language", "")
        word_type = parsed.get("type_word", "")
        phonemic_raw = parsed.get("analysis_phonemic", "")
        phonemic = extract_phonemic(phonemic_raw)
        meaning = parsed.get("meaning", "")
        semantic = parsed.get("field_semantic", "")

        entries.append({
            "title": title,
            "word": word_form,
            "language": language,
            "type_word": word_type,
            "phonemic": phonemic,
            "phonemic_raw": phonemic_raw,
            "meaning": meaning,
            "semantic": semantic,
            "case": parsed.get("case", ""),
            "number": parsed.get("number", ""),
            "gender": parsed.get("gender", ""),
            "stem_class": parsed.get("stem_class", ""),
        })

    logger.info("Parsed %d word entries total", len(entries))
    return entries


def fetch_all_word_data() -> list[dict]:
    """Fetch and parse all word entries from LexLep API."""
    raw_cache = fetch_raw_wikitext_cache()
    return parse_all_word_data(raw_cache)


def load_existing_words(tsv_path: Path) -> set[str]:
    """Load existing Word column values from TSV."""
    existing = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Word\t"):
                    continue
                word = line.split("\t")[0]
                existing.add(word)
    return existing


def main():
    parser = argparse.ArgumentParser(description="Ingest Lepontic from Lexicon Leponticum")
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
        for fname in ("lexlep_words.json", "lexlep_wikitext.json"):
            cache_path = CACHE_DIR / fname
            if cache_path.exists():
                cache_path.unlink()
                logger.info("Cleared cache: %s", cache_path)

    # Fetch all word data
    entries = fetch_all_word_data()
    logger.info("Total entries fetched: %d", len(entries))

    # Filter to valid Lepontic/Celtic entries
    valid_entries = []
    skipped_lang = 0
    skipped_form = 0
    for e in entries:
        word = e["word"]
        if not is_valid_word(word, e["language"], e["type_word"]):
            if e["language"] and e["language"].lower() not in (
                "celtic", "lepontic", "cisalpine celtic", "cisalpine gaulish", ""
            ):
                skipped_lang += 1
            else:
                skipped_form += 1
            continue
        valid_entries.append(e)

    logger.info("Valid entries: %d (skipped: %d non-Celtic, %d invalid forms)",
                len(valid_entries), skipped_lang, skipped_form)

    # Check existing TSV
    tsv_path = LEXICON_DIR / f"{ISO}.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Existing entries in %s: %d", tsv_path.name, len(existing))

    # Process entries
    new_entries = []
    audit_trail = []
    skipped_dup = 0
    no_phonemic = 0

    for e in sorted(valid_entries, key=lambda x: x["word"]):
        word = e["word"]
        if word in existing:
            skipped_dup += 1
            continue

        # Get IPA: prefer the phonemic analysis from LexLep, fall back to transliteration
        phonemic = e["phonemic"]
        if phonemic:
            # The phonemic field uses LexLep's own notation; transliterate it
            ipa = transliterate(phonemic, ISO)
        else:
            # Fall back to transliterating the word form directly
            ipa = transliterate(word, ISO)
            no_phonemic += 1

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
        existing.add(word)

        audit_trail.append({
            "word": word,
            "phonemic_lexlep": e["phonemic"],
            "ipa": ipa,
            "language": e["language"],
            "type_word": e["type_word"],
            "meaning": e["meaning"],
            "semantic": e["semantic"],
            "source": "lexlep",
        })

    logger.info("New entries: %d (skipped: %d duplicates, %d without phonemic)",
                len(new_entries), skipped_dup, no_phonemic)

    # Report
    mode = "DRY RUN: " if args.dry_run else ""
    print(f"\n{mode}Lexicon Leponticum Ingestion (xlp):")
    print("=" * 60)
    print(f"  Source:        https://lexlep.univie.ac.at/")
    print(f"  Method:        MediaWiki API (Category:Word)")
    print(f"  Total fetched: {len(entries)}")
    print(f"  Valid Celtic:  {len(valid_entries)}")
    print(f"  New entries:   {len(new_entries)}")
    print(f"  Existing:      {len(existing) - len(new_entries)}")

    # Sample entries
    if new_entries:
        print(f"\n  Sample entries:")
        for e in new_entries[:10]:
            print(f"    {e['word']:25s} -> {e['ipa']}")

    print("=" * 60)

    if args.dry_run:
        return

    # Write TSV
    if new_entries:
        LEXICON_DIR.mkdir(parents=True, exist_ok=True)
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(f"{e['word']}\t{e['ipa']}\t{e['sca']}\tlexlep\t-\t-\n")

        logger.info("Wrote %d entries to %s", len(new_entries), tsv_path)

    # Save audit trail
    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"lexlep_ingest_{ISO}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        logger.info("Wrote audit trail: %s", audit_path)


if __name__ == "__main__":
    main()
