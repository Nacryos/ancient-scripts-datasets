#!/usr/bin/env python3
"""Ingest Ancient South Arabian word data from Wiktionary.

Primary source: DASI (dasi.cnr.it) — blocked by Anubis bot protection.
Fallback source: Wiktionary categories:
  - Old_South_Arabian_lemmas (128 entries, lang code sem-srb)
  - Sabaean_lemmas (32 entries, lang code xsa)

Strategy:
  1. Fetch all page titles from Wiktionary categories
  2. Batch-fetch page wikitext
  3. Extract transliteration from {{head|...|tr=...}} templates
  4. For pages without tr=, convert Musnad script to Latin transliteration
     using the standard Unicode character names
  5. Apply ASA transliteration map for IPA
  6. Write xsa.tsv

Iron Rule: All words come from HTTP API responses. No hardcoded word lists.

Usage:
    python scripts/ingest_dasi.py [--dry-run]
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
import urllib.request
from pathlib import Path
from urllib.parse import urlencode

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
RAW_DIR = ROOT / "data" / "training" / "raw" / "dasi"

API_URL = "https://en.wiktionary.org/w/api.php"
USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets; academic research)"

# ---- Wiktionary categories to fetch ----
# Old South Arabian (sem-srb) is the umbrella; Sabaean (xsa) is a sub-variety.
# Both are pooled into xsa.tsv since the script/phonology is identical.
CATEGORIES = [
    ("Old_South_Arabian_lemmas", "sem-srb"),
    ("Sabaean_lemmas", "xsa"),
    # Non-lemma forms for additional coverage
    ("Old_South_Arabian_nouns", "sem-srb"),
    ("Old_South_Arabian_verbs", "sem-srb"),
    ("Sabaean_nouns", "xsa"),
    ("Sabaean_verbs", "xsa"),
]

# ---------------------------------------------------------------------------
# Musnad (Old South Arabian) script to Latin transliteration
# Based on Unicode character names (standard Semitist letter names)
# This maps from the Musnad Unicode block (U+10A60-U+10A7C) to the standard
# scholarly transliteration used in ASA studies.
# ---------------------------------------------------------------------------
MUSNAD_TO_LATIN: dict[str, str] = {
    "\U00010A60": "h",    # HE
    "\U00010A61": "l",    # LAMEDH
    "\U00010A62": "\u1e25",  # HETH -> ḥ
    "\U00010A63": "m",    # MEM
    "\U00010A64": "q",    # QOPH
    "\U00010A65": "w",    # WAW
    "\U00010A66": "s\u00b2",  # SHIN -> s² (lateral fricative in ASA)
    "\U00010A67": "r",    # RESH
    "\U00010A68": "b",    # BETH
    "\U00010A69": "t",    # TAW
    "\U00010A6A": "s\u00b9",  # SAT -> s¹ (standard s in ASA)
    "\U00010A6B": "k",    # KAPH
    "\U00010A6C": "n",    # NUN
    "\U00010A6D": "\u1e2b",  # KHETH -> ḫ
    "\U00010A6E": "\u1e63",  # SADHE -> ṣ
    "\U00010A6F": "s",    # SAMEKH -> s (= s¹ in many systems)
    "\U00010A70": "f",    # FE
    "\U00010A71": "\u02be",  # ALEF -> ʾ (glottal stop)
    "\U00010A72": "\u02bf",  # AYN -> ʿ (pharyngeal)
    "\U00010A73": "\u1e93",  # DHADHE -> ẓ (emphatic interdental)
    "\U00010A74": "g",    # GIMEL
    "\U00010A75": "d",    # DALETH
    "\U00010A76": "\u0121",  # GHAYN -> ġ
    "\U00010A77": "\u1e6d",  # TETH -> ṭ (emphatic t)
    "\U00010A78": "z",    # ZAYN
    "\U00010A79": "\u1e0f",  # DHALETH -> ḏ (interdental d)
    "\U00010A7A": "y",    # YODH
    "\U00010A7B": "\u1e6f",  # THAW -> ṯ (interdental t)
    "\U00010A7C": "s\u00b3",  # THETH -> s³ (ts affricate in ASA)
}


def musnad_to_latin(text: str) -> str:
    """Convert Musnad script text to Latin scholarly transliteration.

    Characters not in the Musnad block are passed through.
    Word separators (|) in Musnad are converted to spaces.
    """
    result = []
    for ch in text:
        if ch in MUSNAD_TO_LATIN:
            result.append(MUSNAD_TO_LATIN[ch])
        elif ch == "\U00010A7F":  # NUMERIC INDICATOR — skip
            continue
        elif ch == "\U00010A7D" or ch == "\U00010A7E":  # numbers — skip
            continue
        elif ch == "|" or ch == "\u00b7":  # word separator
            result.append(" ")
        else:
            result.append(ch)
    return "".join(result)


def _api_request(params: dict, retries: int = 3) -> dict:
    """Make a Wiktionary API request with retries."""
    url = f"{API_URL}?" + urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < retries - 1:
                wait = 2 * (attempt + 1)
                logger.warning("Retry %d/%d after error: %s", attempt + 1, retries, exc)
                time.sleep(wait)
            else:
                logger.error("FAILED after %d retries: %s", retries, exc)
                raise
    return {}  # unreachable


def fetch_category_members(category: str, namespace: int = 0) -> list[str]:
    """Fetch all page titles from a Wiktionary category."""
    members = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": f"Category:{category}",
        "cmtype": "page",
        "cmnamespace": str(namespace),
        "cmlimit": "500",
        "format": "json",
    }

    page_num = 0
    while True:
        page_num += 1
        data = _api_request(params)

        for m in data.get("query", {}).get("categorymembers", []):
            members.append(m["title"])

        cont = data.get("continue", {})
        if "cmcontinue" in cont:
            params["cmcontinue"] = cont["cmcontinue"]
            logger.info("  Page %d: %d members so far...", page_num, len(members))
            time.sleep(0.5)
        else:
            break

    return members


def fetch_page_contents(titles: list[str]) -> dict[str, str]:
    """Batch-fetch wikitext for multiple pages (max 50 per request)."""
    all_contents: dict[str, str] = {}

    for i in range(0, len(titles), 50):
        batch = titles[i:i + 50]
        params = {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "main",
            "format": "json",
        }
        data = _api_request(params)
        pages = data.get("query", {}).get("pages", {})
        for pid, page in pages.items():
            title = page.get("title", "")
            revs = page.get("revisions", [])
            if revs:
                content = revs[0].get("slots", {}).get("main", {}).get("*", "")
                all_contents[title] = content

        if i + 50 < len(titles):
            time.sleep(1.0)  # Be respectful

    return all_contents


def extract_entry_from_wikitext(title: str, wikitext: str) -> list[dict]:
    """Extract transliteration, POS, and gloss from wikitext.

    Returns a list of entry dicts with keys: word, translit, pos, gloss, lang_code.
    A single page can have multiple language sections.
    """
    entries = []

    # Extract all {{head|LANG|POS|tr=TRANSLIT}} patterns
    head_pattern = re.compile(
        r"\{\{head\|([^|}]+)\|([^|}]+)"  # lang, pos
        r"(?:\|[^|}]*)*?"  # optional params
        r"(?:\|tr=([^|}]+))?"  # optional tr=
        r"[^}]*\}\}",
        re.DOTALL,
    )

    # Extract glosses: lines starting with # (not ## which are sub-glosses)
    gloss_pattern = re.compile(r"^#\s+(.+)", re.MULTILINE)

    for match in head_pattern.finditer(wikitext):
        lang_code = match.group(1).strip()
        pos = match.group(2).strip()
        tr = match.group(3).strip() if match.group(3) else None

        # Only accept ASA-related language codes
        if lang_code not in ("sem-srb", "xsa", "inm", "xqt", "xhd"):
            continue

        # Find the gloss nearest after this head template
        after_pos = match.end()
        gloss_match = gloss_pattern.search(wikitext[after_pos:after_pos + 500])
        gloss = ""
        if gloss_match:
            gloss = gloss_match.group(1).strip()
            # Clean wiki markup from gloss
            gloss = re.sub(r"\[\[([^\]|]*\|)?([^\]]*)\]\]", r"\2", gloss)
            gloss = re.sub(r"\{\{[^}]*\}\}", "", gloss)
            gloss = gloss.strip(" ,;.")

        # Determine transliteration
        if tr:
            translit = tr.strip()
        else:
            # Convert Musnad script title to Latin
            translit = musnad_to_latin(title)

        entries.append({
            "musnad": title,
            "word": translit,
            "pos": pos,
            "gloss": gloss,
            "lang_code": lang_code,
        })

    return entries


def is_valid_entry(entry: dict) -> bool:
    """Validate an entry for inclusion."""
    word = entry.get("word", "")
    if not word or len(word) > 60:
        return False
    # Skip single-character entries (just letters)
    if len(word) <= 1:
        return False
    # Skip if the word is entirely digits or punctuation
    if all(c.isdigit() or c in ".-_/|" for c in word):
        return False
    # Skip entries that are just letter names
    pos = entry.get("pos", "")
    if pos == "letter":
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Ancient South Arabian from Wiktionary (DASI fallback)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be extracted without writing files")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Ensure directories exist
    LEXICON_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Step 1: Collect all unique titles from all categories ----
    logger.info("=" * 60)
    logger.info("Ancient South Arabian ingestion from Wiktionary")
    logger.info("=" * 60)

    all_titles: set[str] = set()
    category_counts: dict[str, int] = {}

    for cat_name, lang_code in CATEGORIES:
        logger.info("Fetching category: %s (%s)", cat_name, lang_code)
        titles = fetch_category_members(cat_name)
        new_count = len(titles - all_titles) if isinstance(titles, set) else len(set(titles) - all_titles)
        all_titles.update(titles)
        category_counts[cat_name] = len(titles)
        logger.info("  %s: %d titles (%d new)", cat_name, len(titles), new_count)
        time.sleep(1.0)

    logger.info("Total unique titles: %d", len(all_titles))

    # ---- Step 2: Fetch page contents ----
    title_list = sorted(all_titles)
    logger.info("Fetching page contents for %d titles...", len(title_list))
    page_contents = fetch_page_contents(title_list)
    logger.info("Got content for %d pages", len(page_contents))

    # Save raw content for audit trail
    raw_path = RAW_DIR / "wiktionary_pages.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(page_contents, f, ensure_ascii=False, indent=2)
    logger.info("Saved raw content to %s", raw_path)

    # ---- Step 3: Extract entries ----
    all_entries: list[dict] = []
    seen_words: set[str] = set()
    lang_code_counts: dict[str, int] = {}
    pos_counts: dict[str, int] = {}
    tr_from_template = 0
    tr_from_musnad = 0
    skipped = 0

    for title, wikitext in page_contents.items():
        entries = extract_entry_from_wikitext(title, wikitext)
        if not entries:
            skipped += 1
            continue

        for entry in entries:
            if not is_valid_entry(entry):
                skipped += 1
                continue

            word = entry["word"]
            # Normalize
            word = unicodedata.normalize("NFC", word.strip())

            # Deduplicate by word form
            if word in seen_words:
                continue
            seen_words.add(word)

            # Track source of transliteration
            # If the entry came from a {{head}} with tr=, it was from template
            # Check if the Musnad title and word differ (meaning tr= was used)
            musnad_derived = musnad_to_latin(entry["musnad"])
            if word != musnad_derived:
                tr_from_template += 1
            else:
                tr_from_musnad += 1

            # Apply transliteration map for IPA
            try:
                ipa = transliterate(word, "xsa")
            except Exception:
                ipa = word

            if not ipa:
                ipa = word

            # SCA
            try:
                sca = ipa_to_sound_class(ipa)
            except Exception:
                sca = ""

            # Determine source label
            lc = entry["lang_code"]
            lang_code_counts[lc] = lang_code_counts.get(lc, 0) + 1
            pos = entry["pos"]
            pos_counts[pos] = pos_counts.get(pos, 0) + 1

            source_label = f"wiktionary:{lc}"

            all_entries.append({
                "word": word,
                "ipa": ipa,
                "sca": sca,
                "source": source_label,
                "musnad": entry["musnad"],
                "gloss": entry.get("gloss", ""),
                "pos": pos,
            })

    # Sort by word for reproducibility
    all_entries.sort(key=lambda e: e["word"])

    # ---- Step 4: Report ----
    logger.info("")
    logger.info("=" * 60)
    logger.info("EXTRACTION RESULTS")
    logger.info("=" * 60)
    logger.info("Total entries:       %d", len(all_entries))
    logger.info("Skipped:             %d", skipped)
    logger.info("Transliteration from {{head}} tr=: %d", tr_from_template)
    logger.info("Transliteration from Musnad decode: %d", tr_from_musnad)
    logger.info("")
    logger.info("By language code:")
    for lc, count in sorted(lang_code_counts.items(), key=lambda x: -x[1]):
        name = {
            "sem-srb": "Old South Arabian (general)",
            "xsa": "Sabaean",
            "inm": "Minaic",
            "xqt": "Qatabanic",
            "xhd": "Hadramitic",
        }.get(lc, lc)
        logger.info("  %-35s %d", name, count)
    logger.info("")
    logger.info("By POS:")
    for pos, count in sorted(pos_counts.items(), key=lambda x: -x[1]):
        logger.info("  %-20s %d", pos, count)

    # Identity rate
    identity_count = sum(1 for e in all_entries if e["word"] == e["ipa"])
    identity_pct = (identity_count / len(all_entries) * 100) if all_entries else 0
    logger.info("")
    logger.info("Identity rate (word == IPA): %d/%d (%.1f%%)",
                identity_count, len(all_entries), identity_pct)

    # Sample entries
    logger.info("")
    logger.info("Sample entries:")
    logger.info("  %-20s %-25s %-10s %s", "Word", "IPA", "SCA", "Gloss")
    logger.info("  " + "-" * 70)
    for e in all_entries[:20]:
        logger.info("  %-20s %-25s %-10s %s",
                     e["word"], e["ipa"], e["sca"], e.get("gloss", "")[:30])

    # ---- Step 5: Write output ----
    if args.dry_run:
        logger.info("")
        logger.info("DRY RUN — not writing files")
        return

    # Write TSV
    tsv_path = LEXICON_DIR / "xsa.tsv"
    with open(tsv_path, "w", encoding="utf-8") as f:
        f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")
        for e in all_entries:
            f.write(
                f"{e['word']}\t{e['ipa']}\t{e['sca']}\t"
                f"{e['source']}\t-\t-\n"
            )
    logger.info("Wrote %d entries to %s", len(all_entries), tsv_path)

    # Write audit trail
    audit_path = AUDIT_TRAIL_DIR / "xsa_audit.json"
    audit_data = {
        "source": "Wiktionary (DASI blocked by Anubis)",
        "categories": [c[0] for c in CATEGORIES],
        "date": time.strftime("%Y-%m-%d"),
        "total_entries": len(all_entries),
        "lang_code_counts": lang_code_counts,
        "pos_counts": pos_counts,
        "identity_rate": round(identity_pct, 1),
        "tr_from_template": tr_from_template,
        "tr_from_musnad": tr_from_musnad,
        "entries": [
            {
                "word": e["word"],
                "musnad": e["musnad"],
                "ipa": e["ipa"],
                "gloss": e.get("gloss", ""),
                "pos": e["pos"],
                "source": e["source"],
            }
            for e in all_entries
        ],
    }
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit_data, f, ensure_ascii=False, indent=2)
    logger.info("Wrote audit trail to %s", audit_path)


if __name__ == "__main__":
    main()
