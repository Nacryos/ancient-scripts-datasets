#!/usr/bin/env python3
"""Scrape Avestan dictionary from avesta.org.

Source: https://www.avesta.org/avdict/avdict.htm
This is a comprehensive Avestan-English dictionary with 3,000+ headwords.

Extracts headwords + English glosses from the HTML dictionary pages.
Merges with existing ave.tsv (deduplicates by Word column).

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/scrape_avesta.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import unicodedata
import urllib.request
import urllib.error
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402
from transliteration_maps import transliterate  # noqa: E402

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"

USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets)"

# avesta.org dictionary is split across multiple pages (A-Z)
# The main index is at avdict.htm, which links to letter pages
BASE_URL = "https://www.avesta.org/avdict/"
INDEX_URL = f"{BASE_URL}avdict.htm"


class AvestaDictParser(HTMLParser):
    """Parse avesta.org dictionary pages to extract headwords and glosses."""

    def __init__(self) -> None:
        super().__init__()
        self.entries: list[dict] = []
        self.in_bold = False
        self.bold_text = ""
        self.current_text = ""
        self.all_text_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("b", "strong"):
            self.in_bold = True
            self.bold_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag in ("b", "strong") and self.in_bold:
            self.in_bold = False
            if self.bold_text.strip():
                self.all_text_chunks.append(("BOLD", self.bold_text.strip()))

        if tag in ("br", "p", "div"):
            if self.current_text.strip():
                self.all_text_chunks.append(("TEXT", self.current_text.strip()))
            self.current_text = ""

    def handle_data(self, data: str) -> None:
        if self.in_bold:
            self.bold_text += data
        self.current_text += data


def fetch_page(url: str) -> str:
    """Fetch a page with retries."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.error("FAILED to fetch %s: %s", url, exc)
                return ""


def find_subpage_links(html: str) -> list[str]:
    """Extract links to letter-specific dictionary pages."""
    links = re.findall(r'href="(avdict[a-z0-9]*\.htm)"', html, re.IGNORECASE)
    # Also look for other dictionary page patterns
    links += re.findall(r'href="(av5[a-z]+\.htm)"', html, re.IGNORECASE)
    # Deduplicate and sort
    unique = sorted(set(links))
    return [f"{BASE_URL}{link}" for link in unique if link != "avdict.htm"]


def _decode_html_entities(text: str) -> str:
    """Decode HTML entities to Unicode characters."""
    import html as html_mod
    return html_mod.unescape(text)


def extract_entries_from_single_page(html: str) -> list[dict]:
    """Extract dictionary entries from the avesta.org single-page dictionary.

    The dictionary uses <DL COMPACT> definition lists:
      <DT>headword [root]
      <DD>count (grammar) pos. English gloss

    Some entries lack a count (just have gloss text after grammar markers).
    """
    entries: list[dict] = []

    # Strategy 1: Parse <DT>/<DD> pairs directly from HTML
    # Extract all DT/DD pairs
    dt_dd_pattern = re.compile(
        r'<DT>\s*(.*?)\s*(?=<DD>|<DT>|</DL>)'
        r'(?:<DD>\s*(.*?)\s*(?=<DT>|</DL>|<DD>))?',
        re.IGNORECASE | re.DOTALL,
    )

    for m in dt_dd_pattern.finditer(html):
        dt_raw = m.group(1) or ""
        dd_raw = m.group(2) or ""

        # Strip inner HTML tags and decode entities
        dt_text = _decode_html_entities(re.sub(r"<[^>]+>", "", dt_raw)).strip()
        dd_text = _decode_html_entities(re.sub(r"<[^>]+>", "", dd_raw)).strip()

        if not dt_text:
            continue

        # Parse headword: "headword [root]" or just "headword"
        hw_match = re.match(r'^(.+?)(?:\s*\[([^\]]+)\])?\s*$', dt_text)
        if not hw_match:
            continue

        headword = hw_match.group(1).strip()
        root = hw_match.group(2).strip() if hw_match.group(2) else ""

        # Skip section headers, navigation text, and layout explanations
        skip_words = {"NOTE", "Example", "root of word", "... another form of word"}
        if headword.startswith("Avestan dictionary") or headword in skip_words:
            continue
        if "number of times word occurs" in dd_text:
            continue
        # Skip continuation entries (forms like "... aêta")
        if headword.startswith("..."):
            continue

        # Parse gloss from DD text
        # Format: "count (grammar) pos. English gloss (reference)"
        # or: "(grammar) English gloss"
        # or: just "English gloss"
        gloss = dd_text

        # Remove leading count
        gloss = re.sub(r'^\d+\s*', '', gloss)
        # Remove grammatical info in parens at start
        gloss = re.sub(r'^\([^)]*\)\s*', '', gloss)
        # Remove part-of-speech markers
        gloss = re.sub(r'^[mfn]\.\s*', '', gloss)
        # Remove trailing references like (k362) (b55) (Sp A M)
        gloss = re.sub(r'\s*\([^)]*\)\s*$', '', gloss)
        # Remove inner references
        gloss = re.sub(r'\s*\([a-zA-Z]\d+\)', '', gloss)
        # Clean whitespace
        gloss = re.sub(r'\s+', ' ', gloss).strip()
        # Take first meaningful part
        if '. ' in gloss:
            gloss = gloss.split('. ')[0]
        if len(gloss) > 80:
            gloss = gloss[:80].rsplit(' ', 1)[0]
        # Remove trailing punctuation
        gloss = re.sub(r'[,;:\s]+$', '', gloss)

        # Use root form as primary word (it's the citation form)
        word = root if root and root != "-" else headword

        # Validate
        if not word or not gloss or len(word) < 1 or len(word) > 40:
            continue
        if len(gloss) < 2:
            continue
        # Skip if gloss is just a grammar marker or number
        if re.match(r'^[\d()\s]+$', gloss):
            continue

        entries.append({"word": word, "gloss": gloss, "headword": headword})

    # Deduplicate by word
    seen: set[str] = set()
    unique: list[dict] = []
    for e in entries:
        if e["word"] not in seen:
            seen.add(e["word"])
            unique.append(e)

    return unique


def extract_entries_from_page(html: str) -> list[dict]:
    """Extract dictionary entries from an avesta.org dictionary page.

    Entries typically follow pattern:
        <b>headword</b> (romanization) - English gloss
    or:
        <b>headword</b> gloss text follows
    """
    entries: list[dict] = []

    # Strategy 1: regex for bold headword followed by gloss
    # Pattern: <b>avestan_word</b> optional_parens gloss_text
    pattern = re.compile(
        r'<b>([^<]{1,60})</b>'     # Bold headword
        r'\s*'
        r'(?:\([^)]*\)\s*)?'       # Optional parenthetical (romanization)
        r'[-–—:,]?\s*'             # Optional delimiter
        r'([A-Za-z][A-Za-z\s,;/\'-]{2,100}?)'  # English gloss
        r'(?=[.<;]|\n|$)',          # Ends at period, semicolon, newline
        re.DOTALL,
    )

    for m in pattern.finditer(html):
        headword = m.group(1).strip()
        gloss = m.group(2).strip()

        # Clean HTML entities
        headword = headword.replace("&nbsp;", " ")
        gloss = gloss.replace("&nbsp;", " ")

        # Strip HTML tags from headword
        headword = re.sub(r"<[^>]+>", "", headword)
        gloss = re.sub(r"<[^>]+>", "", gloss)

        # Clean up
        gloss = re.sub(r"\s+", " ", gloss).strip()
        gloss = re.sub(r"[,;:\s]+$", "", gloss)

        # Validate
        if not headword or not gloss:
            continue
        if len(headword) < 1 or len(headword) > 50:
            continue
        if len(gloss) < 2 or len(gloss) > 100:
            continue
        # Skip if headword looks like English
        if headword.isascii() and headword[0].isupper() and len(headword) > 5:
            continue

        entries.append({
            "word": headword,
            "gloss": gloss,
        })

    # Strategy 2: Parse bold text chunks
    parser = AvestaDictParser()
    parser.feed(html)

    # Process chunks: BOLD items are headwords, following TEXT items are definitions
    for i, (chunk_type, chunk_text) in enumerate(parser.all_text_chunks):
        if chunk_type != "BOLD":
            continue

        headword = chunk_text.strip()
        if not headword or len(headword) > 50:
            continue

        # Look for next TEXT chunk as gloss
        gloss = ""
        for j in range(i + 1, min(i + 3, len(parser.all_text_chunks))):
            ct, ct_text = parser.all_text_chunks[j]
            if ct == "BOLD":
                break
            if ct == "TEXT":
                # Extract English portion
                eng = re.search(r"([A-Za-z][A-Za-z\s,;/'-]{2,80})", ct_text)
                if eng:
                    gloss = eng.group(1).strip()
                    break

        if headword and gloss:
            entries.append({
                "word": headword,
                "gloss": gloss,
            })

    return entries


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
    parser = argparse.ArgumentParser(description="Scrape avesta.org dictionary")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    tsv_path = LEXICON_DIR / "ave.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Loaded %d existing Avestan entries", len(existing))

    # Fetch index page
    logger.info("Fetching avesta.org dictionary index...")
    index_html = fetch_page(INDEX_URL)
    if not index_html:
        logger.error("Failed to fetch index page")
        sys.exit(1)

    # Save raw HTML for audit trail
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = RAW_DIR / "avesta_org_index.html"
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(index_html)

    # The dictionary is a single large page — extract entries directly
    all_entries = extract_entries_from_single_page(index_html)
    logger.info("Single-page extraction: %d entries", len(all_entries))

    # Also try the multi-strategy parser as fallback
    if len(all_entries) < 100:
        logger.info("Trying multi-strategy parser as supplement...")
        extra = extract_entries_from_page(index_html)
        existing_words = {e["word"] for e in all_entries}
        for e in extra:
            if e["word"] not in existing_words:
                all_entries.append(e)
                existing_words.add(e["word"])
        logger.info("After supplement: %d total entries", len(all_entries))

    # Check for subpages (unlikely, but just in case)
    subpages = find_subpage_links(index_html)
    if subpages:
        logger.info("Found %d subpages", len(subpages))
        for i, url in enumerate(subpages):
            logger.info("Fetching subpage %d/%d: %s", i + 1, len(subpages), url)
            html = fetch_page(url)
            if html:
                page_name = url.split("/")[-1]
                with open(RAW_DIR / f"avesta_org_{page_name}", "w",
                          encoding="utf-8") as f:
                    f.write(html)
                entries = extract_entries_from_single_page(html)
                existing_words = {e["word"] for e in all_entries}
                for e in entries:
                    if e["word"] not in existing_words:
                        all_entries.append(e)
                        existing_words.add(e["word"])
                logger.info("  Extracted %d entries", len(entries))
            time.sleep(2)

    # Deduplicate
    seen: set[str] = set()
    unique_entries: list[dict] = []
    for e in all_entries:
        word = e["word"]
        if word not in seen and word not in existing:
            seen.add(word)
            unique_entries.append(e)

    logger.info("Total new unique entries: %d", len(unique_entries))

    if args.dry_run:
        logger.info("DRY RUN — not writing")
        for e in unique_entries[:20]:
            print(f"  {e['word']:30s} {e['gloss']}")
        return

    # Process and append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    with open(tsv_path, "a", encoding="utf-8") as f:
        for e in unique_entries:
            word = e["word"]
            gloss = e["gloss"]

            try:
                ipa = transliterate(word, "ave")
            except Exception:
                ipa = word

            try:
                sca = ipa_to_sound_class(ipa)
            except Exception:
                sca = ""

            concept = gloss.split(",")[0].split(";")[0].strip()
            concept_id = concept.replace(" ", "_").lower()[:50]

            f.write(f"{word}\t{ipa}\t{sca}\tavesta_org\t{concept_id}\t-\n")
            new_count += 1

            audit_trail.append({
                "word": word,
                "gloss": gloss,
                "ipa": ipa,
                "source": "avesta.org",
            })

    # Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / "avesta_org_ave.jsonl", "w", encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    logger.info("Appended %d new entries to %s", new_count, tsv_path)

    # Summary
    total = len(existing) + new_count
    print(f"\nAvestan (ave): {len(existing)} existing + {new_count} new = {total} total")


if __name__ == "__main__":
    main()
