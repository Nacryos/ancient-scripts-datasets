#!/usr/bin/env python3
"""Parser for ASJP (Automated Similarity Judgment Program) language pages.

ASJP provides Swadesh-list transcriptions in a custom phonetic encoding
(not IPA). The encoding maps special digits/symbols to IPA-like sounds:
    3 -> ʃ, 5 -> ts, 7 -> j, 8 -> ɬ, etc.

Downloads and parses an ASJP language page to extract the ~40-item
Swadesh-list entries with their ASJP transcriptions and glosses.

Reference: https://asjp.clld.org/
"""

from __future__ import annotations

import logging
import re
import urllib.request
import urllib.error
from html.parser import HTMLParser
from typing import Any

logger = logging.getLogger(__name__)

# ASJP transcription -> approximate IPA mapping
ASJP_TO_IPA: dict[str, str] = {
    "p": "p", "b": "b", "m": "m", "f": "f", "v": "v", "w": "w",
    "t": "t", "d": "d", "n": "n", "s": "s", "z": "z", "l": "l",
    "r": "r", "S": "ʃ", "Z": "ʒ", "c": "tʃ", "j": "dʒ",
    "T": "θ", "D": "ð", "k": "k", "g": "g", "x": "x", "N": "ŋ",
    "q": "q", "G": "ɢ", "X": "χ", "h": "h", "H": "ʔ",
    "y": "j", "i": "i", "e": "e", "E": "ɛ", "a": "a",
    "u": "u", "o": "o", "!": "ǃ",
    "3": "ʃ", "5": "ts", "7": "j", "8": "ɬ",
    "4": "ʃ",  # alternate ASJP
    "L": "ɬ",
    "C": "tʃ",
}


class ASJPTableParser(HTMLParser):
    """Parse the ASJP HTML page to extract the Swadesh-list table."""

    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []
        self.cell_text = ""
        self.table_count = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self.table_count += 1
            self.in_table = True
        elif tag == "tr" and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ("td", "th") and self.in_row:
            self.in_cell = True
            self.cell_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "table":
            self.in_table = False
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if self.current_row:
                self.rows.append(self.current_row)
        elif tag in ("td", "th") and self.in_cell:
            self.in_cell = False
            self.current_row.append(self.cell_text.strip())

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell_text += data


def _convert_asjp_to_ipa(asjp_form: str) -> str:
    """Convert an ASJP transcription string to approximate IPA."""
    result = []
    for ch in asjp_form:
        if ch in ASJP_TO_IPA:
            result.append(ASJP_TO_IPA[ch])
        elif ch in ("~", "%", "*", "$", '"', "'", "-", " "):
            # Modifiers and separators -- skip
            continue
        else:
            result.append(ch)
    return "".join(result)


def parse(url: str, **kwargs: Any) -> list[dict]:
    """Download and parse an ASJP language page.

    Args:
        url: Full URL to an ASJP language page, e.g.
             https://asjp.clld.org/languages/HITTITE

    Returns:
        List of dicts with keys: word, transliteration, gloss.
        Returns empty list if URL is unreachable.
    """
    logger.info("ASJP: downloading %s", url)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PhaiPhon/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.warning("ASJP: failed to download %s: %s", url, exc)
        return []

    # Strategy 1: parse HTML tables
    parser = ASJPTableParser()
    parser.feed(html)

    entries: list[dict] = []

    # Look for rows that contain Swadesh-list items
    # ASJP tables typically have columns: [meaning_id, meaning, word_form, ...]
    for row in parser.rows:
        if len(row) < 2:
            continue
        # Skip header rows
        if any(h.lower() in ("meaning", "word", "#", "id") for h in row[:2]):
            continue

        # Try to identify gloss and form columns
        gloss = ""
        form = ""
        if len(row) >= 3:
            # Format: [id, gloss, form, ...]
            gloss = row[1].strip()
            form = row[2].strip()
        elif len(row) == 2:
            gloss = row[0].strip()
            form = row[1].strip()

        if not form or not gloss:
            continue

        # Clean up the ASJP form: remove list-number prefixes
        form = re.sub(r"^\d+\.\s*", "", form)
        # Handle multiple forms separated by comma
        forms = [f.strip() for f in form.split(",") if f.strip()]

        for f in forms:
            entries.append({
                "word": f,
                "transliteration": f,
                "gloss": gloss,
            })

    # Strategy 2: fallback regex parsing if table parsing found nothing
    if not entries:
        # Look for patterns like: 1. I  p~a~r~a
        # or tabular text with meanings and forms
        pattern = re.compile(
            r"(\d+)\.\s+([A-Za-z/()]+(?:\s+[A-Za-z/()]+)*)\s+"
            r"([A-Za-z0-9~%*$\"\'!3578HNSZCGLX]+)",
            re.MULTILINE,
        )
        for m in pattern.finditer(html):
            gloss = m.group(2).strip()
            form = m.group(3).strip()
            if gloss and form:
                entries.append({
                    "word": form,
                    "transliteration": form,
                    "gloss": gloss,
                })

    logger.info("ASJP: extracted %d entries from %s", len(entries), url)
    return entries


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_url = sys.argv[1] if len(sys.argv) > 1 else "https://asjp.clld.org/languages/HITTITE"
    results = parse(test_url)
    print(f"\nExtracted {len(results)} entries:")
    for entry in results[:10]:
        print(f"  {entry['gloss']:20s}  {entry['word']}")
    if len(results) > 10:
        print(f"  ... and {len(results) - 10} more")
