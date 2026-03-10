#!/usr/bin/env python3
"""Parser for avesta.org Avestan dictionary page.

Extracts Avestan words with their transliterations and English glosses
from the online dictionary at https://www.avesta.org/avdict/avdict.htm

The dictionary page is a large HTML file with entries formatted as:
    <b>word</b> (grammar info) gloss text

Uses only stdlib (urllib, html.parser, re).
"""

from __future__ import annotations

import logging
import re
import urllib.request
import urllib.error
from html.parser import HTMLParser
from typing import Any

logger = logging.getLogger(__name__)


class AvestaParser(HTMLParser):
    """Parse the avesta.org dictionary HTML to extract entries.

    The dictionary uses a fairly simple format:
    - Bold (<b>) tags mark headwords
    - Following text contains grammatical info and gloss
    - Entries are separated by <br> or <p> tags
    """

    def __init__(self) -> None:
        super().__init__()
        self.in_bold = False
        self.bold_text = ""
        self.after_bold_text = ""
        self.collecting_gloss = False
        self.entries: list[dict] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("b", "strong"):
            # Save any previous entry
            self._flush_entry()
            self.in_bold = True
            self.bold_text = ""
        elif tag in ("br", "p", "hr") and self.collecting_gloss:
            self._flush_entry()

    def handle_endtag(self, tag: str) -> None:
        if tag in ("b", "strong") and self.in_bold:
            self.in_bold = False
            self.collecting_gloss = True
            self.after_bold_text = ""

    def handle_data(self, data: str) -> None:
        if self.in_bold:
            self.bold_text += data
        elif self.collecting_gloss:
            self.after_bold_text += data

    def _flush_entry(self) -> None:
        """Process the accumulated bold text + gloss text into an entry."""
        if not self.bold_text.strip():
            self.collecting_gloss = False
            return

        word = self.bold_text.strip()
        gloss_raw = self.after_bold_text.strip()

        # Clean the word: remove trailing punctuation, numbers
        word = re.sub(r"[.,;:]+$", "", word).strip()
        if not word or len(word) > 80:
            self.collecting_gloss = False
            return

        # Skip non-word entries (section headers, references, etc.)
        if word.isupper() and len(word) > 3:
            self.collecting_gloss = False
            return

        # Extract gloss from the text after the bold word
        # Strip grammatical info often in parentheses at the start
        gloss = gloss_raw
        # Remove leading grammar markers: (adj.), (n.m.), (vb.), etc.
        gloss = re.sub(r"^\s*\([^)]{0,30}\)\s*", "", gloss)
        # Remove leading dashes
        gloss = re.sub(r"^[-–—\s]+", "", gloss)
        # Take first sentence/clause as gloss
        gloss = re.split(r"[.;]", gloss)[0].strip()
        # Remove parenthetical references
        gloss = re.sub(r"\([^)]*\)", "", gloss).strip()

        if gloss and len(gloss) < 200:
            self.entries.append({
                "word": word,
                "transliteration": word,  # Avestan words are already transliterated
                "gloss": gloss,
            })

        self.collecting_gloss = False


def _fallback_regex_parse(html: str) -> list[dict]:
    """Fallback regex-based parsing for the Avestan dictionary."""
    entries: list[dict] = []

    # Pattern: <b>word</b> optional-grammar gloss
    pattern = re.compile(
        r"<b>([^<]{1,60})</b>"
        r"\s*(?:\([^)]{0,30}\))?\s*"
        r"([\w][\w\s,'-]{3,120}?)(?=[.<]|\n\n)",
        re.IGNORECASE,
    )

    for m in pattern.finditer(html):
        word = m.group(1).strip()
        gloss = m.group(2).strip()
        # Remove trailing whitespace and punctuation
        gloss = re.sub(r"[,;:\s]+$", "", gloss)
        if word and gloss and not word.isupper():
            entries.append({
                "word": word,
                "transliteration": word,
                "gloss": gloss,
            })

    return entries


def parse(url: str, **kwargs: Any) -> list[dict]:
    """Download and parse the avesta.org dictionary page.

    Args:
        url: URL to the Avestan dictionary, typically:
             https://www.avesta.org/avdict/avdict.htm

    Returns:
        List of dicts with keys: word, transliteration, gloss.
        Returns empty list if URL is unreachable.
    """
    logger.info("Avesta: downloading %s", url)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PhaiPhon/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.warning("Avesta: failed to download %s: %s", url, exc)
        return []

    # Try structured parsing first
    parser = AvestaParser()
    parser.feed(html)
    parser._flush_entry()  # Flush any trailing entry
    entries = parser.entries

    # Fallback
    if not entries:
        logger.info("Avesta: structured parsing found 0 entries, trying regex fallback")
        entries = _fallback_regex_parse(html)

    # Deduplicate by word
    seen: set[str] = set()
    unique: list[dict] = []
    for e in entries:
        if e["word"] not in seen:
            seen.add(e["word"])
            unique.append(e)

    logger.info("Avesta: extracted %d entries from %s", len(unique), url)
    return unique


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "https://www.avesta.org/avdict/avdict.htm"
    )
    results = parse(test_url)
    print(f"\nExtracted {len(results)} entries:")
    for entry in results[:15]:
        print(f"  {entry['word']:30s}  {entry['gloss']}")
    if len(results) > 15:
        print(f"  ... and {len(results) - 15} more")
