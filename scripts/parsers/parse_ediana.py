#!/usr/bin/env python3
"""Parser for eDiAna (electronic Dictionary of the Ancient Near East) pages.

eDiAna is hosted at LMU Munich and contains dictionary entries for
Anatolian languages including Lycian, Lydian, and Carian.

URL: https://www.ediana.gwi.uni-muenchen.de/

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


class EDiAnaParser(HTMLParser):
    """Parse eDiAna HTML pages to extract dictionary entries.

    eDiAna pages typically present entries in structured HTML with
    lemma headwords, transliterations, and glosses in various formats:
    definition lists, tables, or div-based layouts.
    """

    def __init__(self) -> None:
        super().__init__()
        self.entries: list[dict] = []
        self.in_entry = False
        self.in_lemma = False
        self.in_gloss = False
        self.in_translit = False
        self.current_text = ""
        self.current_entry: dict[str, str] = {}
        # Track div/span classes
        self._tag_stack: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = dict(attrs)
        cls = attr_dict.get("class", "") or ""

        self._tag_stack.append(tag)

        # Detect entry containers by class name patterns
        if any(kw in cls.lower() for kw in ("entry", "lemma", "dictentry", "result")):
            self.in_entry = True
            self.current_entry = {}

        # Detect lemma / headword
        if any(kw in cls.lower() for kw in ("lemma", "headword", "hw", "orth")):
            self.in_lemma = True
            self.current_text = ""

        # Detect transliteration
        if any(kw in cls.lower() for kw in ("translit", "transliteration", "form")):
            self.in_translit = True
            self.current_text = ""

        # Detect gloss / translation
        if any(kw in cls.lower() for kw in ("gloss", "meaning", "translation", "def", "sense")):
            self.in_gloss = True
            self.current_text = ""

        # Bold within an entry might be the headword
        if tag in ("b", "strong") and self.in_entry and not self.current_entry.get("word"):
            self.in_lemma = True
            self.current_text = ""

    def handle_endtag(self, tag: str) -> None:
        if self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()

        if self.in_lemma and tag in ("span", "div", "b", "strong", "a", "td", "dt"):
            self.in_lemma = False
            text = self.current_text.strip()
            if text:
                self.current_entry["word"] = text

        if self.in_translit and tag in ("span", "div", "td", "i", "em"):
            self.in_translit = False
            text = self.current_text.strip()
            if text:
                self.current_entry["transliteration"] = text

        if self.in_gloss and tag in ("span", "div", "dd", "td", "p"):
            self.in_gloss = False
            text = self.current_text.strip()
            if text:
                self.current_entry["gloss"] = text

        # End of entry
        if self.in_entry and tag in ("div", "tr", "li", "article"):
            if self.current_entry.get("word"):
                word = self.current_entry["word"]
                translit = self.current_entry.get("transliteration", word)
                gloss = self.current_entry.get("gloss", "")
                if gloss:
                    self.entries.append({
                        "word": word,
                        "transliteration": translit,
                        "gloss": gloss,
                    })
            self.in_entry = False
            self.current_entry = {}

    def handle_data(self, data: str) -> None:
        if self.in_lemma or self.in_translit or self.in_gloss:
            self.current_text += data


def _fallback_regex_parse(html: str) -> list[dict]:
    """Fallback: extract entries using regex patterns from raw HTML text.

    Looks for common dictionary-entry patterns in the text:
    - "WORD (transliteration) - gloss"
    - "WORD: gloss"
    - Tabular patterns
    """
    entries: list[dict] = []

    # Strip HTML tags for text-based matching
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)

    # Pattern: "word" followed by dash/colon and gloss
    # Common in dictionary-style pages
    patterns = [
        # word - gloss (with optional transliteration in parens)
        re.compile(
            r"(?:^|\n)\s*([A-Za-zÀ-žḀ-ỿ][A-Za-zÀ-žḀ-ỿ\-]+)"
            r"(?:\s*\(([^)]+)\))?"
            r"\s*[-–—:]\s+"
            r"([A-Za-z].{3,80}?)(?:[.;]|\n|$)"
        ),
        # Numbered entries: 1. word - gloss
        re.compile(
            r"\d+\.\s*([A-Za-zÀ-žḀ-ỿ][A-Za-zÀ-žḀ-ỿ\-]+)"
            r"(?:\s*\(([^)]+)\))?"
            r"\s*[-–—:]\s+"
            r"([A-Za-z].{3,80}?)(?:[.;]|\n|$)"
        ),
    ]

    for pat in patterns:
        for m in pat.finditer(text):
            word = m.group(1).strip()
            translit = m.group(2).strip() if m.group(2) else word
            gloss = m.group(3).strip()
            if word and gloss and len(word) < 50:
                entries.append({
                    "word": word,
                    "transliteration": translit,
                    "gloss": gloss,
                })

    return entries


def parse(url: str, **kwargs: Any) -> list[dict]:
    """Download and parse an eDiAna dictionary page.

    Args:
        url: URL to an eDiAna page, e.g.
             https://www.ediana.gwi.uni-muenchen.de/dictionary/lycian
        **kwargs:
            language: Language name (lycian, lydian, carian) for logging.

    Returns:
        List of dicts with keys: word, transliteration, gloss.
        Returns empty list if URL is unreachable.
    """
    lang = kwargs.get("language", "unknown")
    logger.info("eDiAna: downloading %s (language=%s)", url, lang)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PhaiPhon/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.warning("eDiAna: failed to download %s: %s", url, exc)
        return []

    # Try structured HTML parsing first
    parser = EDiAnaParser()
    parser.feed(html)
    entries = parser.entries

    # Fallback to regex if structured parsing found nothing
    if not entries:
        logger.info("eDiAna: structured parsing found 0 entries, trying regex fallback")
        entries = _fallback_regex_parse(html)

    # Deduplicate
    seen: set[tuple[str, str]] = set()
    unique: list[dict] = []
    for e in entries:
        key = (e["word"], e["gloss"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    logger.info("eDiAna: extracted %d entries from %s", len(unique), url)
    return unique


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "https://www.ediana.gwi.uni-muenchen.de/"
    )
    results = parse(test_url)
    print(f"\nExtracted {len(results)} entries:")
    for entry in results[:15]:
        print(f"  {entry['word']:20s}  {entry['transliteration']:20s}  {entry['gloss']}")
    if len(results) > 15:
        print(f"  ... and {len(results) - 15} more")
