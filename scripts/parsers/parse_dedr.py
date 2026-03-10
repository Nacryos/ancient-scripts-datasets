#!/usr/bin/env python3
"""Parser for DSAL DEDR (Dravidian Etymological Dictionary Revised).

Extracts Proto-Dravidian reconstructions with glosses from the
University of Chicago DSAL digital edition:
    https://dsal.uchicago.edu/dictionaries/burrow/

Entries are organized by entry number and typically contain:
- Proto-Dravidian reconstruction (starred form)
- Cognates across Dravidian languages (Tamil, Telugu, Kannada, etc.)
- English gloss

Uses only stdlib (urllib, html.parser, re).

Reference: Burrow & Emeneau, "A Dravidian Etymological Dictionary" (1984, revised)
"""

from __future__ import annotations

import logging
import re
import urllib.request
import urllib.error
from html.parser import HTMLParser
from typing import Any

logger = logging.getLogger(__name__)


class DEDRPageParser(HTMLParser):
    """Parse DEDR entry pages to extract reconstructions and glosses."""

    def __init__(self) -> None:
        super().__init__()
        self.entries: list[dict] = []
        self.in_entry_div = False
        self.in_bold = False
        self.entry_text = ""
        self.bold_text = ""
        self.current_entry_text = ""
        self._tag_stack: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = dict(attrs)
        cls = attr_dict.get("class", "") or ""

        self._tag_stack.append(tag)

        if any(kw in cls.lower() for kw in ("entry", "hw", "result", "body")):
            self.in_entry_div = True
            self.current_entry_text = ""

        if tag in ("b", "strong"):
            self.in_bold = True
            self.bold_text = ""

    def handle_endtag(self, tag: str) -> None:
        if self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()

        if tag in ("b", "strong") and self.in_bold:
            self.in_bold = False

        if tag in ("div", "p", "br") and self.in_entry_div:
            if self.current_entry_text.strip():
                self._process_entry_text(self.current_entry_text)
            self.current_entry_text = ""

    def handle_data(self, data: str) -> None:
        if self.in_bold:
            self.bold_text += data
        if self.in_entry_div:
            self.current_entry_text += data

    def _process_entry_text(self, text: str) -> None:
        """Process accumulated entry text to extract reconstruction + gloss."""
        text = text.strip()
        if not text or len(text) < 5:
            return

        # Look for reconstruction pattern: *form or starred form
        m = re.search(r"\*([A-Za-zÀ-žḀ-ỿ\-āēīōūṭḍṇṛḷṃṁ]+)", text)
        if m:
            form = m.group(1).strip()
            # Extract gloss: text after the form, typically after comma or dash
            rest = text[m.end():]
            gloss = ""
            # Try various delimiters
            gm = (
                re.match(r"\s*[-–—,]\s+(.+?)(?:\.|;|$)", rest)
                or re.match(r"\s+'(.+?)'", rest)
                or re.match(r'\s+"(.+?)"', rest)
            )
            if gm:
                gloss = gm.group(1).strip()
            if form and gloss and len(form) < 40:
                self.entries.append({
                    "word": form,
                    "transliteration": form,
                    "gloss": gloss,
                })


def _regex_extract(html: str) -> list[dict]:
    """Extract DEDR entries using regex patterns on raw HTML.

    DEDR entries typically look like:
        <b>1234</b> Ta. <i>word</i> meaning. ...
    or:
        1234 *proto-form - meaning
    """
    entries: list[dict] = []

    # Strip HTML tags but keep structure hints
    text = html
    # Replace <br> and <p> with newlines
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</?p[^>]*>", "\n", text, flags=re.IGNORECASE)
    # Strip remaining tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode entities
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&nbsp;", " ")

    # Pattern 1: Entry number + reconstruction
    # e.g. "1234 *kāl- leg, foot"
    pattern1 = re.compile(
        r"(\d{1,5})\s+\*([A-Za-zÀ-žḀ-ỿāēīōūṭḍṇṛḷṃṁ\-]+)"
        r"\s*[-–—,]?\s+"
        r"([A-Za-z][A-Za-z\s,'-]{2,80}?)(?=[.;]|\n|$)"
    )
    for m in pattern1.finditer(text):
        entry_num = m.group(1)
        form = m.group(2).strip()
        gloss = m.group(3).strip()
        gloss = re.sub(r"[,;:\s]+$", "", gloss)
        if form and gloss:
            entries.append({
                "word": form,
                "transliteration": form,
                "gloss": gloss,
            })

    # Pattern 2: Tamil/Dravidian language abbreviation + word + meaning
    # e.g. "Ta. kāl foot, leg"
    if not entries:
        pattern2 = re.compile(
            r"(?:Ta|Te|Ka|Ma|Tu|Ko|Go|Ku|Mal|Kur|Br)\.\s+"
            r"([A-Za-zÀ-žḀ-ỿāēīōūṭḍṇṛḷṃṁ\-]+)"
            r"\s+"
            r"([A-Za-z][A-Za-z\s,'-]{2,60}?)(?=[.;]|\n|$)"
        )
        for m in pattern2.finditer(text):
            form = m.group(1).strip()
            gloss = m.group(2).strip()
            gloss = re.sub(r"[,;:\s]+$", "", gloss)
            if form and gloss and len(form) < 30:
                entries.append({
                    "word": form,
                    "transliteration": form,
                    "gloss": gloss,
                })

    return entries


def parse(url: str, **kwargs: Any) -> list[dict]:
    """Download and parse DEDR dictionary entries.

    Args:
        url: URL to DEDR, e.g.
             https://dsal.uchicago.edu/dictionaries/burrow/
        **kwargs:
            entry_range: Tuple (start, end) of entry numbers to fetch.
                         Default fetches the main page only.
            max_pages: Maximum number of pages to fetch (default 10).

    Returns:
        List of dicts with keys: word, transliteration, gloss.
        Returns empty list if URL is unreachable.
    """
    max_pages = kwargs.get("max_pages", 10)
    logger.info("DEDR: downloading %s", url)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PhaiPhon/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.warning("DEDR: failed to download %s: %s", url, exc)
        return []

    # Try structured parsing
    parser = DEDRPageParser()
    parser.feed(html)
    entries = parser.entries

    # Fallback to regex
    if not entries:
        logger.info("DEDR: structured parsing found 0 entries, trying regex")
        entries = _regex_extract(html)

    # Follow pagination links if present
    page_links: set[str] = set()
    for m in re.finditer(r'href="([^"]*(?:page=\d+|/\d+/?)[^"]*)"', html, re.IGNORECASE):
        href = m.group(1)
        if href.startswith("/"):
            domain_match = re.match(r"(https?://[^/]+)", url)
            if domain_match:
                href = domain_match.group(1) + href
        elif not href.startswith("http"):
            href = url.rstrip("/") + "/" + href
        page_links.add(href)

    pages_fetched = 0
    for page_url in sorted(page_links):
        if pages_fetched >= max_pages:
            break
        logger.info("DEDR: downloading page %s", page_url)
        try:
            req = urllib.request.Request(page_url, headers={"User-Agent": "PhaiPhon/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                page_html = resp.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            logger.warning("DEDR: failed to download page %s: %s", page_url, exc)
            continue

        page_entries = _regex_extract(page_html)
        entries.extend(page_entries)
        pages_fetched += 1

    # Deduplicate
    seen: set[tuple[str, str]] = set()
    unique: list[dict] = []
    for e in entries:
        key = (e["word"], e["gloss"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    logger.info("DEDR: extracted %d unique entries total", len(unique))
    return unique


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "https://dsal.uchicago.edu/dictionaries/burrow/"
    )
    results = parse(test_url)
    print(f"\nExtracted {len(results)} entries:")
    for entry in results[:15]:
        print(f"  *{entry['word']:25s}  {entry['gloss']}")
    if len(results) > 15:
        print(f"  ... and {len(results) - 15} more")
