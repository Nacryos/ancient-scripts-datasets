#!/usr/bin/env python3
"""Parser for Wiktionary appendix pages (reconstructed proto-languages, Swadesh lists).

Handles several page formats commonly used on Wiktionary:
- HTML table-based layouts (e.g. Proto-Semitic appendix tables)
- Definition-list-based layouts (dl/dt/dd)
- Ordered/unordered list-based layouts (ol/ul with li)

Uses only stdlib (urllib, html.parser, re) -- no BeautifulSoup dependency.

Reference: https://en.wiktionary.org/wiki/Appendix:...
"""

from __future__ import annotations

import logging
import re
import urllib.request
import urllib.error
from html.parser import HTMLParser
from typing import Any

logger = logging.getLogger(__name__)


class WiktionaryTableParser(HTMLParser):
    """Extract rows from HTML tables on Wiktionary pages."""

    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.cell_text = ""
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []
        self.depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self.depth += 1
            if self.depth == 1:
                self.in_table = True
        elif tag == "tr" and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ("td", "th") and self.in_row:
            self.in_cell = True
            self.cell_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "table":
            self.depth -= 1
            if self.depth <= 0:
                self.in_table = False
                self.depth = 0
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


class WiktionaryListParser(HTMLParser):
    """Extract items from definition lists (dl/dt/dd) and ordered/unordered lists."""

    def __init__(self) -> None:
        super().__init__()
        self.in_li = False
        self.in_dt = False
        self.in_dd = False
        self.li_text = ""
        self.dt_text = ""
        self.dd_text = ""
        self.list_items: list[str] = []
        self.def_pairs: list[tuple[str, str]] = []
        self._last_dt = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "li":
            self.in_li = True
            self.li_text = ""
        elif tag == "dt":
            self.in_dt = True
            self.dt_text = ""
        elif tag == "dd":
            self.in_dd = True
            self.dd_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "li" and self.in_li:
            self.in_li = False
            text = self.li_text.strip()
            if text:
                self.list_items.append(text)
        elif tag == "dt" and self.in_dt:
            self.in_dt = False
            self._last_dt = self.dt_text.strip()
        elif tag == "dd" and self.in_dd:
            self.in_dd = False
            dd = self.dd_text.strip()
            if self._last_dt and dd:
                self.def_pairs.append((self._last_dt, dd))

    def handle_data(self, data: str) -> None:
        if self.in_li:
            self.li_text += data
        if self.in_dt:
            self.dt_text += data
        if self.in_dd:
            self.dd_text += data


def _strip_html_tags(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text)


def _clean_wikt_form(form: str) -> str:
    """Clean a Wiktionary word form: strip asterisks, HTML entities, etc."""
    form = _strip_html_tags(form)
    form = form.replace("&nbsp;", " ")
    form = form.replace("&#x27;", "'")
    # Remove leading reconstruction asterisk
    form = re.sub(r"^\*+", "", form)
    # Remove citation markers like [1] or (?)
    form = re.sub(r"\[\d+\]", "", form)
    form = re.sub(r"\(\?\)", "", form)
    return form.strip()


def _extract_from_tables(html: str) -> list[dict]:
    """Extract entries from HTML tables."""
    parser = WiktionaryTableParser()
    parser.feed(html)

    entries: list[dict] = []
    for row in parser.rows:
        if len(row) < 2:
            continue
        # Skip rows that look like headers
        lower_cells = [c.lower() for c in row]
        if any(kw in cell for cell in lower_cells
               for kw in ("english", "gloss", "meaning", "proto-", "number")):
            continue

        # Heuristic: find the gloss column and form column
        # Common layouts:
        #   [number, gloss, form]
        #   [gloss, form]
        #   [form, gloss, cognates...]
        gloss = ""
        form = ""

        if len(row) >= 3:
            # Check if first column is numeric
            if re.match(r"^\d+\.?$", row[0].strip()):
                gloss = row[1]
                form = row[2]
            else:
                # Assume [gloss, form, ...] or [form, gloss, ...]
                # If first col has IPA-like chars, it's probably the form
                if re.search(r"[ɑɛɪɔʊʃʒθðŋ*]", row[0]):
                    form = row[0]
                    gloss = row[1]
                else:
                    gloss = row[0]
                    form = row[1]
        elif len(row) == 2:
            # Determine which is gloss and which is form
            if re.search(r"[ɑɛɪɔʊʃʒθðŋ*]", row[0]):
                form = row[0]
                gloss = row[1]
            else:
                gloss = row[0]
                form = row[1]

        form = _clean_wikt_form(form)
        gloss = _clean_wikt_form(gloss)

        if form and gloss and len(form) < 100 and len(gloss) < 200:
            entries.append({
                "word": form,
                "transliteration": form,
                "gloss": gloss,
            })

    return entries


def _extract_from_lists(html: str) -> list[dict]:
    """Extract entries from definition lists and bulleted lists."""
    parser = WiktionaryListParser()
    parser.feed(html)

    entries: list[dict] = []

    # From definition lists (dt/dd pairs)
    for dt, dd in parser.def_pairs:
        form = _clean_wikt_form(dt)
        gloss = _clean_wikt_form(dd)
        if form and gloss:
            entries.append({
                "word": form,
                "transliteration": form,
                "gloss": gloss,
            })

    # From list items: try to split on common delimiters
    # Patterns: "form - gloss", "form: gloss", "form 'gloss'", "form (gloss)"
    for item in parser.list_items:
        item_clean = _strip_html_tags(item)
        m = (
            re.match(r"^(.+?)\s*[-–—]\s+(.+)$", item_clean)
            or re.match(r"^(.+?):\s+(.+)$", item_clean)
            or re.match(r'^(.+?)\s+"(.+?)"', item_clean)
            or re.match(r"^(.+?)\s+'(.+?)'", item_clean)
            or re.match(r"^(.+?)\s+\((.+?)\)\s*$", item_clean)
        )
        if m:
            part1 = _clean_wikt_form(m.group(1))
            part2 = _clean_wikt_form(m.group(2))
            if part1 and part2:
                # Heuristic: shorter/IPA-like part is the form
                if re.search(r"[ɑɛɪɔʊʃʒθðŋ*]", part1) or len(part1) < len(part2):
                    entries.append({
                        "word": part1,
                        "transliteration": part1,
                        "gloss": part2,
                    })
                else:
                    entries.append({
                        "word": part2,
                        "transliteration": part2,
                        "gloss": part1,
                    })

    return entries


def parse(url: str, **kwargs: Any) -> list[dict]:
    """Download and parse a Wiktionary appendix/Swadesh-list page.

    Args:
        url: Full URL to a Wiktionary page, e.g.
             https://en.wiktionary.org/wiki/Appendix:Proto-Semitic_roots

    Returns:
        List of dicts with keys: word, transliteration, gloss.
        Returns empty list if URL is unreachable.
    """
    logger.info("Wiktionary: downloading %s", url)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PhaiPhon/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.warning("Wiktionary: failed to download %s: %s", url, exc)
        return []

    # Try tables first (most structured), then lists
    entries = _extract_from_tables(html)

    if not entries:
        entries = _extract_from_lists(html)

    # Deduplicate by (word, gloss)
    seen: set[tuple[str, str]] = set()
    unique: list[dict] = []
    for e in entries:
        key = (e["word"], e["gloss"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    logger.info("Wiktionary: extracted %d entries from %s", len(unique), url)
    return unique


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "https://en.wiktionary.org/wiki/Appendix:Swadesh_lists"
    )
    results = parse(test_url)
    print(f"\nExtracted {len(results)} entries:")
    for entry in results[:15]:
        print(f"  {entry['word']:30s}  {entry['gloss']}")
    if len(results) > 15:
        print(f"  ... and {len(results) - 15} more")
