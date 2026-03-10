#!/usr/bin/env python3
"""Parser for the UT Austin LRC (Linguistics Research Center) PIE lexicon.

Extracts Proto-Indo-European etyma with reconstructed forms and glosses
from the online lexicon at https://lrc.la.utexas.edu/lex

The LRC presents a multi-page index of PIE roots organized alphabetically,
with each root linking to a detail page.

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


class LRCIndexParser(HTMLParser):
    """Parse the LRC lexicon index page to extract root links and glosses."""

    def __init__(self) -> None:
        super().__init__()
        self.in_link = False
        self.link_href = ""
        self.link_text = ""
        self.entries: list[dict] = []
        self.in_table = False
        self.in_cell = False
        self.cell_text = ""
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = dict(attrs)
        if tag == "a":
            href = attr_dict.get("href", "")
            if href:
                self.in_link = True
                self.link_href = href
                self.link_text = ""
        elif tag == "table":
            self.in_table = True
        elif tag == "tr" and self.in_table:
            self.current_row = []
        elif tag in ("td", "th") and self.in_table:
            self.in_cell = True
            self.cell_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.in_link:
            self.in_link = False
            text = self.link_text.strip()
            if text:
                self.entries.append({
                    "text": text,
                    "href": self.link_href,
                })
        elif tag == "table":
            self.in_table = False
        elif tag == "tr" and self.in_table:
            if self.current_row:
                self.rows.append(self.current_row)
        elif tag in ("td", "th") and self.in_cell:
            self.in_cell = False
            self.current_row.append(self.cell_text.strip())

    def handle_data(self, data: str) -> None:
        if self.in_link:
            self.link_text += data
        if self.in_cell:
            self.cell_text += data


def _clean_pie_form(form: str) -> str:
    """Clean a PIE reconstructed form."""
    form = form.strip()
    # Remove leading asterisk (reconstruction marker)
    form = re.sub(r"^\*+", "", form)
    # Remove trailing punctuation
    form = re.sub(r"[.,;:]+$", "", form)
    # Remove parenthetical variants
    form = re.sub(r"\s*\([^)]*\)\s*$", "", form)
    return form.strip()


def _extract_from_index_page(html: str) -> list[dict]:
    """Extract entries from the LRC index page HTML."""
    parser = LRCIndexParser()
    parser.feed(html)

    entries: list[dict] = []

    # Strategy 1: Extract from table rows
    for row in parser.rows:
        if len(row) < 2:
            continue
        # Skip header rows
        if any(kw in c.lower() for c in row for kw in ("root", "meaning", "gloss", "#")):
            continue

        form = _clean_pie_form(row[0])
        gloss = row[1].strip() if len(row) >= 2 else ""
        # Sometimes gloss is in column 2 (after POS)
        if len(row) >= 3 and not gloss:
            gloss = row[2].strip()

        if form and gloss and len(form) < 50:
            entries.append({
                "word": form,
                "transliteration": form,
                "gloss": gloss,
            })

    # Strategy 2: Extract from link text patterns
    if not entries:
        for link_entry in parser.entries:
            text = link_entry["text"]
            # Pattern: "*root - gloss" or "*root 'gloss'"
            m = (
                re.match(r"^\*?(.+?)\s*[-–—]\s+(.+)$", text)
                or re.match(r"^\*?(.+?)\s+'(.+?)'", text)
                or re.match(r"^\*?(.+?)\s+\"(.+?)\"", text)
            )
            if m:
                form = _clean_pie_form(m.group(1))
                gloss = m.group(2).strip()
                if form and gloss:
                    entries.append({
                        "word": form,
                        "transliteration": form,
                        "gloss": gloss,
                    })

    # Strategy 3: regex fallback on raw text
    if not entries:
        text = re.sub(r"<[^>]+>", " ", html)
        # Pattern: *form meaning
        for m in re.finditer(
            r"\*([a-zA-ZÀ-žḀ-ỿəɛɪɔʊ\-]+)"
            r"\s+['\"]?([A-Za-z][A-Za-z\s,'-]{2,60}?)['\"]?"
            r"(?=[,;.\n]|$)",
            text,
        ):
            form = _clean_pie_form(m.group(1))
            gloss = m.group(2).strip()
            gloss = re.sub(r"[,;:\s]+$", "", gloss)
            if form and gloss and len(form) < 40:
                entries.append({
                    "word": form,
                    "transliteration": form,
                    "gloss": gloss,
                })

    return entries


def _fetch_page(url: str) -> str:
    """Fetch a single page, returning empty string on failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PhaiPhon/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.warning("LRC: failed to download %s: %s", url, exc)
        return ""


def parse(url: str, **kwargs: Any) -> list[dict]:
    """Download and parse the LRC PIE lexicon.

    Args:
        url: Base URL to the LRC lexicon, e.g.
             https://lrc.la.utexas.edu/lex
        **kwargs:
            max_pages: Maximum number of subpages to fetch (default 30).

    Returns:
        List of dicts with keys: word, transliteration, gloss.
        Returns empty list if URL is unreachable.
    """
    # No artificial page limit — follow ALL pagination links
    max_pages = kwargs.get("max_pages", 9999)
    logger.info("LRC: downloading index from %s", url)

    html = _fetch_page(url)
    if not html:
        return []

    entries = _extract_from_index_page(html)

    # If the index page has pagination links, follow them
    # Look for page links like ?page=2 or /lex/2 etc.
    page_pattern = re.compile(
        r'href="([^"]*(?:page=\d+|/lex/[a-z]|/lex\?letter=[a-z])[^"]*)"',
        re.IGNORECASE,
    )
    subpage_urls: set[str] = set()
    for m in page_pattern.finditer(html):
        href = m.group(1)
        # Make absolute URL
        if href.startswith("/"):
            # Extract base domain
            domain_match = re.match(r"(https?://[^/]+)", url)
            if domain_match:
                href = domain_match.group(1) + href
        elif href.startswith("?"):
            href = url.rstrip("/") + href
        elif not href.startswith("http"):
            href = url.rstrip("/") + "/" + href
        subpage_urls.add(href)

    # Fetch subpages (up to max_pages)
    pages_fetched = 0
    for subpage_url in sorted(subpage_urls):
        if pages_fetched >= max_pages:
            break
        logger.info("LRC: downloading subpage %s", subpage_url)
        subpage_html = _fetch_page(subpage_url)
        if subpage_html:
            sub_entries = _extract_from_index_page(subpage_html)
            entries.extend(sub_entries)
            pages_fetched += 1

    # Deduplicate
    seen: set[tuple[str, str]] = set()
    unique: list[dict] = []
    for e in entries:
        key = (e["word"], e["gloss"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    logger.info("LRC: extracted %d unique entries total", len(unique))
    return unique


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "https://lrc.la.utexas.edu/lex"
    )
    results = parse(test_url)
    print(f"\nExtracted {len(results)} entries:")
    for entry in results[:15]:
        print(f"  *{entry['word']:25s}  {entry['gloss']}")
    if len(results) > 15:
        print(f"  ... and {len(results) - 15} more")
