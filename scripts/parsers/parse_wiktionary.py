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
import urllib.parse
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


# ---------------------------------------------------------------------------
# MediaWiki API-based category pagination
# ---------------------------------------------------------------------------

import json
import time

_MW_API = "https://en.wiktionary.org/w/api.php"
_MW_UA = "PhaiPhon/1.0 (ancient-scripts-datasets)"


def _mw_api(params: dict) -> dict:
    """Call the MediaWiki API with retry on 429."""
    params["format"] = "json"
    qs = "&".join(
        f"{k}={urllib.parse.quote(str(v))}"
        for k, v in params.items()
    )
    url = f"{_MW_API}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": _MW_UA})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                wait = 10 * (attempt + 1)
                logger.info("Rate limited, waiting %ds...", wait)
                time.sleep(wait)
            else:
                raise


def fetch_category_members(category: str, namespace: int = 0) -> list[dict]:
    """Fetch ALL members of a Wiktionary category, paginating via cmcontinue.

    Args:
        category: Full category name, e.g. "Category:Avestan_lemmas"
        namespace: MediaWiki namespace (0=main, 118=Reconstruction)

    Returns:
        List of dicts with keys: pageid, ns, title
    """
    all_members: list[dict] = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": category,
        "cmlimit": "500",
        "cmnamespace": str(namespace),
    }
    while True:
        data = _mw_api(params)
        members = data.get("query", {}).get("categorymembers", [])
        all_members.extend(members)
        if "continue" not in data:
            break
        params["cmcontinue"] = data["continue"]["cmcontinue"]
        time.sleep(1)  # Rate limiting
    logger.info("Category %s: fetched %d members (ns=%d)",
                category, len(all_members), namespace)
    return all_members


def fetch_page_wikitext(page_title: str) -> str:
    """Fetch the raw wikitext of a page (for definition extraction)."""
    data = _mw_api({
        "action": "parse",
        "page": page_title,
        "prop": "wikitext",
    })
    if "parse" not in data:
        return ""
    return data["parse"]["wikitext"].get("*", "")


def fetch_page_html(page_title: str) -> str:
    """Fetch the rendered HTML of a page (for gloss extraction)."""
    data = _mw_api({
        "action": "parse",
        "page": page_title,
        "prop": "text",
    })
    if "parse" not in data:
        return ""
    return data["parse"]["text"].get("*", "")


def extract_gloss_from_html(html: str, language_name: str) -> str:
    """Extract the first English gloss for a specific language section from HTML.

    Looks for the language heading, then finds the first definition <li>.
    """
    # Find the language section
    # Wiktionary uses <h2><span id="Language_Name">...</span></h2>
    lang_pattern = re.compile(
        rf'<span[^>]*id="{re.escape(language_name)}"[^>]*>',
        re.IGNORECASE,
    )
    m = lang_pattern.search(html)
    if not m:
        # Try without escaping (for "Proto-Indo-European" etc.)
        lang_id = language_name.replace(" ", "_")
        lang_pattern = re.compile(
            rf'<span[^>]*id="{re.escape(lang_id)}"[^>]*>',
            re.IGNORECASE,
        )
        m = lang_pattern.search(html)
    if not m:
        return ""

    # Get text after language heading, up to next h2
    rest = html[m.end():]
    next_h2 = re.search(r"<h2[^>]*>", rest)
    section = rest[:next_h2.start()] if next_h2 else rest[:5000]

    # Find first <li> with a definition (skip empty ones)
    li_pattern = re.compile(r"<li[^>]*>(.*?)</li>", re.DOTALL)
    for li_match in li_pattern.finditer(section):
        li_text = li_match.group(1)
        # Strip HTML tags
        clean = re.sub(r"<[^>]+>", "", li_text).strip()
        # Skip navigation/category items
        if not clean or clean.startswith("(") or len(clean) < 2:
            continue
        # Truncate to first sentence
        clean = re.sub(r"\s+", " ", clean)
        if len(clean) > 100:
            clean = clean[:100].rsplit(" ", 1)[0]
        return clean

    return ""


def extract_romanization_from_html(html: str, language_name: str) -> str:
    """Extract romanization/transliteration from a Wiktionary entry HTML."""
    lang_id = language_name.replace(" ", "_")
    m = re.search(rf'id="{re.escape(lang_id)}"', html, re.IGNORECASE)
    if not m:
        return ""

    rest = html[m.end():]
    next_h2 = re.search(r"<h2[^>]*>", rest)
    section = rest[:next_h2.start()] if next_h2 else rest[:5000]

    # Pattern: (romanization) after script form
    romans = re.findall(
        r'•\s*\(\s*([a-zA-ZÀ-žḀ-ỿāēīōūəąęðθšžŋɣβγñδ'
        r'\u0300-\u036f\u0323\u0331\u0325ᵛ\s\-]+?)\s*\)',
        section
    )
    if romans:
        return romans[0].strip()

    # Pattern: Romanization in transliteration span
    translit = re.findall(r'class="tr[^"]*"[^>]*>([^<]+)<', section)
    if translit:
        return translit[0].strip()

    return ""


def fetch_category_lemmas(
    category: str,
    language_name: str,
    namespace: int = 0,
    fetch_glosses: bool = True,
    rate_limit: float = 1.5,
) -> list[dict]:
    """Fetch all lemmas from a Wiktionary category with optional gloss extraction.

    This is the main entry point for Phase 2 Wiktionary expansion.
    Paginates through ALL category members and fetches individual pages.

    Args:
        category: e.g. "Category:Avestan_lemmas"
        language_name: e.g. "Avestan" (for section detection in HTML)
        namespace: 0 for main, 118 for Reconstruction
        fetch_glosses: If True, fetch each page to extract gloss
        rate_limit: Seconds between page fetches

    Returns:
        List of dicts with keys: word, transliteration, gloss
    """
    members = fetch_category_members(category, namespace=namespace)
    logger.info("Fetching %d individual pages for %s...", len(members), language_name)

    entries: list[dict] = []
    for i, m in enumerate(members):
        title = m.get("title", "")
        if not title:
            continue

        # Extract word from title
        if namespace == 118:
            # Reconstruction namespace: "Reconstruction:Language/word"
            parts = title.split("/")
            word = parts[-1].strip() if len(parts) >= 2 else ""
        else:
            word = title.strip()

        # Clean reconstruction markers
        word = re.sub(r"^\*+", "", word)
        if not word or len(word) > 50:
            continue

        gloss = ""
        romanization = ""

        if fetch_glosses:
            try:
                html = fetch_page_html(title)
                if html:
                    gloss = extract_gloss_from_html(html, language_name)
                    romanization = extract_romanization_from_html(html, language_name)
            except Exception as exc:
                logger.warning("Failed to fetch page '%s': %s", title, exc)

            if (i + 1) % 50 == 0:
                logger.info("  Progress: %d/%d pages fetched", i + 1, len(members))
                time.sleep(3)  # Extra pause at milestones
            else:
                time.sleep(rate_limit)

        entries.append({
            "word": word,
            "transliteration": romanization or word,
            "gloss": gloss,
        })

    # Deduplicate by word
    seen: set[str] = set()
    unique: list[dict] = []
    for e in entries:
        key = e["word"].lower()
        if key not in seen:
            seen.add(key)
            unique.append(e)

    logger.info("fetch_category_lemmas: %d unique entries for %s",
                len(unique), language_name)
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
