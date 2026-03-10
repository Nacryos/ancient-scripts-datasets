#!/usr/bin/env python3
"""Parser for the Oracc (Open Richly Annotated Cuneiform Corpus) API.

Fetches lemma data from Oracc project JSON endpoints. Oracc hosts
multiple cuneiform language corpora; the project identifier determines
the language:
    - "ecut" -> Urartian
    - "dcclt" -> Sumerian lexical texts
    - "saao/saa01" -> Neo-Assyrian

Endpoint pattern: http://oracc.museum.upenn.edu/{project}/json

Reference: http://oracc.museum.upenn.edu/doc/opendata/
"""

from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error
from typing import Any

logger = logging.getLogger(__name__)


def _extract_lemmas_from_catalog(data: dict) -> list[dict]:
    """Extract lemmas from the catalogue/index JSON structure."""
    entries: list[dict] = []

    # Oracc JSON has nested structure:
    # {"members": {"P123456": {"cdl": [...]}}}
    # or {"index": {"cat": {...}, "lem": {...}}}
    # or {"glossary": {"entries": [...]}}

    # Strategy 1: glossary format
    if "glossary" in data:
        glossary = data["glossary"]
        glo_entries = glossary.get("entries", [])
        if isinstance(glo_entries, list):
            for entry in glo_entries:
                cf = entry.get("cf", "")  # citation form
                gw = entry.get("gw", "")  # guide word (gloss)
                form_list = entry.get("forms", [])
                if cf and gw:
                    translit = cf
                    # If forms are available, use the first one
                    if form_list and isinstance(form_list, list):
                        first_form = form_list[0] if form_list else {}
                        if isinstance(first_form, dict):
                            translit = first_form.get("n", cf)
                    entries.append({
                        "word": cf,
                        "transliteration": translit,
                        "gloss": gw,
                    })
        elif isinstance(glo_entries, dict):
            for key, entry in glo_entries.items():
                if isinstance(entry, dict):
                    cf = entry.get("cf", key)
                    gw = entry.get("gw", "")
                    if cf and gw:
                        entries.append({
                            "word": cf,
                            "transliteration": cf,
                            "gloss": gw,
                        })

    # Strategy 2: index/lem format
    if not entries and "index" in data:
        index = data["index"]
        lem_data = index.get("lem", {})
        if isinstance(lem_data, dict):
            for lemma_key, lemma_info in lem_data.items():
                # lemma_key format: "cf[gw]POS" e.g. "šarru[king]N"
                import re
                m = re.match(r"^(.+?)\[(.+?)\]", lemma_key)
                if m:
                    entries.append({
                        "word": m.group(1),
                        "transliteration": m.group(1),
                        "gloss": m.group(2),
                    })

    # Strategy 3: members/cdl format (text corpus)
    if not entries and "members" in data:
        members = data["members"]
        if isinstance(members, dict):
            for text_id, text_data in members.items():
                if not isinstance(text_data, dict):
                    continue
                _extract_cdl_lemmas(text_data.get("cdl", []), entries)

    return entries


def _extract_cdl_lemmas(cdl_list: list, entries: list[dict]) -> None:
    """Recursively extract lemmas from CDL (Corpus Description Language) nodes."""
    if not isinstance(cdl_list, list):
        return
    for node in cdl_list:
        if not isinstance(node, dict):
            continue
        # Leaf node with lemma
        if "f" in node:
            f = node["f"]
            if isinstance(f, dict):
                cf = f.get("cf", "")
                gw = f.get("gw", "")
                form = f.get("form", cf)
                if cf and gw:
                    entries.append({
                        "word": cf,
                        "transliteration": form if form else cf,
                        "gloss": gw,
                    })
        # Recurse into children
        if "cdl" in node:
            _extract_cdl_lemmas(node["cdl"], entries)


def parse(url: str, **kwargs: Any) -> list[dict]:
    """Fetch and parse lemma data from an Oracc project JSON endpoint.

    Args:
        url: Oracc URL. Can be:
             - Direct JSON: http://oracc.museum.upenn.edu/ecut/json
             - Project page: http://oracc.museum.upenn.edu/ecut
             The parser will append /json if needed.
        **kwargs:
            project: Override project name (e.g., "ecut", "dcclt")

    Returns:
        List of dicts with keys: word, transliteration, gloss.
        Returns empty list if URL is unreachable.
    """
    # Normalize URL to JSON endpoint
    json_url = url.rstrip("/")
    if not json_url.endswith("/json"):
        json_url += "/json"

    logger.info("Oracc: downloading %s", json_url)
    try:
        req = urllib.request.Request(json_url, headers={"User-Agent": "PhaiPhon/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.warning("Oracc: failed to download %s: %s", json_url, exc)
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Oracc: invalid JSON from %s: %s", json_url, exc)
        return []

    entries = _extract_lemmas_from_catalog(data)

    # Deduplicate by (word, gloss)
    seen: set[tuple[str, str]] = set()
    unique: list[dict] = []
    for e in entries:
        key = (e["word"], e["gloss"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    logger.info("Oracc: extracted %d unique entries from %s", len(unique), json_url)
    return unique


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "http://oracc.museum.upenn.edu/ecut/json"
    )
    results = parse(test_url)
    print(f"\nExtracted {len(results)} entries:")
    for entry in results[:15]:
        print(f"  {entry['word']:20s}  {entry['transliteration']:20s}  {entry['gloss']}")
    if len(results) > 15:
        print(f"  ... and {len(results) - 15} more")
