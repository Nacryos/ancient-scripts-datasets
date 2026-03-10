#!/usr/bin/env python3
"""Scrape Palaeolexicon for Lycian, Lydian, and Carian word lists.

Source: https://www.palaeolexicon.com/
Palaeolexicon is a searchable ancient language database.

Extracts word lists per language with glosses. Deduplicates against
existing TSV entries and appends new ones.

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/scrape_palaeolexicon.py [--language xlc|xld|xcr] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import urllib.request
import urllib.error
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

# Palaeolexicon language IDs (from their URL structure)
LANGUAGE_CONFIGS = {
    "xlc": {
        "name": "Lycian",
        "search_term": "Lycian",
        "iso_for_translit": "xlc",
        "tsv_filename": "xlc.tsv",
    },
    "xld": {
        "name": "Lydian",
        "search_term": "Lydian",
        "iso_for_translit": "xld",
        "tsv_filename": "xld.tsv",
    },
    "xcr": {
        "name": "Carian",
        "search_term": "Carian",
        "iso_for_translit": "xcr",
        "tsv_filename": "xcr.tsv",
    },
}

# Palaeolexicon uses a word list page per language
# URL pattern: https://www.palaeolexicon.com/Word/BasicSearch?language=Lycian
SEARCH_URL = "https://www.palaeolexicon.com/Word/BasicSearch"


def fetch_page(url: str) -> str:
    """Fetch HTML page with retries."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
    })
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED to fetch %s: %s", url, exc)
                return ""


def extract_words_from_html(html: str) -> list[dict]:
    """Extract word entries from Palaeolexicon search results HTML.

    Palaeolexicon uses tables or div-based layouts for word lists.
    """
    entries: list[dict] = []

    # Strategy 1: Table rows with word + meaning columns
    table_pattern = re.compile(
        r'<tr[^>]*>\s*<td[^>]*>([^<]+)</td>\s*<td[^>]*>([^<]+)</td>',
        re.DOTALL,
    )
    for m in table_pattern.finditer(html):
        word = m.group(1).strip()
        gloss = m.group(2).strip()
        word = re.sub(r"<[^>]+>", "", word)
        gloss = re.sub(r"<[^>]+>", "", gloss)
        if word and gloss and len(word) < 50 and len(gloss) < 200:
            entries.append({"word": word, "gloss": gloss})

    # Strategy 2: div-based entries
    div_pattern = re.compile(
        r'class="word[^"]*"[^>]*>([^<]+)<.*?'
        r'class="(?:meaning|translation|gloss)[^"]*"[^>]*>([^<]+)<',
        re.DOTALL,
    )
    for m in div_pattern.finditer(html):
        word = m.group(1).strip()
        gloss = m.group(2).strip()
        if word and gloss:
            entries.append({"word": word, "gloss": gloss})

    # Strategy 3: Link-based entries (common in Palaeolexicon)
    link_pattern = re.compile(
        r'<a[^>]*href="/Word/Show/\d+"[^>]*>([^<]+)</a>\s*'
        r'[-–—:]?\s*([A-Za-z][A-Za-z\s,;/\'-]{2,80})',
        re.DOTALL,
    )
    for m in link_pattern.finditer(html):
        word = m.group(1).strip()
        gloss = m.group(2).strip()
        gloss = re.sub(r"[,;:\s]+$", "", gloss)
        if word and gloss:
            entries.append({"word": word, "gloss": gloss})

    return entries


def fetch_language_words(language_name: str) -> list[dict]:
    """Fetch all words for a language from Palaeolexicon."""
    all_entries: list[dict] = []

    # Try the basic search page
    url = f"{SEARCH_URL}?language={language_name}"
    logger.info("Fetching %s words from %s", language_name, url)
    html = fetch_page(url)

    if html:
        # Save raw HTML
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        with open(RAW_DIR / f"palaeolexicon_{language_name}.html", "w",
                  encoding="utf-8") as f:
            f.write(html)

        entries = extract_words_from_html(html)
        all_entries.extend(entries)
        logger.info("  Page 1: %d entries", len(entries))

        # Check for pagination links
        page_links = re.findall(
            r'href="([^"]*(?:page=\d+|pageIndex=\d+)[^"]*)"',
            html,
        )
        for i, link in enumerate(sorted(set(page_links))):
            if not link.startswith("http"):
                link = f"https://www.palaeolexicon.com{link}"
            logger.info("  Fetching page %d: %s", i + 2, link)
            page_html = fetch_page(link)
            if page_html:
                page_entries = extract_words_from_html(page_html)
                all_entries.extend(page_entries)
            time.sleep(2)

    # Also try the language-specific word list page
    alt_url = f"https://www.palaeolexicon.com/default.aspx?static=true&wl={language_name}"
    logger.info("Trying alt URL: %s", alt_url)
    alt_html = fetch_page(alt_url)
    if alt_html:
        alt_entries = extract_words_from_html(alt_html)
        all_entries.extend(alt_entries)
        logger.info("  Alt page: %d entries", len(alt_entries))

    return all_entries


def load_existing_words(tsv_path: Path) -> set[str]:
    """Load existing Word column values."""
    existing = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Word\t"):
                    continue
                existing.add(line.split("\t")[0])
    return existing


def process_language(iso: str, config: dict, dry_run: bool = False) -> dict:
    """Process a single language."""
    tsv_path = LEXICON_DIR / config["tsv_filename"]
    existing = load_existing_words(tsv_path)
    logger.info("%s: loaded %d existing entries", iso, len(existing))

    # Fetch from Palaeolexicon
    all_entries = fetch_language_words(config["search_term"])

    # Deduplicate
    seen: set[str] = set()
    new_entries: list[dict] = []
    for e in all_entries:
        word = e["word"].strip()
        if not word or word in seen or word in existing:
            continue
        if len(word) > 50:
            continue
        seen.add(word)
        new_entries.append(e)

    logger.info("%s: %d new unique entries", iso, len(new_entries))

    if dry_run:
        for e in new_entries[:20]:
            print(f"  {e['word']:30s} {e['gloss']}")
        return {"iso": iso, "existing": len(existing), "new": len(new_entries)}

    # Append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    if new_entries:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                word = e["word"]
                gloss = e["gloss"]

                try:
                    ipa = transliterate(word, config["iso_for_translit"])
                except Exception:
                    ipa = word

                try:
                    sca = ipa_to_sound_class(ipa)
                except Exception:
                    sca = ""

                concept = gloss.split(",")[0].split(";")[0].strip()
                concept_id = concept.replace(" ", "_").lower()[:50]

                f.write(f"{word}\t{ipa}\t{sca}\tpalaeolexicon\t{concept_id}\t-\n")
                new_count += 1

                audit_trail.append({
                    "word": word,
                    "gloss": gloss,
                    "ipa": ipa,
                    "source": "palaeolexicon",
                })

    # Write audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / f"palaeolexicon_{iso}.jsonl", "w",
              encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(existing) + new_count
    return {"iso": iso, "existing": len(existing), "new": new_count, "total": total}


def main():
    parser = argparse.ArgumentParser(description="Scrape Palaeolexicon")
    parser.add_argument("--language", "-l",
                        help="ISO code: xlc, xld, or xcr (default: all 3)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.language:
        if args.language not in LANGUAGE_CONFIGS:
            print(f"Unknown language: {args.language}", file=sys.stderr)
            sys.exit(1)
        languages = {args.language: LANGUAGE_CONFIGS[args.language]}
    else:
        languages = LANGUAGE_CONFIGS

    print("=" * 60)
    print("Palaeolexicon Scraper")
    print(f"Languages: {', '.join(languages)}")
    print("=" * 60)

    results = []
    for iso, config in languages.items():
        print(f"\n--- {config['name']} ({iso}) ---")
        try:
            result = process_language(iso, config, dry_run=args.dry_run)
            results.append(result)
        except Exception as exc:
            logger.error("FAILED for %s: %s", iso, exc, exc_info=True)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    for r in results:
        print(f"  {r['iso']:10s} existing={r['existing']}, new={r['new']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
