#!/usr/bin/env python3
"""Scrape Rhaetic word forms from TIR (Thesaurus Inscriptionum Raeticarum).

Source: https://www.univie.ac.at/raetica/
TIR is a Semantic MediaWiki with 389+ Rhaetic inscriptions from which
we can extract unique word forms.

Iron Rule: All data comes from HTTP requests. No hardcoded lexical content.

Usage:
    python scripts/scrape_tir_rhaetic.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import ssl
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

# Create unverified SSL context for sites with certificate issues
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

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

# TIR uses Semantic MediaWiki — we can query via the API
TIR_BASE = "https://www.univie.ac.at/raetica"
TIR_API = f"{TIR_BASE}/wiki/api.php"


def fetch_page(url: str) -> str:
    """Fetch HTML page with retries."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED to fetch %s: %s", url, exc)
                return ""


def fetch_json(url: str) -> dict | None:
    """Fetch JSON from URL."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as resp:
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            if attempt < 2:
                logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning("FAILED to fetch %s: %s", url, exc)
                return None


def strategy_smw_ask() -> list[dict]:
    """Strategy 1: Use Semantic MediaWiki ask API to query for word forms.

    TIR categorizes inscriptions with properties like [[Has word::...]].
    """
    entries = []

    # Query for pages in the "Word" category or namespace
    queries = [
        "[[Category:Word]]|?Has transliteration|?Has meaning|limit=500",
        "[[Category:Words]]|?Has transliteration|?Has meaning|limit=500",
        "[[Category:Lexeme]]|?Has transliteration|?Has meaning|limit=500",
    ]

    for query in queries:
        url = (
            f"{TIR_API}?action=ask&query={urllib.parse.quote(query)}&format=json"
        )
        logger.info("SMW ask query: %s", url)
        data = fetch_json(url)
        if not data:
            continue

        results = data.get("query", {}).get("results", {})
        logger.info("  Got %d results", len(results))

        for page_name, page_data in results.items():
            printouts = page_data.get("printouts", {})
            translit_list = printouts.get("Has transliteration", [])
            meaning_list = printouts.get("Has meaning", [])

            word = translit_list[0] if translit_list else page_name
            gloss = meaning_list[0] if meaning_list else ""

            if isinstance(word, dict):
                word = word.get("fulltext", str(word))
            if isinstance(gloss, dict):
                gloss = gloss.get("fulltext", str(gloss))

            word = str(word).strip()
            if word:
                entries.append({"word": word, "gloss": str(gloss).strip()})

        if entries:
            return entries

    return entries


def strategy_category_pages() -> list[dict]:
    """Strategy 2: Fetch pages from inscription categories and extract word forms."""
    entries = []

    # Get category members for inscriptions
    categories = [
        "Category:Rhaetic_inscriptions",
        "Category:Inscriptions",
        "Category:Inscription",
    ]

    for cat in categories:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": cat,
            "cmlimit": "500",
            "format": "json",
        }
        qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
        url = f"{TIR_API}?{qs}"
        logger.info("Fetching category %s...", cat)
        data = fetch_json(url)
        if not data:
            continue

        members = data.get("query", {}).get("categorymembers", [])
        logger.info("  Category %s: %d members", cat, len(members))

        # Fetch each inscription page to extract words
        for i, m in enumerate(members[:200]):
            title = m.get("title", "")
            if not title:
                continue

            page_url = f"{TIR_BASE}/wiki/{urllib.parse.quote(title)}"
            html = fetch_page(page_url)
            if not html:
                continue

            # Extract word forms from inscription page
            # TIR typically has transliterations in specific divs or tables
            words = extract_words_from_inscription(html)
            entries.extend(words)

            if (i + 1) % 20 == 0:
                logger.info("  Processed %d/%d inscriptions, %d words so far",
                            i + 1, len(members), len(entries))
                time.sleep(3)
            else:
                time.sleep(1.5)

        if entries:
            break

    return entries


def extract_words_from_inscription(html: str) -> list[dict]:
    """Extract word forms from a TIR inscription page."""
    words: list[dict] = []

    # Clean HTML
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)

    # Pattern 1: Transliteration in specific format
    # TIR uses patterns like: "transliteration: word1 · word2 · word3"
    translit_match = re.search(
        r"(?:transliteration|reading|text)[:\s]*([a-zA-Zāēīōūšśṣṭḍṇḷθφχ·\s\-]+?)(?:\n|$|<)",
        text, re.IGNORECASE,
    )
    if translit_match:
        translit_text = translit_match.group(1)
        # Split on word separators
        raw_words = re.split(r"[·\s]+", translit_text)
        for w in raw_words:
            w = w.strip().strip("-")
            if w and len(w) >= 2 and len(w) <= 30:
                words.append({"word": w, "gloss": ""})

    # Pattern 2: Individual word entries linked to glossary
    word_links = re.findall(
        r'(?:word|lexeme|form)[:\s]*<[^>]*>([^<]+)<',
        html, re.IGNORECASE,
    )
    for w in word_links:
        w = w.strip()
        if w and len(w) >= 2 and len(w) <= 30:
            words.append({"word": w, "gloss": ""})

    return words


def strategy_allpages() -> list[dict]:
    """Strategy 3: Use the MediaWiki allpages API to find word-related pages."""
    entries = []

    # Search for pages that might be word entries
    params = {
        "action": "query",
        "list": "allpages",
        "aplimit": "500",
        "format": "json",
    }
    qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    url = f"{TIR_API}?{qs}"
    logger.info("Fetching all pages...")
    data = fetch_json(url)
    if not data:
        return entries

    pages = data.get("query", {}).get("allpages", [])
    logger.info("  Total pages: %d", len(pages))

    # Filter for pages that look like word/inscription entries
    for p in pages:
        title = p.get("title", "")
        # TIR inscription IDs follow patterns like "SZ-1", "NO-12", etc.
        if re.match(r"^[A-Z]{2,3}-\d+", title):
            # This is an inscription, skip for now
            continue
        # Short titles might be word forms
        if (len(title) <= 20 and title.isascii() and
                not title.startswith("Category:") and
                not title.startswith("Template:") and
                not title.startswith("File:")):
            entries.append({"word": title, "gloss": ""})

    return entries


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


def main():
    parser = argparse.ArgumentParser(description="Scrape TIR for Rhaetic")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    tsv_path = LEXICON_DIR / "xrr.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("Loaded %d existing Rhaetic entries", len(existing))

    # Try strategies in order
    all_entries: list[dict] = []

    logger.info("=== Strategy 1: SMW Ask ===")
    entries = strategy_smw_ask()
    all_entries.extend(entries)
    logger.info("Strategy 1: %d entries", len(entries))

    if not entries:
        logger.info("=== Strategy 2: Category pages ===")
        entries = strategy_category_pages()
        all_entries.extend(entries)
        logger.info("Strategy 2: %d entries", len(entries))

    if not all_entries:
        logger.info("=== Strategy 3: All pages ===")
        entries = strategy_allpages()
        all_entries.extend(entries)
        logger.info("Strategy 3: %d entries", len(entries))

    # Deduplicate
    seen: set[str] = set()
    new_entries: list[dict] = []
    for e in all_entries:
        word = e["word"].strip().lower()
        if not word or word in seen or word in existing:
            continue
        if len(word) > 30:
            continue
        seen.add(word)
        new_entries.append({"word": e["word"].strip(), "gloss": e.get("gloss", "")})

    logger.info("Total new unique entries: %d", len(new_entries))

    if args.dry_run:
        for e in new_entries[:30]:
            print(f"  {e['word']:25s} {e['gloss']}")
        return

    # Append to TSV
    new_count = 0
    audit_trail: list[dict] = []

    if new_entries:
        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                word = e["word"]
                gloss = e["gloss"]

                try:
                    ipa = transliterate(word, "xrr")
                except Exception:
                    ipa = word

                try:
                    sca = ipa_to_sound_class(ipa)
                except Exception:
                    sca = ""

                concept = gloss.split(",")[0].strip() if gloss else ""
                concept_id = concept.replace(" ", "_").lower()[:50] if concept else "-"

                f.write(f"{word}\t{ipa}\t{sca}\ttir_raetica\t{concept_id}\t-\n")
                new_count += 1

                audit_trail.append({
                    "word": word,
                    "gloss": gloss,
                    "ipa": ipa,
                    "source": "tir_raetica",
                })

    # Audit trail
    AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUDIT_TRAIL_DIR / "tir_raetica_xrr.jsonl", "w", encoding="utf-8") as f:
        for r in audit_trail:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(existing) + new_count
    print(f"\nRhaetic (xrr): {len(existing)} existing + {new_count} new = {total} total")


if __name__ == "__main__":
    main()
