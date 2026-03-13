#!/usr/bin/env python3
"""Fetch Wiktionary category members and save as raw JSON for later processing.

Uses curl with retry-after header respect. Designed to handle rate limiting
gracefully by waiting the specified time between retries.

Iron Rule: All data comes from HTTP API responses.

Usage:
    python scripts/fetch_wiktionary_raw.py [--language ISO]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "training" / "raw"

logger = logging.getLogger(__name__)

API_URL = "https://en.wiktionary.org/w/api.php"
USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets; academic research)"

# All categories: (category_name, namespace)
CATEGORIES = {
    "cop": ("Coptic_lemmas", 0),
    "pli": ("Pali_lemmas", 0),
    "xcl": ("Old_Armenian_lemmas", 0),
    "ang": ("Old_English_lemmas", 0),
    "gez": ("Ge%27ez_lemmas", 0),
    "hbo": ("Hebrew_lemmas", 0),
    "xht": ("Hattic_lemmas", 0),
    # Tier 3 + Proto-languages
    "gem-pro": ("Proto-Germanic_lemmas", 118),
    "cel-pro": ("Proto-Celtic_lemmas", 118),
    "urj-pro": ("Proto-Uralic_lemmas", 118),
    "nci": ("Classical_Nahuatl_lemmas", 0),
    "sga": ("Old_Irish_lemmas", 0),
}


def fetch_one_page(url: str) -> tuple[str, int]:
    """Fetch one URL via curl. Returns (body, retry_after_secs)."""
    result = subprocess.run(
        ["curl", "-s", "-D", "-",
         "-H", f"User-Agent: {USER_AGENT}",
         url],
        capture_output=True, text=True, timeout=60,
    )
    output = result.stdout
    # Split headers from body
    parts = output.split("\r\n\r\n", 1)
    if len(parts) < 2:
        parts = output.split("\n\n", 1)

    headers = parts[0] if parts else ""
    body = parts[1] if len(parts) > 1 else ""

    # Check for 429
    retry_after = 0
    if "429" in headers.split("\n")[0]:
        for line in headers.split("\n"):
            if line.lower().startswith("retry-after:"):
                try:
                    retry_after = int(line.split(":", 1)[1].strip())
                except ValueError:
                    retry_after = 300  # Default 5 min
        if retry_after == 0:
            retry_after = 300

    return body, retry_after


def fetch_category(iso: str, category: str, namespace: int = 0) -> list[str]:
    """Fetch all members of a Wiktionary category, respecting rate limits."""
    members = []
    base = (
        f"action=query&list=categorymembers&cmtitle=Category:{category}"
        f"&cmtype=page&cmnamespace={namespace}&cmlimit=500&format=json"
    )
    extra = ""
    page = 0

    while True:
        page += 1
        url = f"{API_URL}?{base}{extra}"

        body, retry_after = fetch_one_page(url)

        if retry_after > 0:
            logger.warning("%s: Rate limited. Retry-After=%d seconds (%.1f min). Waiting...",
                           iso, retry_after, retry_after / 60)
            time.sleep(retry_after + 5)
            # Retry after waiting
            body, retry_after = fetch_one_page(url)
            if retry_after > 0:
                logger.error("%s: Still rate limited after waiting. Aborting.", iso)
                return members

        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            logger.error("%s: Invalid JSON on page %d. Body: %s", iso, page, body[:200])
            return members

        for m in data.get("query", {}).get("categorymembers", []):
            members.append(m["title"])

        cont = data.get("continue", {})
        if "cmcontinue" in cont:
            extra = f"&cmcontinue={cont['cmcontinue']}"
            if page % 5 == 0:
                logger.info("  %s page %d: %d members...", iso, page, len(members))
            time.sleep(1.5)  # Be nice
        else:
            break

    return members


def main():
    parser = argparse.ArgumentParser(description="Fetch Wiktionary category raw data")
    parser.add_argument("--language", "-l", help="Specific ISO code")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if args.language:
        cats = {args.language: CATEGORIES[args.language]}
    else:
        cats = CATEGORIES

    for iso, cat_info in cats.items():
        category, namespace = cat_info
        raw_path = RAW_DIR / f"wiktionary_category_{iso}.json"
        if raw_path.exists():
            with open(raw_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            logger.info("%s: Already cached (%d members). Skipping.", iso, len(existing.get("members", [])))
            continue

        logger.info("%s: Fetching %s (ns=%d)...", iso, category, namespace)
        members = fetch_category(iso, category, namespace)
        logger.info("%s: Got %d members", iso, len(members))

        if members:
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump({"category": category, "members": members}, f, ensure_ascii=False)
            logger.info("%s: Saved to %s", iso, raw_path)

        # Pause between languages to be polite
        time.sleep(5)


if __name__ == "__main__":
    main()
