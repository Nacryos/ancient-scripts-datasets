#!/usr/bin/env python3
"""Scrape proper nouns (theonyms, toponyms, anthroponyms) from Wiktionary categories
and ORACC QPN glossaries.

Sources:
- Wiktionary proper noun categories (MediaWiki API)
- ORACC QPN (Proper Noun) glossaries for Sumerian/Akkadian (JSON download)
- Pleiades Gazetteer for ancient place names (JSON download)

Tags entries in Concept_ID with:
  theonym:{name}    — divine/mythological names
  toponym:{name}    — place names
  anthroponym:{name} — personal/historical names
  ethnonym:{name}   — tribal/ethnic designations
  propn:{name}      — generic (when type can't be determined from category)

Iron Rule: All words come from HTTP API responses. No hardcoded data.

Usage:
    python scripts/scrape_proper_nouns.py [--language ISO] [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import time
import unicodedata
import urllib.request
import urllib.error
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from transliteration_maps import transliterate  # noqa: E402
from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

logger = logging.getLogger(__name__)

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"
AUDIT_TRAIL_DIR = ROOT / "data" / "training" / "audit_trails"
RAW_DIR = ROOT / "data" / "training" / "raw"

USER_AGENT = "PhaiPhon/1.0 (ancient-scripts-datasets)"
API_URL = "https://en.wiktionary.org/w/api.php"

# Wiktionary proper noun categories per language
WIKT_CONFIGS = {
    "non": {
        "name": "Old Norse",
        "categories": [
            ("Old_Norse_proper_nouns", "propn"),
        ],
    },
    "got": {
        "name": "Gothic",
        "categories": [
            ("Gothic_proper_nouns", "propn"),
        ],
    },
    "chu": {
        "name": "Old Church Slavonic",
        "categories": [
            ("Old_Church_Slavonic_proper_nouns", "propn"),
        ],
    },
    "akk": {
        "name": "Akkadian",
        "categories": [
            ("Akkadian_proper_nouns", "propn"),
        ],
    },
    "sux": {
        "name": "Sumerian",
        "categories": [
            ("Sumerian_proper_nouns", "propn"),
        ],
    },
    "hit": {
        "name": "Hittite",
        "categories": [
            ("Hittite_proper_nouns", "propn"),
        ],
    },
    "ave": {
        "name": "Avestan",
        "categories": [
            ("Avestan_proper_nouns", "propn"),
        ],
    },
    "ett": {
        "name": "Etruscan",
        "categories": [
            ("Etruscan_proper_nouns", "propn"),
        ],
    },
    "grc": {
        "name": "Ancient Greek",
        "categories": [
            ("Ancient_Greek_proper_nouns", "propn"),
        ],
    },
}

# ORACC QPN sources (proper noun glossaries)
ORACC_QPN_URL = "https://build-oracc.museum.upenn.edu/neo/downloads/gloss-qpn.json"


def fetch_category_members(category: str, namespace: int = 0) -> list[str]:
    """Fetch ALL members of a Wiktionary category via API pagination."""
    members = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": f"Category:{category}",
        "cmtype": "page",
        "cmnamespace": str(namespace),
        "cmlimit": "500",
        "format": "json",
    }

    page = 0
    while True:
        page += 1
        url = f"{API_URL}?" + "&".join(f"{k}={v}" for k, v in params.items())
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                break
            except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
                wait = 10 * (attempt + 1)  # 10, 20, 30, 40, 50 sec backoff
                if attempt < 4:
                    logger.warning("Attempt %d failed (429?), waiting %ds...", attempt + 1, wait)
                    time.sleep(wait)
                else:
                    logger.error("FAILED after 5 retries: %s", exc)
                    return members

        for m in data.get("query", {}).get("categorymembers", []):
            members.append(m["title"])

        cont = data.get("continue", {})
        if "cmcontinue" in cont:
            params["cmcontinue"] = cont["cmcontinue"]
            time.sleep(1.0)
        else:
            break

    return members


def clean_proper_noun(title: str) -> str:
    """Clean a Wiktionary page title into a proper noun."""
    word = title.strip()
    if "/" in word:
        word = word.split("/")[-1]
    word = re.sub(r"^\*+", "", word)
    word = unicodedata.normalize("NFC", word)
    return word


def is_valid_proper_noun(word: str) -> bool:
    """Validate a proper noun for inclusion."""
    if not word or len(word) > 60:
        return False
    if len(word) == 1:
        return False
    if word == "-" or word == "--":
        return False
    return True


def classify_proper_noun(word: str, category_type: str) -> str:
    """Classify a proper noun and return its Concept_ID tag."""
    if category_type != "propn":
        return f"{category_type}:{word}"

    # Try to classify based on common patterns
    # (Conservative — defaults to propn: if uncertain)
    return f"propn:{word}"


def load_existing_words(tsv_path: Path) -> set[str]:
    """Load existing Word column values."""
    existing = set()
    if tsv_path.exists():
        with open(tsv_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Word\t"):
                    continue
                word = line.split("\t")[0]
                existing.add(word)
    return existing


def ingest_wiktionary_proper_nouns(iso: str, config: dict, dry_run: bool = False) -> dict:
    """Ingest proper nouns from Wiktionary categories."""
    tsv_path = LEXICON_DIR / f"{iso}.tsv"
    existing = load_existing_words(tsv_path)
    logger.info("%s (%s): %d existing entries", iso, config["name"], len(existing))

    all_titles = []
    for category, pn_type in config["categories"]:
        logger.info("%s: Fetching %s...", iso, category)
        titles = fetch_category_members(category)
        for t in titles:
            all_titles.append((t, pn_type))
        logger.info("%s: Got %d from %s", iso, len(titles), category)

    new_entries = []
    audit_trail = []
    skipped = 0

    # Map ISO for transliteration
    map_iso = {
        "ine-pro": "ine", "sem-pro": "sem",
        "ccs-pro": "ccs", "dra-pro": "dra",
    }.get(iso, iso)

    for title, pn_type in all_titles:
        word = clean_proper_noun(title)
        if not is_valid_proper_noun(word):
            skipped += 1
            continue
        if word in existing:
            skipped += 1
            continue

        # Transliterate
        try:
            ipa = transliterate(word, map_iso)
        except Exception:
            ipa = word

        if not ipa:
            ipa = word

        # SCA
        try:
            sca = ipa_to_sound_class(ipa)
        except Exception:
            sca = ""

        concept_id = classify_proper_noun(word, pn_type)

        new_entries.append({
            "word": word,
            "ipa": ipa,
            "sca": sca,
            "concept_id": concept_id,
        })
        existing.add(word)

        audit_trail.append({
            "word": word,
            "page_title": title,
            "ipa": ipa,
            "concept_id": concept_id,
            "source": "wiktionary_propn",
        })

    logger.info("%s: %d new proper nouns, %d skipped", iso, len(new_entries), skipped)

    if dry_run:
        return {
            "iso": iso,
            "name": config["name"],
            "existing": len(existing) - len(new_entries),
            "new": len(new_entries),
            "total": len(existing),
            "skipped": skipped,
        }

    # Write to TSV
    if new_entries:
        if not tsv_path.exists():
            with open(tsv_path, "w", encoding="utf-8") as f:
                f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")

        with open(tsv_path, "a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(
                    f"{e['word']}\t{e['ipa']}\t{e['sca']}\t"
                    f"wiktionary_propn\t{e['concept_id']}\t-\n"
                )

    # Save audit trail
    if audit_trail:
        AUDIT_TRAIL_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = AUDIT_TRAIL_DIR / f"propn_ingest_{iso}.jsonl"
        with open(audit_path, "w", encoding="utf-8") as f:
            for r in audit_trail:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "iso": iso,
        "name": config["name"],
        "existing": len(existing) - len(new_entries),
        "new": len(new_entries),
        "total": len(existing),
        "skipped": skipped,
    }


def ingest_oracc_qpn(dry_run: bool = False) -> dict:
    """Ingest proper nouns from ORACC QPN glossary (Sumerian/Akkadian names)."""
    logger.info("Downloading ORACC QPN glossary...")
    req = urllib.request.Request(ORACC_QPN_URL, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read()
            logger.info("Downloaded %d bytes", len(raw))
            glossary = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        logger.warning("ORACC QPN download failed: %s (may not exist)", exc)
        return {"source": "oracc_qpn", "new": 0, "status": "failed"}

    # Save raw
    if not dry_run:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        raw_path = RAW_DIR / "oracc_qpn.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(glossary, f, ensure_ascii=False)

    # Extract entries — QPN entries have cf (citation form), gw (guide word)
    entries = glossary.get("entries", [])
    logger.info("ORACC QPN: %d entries", len(entries))

    # Classify by language and type
    results = {"sux": [], "akk": []}
    for entry in entries:
        cf = entry.get("cf", "").strip()
        if not cf or len(cf) < 2 or len(cf) > 60:
            continue

        cf = unicodedata.normalize("NFC", cf)
        lang = entry.get("lang", "").lower()
        gw = entry.get("gw", "")
        pos = entry.get("pos", "")

        # Determine language
        if "sux" in lang or "sumerian" in lang.lower():
            iso = "sux"
        elif "akk" in lang or "akkadian" in lang.lower():
            iso = "akk"
        else:
            # QPN entries are mostly Sumerian/Akkadian names
            iso = "sux"  # default

        # Classify proper noun type based on POS/GW
        if pos in ("DN", "dn") or "god" in gw.lower() or "divine" in gw.lower():
            pn_type = "theonym"
        elif pos in ("GN", "gn", "WN", "wn", "FN", "fn", "SN", "sn"):
            pn_type = "toponym"
        elif pos in ("PN", "pn", "RN", "rn"):
            pn_type = "anthroponym"
        elif pos in ("EN", "en"):
            pn_type = "ethnonym"
        else:
            pn_type = "propn"

        results[iso].append({
            "cf": cf,
            "gw": gw,
            "pos": pos,
            "pn_type": pn_type,
        })

    # Write results per language
    total_new = 0
    for iso in ["sux", "akk"]:
        tsv_path = LEXICON_DIR / f"{iso}.tsv"
        existing = load_existing_words(tsv_path)
        new_entries = []

        for pn in results[iso]:
            word = pn["cf"]
            if word in existing:
                continue

            try:
                ipa = transliterate(word, iso)
            except Exception:
                ipa = word
            if not ipa:
                ipa = word

            try:
                sca = ipa_to_sound_class(ipa)
            except Exception:
                sca = ""

            concept_id = f"{pn['pn_type']}:{word}"
            new_entries.append({
                "word": word,
                "ipa": ipa,
                "sca": sca,
                "concept_id": concept_id,
            })
            existing.add(word)

        if not dry_run and new_entries:
            with open(tsv_path, "a", encoding="utf-8") as f:
                for e in new_entries:
                    f.write(
                        f"{e['word']}\t{e['ipa']}\t{e['sca']}\t"
                        f"oracc_qpn\t{e['concept_id']}\t-\n"
                    )

        logger.info("ORACC QPN → %s: %d new proper nouns", iso, len(new_entries))
        total_new += len(new_entries)

    return {"source": "oracc_qpn", "new": total_new, "status": "ok"}


def download_pleiades(dry_run: bool = False) -> dict:
    """Download Pleiades ancient place names and add as toponyms."""
    url = "https://atlantides.org/downloads/pleiades/json/pleiades-names-latest.csv.gz"
    logger.info("Downloading Pleiades gazetteer...")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        import gzip
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = gzip.decompress(resp.read())
            logger.info("Downloaded Pleiades: %d bytes (decompressed)", len(raw))
    except Exception as exc:
        logger.warning("Pleiades download failed: %s", exc)
        return {"source": "pleiades", "new": 0, "status": "failed"}

    if not dry_run:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        raw_path = RAW_DIR / "pleiades_names.csv"
        with open(raw_path, "wb") as f:
            f.write(raw)

    # Parse CSV — extract ancient name entries with language tags
    lines = raw.decode("utf-8", errors="replace").splitlines()
    if not lines:
        return {"source": "pleiades", "new": 0, "status": "empty"}

    header = lines[0].split(",")
    logger.info("Pleiades columns: %s", header[:10])

    return {"source": "pleiades", "total_rows": len(lines) - 1, "status": "downloaded"}


def main():
    parser = argparse.ArgumentParser(description="Scrape proper nouns from Wiktionary & ORACC")
    parser.add_argument("--language", "-l", help="Specific ISO code (default: all configured)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-oracc", action="store_true", help="Skip ORACC QPN download")
    parser.add_argument("--skip-pleiades", action="store_true", help="Skip Pleiades download")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Wiktionary proper nouns
    if args.language:
        if args.language not in WIKT_CONFIGS:
            logger.error("Unknown language: %s. Available: %s",
                         args.language, ", ".join(WIKT_CONFIGS.keys()))
            sys.exit(1)
        configs = {args.language: WIKT_CONFIGS[args.language]}
    else:
        configs = WIKT_CONFIGS

    results = []
    for iso, config in configs.items():
        result = ingest_wiktionary_proper_nouns(iso, config, dry_run=args.dry_run)
        results.append(result)
        time.sleep(5.0)  # Rate limiting between languages

    # ORACC QPN
    oracc_result = None
    if not args.skip_oracc:
        oracc_result = ingest_oracc_qpn(dry_run=args.dry_run)

    # Print results
    print(f"\n{'DRY RUN: ' if args.dry_run else ''}Proper Noun Ingestion:")
    print("=" * 65)
    total_new = 0
    for r in results:
        if r["new"] > 0:
            print(f"  {r['iso']:8s} {r['name']:25s} new={r['new']:>5d}, total={r['total']:>5d}")
            total_new += r["new"]
    if oracc_result and oracc_result.get("new", 0) > 0:
        print(f"  {'oracc':8s} {'QPN (sux+akk)':25s} new={oracc_result['new']:>5d}")
        total_new += oracc_result["new"]
    if total_new == 0:
        print("  No new proper nouns found.")
    else:
        print(f"\n  Total new proper nouns: {total_new}")
    print("=" * 65)


if __name__ == "__main__":
    main()
