#!/usr/bin/env python3
"""Reusable framework for extracting ancient language data from online sources.

Orchestrates multiple source-specific parsers (ASJP, Wiktionary, Oracc,
eDiAna, Avesta, LRC, DEDR) to build a unified TSV lexicon file for any
ancient or reconstructed language.

Pipeline:
    1. For each source in the config, call the appropriate parser
    2. Apply transliteration -> IPA mapping via transliteration_maps
    3. Generate SCA sound-class codes via cognate_pipeline
    4. Deduplicate by (word, ipa)
    5. Write output TSV

Usage:
    python extract_ancient_language.py \\
        --iso hit --name Hittite --family anatolian \\
        --source asjp:https://asjp.clld.org/languages/HITTITE \\
        --source wiktionary:https://en.wiktionary.org/wiki/Appendix:Hittite_Swadesh_list

Output: data/training/lexicons/{iso}.tsv
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup: make cognate_pipeline importable
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402

# ---------------------------------------------------------------------------
# Transliteration maps (same scripts/ directory)
# ---------------------------------------------------------------------------
# transliteration_maps is expected to live alongside this script and
# expose a function: transliterate(translit: str, iso: str) -> str
# If it doesn't exist yet, we provide a passthrough fallback.
try:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from transliteration_maps import transliterate as _transliterate  # type: ignore
except ImportError:
    logging.getLogger(__name__).warning(
        "transliteration_maps not found in scripts/; "
        "IPA output will use raw transliteration forms"
    )

    def _transliterate(translit: str, iso: str) -> str:  # type: ignore[misc]
        """Passthrough: return transliteration as-is when no map is available."""
        return translit

# ---------------------------------------------------------------------------
# Parser registry
# ---------------------------------------------------------------------------
PARSER_MODULES: dict[str, str] = {
    "asjp": "scripts.parsers.parse_asjp",
    "wiktionary": "scripts.parsers.parse_wiktionary",
    "oracc": "scripts.parsers.parse_oracc",
    "ediana": "scripts.parsers.parse_ediana",
    "avesta": "scripts.parsers.parse_avesta",
    "lrc": "scripts.parsers.parse_lrc",
    "dedr": "scripts.parsers.parse_dedr",
}

# Also support direct import if running from the scripts/ directory
_PARSER_MODULES_ALT: dict[str, str] = {
    "asjp": "parsers.parse_asjp",
    "wiktionary": "parsers.parse_wiktionary",
    "oracc": "parsers.parse_oracc",
    "ediana": "parsers.parse_ediana",
    "avesta": "parsers.parse_avesta",
    "lrc": "parsers.parse_lrc",
    "dedr": "parsers.parse_dedr",
}

logger = logging.getLogger(__name__)


def _load_parser(source_type: str):
    """Dynamically import and return the parse() function for a source type."""
    if source_type not in PARSER_MODULES:
        raise ValueError(
            f"Unknown source type '{source_type}'. "
            f"Available: {', '.join(sorted(PARSER_MODULES))}"
        )

    # Try the fully-qualified module path first, then the relative one
    for module_map in (PARSER_MODULES, _PARSER_MODULES_ALT):
        module_name = module_map[source_type]
        try:
            mod = importlib.import_module(module_name)
            return mod.parse
        except ImportError:
            continue

    raise ImportError(
        f"Could not import parser module for '{source_type}'. "
        f"Tried: {PARSER_MODULES[source_type]}, {_PARSER_MODULES_ALT[source_type]}"
    )


def extract_language(config: dict) -> Path:
    """Extract language data from multiple sources and write a unified TSV.

    Args:
        config: Dictionary with keys:
            iso (str): ISO 639-3 code (e.g. "hit" for Hittite)
            name (str): Human-readable language name
            family (str): Language family (e.g. "anatolian")
            sources (list[dict]): List of source descriptors, each with:
                type (str): Parser type (asjp, wiktionary, oracc, etc.)
                url (str): URL to download from
                ... additional kwargs passed to the parser
            target_dir (str | Path | None): Output directory. Defaults to
                data/training/lexicons/ relative to repo root.

    Returns:
        Path to the output TSV file.
    """
    iso = config["iso"]
    name = config["name"]
    family = config.get("family", "unknown")
    sources = config.get("sources", [])
    target_dir = config.get("target_dir")

    if target_dir is None:
        target_dir = ROOT / "data" / "training" / "lexicons"
    else:
        target_dir = Path(target_dir)

    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / f"{iso}.tsv"

    logger.info(
        "Extracting %s (%s, family=%s) from %d source(s)",
        name, iso, family, len(sources),
    )

    # ------------------------------------------------------------------
    # Step 1: Collect raw entries from all sources
    # ------------------------------------------------------------------
    all_entries: list[dict] = []

    for source in sources:
        source_type = source["type"]
        source_url = source["url"]
        # Pass any extra kwargs to the parser
        extra_kwargs = {k: v for k, v in source.items() if k not in ("type", "url")}

        logger.info("  Source: %s -> %s", source_type, source_url)
        try:
            parse_fn = _load_parser(source_type)
            raw_entries = parse_fn(source_url, **extra_kwargs)
        except Exception:
            logger.exception(
                "  Failed to parse source %s (%s)", source_type, source_url
            )
            raw_entries = []

        # Tag each entry with its source type
        for entry in raw_entries:
            entry["_source"] = source_type
        all_entries.extend(raw_entries)

    logger.info("  Total raw entries: %d", len(all_entries))

    if not all_entries:
        logger.warning("  No entries extracted for %s -- writing empty TSV", name)
        output_path.write_text(
            "Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n",
            encoding="utf-8",
        )
        return output_path

    # ------------------------------------------------------------------
    # Step 2: Transliteration -> IPA -> SCA
    # ------------------------------------------------------------------
    processed: list[tuple[str, str, str, str, str, str]] = []
    for entry in all_entries:
        word = entry.get("word", "").strip()
        translit = entry.get("transliteration", word).strip()
        gloss = entry.get("gloss", "").strip()
        source_tag = entry.get("_source", "unknown")

        if not word:
            continue

        # Apply transliteration -> IPA mapping
        ipa = _transliterate(translit, iso)
        if not ipa:
            ipa = translit  # Fallback: use raw transliteration

        # Generate SCA code
        sca = ipa_to_sound_class(ipa)

        # Use gloss as a concept ID (normalized)
        concept_id = gloss.lower().replace(" ", "_")[:50] if gloss else "-"
        cognate_set_id = "-"

        processed.append((word, ipa, sca, source_tag, concept_id, cognate_set_id))

    # ------------------------------------------------------------------
    # Step 3: Deduplicate by (word, ipa)
    # ------------------------------------------------------------------
    seen: set[tuple[str, str]] = set()
    deduplicated: list[tuple[str, str, str, str, str, str]] = []
    for entry_tuple in processed:
        key = (entry_tuple[0], entry_tuple[1])  # (word, ipa)
        if key not in seen:
            seen.add(key)
            deduplicated.append(entry_tuple)

    dupes_removed = len(processed) - len(deduplicated)
    if dupes_removed > 0:
        logger.info("  Removed %d duplicate entries", dupes_removed)

    # ------------------------------------------------------------------
    # Step 4: Write output TSV
    # ------------------------------------------------------------------
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")
        for word, ipa, sca, source_tag, concept_id, cognate_set_id in sorted(
            deduplicated, key=lambda x: x[0].lower()
        ):
            f.write(
                f"{word}\t{ipa}\t{sca}\t{source_tag}\t{concept_id}\t{cognate_set_id}\n"
            )

    logger.info(
        "  Wrote %d entries to %s", len(deduplicated), output_path
    )
    return output_path


def _parse_source_arg(source_str: str) -> dict:
    """Parse a --source argument of the form 'type:url'.

    Returns:
        Dict with keys 'type' and 'url'.
    """
    if ":" not in source_str:
        raise argparse.ArgumentTypeError(
            f"Invalid source format: '{source_str}'. Expected 'type:url'"
        )
    # Split on first colon only (URLs contain colons)
    source_type, url = source_str.split(":", 1)
    source_type = source_type.strip().lower()
    url = url.strip()

    if source_type not in PARSER_MODULES:
        raise argparse.ArgumentTypeError(
            f"Unknown source type '{source_type}'. "
            f"Available: {', '.join(sorted(PARSER_MODULES))}"
        )
    if not url:
        raise argparse.ArgumentTypeError(
            f"Empty URL for source type '{source_type}'"
        )

    return {"type": source_type, "url": url}


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract ancient language data from online sources into a TSV lexicon.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python extract_ancient_language.py \\\n"
            "      --iso hit --name Hittite --family anatolian \\\n"
            "      --source asjp:https://asjp.clld.org/languages/HITTITE \\\n"
            "      --source wiktionary:https://en.wiktionary.org/wiki/Appendix:Hittite_Swadesh_list\n"
            "\n"
            "  python extract_ancient_language.py \\\n"
            "      --iso xur --name Urartian --family hurro-urartian \\\n"
            "      --source oracc:http://oracc.museum.upenn.edu/ecut\n"
            "\n"
            f"Available source types: {', '.join(sorted(PARSER_MODULES))}\n"
            "Source format: type:url\n"
        ),
    )
    parser.add_argument(
        "--iso", required=True,
        help="ISO 639-3 language code (e.g. hit, xur, xcl)",
    )
    parser.add_argument(
        "--name", required=True,
        help="Human-readable language name (e.g. Hittite, Urartian)",
    )
    parser.add_argument(
        "--family", default="unknown",
        help="Language family (e.g. anatolian, hurro-urartian, semitic)",
    )
    parser.add_argument(
        "--source", action="append", dest="sources", default=[],
        type=_parse_source_arg,
        help="Source in format type:url (can be specified multiple times)",
    )
    parser.add_argument(
        "--target-dir", default=None,
        help="Output directory (default: data/training/lexicons/)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.sources:
        parser.error("At least one --source is required")

    config = {
        "iso": args.iso,
        "name": args.name,
        "family": args.family,
        "sources": args.sources,
        "target_dir": args.target_dir,
    }

    output_path = extract_language(config)
    print(f"\nOutput written to: {output_path}")


if __name__ == "__main__":
    main()
