"""Generate cognate candidate pairs from normalised lexemes."""

from __future__ import annotations

import logging
from collections import defaultdict
from itertools import combinations

from cognate_pipeline.ingest.language_resolver import LanguageResolver
from cognate_pipeline.normalise.models import NormalisedLexeme

logger = logging.getLogger(__name__)

# Known language family groupings for ancient languages in this pipeline.
# Maps language IDs to family labels so we can distinguish inherited
# cognacy (within-family) from areal/chance similarity (cross-family).
_FAMILY_MAP: dict[str, str] = {
    # --- Semitic ---
    "uga": "semitic",
    "heb": "semitic",
    "akk": "semitic",
    "arc": "semitic",
    "phn": "semitic",
    "syc": "semitic",
    "arb": "semitic",
    "amh": "semitic",
    # --- Germanic ---
    "got": "germanic",
    "ang": "germanic",
    "non": "germanic",
    "goh": "germanic",
    # --- Italic ---
    "lat": "italic",
    "osc": "italic",
    "xum": "italic",
    # --- Hellenic ---
    "grc": "hellenic",
    "gmy": "hellenic",
    # --- Celtic ---
    "sga": "celtic",
    "cym": "celtic",
    "bre": "celtic",
    # --- Balto-Slavic ---
    "lit": "balto_slavic",
    "chu": "balto_slavic",
    "rus": "balto_slavic",
    # --- Indo-Iranian ---
    "san": "indo_iranian",
    "ave": "indo_iranian",
    "fas": "indo_iranian",
    # --- Turkic ---
    "otk": "turkic",
    "tur": "turkic",
    "aze": "turkic",
    # --- Uralic ---
    "fin": "uralic",
    "hun": "uralic",
    "est": "uralic",
    # --- Isolates / other ---
    "sux": "sumerian",
    "egy": "egyptian",
    "hit": "anatolian",
    "xib": "iberian",
    "eus": "basque",
}


def _get_family(language_id: str, glottocode: str) -> str:
    """Resolve a language to its family label."""
    lid = language_id.lower()
    if lid in _FAMILY_MAP:
        return _FAMILY_MAP[lid]
    return f"unknown_{lid}"


def generate_candidates(
    lexemes: list[NormalisedLexeme],
    family_aware: bool = False,
) -> list[tuple[NormalisedLexeme, NormalisedLexeme, str]]:
    """Generate candidate pairs conditioned on shared concept_id.

    Within each concept group, all cross-language pairs are generated.
    If family_aware is True, each pair is tagged with a relationship_type:
      - "cognate_inherited": both languages in the same family
      - "similarity_only": languages in different families

    Returns:
        List of (lexeme_a, lexeme_b, relationship_type) tuples
        with lexeme_a.id < lexeme_b.id.
    """
    # Group by concept_id
    by_concept: dict[str, list[NormalisedLexeme]] = defaultdict(list)
    for lex in lexemes:
        if lex.concept_id:
            by_concept[lex.concept_id].append(lex)

    pairs: list[tuple[NormalisedLexeme, NormalisedLexeme, str]] = []
    for concept_id, group in by_concept.items():
        if len(group) < 2:
            continue
        for a, b in combinations(group, 2):
            # Skip same-language pairs
            if a.language_id == b.language_id:
                continue
            # Ensure consistent ordering
            if a.id > b.id:
                a, b = b, a

            rel_type = "cognate_candidate"
            if family_aware:
                fam_a = _get_family(a.language_id, a.glottocode)
                fam_b = _get_family(b.language_id, b.glottocode)
                if fam_a == fam_b:
                    rel_type = "cognate_inherited"
                else:
                    rel_type = "similarity_only"

            pairs.append((a, b, rel_type))

    logger.info(
        "Generated %d candidate pairs from %d concepts",
        len(pairs),
        len(by_concept),
    )
    return pairs
