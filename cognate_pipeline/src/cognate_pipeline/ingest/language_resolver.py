"""Resolve language identifiers to Glottocodes."""

from __future__ import annotations

import logging
from typing import Any

from cognate_pipeline.utils.glottolog import GlottologTree

logger = logging.getLogger(__name__)

# Well-known mappings for ancient languages that may not be in Glottolog
_HARDCODED: dict[str, str] = {
    "uga": "ugar1238",
    "heb": "hebr1245",
    "got": "goth1244",
    "xib": "iber1250",
    "akk": "akka1240",
    "sux": "sume1241",
    "lat": "lati1261",
    "grc": "anci1242",
    "arc": "offi1241",
    "egy": "egyp1253",
    "hit": "hitt1242",
    "phn": "phoe1239",
    "syc": "clas1252",
    "eus": "basq1248",
}


class LanguageResolver:
    """Resolve language identifiers to Glottocodes.

    Resolution chain:
    1. Direct Glottocode (4-char alpha + 4-digit pattern)
    2. Hardcoded ancient language mappings
    3. Glottolog tree lookup (by code, ISO-639-3, or name)
    """

    def __init__(self, glottolog_tree: GlottologTree | None = None) -> None:
        self._tree = glottolog_tree

    def resolve(self, identifier: str) -> str:
        """Return a Glottocode for the given identifier, or '' if unresolved."""
        identifier = identifier.strip()
        if not identifier:
            return ""

        # Check if it's already a Glottocode pattern (xxxx1234)
        if len(identifier) == 8 and identifier[:4].isalpha() and identifier[4:].isdigit():
            return identifier

        # Hardcoded mappings
        if identifier.lower() in _HARDCODED:
            return _HARDCODED[identifier.lower()]

        # Glottolog tree lookup
        if self._tree is not None:
            lang = self._tree.lookup(identifier)
            if lang is not None:
                return lang.glottocode

        logger.debug("Could not resolve language identifier: %s", identifier)
        return ""
