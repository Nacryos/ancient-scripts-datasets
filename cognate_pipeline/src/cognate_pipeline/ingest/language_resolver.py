"""Resolve language identifiers to Glottocodes."""

from __future__ import annotations

import logging
from typing import Any

from cognate_pipeline.utils.glottolog import GlottologTree

logger = logging.getLogger(__name__)

# Well-known mappings for ancient languages that may not be in Glottolog
_HARDCODED: dict[str, str] = {
    # --- Existing ---
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
    # --- Germanic ---
    "ang": "olde1238",
    "non": "oldn1244",
    "goh": "oldh1241",
    # --- Celtic ---
    "sga": "oldi1245",
    "cym": "wels1247",
    "bre": "bret1244",
    # --- Balto-Slavic ---
    "lit": "lith1251",
    "chu": "chur1257",
    "rus": "russ1263",
    # --- Indo-Iranian ---
    "san": "sans1269",
    "ave": "aves1237",
    "fas": "west2369",
    # --- Italic ---
    "osc": "osca1245",
    "xum": "umbr1253",
    # --- Hellenic ---
    "gmy": "myce1241",
    # --- Semitic ---
    "arb": "stan1318",
    "amh": "amha1245",
    # --- Turkic ---
    "otk": "oldt1247",
    "tur": "nucl1301",
    "aze": "nort2697",
    # --- Uralic ---
    "fin": "finn1318",
    "hun": "hung1274",
    "est": "esto1258",
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
