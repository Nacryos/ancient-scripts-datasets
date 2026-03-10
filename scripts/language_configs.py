#!/usr/bin/env python3
"""Language extraction configurations for all 19 target ancient languages.

Each config defines:
- iso: ISO 639-3 code (or conventional code for proto-languages)
- name: Language name
- family: Language family
- glottocode: Glottolog code (or NA)
- sources: List of {type, url} dicts specifying data sources
- target_min: Minimum expected entries
- target_max: Maximum expected entries
- priority: P0, P1, or P2
- batch: Which adversarial batch (1, 2, or 3)
"""

from __future__ import annotations

LANGUAGE_CONFIGS: list[dict] = [
    # ===== BATCH 1 (P0) =====
    {
        "iso": "hit",
        "name": "Hittite",
        "family": "anatolian",
        "glottocode": "hitt1242",
        "sources": [
            {"type": "asjp", "url": "https://asjp.clld.org/languages/HITTITE"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Hittite_lemmas"},
        ],
        "target_min": 200,
        "target_max": 800,
        "priority": "P0",
        "batch": 1,
    },
    {
        "iso": "uga",
        "name": "Ugaritic",
        "family": "semitic",
        "glottocode": "ugar1238",
        "sources": [
            {"type": "asjp", "url": "https://asjp.clld.org/languages/UGARITIC"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Ugaritic_lemmas"},
        ],
        "target_min": 200,
        "target_max": 600,
        "priority": "P0",
        "batch": 1,
    },
    {
        "iso": "phn",
        "name": "Phoenician",
        "family": "semitic",
        "glottocode": "phoe1239",
        "sources": [
            {"type": "asjp", "url": "https://asjp.clld.org/languages/PHOENICIAN"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Phoenician_lemmas"},
        ],
        "target_min": 200,
        "target_max": 500,
        "priority": "P0",
        "batch": 1,
    },
    {
        "iso": "xur",
        "name": "Urartian",
        "family": "hurro_urartian",
        "glottocode": "urar1245",
        "sources": [
            {"type": "oracc", "url": "http://oracc.museum.upenn.edu/ecut/json"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Urartian_lemmas"},
        ],
        "target_min": 100,
        "target_max": 400,
        "priority": "P0",
        "batch": 1,
    },
    {
        "iso": "elx",
        "name": "Elamite",
        "family": "elamite",
        "glottocode": "elam1244",
        "sources": [
            {"type": "asjp", "url": "https://asjp.clld.org/languages/ELAMITE"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Elamite_lemmas"},
        ],
        "target_min": 100,
        "target_max": 500,
        "priority": "P0",
        "batch": 1,
    },
    {
        "iso": "ine-pro",
        "name": "Proto-Indo-European",
        "family": "indo_european",
        "glottocode": "NA",
        "sources": [
            {"type": "lrc", "url": "https://lrc.la.utexas.edu/lex"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Appendix:List_of_Proto-Indo-European_roots"},
        ],
        "target_min": 500,
        "target_max": 1000,
        "priority": "P0",
        "batch": 1,
    },
    # ===== BATCH 2 (P1) =====
    {
        "iso": "xlc",
        "name": "Lycian",
        "family": "anatolian",
        "glottocode": "lyci1241",
        "sources": [
            {"type": "ediana", "url": "https://www.ediana.gwi.uni-muenchen.de/"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Lycian_lemmas"},
        ],
        "target_min": 200,
        "target_max": 500,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "xld",
        "name": "Lydian",
        "family": "anatolian",
        "glottocode": "lydi1241",
        "sources": [
            {"type": "ediana", "url": "https://www.ediana.gwi.uni-muenchen.de/"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Lydian_lemmas"},
        ],
        "target_min": 100,
        "target_max": 200,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "xcr",
        "name": "Carian",
        "family": "anatolian",
        "glottocode": "cari1275",
        "sources": [
            {"type": "ediana", "url": "https://www.ediana.gwi.uni-muenchen.de/"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Carian_lemmas"},
        ],
        "target_min": 100,
        "target_max": 300,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "ave",
        "name": "Avestan",
        "family": "iranian",
        "glottocode": "aves1237",
        "sources": [
            {"type": "avesta", "url": "https://www.avesta.org/avdict/avdict.htm"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Avestan_lemmas"},
        ],
        "target_min": 500,
        "target_max": 1000,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "peo",
        "name": "Old Persian",
        "family": "iranian",
        "glottocode": "oldp1254",
        "sources": [
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Old_Persian_lemmas"},
        ],
        "target_min": 300,
        "target_max": 800,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "xle",
        "name": "Lemnian",
        "family": "tyrsenian",
        "glottocode": "lemn1247",
        "sources": [
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Lemnian_lemmas"},
        ],
        "target_min": 30,
        "target_max": 80,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "xpg",
        "name": "Phrygian",
        "family": "paleo_balkan",
        "glottocode": "phry1239",
        "sources": [
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Phrygian_lemmas"},
        ],
        "target_min": 100,
        "target_max": 200,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "sem-pro",
        "name": "Proto-Semitic",
        "family": "semitic",
        "glottocode": "NA",
        "sources": [
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Appendix:Proto-Semitic_reconstructions"},
        ],
        "target_min": 200,
        "target_max": 500,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "ccs-pro",
        "name": "Proto-Kartvelian",
        "family": "kartvelian",
        "glottocode": "NA",
        "sources": [
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Appendix:Proto-Kartvelian_reconstructions"},
        ],
        "target_min": 100,
        "target_max": 400,
        "priority": "P1",
        "batch": 2,
    },
    {
        "iso": "dra-pro",
        "name": "Proto-Dravidian",
        "family": "dravidian",
        "glottocode": "NA",
        "sources": [
            {"type": "dedr", "url": "https://dsal.uchicago.edu/dictionaries/burrow/"},
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Proto-Dravidian_lemmas"},
        ],
        "target_min": 200,
        "target_max": 500,
        "priority": "P1",
        "batch": 2,
    },
    # ===== BATCH 3 (P2) =====
    {
        "iso": "xrr",
        "name": "Rhaetic",
        "family": "tyrsenian",
        "glottocode": "rhae1241",
        "sources": [
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Rhaetic_lemmas"},
        ],
        "target_min": 30,
        "target_max": 100,
        "priority": "P2",
        "batch": 3,
    },
    {
        "iso": "cms",
        "name": "Messapic",
        "family": "paleo_balkan",
        "glottocode": "mess1245",
        "sources": [
            {"type": "wiktionary", "url": "https://en.wiktionary.org/wiki/Category:Messapian_lemmas"},
        ],
        "target_min": 50,
        "target_max": 250,
        "priority": "P2",
        "batch": 3,
    },
]


def get_batch(batch_num: int) -> list[dict]:
    """Get all language configs for a given batch number."""
    return [c for c in LANGUAGE_CONFIGS if c["batch"] == batch_num]


def get_config(iso: str) -> dict | None:
    """Get config for a specific ISO code."""
    for c in LANGUAGE_CONFIGS:
        if c["iso"] == iso:
            return c
    return None
