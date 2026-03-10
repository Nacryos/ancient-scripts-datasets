"""
Transliteration-to-IPA mapping dictionaries for ancient and reconstructed languages.

Each map converts conventional scholarly transliteration to broad IPA transcription.
Maps are pure data constants with no logic beyond the lookup utilities at the bottom.

Sources
-------
- Hittite:          Hoffner & Melchert (2008), *A Grammar of the Hittite Language*
- Ugaritic:         Tropper (2000), *Ugaritische Grammatik*
- Phoenician:       Standard 22-letter abjad reconstruction
- Urartian:         Wegner (2007), *Hurritisch: Eine Einfuehrung* (Urartian appendix)
- Elamite:          Grillot-Susini (1987); Stolper (2004)
- Lycian:           Melchert (2004), *A Dictionary of the Lycian Language*
- Lydian:           Gusmani (1964), *Lydisches Woerterbuch*; Melchert supplements
- Carian:           Adiego (2007), *The Carian Language*
- Avestan:          Hoffmann & Forssman (1996), *Avestische Laut- und Flexionslehre*
- Old Persian:      Kent (1953), *Old Persian: Grammar, Texts, Lexicon*
- Proto-Indo-European: Standard comparative notation (Fortson 2010, Beekes 2011)
- Proto-Semitic:    Standard Semitist notation (Huehnergard 2019)
- Proto-Kartvelian: Klimov (1998), *Etymological Dictionary of the Kartvelian Languages*
- Proto-Dravidian (DEDR): Krishnamurti (2003), *The Dravidian Languages*
- Phrygian:         Brixhe & Lejeune (1984); Obrador-Cursach (2020)
- Lemnian:          Greek-alphabet based reconstruction
- Rhaetic:          North Italic alphabet reconstruction
- Messapic:         Greek-alphabet based reconstruction
"""

from __future__ import annotations

from typing import Dict, Optional


# ---------------------------------------------------------------------------
# 1. HITTITE  (Hoffner & Melchert 2008)
# ---------------------------------------------------------------------------
HITTITE_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "aa": "aː", "e": "e", "ee": "eː",
    "i": "i", "ii": "iː", "u": "u", "uu": "uː",
    # Stops (lenis)
    "p": "p", "b": "p", "t": "t", "d": "t", "k": "k", "g": "k",
    # Stops (fortis/geminate)
    "pp": "pː", "bb": "pː", "tt": "tː", "dd": "tː", "kk": "kː", "gg": "kː",
    # Fricatives
    "h": "x", "hh": "xː",
    # Sibilants
    "s": "s", "ss": "sː", "z": "ts", "zz": "tsː",
    # Sonorants
    "l": "l", "ll": "lː", "m": "m", "n": "n", "r": "r",
    # Glides
    "w": "w", "y": "j",
}

# ---------------------------------------------------------------------------
# 2. UGARITIC  (Tropper 2000)
# ---------------------------------------------------------------------------
UGARITIC_MAP: Dict[str, str] = {
    "'a": "ʔa", "'i": "ʔi", "'u": "ʔu",
    "b": "b", "g": "ɡ", "d": "d", "h": "h", "w": "w",
    "z": "z", "ḥ": "ħ", "ṭ": "tˤ", "y": "j", "k": "k",
    "š": "ʃ", "l": "l", "m": "m", "ḏ": "ð", "n": "n",
    "ẓ": "sˤ", "s": "s", "ʿ": "ʕ", "p": "p", "ṯ": "θ",
    "q": "q", "r": "r", "ś": "ɬ", "t": "t", "ġ": "ɣ", "ṱ": "θˤ",
    # Simple fallbacks
    "'": "ʔ",
}

# ---------------------------------------------------------------------------
# 3. PHOENICIAN  (22-letter abjad)
# ---------------------------------------------------------------------------
PHOENICIAN_MAP: Dict[str, str] = {
    "'": "ʔ", "ʾ": "ʔ", "b": "b", "g": "ɡ", "d": "d", "h": "h",
    "w": "w", "z": "z", "ḥ": "ħ", "ṭ": "tˤ", "y": "j",
    "k": "k", "l": "l", "m": "m", "n": "n", "s": "s",
    "ʿ": "ʕ", "p": "p", "ṣ": "sˤ", "q": "q", "r": "r",
    "š": "ʃ", "t": "t",
}

# ---------------------------------------------------------------------------
# 4. URARTIAN  (Wegner 2007)
# ---------------------------------------------------------------------------
URARTIAN_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "u": "u",
    "b": "b", "d": "d", "g": "ɡ", "ḫ": "x", "h": "x",
    "k": "k", "l": "l", "m": "m", "n": "n", "p": "p",
    "q": "q", "r": "r", "s": "s", "š": "ʃ", "t": "t", "z": "ts",
}

# ---------------------------------------------------------------------------
# 5. ELAMITE  (Grillot-Susini 1987, Stolper 2004)
# ---------------------------------------------------------------------------
ELAMITE_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "u": "u",
    "b": "b", "d": "d", "g": "ɡ", "h": "h", "ḫ": "x",
    "k": "k", "l": "l", "m": "m", "n": "n", "p": "p",
    "r": "r", "s": "s", "š": "ʃ", "t": "t", "z": "ts",
}

# ---------------------------------------------------------------------------
# 6. LYCIAN  (Melchert 2004)
# ---------------------------------------------------------------------------
LYCIAN_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "u": "u",
    "ã": "ã", "ẽ": "ẽ",  # nasalized vowels
    "b": "b", "d": "d", "g": "ɡ", "h": "x",
    "k": "k", "l": "l", "m": "m", "n": "n", "p": "p",
    "q": "kʷ", "r": "r", "s": "s", "t": "t", "w": "w",
    "z": "ts", "θ": "θ", "χ": "kʰ", "ñ": "ɲ",
    "λ": "l̩", "τ": "tʰ",
}

# ---------------------------------------------------------------------------
# 7. LYDIAN  (Gusmani 1964, Melchert)
# ---------------------------------------------------------------------------
LYDIAN_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    "b": "b", "d": "d", "g": "ɡ", "k": "k", "l": "l",
    "m": "m", "n": "n", "p": "p", "r": "r", "s": "s",
    "t": "t", "v": "v", "w": "w", "y": "j",
    "ś": "ʃ", "τ": "tʰ", "λ": "lː", "ñ": "ɲ", "q": "kʷ",
    "f": "f",
}

# ---------------------------------------------------------------------------
# 8. CARIAN  (Adiego 2007)
# ---------------------------------------------------------------------------
CARIAN_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    "b": "b", "d": "d", "g": "ɡ", "k": "k", "l": "l",
    "m": "m", "n": "n", "p": "p", "r": "r", "s": "s",
    "t": "t", "w": "w", "y": "j",
    "š": "ʃ", "ś": "ɕ", "q": "kʷ", "λ": "l̩",
    "τ": "tʰ", "δ": "ð", "χ": "kʰ", "ñ": "ɲ",
}

# ---------------------------------------------------------------------------
# 9. AVESTAN  (Hoffmann & Forssman 1996)
# ---------------------------------------------------------------------------
AVESTAN_MAP: Dict[str, str] = {
    # Short vowels
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    # Long vowels
    "ā": "aː", "ē": "eː", "ī": "iː", "ō": "oː", "ū": "uː",
    # Nasalized vowels
    "ą": "ã", "ę": "ẽ",
    # Schwa
    "ə": "ə",
    # Stops
    "p": "p", "b": "b", "t": "t", "d": "d",
    "k": "k", "g": "ɡ",
    # Fricatives
    "f": "f", "v": "v", "θ": "θ", "δ": "ð",
    "x": "x", "xᵛ": "xʷ", "γ": "ɣ",
    "s": "s", "z": "z", "š": "ʃ", "ž": "ʒ",
    # Affricates
    "c": "tʃ", "j": "dʒ",
    # Nasals
    "m": "m", "n": "n", "ń": "ɲ", "ŋ": "ŋ", "ŋᵛ": "ŋʷ",
    # Liquids
    "r": "r", "l": "l",
    # Glides
    "y": "j", "w": "w",
    # Other
    "β": "β",
    "h": "h",
}

# ---------------------------------------------------------------------------
# 10. OLD PERSIAN  (Kent 1953)
# ---------------------------------------------------------------------------
OLD_PERSIAN_MAP: Dict[str, str] = {
    "a": "a", "i": "i", "u": "u",
    "ā": "aː", "ī": "iː", "ū": "uː",
    "p": "p", "b": "b", "t": "t", "d": "d",
    "k": "k", "g": "ɡ", "c": "tʃ", "j": "dʒ",
    "f": "f", "θ": "θ", "s": "s", "š": "ʃ",
    "x": "x", "h": "h",
    "m": "m", "n": "n",
    "r": "r", "l": "l",
    "v": "v", "w": "w", "y": "j",
}

# ---------------------------------------------------------------------------
# 11. PROTO-INDO-EUROPEAN  (standard comparative notation)
# ---------------------------------------------------------------------------
PIE_MAP: Dict[str, str] = {
    # Vowels
    "e": "e", "o": "o", "a": "a", "i": "i", "u": "u",
    "ē": "eː", "ō": "oː", "ā": "aː", "ī": "iː", "ū": "uː",
    # Schwa
    "ə": "ə",
    # Stops (plain voiceless)
    "p": "p", "t": "t", "ḱ": "k", "k": "k", "kʷ": "kʷ",
    # Stops (voiced)
    "b": "b", "d": "d", "ǵ": "ɡ", "g": "ɡ", "gʷ": "ɡʷ",
    # Stops (voiced aspirate)
    "bʰ": "bʱ", "dʰ": "dʱ", "ǵʰ": "ɡʱ", "gʰ": "ɡʱ", "gʷʰ": "ɡʷʱ",
    # Laryngeals
    "h₁": "h", "h₂": "ħ", "h₃": "ɣʷ",
    "H": "h",  # generic laryngeal
    # Fricatives/sibilant
    "s": "s",
    # Sonorants
    "m": "m", "n": "n", "l": "l", "r": "r",
    # Glides
    "w": "w", "y": "j",
    # Syllabic sonorants
    "m̥": "m̩", "n̥": "n̩", "l̥": "l̩", "r̥": "r̩",
}

# ---------------------------------------------------------------------------
# 12. PROTO-SEMITIC
# ---------------------------------------------------------------------------
PROTO_SEMITIC_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "i": "i", "u": "u",
    "ā": "aː", "ī": "iː", "ū": "uː",
    "e": "e", "ē": "eː", "o": "o", "ō": "oː",
    # Unspecified vowel (Kogan convention)
    "V": "a",
    # Plain stops
    "b": "b", "d": "d", "g": "ɡ", "k": "k", "p": "p", "t": "t",
    # Emphatic stops
    "ṭ": "tˤ", "ḳ": "kˤ", "ḍ": "dˤ",
    # Fricatives
    "f": "f", "s": "s", "z": "z", "š": "ʃ",
    "ś": "ɬ", "ṣ": "sˤ", "ẓ": "zˤ",
    "ḏ": "ð", "ṯ": "θ", "ṱ": "θˤ", "ġ": "ɣ",
    # Pharyngeals / Laryngeals
    "ḥ": "ħ", "ʿ": "ʕ", "ʾ": "ʔ", "'": "ʔ",
    "h": "h",
    # Velar/uvular fricative (used in Kogan notation)
    "ḫ": "x",
    # Uvular
    "q": "q",
    # Sonorants
    "m": "m", "n": "n", "l": "l", "r": "r",
    # Glides
    "w": "w", "y": "j",
}

# ---------------------------------------------------------------------------
# 13. PROTO-KARTVELIAN  (Klimov 1998)
# ---------------------------------------------------------------------------
PROTO_KARTVELIAN_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    # Plain stops
    "p": "p", "t": "t", "k": "k", "q": "q",
    "b": "b", "d": "d", "g": "ɡ",
    # Ejectives (apostrophe notation)
    "p'": "pʼ", "t'": "tʼ", "k'": "kʼ", "q'": "qʼ",
    "c'": "tsʼ", "č'": "tʃʼ", "ċ'": "tsʼ",
    # Ejectives (Klimov underdot notation from Wiktionary)
    "ḳ": "kʼ", "ṭ": "tʼ",
    "p̣": "pʼ", "q̣": "qʼ",
    "č̣": "tʃʼ", "c̣": "tsʼ",
    # Aspirates
    "pʰ": "pʰ", "tʰ": "tʰ", "kʰ": "kʰ", "qʰ": "qʰ",
    # Affricates
    "c": "ts", "č": "tʃ", "ċ": "ts",
    "ʒ": "dz", "ǯ": "dʒ",
    # Fricatives
    "s": "s", "z": "z", "š": "ʃ", "ž": "ʒ",
    "x": "x", "γ": "ɣ", "ɣ": "ɣ", "h": "h",
    # Voiced affricates / fricatives (Klimov)
    "ʓ": "dz", "ʓ̌": "dʒ",
    # Sonorants
    "m": "m", "n": "n", "l": "l", "r": "r",
    # Glides
    "w": "w", "y": "j",
}

# ---------------------------------------------------------------------------
# 14. DEDR / PROTO-DRAVIDIAN  (Krishnamurti 2003)
# ---------------------------------------------------------------------------
DEDR_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "ā": "aː", "i": "i", "ī": "iː",
    "u": "u", "ū": "uː", "e": "e", "ē": "eː",
    "o": "o", "ō": "oː",
    # Unspecified vowel (DEDR convention)
    "V": "a",  # map to default vowel 'a'
    "H": "h",  # laryngeal (DEDR convention)
    # Capital vowels with macron (used in some Wiktionary entries)
    "Ā": "aː", "Ā": "aː",
    # Stops (dental)
    "t": "t̪", "d": "d̪",
    # Stops (alveolar)
    "ṯ": "t",  # alveolar stop (Krishnamurti notation)
    # Stops (retroflex)
    "ṭ": "ʈ", "ḍ": "ɖ", "T": "ʈ", "D": "ɖ",
    # Stops (other)
    "p": "p", "b": "b", "k": "k", "g": "ɡ",
    "c": "tʃ",
    # Nasals
    "m": "m", "n": "n̪", "ṇ": "ɳ", "N": "ɳ",
    "ñ": "ɲ", "ŋ": "ŋ",
    # Liquids
    "l": "l", "ḷ": "ɭ", "L": "ɭ",
    "r": "r", "ṛ": "ɽ", "ḻ": "ɻ",
    # Fricatives
    "s": "s", "ṣ": "ʂ", "S": "ʂ", "h": "h",
    # Glides
    "v": "ʋ", "y": "j", "w": "w",
    # Special
    "ẓ": "ɻ",  # DEDR convention for retroflex approximant
}

# ---------------------------------------------------------------------------
# 15. PHRYGIAN  (Brixhe & Lejeune 1984, Obrador-Cursach 2020)
# ---------------------------------------------------------------------------
PHRYGIAN_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    # Stops
    "b": "b", "d": "d", "g": "ɡ",
    "p": "p", "t": "t", "k": "k",
    # Fricatives / other
    "s": "s", "v": "w", "w": "w",
    "m": "m", "n": "n", "l": "l", "r": "r",
    "y": "j",
    # Aspirated (New Phrygian Greek-script)
    "ph": "pʰ", "th": "tʰ", "kh": "kʰ",
}

# ---------------------------------------------------------------------------
# 16. LEMNIAN  (Greek-alphabet based)
# ---------------------------------------------------------------------------
LEMNIAN_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    "b": "b", "d": "d", "g": "ɡ",
    "p": "p", "t": "t", "k": "k",
    "ph": "pʰ", "th": "tʰ", "kh": "kʰ",
    "s": "s", "z": "z",
    "m": "m", "n": "n", "l": "l", "r": "r",
    "v": "w", "w": "w",
    "f": "f", "h": "h",
}

# ---------------------------------------------------------------------------
# 17. RHAETIC  (North Italic alphabet)
# ---------------------------------------------------------------------------
RHAETIC_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    "b": "b", "d": "d", "g": "ɡ",
    "p": "p", "t": "t", "k": "k",
    "ph": "pʰ", "th": "tʰ", "kh": "kʰ",
    "s": "s", "z": "ts",
    "m": "m", "n": "n", "l": "l", "r": "r",
    "v": "w", "f": "f",
    "ś": "ʃ",
}

# ---------------------------------------------------------------------------
# 18. MESSAPIC  (Greek-alphabet based)
# ---------------------------------------------------------------------------
MESSAPIC_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    "b": "b", "d": "d", "g": "ɡ",
    "p": "p", "t": "t", "k": "k",
    "ph": "pʰ", "th": "tʰ", "kh": "kʰ",
    "s": "s", "z": "z",
    "m": "m", "n": "n", "l": "l", "r": "r",
    "v": "w", "w": "w",
    "h": "h", "f": "f",
    "θ": "θ",
}


# ---------------------------------------------------------------------------
# Lookup table:  ISO 639-3 code  ->  transliteration map
# ---------------------------------------------------------------------------
ALL_MAPS: Dict[str, Dict[str, str]] = {
    "hit": HITTITE_MAP,         # Hittite
    "uga": UGARITIC_MAP,        # Ugaritic
    "phn": PHOENICIAN_MAP,      # Phoenician
    "xur": URARTIAN_MAP,        # Urartian
    "elx": ELAMITE_MAP,         # Elamite
    "xlc": LYCIAN_MAP,          # Lycian
    "xld": LYDIAN_MAP,          # Lydian
    "xcr": CARIAN_MAP,          # Carian
    "ave": AVESTAN_MAP,         # Avestan
    "peo": OLD_PERSIAN_MAP,     # Old Persian
    "ine": PIE_MAP,             # Proto-Indo-European
    "sem": PROTO_SEMITIC_MAP,   # Proto-Semitic
    "ccs": PROTO_KARTVELIAN_MAP,  # Proto-Kartvelian (South Caucasian)
    "dra": DEDR_MAP,            # Proto-Dravidian
    "xpg": PHRYGIAN_MAP,       # Phrygian
    "xle": LEMNIAN_MAP,         # Lemnian
    "xrr": RHAETIC_MAP,         # Rhaetic
    "cms": MESSAPIC_MAP,        # Messapic
}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def get_map(iso: str) -> Optional[Dict[str, str]]:
    """Return the transliteration map for the given ISO 639-3 code, or None."""
    return ALL_MAPS.get(iso)


def transliterate(text: str, iso: str) -> str:
    """Apply the transliteration map for *iso* to *text*, returning an IPA string.

    Multi-character transliteration keys are handled via greedy longest-match:
    keys are sorted by descending length and the first match at each position
    is consumed.  Characters with no mapping are passed through unchanged.
    """
    mapping = ALL_MAPS.get(iso)
    if mapping is None:
        return text

    # Sort keys longest-first for greedy matching
    keys_by_length = sorted(mapping.keys(), key=len, reverse=True)

    result: list[str] = []
    i = 0
    while i < len(text):
        matched = False
        for key in keys_by_length:
            if text[i:i + len(key)] == key:
                result.append(mapping[key])
                i += len(key)
                matched = True
                break
        if not matched:
            result.append(text[i])
            i += 1

    return "".join(result)
