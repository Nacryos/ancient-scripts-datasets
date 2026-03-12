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
- Avestan script:   Unicode Standard 5.2 (U+10B00-10B3F); Hoffmann & Forssman (1996)
- Old Persian:      Kent (1953), *Old Persian: Grammar, Texts, Lexicon*
- Proto-Indo-European: Standard comparative notation (Fortson 2010, Beekes 2011)
- Proto-Semitic:    Standard Semitist notation (Huehnergard 2019)
- Proto-Kartvelian: Klimov (1998), *Etymological Dictionary of the Kartvelian Languages*
- Proto-Dravidian (DEDR): Krishnamurti (2003), *The Dravidian Languages*
- Phrygian:         Brixhe & Lejeune (1984); Obrador-Cursach (2020)
- Lemnian:          Greek-alphabet based reconstruction
- Rhaetic:          North Italic alphabet reconstruction
- Messapic:         Greek-alphabet based reconstruction
- Luwian:           Melchert (2003), *A Dictionary of the Luwian Language*;
                    Yakubovich (2010)
- Hurrian:          Wegner (2007), *Hurritisch: Eine Einfuehrung*;
                    Wilhelm (2008)
- Etruscan:         Bonfante & Bonfante (2002), *The Etruscan Language*;
                    Rix (1963), *Das etruskische Cognomen*
- Tocharian A/B:    Krause & Thomas (1960), *Tocharisches Elementarbuch*;
                    Adams (2013), *A Dictionary of Tocharian B*; Peyrot (2008)
"""

from __future__ import annotations

import unicodedata
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# 1. HITTITE  (Hoffner & Melchert 2008)
# ---------------------------------------------------------------------------
HITTITE_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "aa": "aː", "e": "e", "ee": "eː",
    "i": "i", "ii": "iː", "u": "u", "uu": "uː",
    # Long vowels (macron notation — common in modern Hittitological literature)
    "ā": "aː", "ē": "eː", "ī": "iː", "ū": "uː",
    # Accented vowels (editorial accent, strip to base vowel)
    "à": "a", "á": "a", "é": "e", "í": "i", "ú": "u",
    # Stops (lenis)
    "p": "p", "b": "p", "t": "t", "d": "t", "k": "k", "g": "k",
    # Stops (fortis/geminate)
    "pp": "pː", "bb": "pː", "tt": "tː", "dd": "tː", "kk": "kː", "gg": "kː",
    # Fricatives
    "h": "x", "hh": "xː",
    "ḫ": "x", "ḫḫ": "xː",  # Hittitological ḫ convention (Hoffner & Melchert)
    # Sibilants
    "s": "s", "ss": "sː", "z": "ts", "zz": "tsː",
    # š → ʃ: Conventional. Kloekhorst (2008) argues for [s]; Hoffner & Melchert (2008) use conventional ʃ.
    "š": "ʃ", "šš": "ʃː",  # Hittite sibilant š (77+ entries)
    "ṣ": "sˤ",              # emphatic sibilant (rare)
    # Sonorants
    "l": "l", "ll": "lː", "m": "m", "n": "n", "r": "r",
    # Glides
    "w": "w", "y": "j", "j": "j",
    # Plain x (used in some transliteration conventions, e.g. xšap)
    "x": "x",
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
    # Modifier letter right half ring (Wiktionary aleph convention)
    "ʾ": "ʔ",
    # IPA glottal stop used directly in some transliterations
    "ʔ": "ʔ",
    # Missing consonants
    "ḫ": "x",   # voiceless velar/uvular fricative
    "ṣ": "sˤ",  # emphatic (pharyngealized) s
    # Long vowels (macron notation)
    "ā": "aː", "ē": "eː", "ī": "iː", "ū": "uː",
    # Circumflex vowels (Canaanite shift markers — strip to plain vowel)
    "â": "a", "ê": "e", "î": "i", "ô": "o", "û": "u",
    # Capital variants (proper nouns)
    "Ḫ": "x", "Ḥ": "ħ",
    # Semitic conventions
    "j": "j",  # Latin j = /j/ in some sources
    # Ugaritic script characters (U+10380–U+1039F, Tropper 2000)
    "\U00010380": "ʔ",   # 𐎀 ALPA
    "\U00010381": "b",    # 𐎁 BETA
    "\U00010382": "ɡ",    # 𐎂 GAMLA
    "\U00010384": "d",    # 𐎄 DELTA
    "\U00010385": "h",    # 𐎅 HO
    "\U00010388": "ħ",    # 𐎈 HOTA
    "\U0001038A": "j",    # 𐎊 YOD
    "\U0001038B": "k",    # 𐎋 KAF
    "\U0001038C": "ʃ",    # 𐎌 SHIN
    "\U0001038D": "l",    # 𐎍 LAMDA
    "\U0001038E": "m",    # 𐎎 MEM
    "\U00010390": "n",    # 𐎐 NUN
    "\U00010393": "ʕ",    # 𐎓 AIN
    "\U00010394": "p",    # 𐎔 PU
    "\U00010395": "sˤ",   # 𐎕 SADE
    "\U00010396": "q",    # 𐎖 QOPA
    "\U00010397": "r",    # 𐎗 RASHA
    "\U00010398": "θ",    # 𐎘 THANNA
    "\U00010399": "ɣ",    # 𐎙 GHAIN
    "\U0001039A": "t",    # 𐎚 TO
    "\U0001039B": "ʔi",   # 𐎛 I
    "\U0001039C": "ʔu",   # 𐎜 U
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
    "ī": "iː",  # long i (rare)
    "ə": "ə",   # schwa
    "b": "b", "d": "d", "g": "ɡ", "ḫ": "x", "h": "x",
    "k": "k", "l": "l", "m": "m", "n": "n", "p": "p",
    "q": "q", "r": "r", "s": "s", "š": "ʃ", "t": "t", "z": "ts",
    # Emphatic consonants (Wegner 2007)
    "ṣ": "tsʼ",  # emphatic sibilant
    "ṭ": "tʼ",   # emphatic stop
    # Glides
    "y": "j", "w": "w",
    # Glottal stop (modifier letter right half ring)
    "ʾ": "ʔ",
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
    "a": "a", "e": "e", "i": "i", "u": "u", "o": "o",
    "ã": "ã", "ẽ": "ẽ",  # nasalized vowels (IPA-valid)
    "ā": "aː", "ē": "eː",  # long vowels (rare, Wiktionary entries)
    "b": "b", "d": "d", "g": "ɡ", "h": "x",
    "k": "k", "l": "l", "m": "m", "n": "n", "p": "p",
    "q": "kʷ", "r": "r", "s": "s", "t": "t", "w": "w",
    "z": "ts", "θ": "θ", "χ": "kʰ", "ñ": "ɲ",
    "λ": "l̩", "τ": "tʰ",
    # Additional consonants (Melchert 2004)
    "x": "x",   # voiceless velar fricative (direct notation)
    "j": "j",   # palatal glide
    "c": "k",   # velar (context-dependent)
    # Reconstructed form marker
    "*": "",
}

# ---------------------------------------------------------------------------
# 7. LYDIAN  (Gusmani 1964, Melchert)
# ---------------------------------------------------------------------------
LYDIAN_MAP: Dict[str, str] = {
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    # Nasalized vowels (Gusmani 1964 — 121+ entries affected)
    "ã": "ã", "ẽ": "ẽ", "ũ": "ũ",
    "b": "b", "d": "d", "g": "ɡ", "k": "k", "l": "l",
    "m": "m", "n": "n", "p": "p", "r": "r", "s": "s",
    "t": "t", "v": "v", "w": "w", "y": "j",
    "š": "ʃ", "ś": "ɕ", "τ": "tʰ", "λ": "lː", "ñ": "ɲ", "q": "kʷ",
    "f": "f",
    # Additional consonants
    "c": "ts",  # affricate
    "h": "h",   # aspirate/laryngeal
    "z": "z",   # voiced sibilant
    "x": "x",   # velar fricative (rare)
    # Long vowels (macron notation)
    "ō": "oː", "ē": "eː",
    # Circumflex vowel
    "ê": "eː",
    # Reconstructed form marker
    "*": "",
    # Greek nu (appears in some source conventions)
    "ν": "n",
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
    # Additional phonemes (Adiego 2007)
    "β": "β",    # bilabial fricative
    "z": "z",    # voiced sibilant
    "v": "v",    # labiodental fricative
    "j": "j",    # palatal glide
    "f": "f",    # labiodental fricative
    "ŋ": "ŋ",   # velar nasal
    "ĺ": "lʲ",  # palatalized l
    "ỳ": "ə",   # y-grave: tentative vocalic value (Adiego 2007)
    "ý": "e",   # y-acute: tentative vocalic value (Adiego 2007)
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
    # ----- Avestan Unicode script characters (U+10B00-10B35) -----
    # Vowels
    "\U00010B00": "a",     # AVESTAN LETTER A
    "\U00010B01": "aː",   # AVESTAN LETTER AA
    "\U00010B02": "ɔ",     # AVESTAN LETTER AO
    "\U00010B03": "ɔː",   # AVESTAN LETTER AAO
    "\U00010B04": "ã",     # AVESTAN LETTER AN
    "\U00010B05": "ãː",   # AVESTAN LETTER AAN
    "\U00010B06": "ai",    # AVESTAN LETTER AE
    "\U00010B07": "oi",    # AVESTAN LETTER AEE
    "\U00010B08": "e",     # AVESTAN LETTER E
    "\U00010B09": "eː",   # AVESTAN LETTER EE
    "\U00010B0A": "o",     # AVESTAN LETTER O
    "\U00010B0B": "oː",   # AVESTAN LETTER OO
    "\U00010B0C": "i",     # AVESTAN LETTER I
    "\U00010B0D": "iː",   # AVESTAN LETTER II
    "\U00010B0E": "u",     # AVESTAN LETTER U
    "\U00010B0F": "uː",   # AVESTAN LETTER UU
    # Consonants
    "\U00010B10": "k",     # AVESTAN LETTER KE
    "\U00010B11": "x",     # AVESTAN LETTER XE
    "\U00010B12": "xʲ",   # AVESTAN LETTER XYE (palatalized x)
    "\U00010B13": "xʷ",   # AVESTAN LETTER XVE (labialized x)
    "\U00010B14": "ɡ",     # AVESTAN LETTER GE
    "\U00010B15": "ɣ",     # AVESTAN LETTER GGE (voiced velar fricative)
    "\U00010B16": "ɣ",     # AVESTAN LETTER GHE (voiced velar fricative variant)
    "\U00010B17": "tʃ",   # AVESTAN LETTER CE
    "\U00010B18": "dʒ",   # AVESTAN LETTER JE
    "\U00010B19": "t",     # AVESTAN LETTER TE
    "\U00010B1A": "θ",     # AVESTAN LETTER THE
    "\U00010B1B": "d",     # AVESTAN LETTER DE
    "\U00010B1C": "ð",     # AVESTAN LETTER DHE
    "\U00010B1D": "t",     # AVESTAN LETTER TTE (alveolar t variant)
    "\U00010B1E": "p",     # AVESTAN LETTER PE
    "\U00010B1F": "f",     # AVESTAN LETTER FE
    "\U00010B20": "b",     # AVESTAN LETTER BE
    "\U00010B21": "β",     # AVESTAN LETTER BHE (voiced bilabial fricative)
    "\U00010B22": "ŋ",     # AVESTAN LETTER NGE
    "\U00010B23": "ŋʲ",   # AVESTAN LETTER NGYE (palatalized ng)
    "\U00010B24": "ŋʷ",   # AVESTAN LETTER NGVE (labialized ng)
    "\U00010B25": "n",     # AVESTAN LETTER NE
    "\U00010B26": "ɲ",     # AVESTAN LETTER NYE
    "\U00010B27": "n",     # AVESTAN LETTER NNE (variant n)
    "\U00010B28": "m",     # AVESTAN LETTER ME
    "\U00010B29": "hm",    # AVESTAN LETTER HME (voiceless nasal cluster)
    "\U00010B2A": "jː",   # AVESTAN LETTER YYE (geminate y)
    "\U00010B2B": "j",     # AVESTAN LETTER YE
    "\U00010B2C": "v",     # AVESTAN LETTER VE
    "\U00010B2D": "r",     # AVESTAN LETTER RE
    "\U00010B2E": "l",     # AVESTAN LETTER LE
    "\U00010B2F": "s",     # AVESTAN LETTER SE
    "\U00010B30": "z",     # AVESTAN LETTER ZE
    "\U00010B31": "ʃ",     # AVESTAN LETTER SHE
    "\U00010B32": "ʒ",     # AVESTAN LETTER ZHE
    "\U00010B33": "ʃʲ",   # AVESTAN LETTER SHYE (palatalized sh)
    "\U00010B34": "ʃː",   # AVESTAN LETTER SSHE (geminate sh)
    "\U00010B35": "h",     # AVESTAN LETTER HE
}

# ---------------------------------------------------------------------------
# 10. OLD PERSIAN  (Kent 1953)
# ---------------------------------------------------------------------------
OLD_PERSIAN_MAP: Dict[str, str] = {
    "a": "a", "i": "i", "u": "u",
    "ā": "aː", "ī": "iː", "ū": "uː",
    "ē": "eː", "ō": "oː",  # long mid vowels (Kent 1953)
    "e": "e", "o": "o",      # short mid vowels
    "p": "p", "b": "b", "t": "t", "d": "d",
    "k": "k", "g": "ɡ", "c": "tʃ", "j": "dʒ",
    "f": "f", "θ": "θ", "s": "s", "š": "ʃ",
    # ç → θ: Per Kent (1953). Kloekhorst (2008) argues for /ts/.
    "ç": "θ",  # Kent's sibilant/fricative (conservative: θ)
    "x": "x", "h": "h",
    "z": "z",  # voiced sibilant (30 entries)
    "č": "tʃ",  # postalveolar affricate (c-caron notation)
    "m": "m", "n": "n",
    "r": "r", "l": "l",
    "v": "v", "w": "w", "y": "j",
    # Capital macron vowels (proper noun initials)
    "Ā": "aː",
    # Old Persian cuneiform syllabary (U+103A0–U+103C3, Kent 1953)
    # NOTE: Syllabary signs represent CV syllables; inherent vowels are included.
    "\U000103A0": "a",    # 𐎠 a
    "\U000103A1": "i",    # 𐎡 i
    "\U000103A2": "u",    # 𐎢 u
    "\U000103A3": "ka",   # 𐎣 ka
    "\U000103A4": "ku",   # 𐎤 ku
    "\U000103A5": "ga",   # 𐎥 ga
    "\U000103A6": "gu",   # 𐎦 gu
    "\U000103A7": "xa",   # 𐎧 xa
    "\U000103A8": "tʃa",  # 𐎨 ca
    "\U000103A9": "dʒa",  # 𐎩 ja
    "\U000103AA": "dʒi",  # 𐎪 ji
    "\U000103AB": "ta",   # 𐎫 ta
    "\U000103AC": "tu",   # 𐎬 tu
    "\U000103AD": "da",   # 𐎭 da
    "\U000103AE": "di",   # 𐎮 di
    "\U000103AF": "du",   # 𐎯 du
    "\U000103B0": "θa",   # 𐎰 tha
    "\U000103B1": "pa",   # 𐎱 pa
    "\U000103B2": "ba",   # 𐎲 ba
    "\U000103B3": "fa",   # 𐎳 fa
    "\U000103B4": "na",   # 𐎴 na
    "\U000103B5": "nu",   # 𐎵 nu
    "\U000103B6": "ma",   # 𐎶 ma
    "\U000103B7": "mi",   # 𐎷 mi
    "\U000103B8": "mu",   # 𐎸 mu
    "\U000103B9": "ja",   # 𐎹 ya
    "\U000103BA": "va",   # 𐎺 va
    "\U000103BB": "vi",   # 𐎻 vi
    "\U000103BC": "ra",   # 𐎼 ra
    "\U000103BD": "ru",   # 𐎽 ru
    "\U000103BE": "la",   # 𐎾 la
    "\U000103BF": "sa",   # 𐎿 sa
    "\U000103C0": "za",   # 𐏀 za
    "\U000103C1": "ʃa",   # 𐏁 sha
    "\U000103C2": "sa",   # 𐏂 ssa
    "\U000103C3": "ha",   # 𐏃 ha
}

# ---------------------------------------------------------------------------
# 11. PROTO-INDO-EUROPEAN  (standard comparative notation)
# ---------------------------------------------------------------------------
PIE_MAP: Dict[str, str] = {
    # Reconstruction marker (strip asterisk)
    "*": "",
    # Morpheme boundary & optional-segment markers
    "-": "", "(": "", ")": "",
    "⁽": "", "⁾": "",  # superscript parens (g⁽ʷ⁾ notation)
    # Vowels
    "e": "e", "o": "o", "a": "a", "i": "i", "u": "u",
    "ē": "eː", "ō": "oː", "ā": "aː", "ī": "iː", "ū": "uː",
    # Accented vowels (Wiktionary accentological notation — strip accent)
    "é": "e", "ó": "o", "á": "a", "í": "i", "ú": "u",
    # Accented long vowels (precomposed — Wiktionary notation)
    "ḗ": "eː",  # U+1E17: e + macron + acute
    "ṓ": "oː",  # U+1E53: o + macron + acute
    # Schwa
    "ə": "ə",
    # Stops (plain voiceless)
    "p": "p", "t": "t", "ḱ": "k", "k": "k", "kʷ": "kʷ",
    # Stops (voiced)
    "b": "b", "d": "d", "ǵ": "ɡ", "g": "ɡ", "gʷ": "ɡʷ",
    # Stops (voiced aspirate)
    "bʰ": "bʱ", "dʰ": "dʱ", "ǵʰ": "ɡʱ", "gʰ": "ɡʱ", "gʷʰ": "ɡʷʱ",
    # Laryngeals
    # h₃ → ɣʷ: Leiden school (Beekes 2011). Many scholars leave h₃ phonetically unspecified.
    "h₁": "h", "h₂": "ħ", "h₃": "ɣʷ",
    "H": "h",  # generic laryngeal
    # Fricatives/sibilant
    "s": "s",
    # Sonorants
    "m": "m", "n": "n", "l": "l", "r": "r",
    # Glides
    "w": "w", "y": "j",
    # Labialization modifier (stranded after superscript-paren strip)
    "ʷ": "ʷ",
    # Syllabic sonorants
    "m̥": "m̩", "n̥": "n̩", "l̥": "l̩", "r̥": "r̩",
    # Accented syllabic sonorants (acute + ring below)
    "ĺ\u0325": "l̩", "ŕ\u0325": "r̩", "ḿ\u0325": "m̩",
    # Plain accented sonorants (strip accent)
    "ĺ": "l", "ŕ": "r", "ḿ": "m",
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
    "ē": "eː",  # long e (occasional in Wiktionary Kartvelian entries)
    # Morpheme boundary
    "-": "",
    "(": "", ")": "",
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
    # Ejectives (combining dot above notation — Wiktionary variant)
    "k̇": "kʼ", "ṫ": "tʼ", "ṗ": "pʼ", "q̇": "qʼ",
    # Aspirates
    "pʰ": "pʰ", "tʰ": "tʰ", "kʰ": "kʰ", "qʰ": "qʰ",
    "cʰ": "tsʰ", "čʰ": "tʃʰ",
    # Affricates
    "c": "ts", "č": "tʃ", "ċ": "ts",
    "ʒ": "dz", "ǯ": "dʒ",
    # Klimov sibilant series (subscript-1 variants — 92 occurrences)
    "s₁": "s", "z₁": "z", "c₁": "ts",
    "ʒ₁": "dz", "c̣₁": "tsʼ", "ʓ₁": "dz",
    # Fricatives
    "s": "s", "z": "z", "š": "ʃ", "ž": "ʒ",
    "x": "x", "γ": "ɣ", "ɣ": "ɣ", "h": "h",
    # Voiced affricates / fricatives (Klimov)
    "ʓ": "dz", "ʓ̌": "dʒ",
    # Sonorants
    "m": "m", "n": "n", "l": "l", "r": "r",
    # Glides
    "w": "w", "y": "j", "j": "j",
    # Capital L (3 entries: Lad-, Lam-, Luc̣₁-)
    "L": "l",
}

# ---------------------------------------------------------------------------
# 14. DEDR / PROTO-DRAVIDIAN  (Krishnamurti 2003)
# ---------------------------------------------------------------------------
DEDR_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "ā": "aː", "i": "i", "ī": "iː",
    "u": "u", "ū": "uː", "e": "e", "ē": "eː",
    "o": "o", "ō": "oː",
    "ŭ": "u",  # breve u = short u
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
    "ṅ": "ŋ",  # velar nasal (overdot notation — Krishnamurti)
    # Liquids
    "l": "l", "ḷ": "ɭ", "L": "ɭ",
    "r": "r", "ṛ": "ɽ", "ḻ": "ɻ",
    "n̤": "n",  # n with line below — simplified to plain n
    "r̤": "r",  # r with line below — conservative: plain r
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
    # Long vowels (Obrador-Cursach 2020)
    "ō": "oː", "ē": "eː",
    # Accented vowels (strip accent)
    "é": "e", "ṓ": "oː",
    # Stops
    "b": "b", "d": "d", "g": "ɡ",
    "p": "p", "t": "t", "k": "k",
    # Fricatives / other
    "s": "s", "z": "z", "v": "w", "w": "w",
    "ϝ": "w",  # Greek digamma = /w/
    "m": "m", "n": "n", "l": "l", "r": "r",
    "y": "j",
    # Aspirated (New Phrygian Greek-script)
    "ph": "pʰ", "th": "tʰ", "kh": "kʰ",
    # Greek-script Phrygian entries (Brixhe & Lejeune 1984)
    "α": "a", "β": "b", "γ": "ɡ", "δ": "d", "ε": "e",
    "ζ": "z", "η": "eː", "θ": "tʰ", "ι": "i", "κ": "k",
    "λ": "l", "μ": "m", "ν": "n", "ο": "o", "π": "p",
    "ρ": "r", "σ": "s", "ς": "s", "τ": "t", "υ": "u",
    "ξ": "ks", "ψ": "ps", "φ": "pʰ", "χ": "kʰ",
    "ω": "oː",
    # Greek accented vowels (strip accent)
    "έ": "e", "ώ": "oː", "ά": "a", "ί": "i", "ό": "o", "ύ": "u",
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
    "χ": "kʰ",  # Greek chi (TIR data)
    "φ": "pʰ",  # Greek phi (TIR data)
    "þ": "θ",   # thorn (TIR data)
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
# 19. LUWIAN  (Melchert 2003, Yakubovich 2010)
# ---------------------------------------------------------------------------
LUWIAN_MAP: Dict[str, str] = {
    # Vowels (short)
    "a": "a", "e": "e", "i": "i", "u": "u",
    # Vowels (long — macron notation)
    "ā": "aː", "ē": "eː", "ī": "iː", "ū": "uː",
    # Vowels (long — doubled notation, inherited from Hittite cuneiform)
    "aa": "aː", "ee": "eː", "ii": "iː", "uu": "uː",
    # Stops (lenis — Luwian preserves Hittite fortis/lenis distinction)
    "p": "p", "b": "p", "t": "t", "d": "t", "k": "k", "g": "k",
    # Stops (fortis/geminate)
    "pp": "pː", "bb": "pː", "tt": "tː", "dd": "tː", "kk": "kː", "gg": "kː",
    # Fricatives
    "h": "x", "hh": "xː", "ḫ": "x",
    # Sibilants (CRITICAL: š → ʃ is the #1 missing character in xlw data)
    "s": "s", "ss": "sː", "š": "ʃ", "z": "ts", "zz": "tsː",
    # Sonorants
    "l": "l", "ll": "lː", "m": "m", "n": "n", "r": "r",
    # Glides
    "w": "w", "y": "j",
}

# ---------------------------------------------------------------------------
# 20. HURRIAN  (Wegner 2007, Wilhelm 2008)
# ---------------------------------------------------------------------------
HURRIAN_MAP: Dict[str, str] = {
    # Vowels (short)
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    # Vowels (long — macron notation)
    "ā": "aː", "ē": "eː", "ī": "iː", "ū": "uː",
    # Stops
    "p": "p", "b": "b", "t": "t", "d": "d", "k": "k", "g": "ɡ",
    # Fricatives
    "ḫ": "x", "h": "x", "f": "f", "v": "v",
    "s": "s", "š": "ʃ", "ž": "ʒ", "ġ": "ɣ",
    # Affricates
    "z": "ts",
    # Uvular
    "q": "q",
    # Sonorants
    "l": "l", "m": "m", "n": "n", "r": "r",
    # Glides
    "w": "w", "y": "j",
}

# ---------------------------------------------------------------------------
# 21. ETRUSCAN  (Bonfante & Bonfante 2002, Rix 1963)
# ---------------------------------------------------------------------------
ETRUSCAN_MAP: Dict[str, str] = {
    # Vowels (Etruscan lacks /o/ in native words, but appears in loanwords)
    "a": "a", "e": "e", "i": "i", "u": "u", "o": "o",
    # Stops (voiceless only — Etruscan lacks voiced stops in native words)
    "p": "p", "t": "t", "k": "k",
    "c": "k",  # Etruscan c = voiceless velar stop
    # Voiced stops (from loanwords)
    "b": "b", "d": "d", "g": "ɡ",
    # Aspirated stops
    "θ": "tʰ",   # theta = aspirated dental (Bonfante & Bonfante 2002)
    "φ": "pʰ",   # phi = aspirated labial
    "χ": "kʰ",   # chi = aspirated velar
    "ph": "pʰ",  # alternate digraph notation
    "th": "tʰ",  # alternate digraph notation
    "kh": "kʰ",  # alternate digraph notation
    # Sibilants
    "s": "s",
    "ś": "ʃ",    # palatal sibilant
    "š": "s",    # alveolar sibilant variant notation
    "z": "ts",   # Etruscan z = /ts/ affricate
    # Fricatives
    "f": "f", "h": "h", "v": "v",
    # Sonorants
    "m": "m", "n": "n", "l": "l", "r": "r",
    # Labiovelar and rare consonants
    "q": "kʷ",  # labiovelar
    "x": "ks",  # rare, loanwords
    "y": "j",   # rare, loanwords
    # Greek letter leaks (normalize to Latin equivalents)
    "σ": "s", "ο": "o",
    # Old Italic Unicode characters (U+10300–U+1032F)
    "\U00010300": "a",    # OLD ITALIC LETTER A
    "\U00010301": "b",    # OLD ITALIC LETTER BE
    "\U00010302": "k",    # OLD ITALIC LETTER KE
    "\U00010303": "d",    # OLD ITALIC LETTER DE
    "\U00010304": "e",    # OLD ITALIC LETTER E
    "\U00010305": "v",    # OLD ITALIC LETTER VE
    "\U00010306": "ts",   # OLD ITALIC LETTER ZE
    "\U00010307": "h",    # OLD ITALIC LETTER HE
    "\U00010308": "θ",    # OLD ITALIC LETTER THE
    "\U00010309": "i",    # OLD ITALIC LETTER I
    "\U0001030A": "k",    # OLD ITALIC LETTER KA
    "\U0001030B": "l",    # OLD ITALIC LETTER EL
    "\U0001030C": "m",    # OLD ITALIC LETTER EM
    "\U0001030D": "n",    # OLD ITALIC LETTER EN
    "\U0001030E": "ʃ",    # OLD ITALIC LETTER ESH (palatal sibilant)
    "\U0001030F": "o",    # OLD ITALIC LETTER O
    "\U00010310": "p",    # OLD ITALIC LETTER PE
    "\U00010311": "ʃ",    # OLD ITALIC LETTER SHE
    "\U00010312": "kʷ",   # OLD ITALIC LETTER KU (labiovelar)
    "\U00010313": "r",    # OLD ITALIC LETTER ER
    "\U00010314": "s",    # OLD ITALIC LETTER ES
    "\U00010315": "t",    # OLD ITALIC LETTER TE
    "\U00010316": "u",    # OLD ITALIC LETTER U
    "\U00010317": "ks",   # OLD ITALIC LETTER EKS
    "\U00010318": "pʰ",   # OLD ITALIC LETTER PHE
    "\U00010319": "kʰ",   # OLD ITALIC LETTER KHE
    "\U0001031A": "f",    # OLD ITALIC LETTER EF
}


# ---------------------------------------------------------------------------
# 22. TOCHARIAN A/B  (Krause & Thomas 1960, Adams 2013, Peyrot 2008)
#     Used for both Tocharian A (xto) and Tocharian B (txb).
#     Brahmi-derived transliteration conventions.
# ---------------------------------------------------------------------------
TOCHARIAN_MAP: Dict[str, str] = {
    # Vowels (short)
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    # Tocharian specific schwa
    "ä": "ə",
    # Long vowels (macron notation)
    "ā": "aː", "ī": "iː", "ū": "uː",
    # Stops
    "p": "p", "b": "b", "t": "t", "d": "d", "k": "k", "g": "ɡ",
    # Retroflex stops (Adams 2013)
    "ṭ": "ʈ", "ḍ": "ɖ",
    # Affricates (Tocharian c = affricate /ts/)
    "c": "ts",
    # Sibilants
    "s": "s",
    "ṣ": "ʂ",    # retroflex sibilant
    "ś": "ɕ",    # palatal sibilant
    # Nasals
    "m": "m", "n": "n",
    "ṃ": "m",    # nasalized, simplified to m in broad transcription
    "ñ": "ɲ",    # palatal nasal
    "ṅ": "ŋ",    # velar nasal
    "ṇ": "ɳ",    # retroflex nasal
    # Liquids
    "l": "l", "r": "r",
    "ly": "ʎ",   # palatal lateral
    "ḷ": "ɭ",    # retroflex lateral
    # Subscript u (Brahmi notation)
    "ᵤ": "u",    # modifier letter small u
    # Aspiration
    "h": "h",
    # Glides
    "w": "w",
    "y": "j",    # standard IPA
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
    "xlw": LUWIAN_MAP,          # Luwian
    "xhu": HURRIAN_MAP,         # Hurrian
    "ett": ETRUSCAN_MAP,        # Etruscan
    "txb": TOCHARIAN_MAP,       # Tocharian B
    "xto": TOCHARIAN_MAP,       # Tocharian A
}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def get_map(iso: str) -> Optional[Dict[str, str]]:
    """Return the transliteration map for the given ISO 639-3 code, or None."""
    return ALL_MAPS.get(iso)


# Cache for NFC-normalized map data, keyed by ISO code.
# Each entry is a tuple of (nfc_mapping, nfc_keys_by_length).
_nfc_cache: Dict[str, tuple] = {}


def _get_nfc_mapping(iso: str, mapping: Dict[str, str]):
    """Return (nfc_mapping, nfc_keys_by_length) for the given map, cached."""
    if iso not in _nfc_cache:
        nfc_mapping: Dict[str, str] = {
            unicodedata.normalize("NFC", k): v for k, v in mapping.items()
        }
        nfc_keys = sorted(nfc_mapping.keys(), key=len, reverse=True)
        _nfc_cache[iso] = (nfc_mapping, nfc_keys)
    return _nfc_cache[iso]


def transliterate(text: str, iso: str) -> str:
    """Apply the transliteration map for *iso* to *text*, returning an IPA string.

    Multi-character transliteration keys are handled via greedy longest-match:
    keys are sorted by descending length and the first match at each position
    is consumed.  Characters with no mapping are passed through unchanged.

    Both the input *text* and the map keys are NFC-normalised before comparison
    so that composed and decomposed Unicode representations match correctly.
    """
    mapping = ALL_MAPS.get(iso)
    if mapping is None:
        return text

    # NFC-normalize the input text
    text = unicodedata.normalize("NFC", text)

    # Get NFC-normalized map keys (cached per ISO code)
    nfc_mapping, keys_by_length = _get_nfc_mapping(iso, mapping)

    result: list[str] = []
    i = 0
    while i < len(text):
        matched = False
        for key in keys_by_length:
            if text[i:i + len(key)] == key:
                result.append(nfc_mapping[key])
                i += len(key)
                matched = True
                break
        if not matched:
            result.append(text[i])
            i += 1

    return "".join(result)
