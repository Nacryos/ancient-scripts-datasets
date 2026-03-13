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
- Coptic:           Layton (2000), *A Coptic Grammar*; Loprieno (1995);
                    Sahidic dialect standard pronunciation
- Pali (IAST):      Geiger (1943), *Pali Literature and Language*;
                    Oberlies (2001), *Pali: A Grammar of the Language of the
                    Theravada Tipitaka*
- Old Armenian:     Meillet (1913), *Altarmenisches Elementarbuch*;
                    Schmitt (1981), *Grammatik des Klassisch-Armenischen*
- Old English:      Hogg (1992), *A Grammar of Old English, Vol. 1: Phonology*;
                    Campbell (1959), *Old English Grammar*
- Ge'ez (Ethiopic): Dillmann (1857/1907), *Ethiopic Grammar*; Tropper (2002);
                    Gragg (1997) in *The Semitic Languages*
- Biblical Hebrew:  Blau (2010), *Phonology and Morphology of Biblical Hebrew*;
                    Khan (2020), *The Tiberian Pronunciation Tradition*
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
# Old Norse (non) — Normalized Old Norse orthography → IPA
# Based on standard Norse philological transcription (Gordon & Taylor 1956)
# ---------------------------------------------------------------------------
OLD_NORSE_MAP: Dict[str, str] = {
    # Short vowels
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    "ǫ": "ɔ", "ö": "ø", "ø": "ø", "y": "y",
    "æ": "æ",
    # Long vowels (acute accent)
    "á": "aː", "é": "eː", "í": "iː", "ó": "oː", "ú": "uː",
    "ý": "yː", "ǿ": "øː", "ǽ": "æː",
    # Consonants
    "b": "b", "d": "d", "f": "f", "g": "ɡ", "h": "h",
    "j": "j", "k": "k", "l": "l", "m": "m", "n": "n",
    "p": "p", "r": "r", "s": "s", "t": "t", "v": "v",
    "w": "w", "z": "ts",
    # Special consonants
    "þ": "θ", "ð": "ð",
    # Digraphs
    "ng": "ŋɡ", "nk": "ŋk",
    # Common Latin-script loanword letters
    "c": "k", "q": "k", "x": "ks",
}

# ---------------------------------------------------------------------------
# Gothic (got) — Gothic transliteration → IPA
# Based on standard Gothicist romanization (Wright 1910, Braune & Heidermanns 2004)
# ---------------------------------------------------------------------------
GOTHIC_MAP: Dict[str, str] = {
    # Short vowels
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    # Long vowels (macron or digraph convention)
    "ā": "aː", "ē": "eː", "ī": "iː", "ō": "oː", "ū": "uː",
    # Gothic diphthongs (typically written ai, au, ei, iu)
    "ai": "ɛ", "au": "ɔ",  # Short before r,h,ƕ = [ɛ],[ɔ]; otherwise diphthong
    "ei": "iː", "iu": "iu",
    # Consonants
    "b": "b", "d": "d", "f": "f", "g": "ɡ", "h": "h",
    "j": "j", "k": "k", "l": "l", "m": "m", "n": "n",
    "p": "p", "r": "r", "s": "s", "t": "t", "w": "w",
    "z": "z",
    # Special Gothic consonants
    "þ": "θ", "ƕ": "ʍ",  # hw-ligature
    "q": "kʷ",  # labiovelar
    "x": "ks",
    # Gothic alphabet Unicode block (U+10330-10340)
    "\U00010330": "a",   # 𐌰 ahsa
    "\U00010331": "b",   # 𐌱 bairkan
    "\U00010332": "ɡ",   # 𐌲 giba
    "\U00010333": "d",   # 𐌳 dags
    "\U00010334": "e",   # 𐌴 aihvus
    "\U00010335": "kʷ",  # 𐌵 qairthra
    "\U00010336": "z",   # 𐌶 ezec
    "\U00010337": "h",   # 𐌷 hagl
    "\U00010338": "θ",   # 𐌸 thyth
    "\U00010339": "i",   # 𐌹 eis
    "\U0001033A": "k",   # 𐌺 kusma
    "\U0001033B": "l",   # 𐌻 lagus
    "\U0001033C": "m",   # 𐌼 manna
    "\U0001033D": "n",   # 𐌽 nauths
    "\U0001033E": "j",   # 𐌾 jer
    "\U0001033F": "u",   # 𐌿 urus
    "\U00010340": "p",   # 𐍀 pairthra
    "\U00010341": "90",  # 𐍁 ninety (numeral only)
    "\U00010342": "r",   # 𐍂 raida
    "\U00010343": "s",   # 𐍃 sauil
    "\U00010344": "t",   # 𐍄 teiws
    "\U00010345": "w",   # 𐍅 winja
    "\U00010346": "f",   # 𐍆 faihu
    "\U00010347": "ŋ",   # 𐍇 iggws (enguz — represents /ŋ/)
    "\U00010348": "ʍ",   # 𐍈 hwair (voiceless w)
    "\U00010349": "o",   # 𐍉 othal
}

# ---------------------------------------------------------------------------
# Old Church Slavonic (chu) — Early Cyrillic → IPA
# Based on Lunt (2001) "Old Church Slavonic Grammar"
# ---------------------------------------------------------------------------
OCS_MAP: Dict[str, str] = {
    # === Cyrillic lowercase (primary OCS orthography) ===
    # Vowels
    "а": "a", "е": "e", "и": "i", "о": "o", "у": "u",
    "ы": "ɨ", "ь": "ĭ", "ъ": "ŭ",
    # Jotated vowels
    "ꙗ": "ja", "ѥ": "je", "ю": "ju",
    "я": "ja",  # later form
    "є": "e",   # variant of е
    # Nasal vowels
    "ѧ": "ɛ̃", "ѫ": "ɔ̃",
    "ꙙ": "jɛ̃", "ꙛ": "jɔ̃",  # jotated nasals
    # Special vowels
    "ѣ": "æ",  # yat (jat')
    "і": "i", "ї": "ji",
    "ѹ": "u",   # digraph ou
    # Consonants
    "б": "b", "в": "v", "г": "ɡ", "д": "d",
    "ж": "ʒ", "з": "z", "к": "k", "л": "l",
    "м": "m", "н": "n", "п": "p", "р": "r",
    "с": "s", "т": "t", "ф": "f", "х": "x",
    # Palatalized/special
    "ц": "ts", "ч": "tʃ", "ш": "ʃ", "щ": "ʃt",
    "ѕ": "dz",  # dzělo
    "ꙃ": "dz",  # older form
    # Archaic Cyrillic
    "ѳ": "θ",  # theta (Greek loanwords)
    "ѯ": "ks", # ksi
    "ѱ": "ps", # psi
    "ѵ": "y",  # izhitsa (from Greek upsilon)
    # === Cyrillic uppercase (Wiktionary capitalizes some headwords) ===
    "А": "a", "Б": "b", "В": "v", "Г": "ɡ", "Д": "d",
    "Е": "e", "Ж": "ʒ", "З": "z", "И": "i", "К": "k",
    "Л": "l", "М": "m", "Н": "n", "О": "o", "П": "p",
    "Р": "r", "С": "s", "Т": "t", "У": "u", "Ф": "f",
    "Х": "x", "Ц": "ts", "Ч": "tʃ", "Ш": "ʃ", "Щ": "ʃt",
    "Ъ": "ŭ", "Ы": "ɨ", "Ь": "ĭ", "Ю": "ju", "Я": "ja",
    "Ѣ": "æ", "Ѧ": "ɛ̃", "Ѫ": "ɔ̃", "Ѕ": "dz",
    "Ѳ": "θ", "Ѵ": "y", "І": "i",
    # === Glagolitic script (U+2C00-U+2C5F) ===
    # Many OCS words on Wiktionary use Glagolitic instead of Cyrillic.
    # Based on standard Glagolitic-Cyrillic equivalence (Lunt 2001).
    # Lowercase
    "ⰰ": "a",   # AZU = а
    "ⰱ": "b",   # BUKY = б
    "ⰲ": "v",   # VEDE = в
    "ⰳ": "ɡ",   # GLAGOLI = г
    "ⰴ": "d",   # DOBRO = д
    "ⰵ": "e",   # YESTU = е
    "ⰶ": "ʒ",   # ZHIVETE = ж
    "ⰷ": "dz",  # DZELO = ꙃ/ѕ
    "ⰸ": "z",   # ZEMLJA = з
    "ⰹ": "i",   # IZHE = и
    "ⰺ": "i",   # INITIAL IZHE = і
    "ⰻ": "i",   # I = і
    "ⰼ": "dʒ",  # DJERVI = ꙉ
    "ⰽ": "k",   # KAKO = к
    "ⰾ": "l",   # LJUDIJE = л
    "ⰿ": "m",   # MYSLITE = м
    "ⱀ": "n",   # NASHI = н
    "ⱁ": "o",   # ONU = о
    "ⱂ": "p",   # POKOJI = п
    "ⱃ": "r",   # RITSI = р
    "ⱄ": "s",   # SLOVO = с
    "ⱅ": "t",   # TVRIDO = т
    "ⱆ": "u",   # UKU = у
    "ⱇ": "f",   # FRITU = ф
    "ⱈ": "x",   # HERU = х
    "ⱉ": "o",   # OTU = ω (variant of о)
    "ⱊ": "p",   # PE = variant п
    "ⱋ": "ʃt",  # SHTA = щ
    "ⱌ": "ts",  # TSI = ц
    "ⱍ": "tʃ",  # CHRIVI = ч
    "ⱎ": "ʃ",   # SHA = ш
    "ⱏ": "ŭ",   # YERU = ъ
    "ⱐ": "ĭ",   # YERI = ь
    "ⱑ": "æ",   # YATI = ѣ
    "ⱒ": "x",   # SPIDERY HA
    "ⱓ": "ju",  # YU = ю
    "ⱔ": "ɛ̃",   # SMALL YUS = ѧ
    "ⱕ": "ɛ̃",   # SMALL YUS WITH TAIL (variant)
    "ⱖ": "jo",  # YO
    "ⱗ": "jɛ̃",  # IOTATED SMALL YUS
    "ⱘ": "ɔ̃",   # BIG YUS = ѫ
    "ⱙ": "jɔ̃",  # IOTATED BIG YUS
    "ⱚ": "θ",   # FITA = ѳ
    "ⱛ": "y",   # IZHITSA = ѵ
    "ⱜ": "ŭ",   # SHTAPIC (yer variant)
    "ⱝ": "a",   # TROKUTASTI A (a variant)
    "ⱞ": "m",   # LATINATE MYSLITE (m variant)
    "ⱟ": "tʃ",  # CAUDATE CHRIVI (ch variant)
    # Uppercase Glagolitic (same IPA values)
    "Ⰰ": "a", "Ⰱ": "b", "Ⰲ": "v", "Ⰳ": "ɡ", "Ⰴ": "d",
    "Ⰵ": "e", "Ⰶ": "ʒ", "Ⰷ": "dz", "Ⰸ": "z", "Ⰹ": "i",
    "Ⰺ": "i", "Ⰻ": "i", "Ⰼ": "dʒ", "Ⰽ": "k", "Ⰾ": "l",
    "Ⰿ": "m", "Ⱀ": "n", "Ⱁ": "o", "Ⱂ": "p", "Ⱃ": "r",
    "Ⱄ": "s", "Ⱅ": "t", "Ⱆ": "u", "Ⱇ": "f", "Ⱈ": "x",
    "Ⱉ": "o", "Ⱊ": "p", "Ⱋ": "ʃt", "Ⱌ": "ts", "Ⱍ": "tʃ",
    "Ⱎ": "ʃ", "Ⱏ": "ŭ", "Ⱐ": "ĭ", "Ⱑ": "æ", "Ⱒ": "x",
    "Ⱓ": "ju", "Ⱔ": "ɛ̃", "Ⱕ": "ɛ̃", "Ⱖ": "jo", "Ⱗ": "jɛ̃",
    "Ⱘ": "ɔ̃", "Ⱙ": "jɔ̃", "Ⱚ": "θ", "Ⱛ": "y", "Ⱜ": "ŭ",
    "Ⱝ": "a", "Ⱞ": "m", "Ⱟ": "tʃ",
    # === Latin transliteration forms (Wiktionary sometimes uses these) ===
    "š": "ʃ", "ž": "ʒ", "č": "tʃ",
    "ě": "æ",  # yat
    "ǫ": "ɔ̃", "ę": "ɛ̃",  # nasal vowels in Latin translit
}

# ---------------------------------------------------------------------------
# Akkadian (akk) — Standard Assyriological transliteration → IPA
# Based on Huehnergard (2011) "A Grammar of Akkadian"
# ---------------------------------------------------------------------------
AKKADIAN_MAP: Dict[str, str] = {
    # Vowels (short)
    "a": "a", "e": "e", "i": "i", "u": "u",
    # Vowels (long — macron)
    "ā": "aː", "ē": "eː", "ī": "iː", "ū": "uː",
    # Consonants
    "b": "b", "d": "d", "g": "ɡ", "k": "k",
    "l": "l", "m": "m", "n": "n", "p": "p",
    "r": "r", "s": "s", "t": "t", "w": "w",
    "y": "j", "z": "z",
    # Emphatic consonants
    "ṭ": "tˤ", "ṣ": "sˤ", "q": "kˤ",
    # Pharyngeals and glottals
    "ḫ": "x", "ʾ": "ʔ", "'": "ʔ",
    # Sibilants
    "š": "ʃ",
}

# ---------------------------------------------------------------------------
# Sumerian (sux) — Standard Sumerological transliteration → IPA
# Phonology is partially speculative; follows Jagersma (2010) and Edzard (2003)
# ---------------------------------------------------------------------------
SUMERIAN_MAP: Dict[str, str] = {
    # Vowels
    "a": "a", "e": "e", "i": "i", "u": "u",
    # Long vowels
    "ā": "aː", "ē": "eː", "ī": "iː", "ū": "uː",
    # Consonants
    "b": "b", "d": "d", "g": "ɡ", "k": "k",
    "l": "l", "m": "m", "n": "n", "p": "p",
    "r": "r", "s": "s", "t": "t", "z": "z",
    # Special Sumerian consonants
    "š": "ʃ", "ḫ": "x",
    "ĝ": "ŋ", "ŋ": "ŋ",  # velar nasal (ĝ or ŋ notation)
    # Sign index numbers (strip — not phonological)
    "₁": "", "₂": "", "₃": "", "₄": "", "₅": "",
    "₆": "", "₇": "", "₈": "", "₉": "", "₀": "",
}

# ---------------------------------------------------------------------------
# Mycenaean Greek (gmy) — Linear B transliteration → IPA
# Based on Ventris & Chadwick (1973) and standard Mycenological conventions
# Mycenaean is written in syllabary; transliteration values are conventional
# ---------------------------------------------------------------------------
MYCENAEAN_MAP: Dict[str, str] = {
    # === Linear B Syllabary Unicode (U+10000-U+1005D) ===
    # Direct Unicode → IPA mapping (Ventris & Chadwick 1973)
    # Pure vowels
    "𐀀": "a", "𐀁": "e", "𐀂": "i", "𐀃": "o", "𐀄": "u",
    # d-series
    "𐀅": "da", "𐀆": "de", "𐀇": "di", "𐀈": "do", "𐀉": "du",
    # j-series (y-glide)
    "𐀊": "ja", "𐀋": "je", "𐀍": "jo", "𐀎": "ju",
    # k-series
    "𐀏": "ka", "𐀐": "ke", "𐀑": "ki", "𐀒": "ko", "𐀓": "ku",
    # m-series
    "𐀔": "ma", "𐀕": "me", "𐀖": "mi", "𐀗": "mo", "𐀘": "mu",
    # n-series
    "𐀙": "na", "𐀚": "ne", "𐀛": "ni", "𐀜": "no", "𐀝": "nu",
    # p-series
    "𐀞": "pa", "𐀟": "pe", "𐀠": "pi", "𐀡": "po", "𐀢": "pu",
    # q-series (labiovelars: *kʷ)
    "𐀣": "kʷa", "𐀤": "kʷe", "𐀥": "kʷi", "𐀦": "kʷo",
    # r-series (r/l undistinguished in Linear B)
    "𐀨": "ra", "𐀩": "re", "𐀪": "ri", "𐀫": "ro", "𐀬": "ru",
    # s-series
    "𐀭": "sa", "𐀮": "se", "𐀯": "si", "𐀰": "so", "𐀱": "su",
    # t-series
    "𐀲": "ta", "𐀳": "te", "𐀴": "ti", "𐀵": "to", "𐀶": "tu",
    # w-series
    "𐀷": "wa", "𐀸": "we", "𐀹": "wi", "𐀺": "wo",
    # z-series (affricate *ts or *dz)
    "𐀼": "dza", "𐀽": "dze", "𐀿": "dzo",
    # Special/complex signs
    "𐁀": "ha",   # a2 (aspirated a)
    "𐁁": "ai",   # a3
    "𐁂": "au",   # au diphthong
    "𐁃": "dwe", "𐁄": "dwo",
    "𐁅": "nwa",
    "𐁆": "pʰu",  # pu2 (aspirated pu)
    "𐁇": "pte",
    "𐁈": "rja",  # ra2
    "𐁉": "rai",  # ra3
    "𐁊": "rjo",  # ro2
    "𐁋": "tja",  # ta2
    "𐁌": "twe", "𐁍": "two",
    # === Latin transliteration forms (scholarly conventions) ===
    # Vowels
    "a": "a", "e": "e", "i": "i", "o": "o", "u": "u",
    "h": "h",
    "j": "j", "w": "w",
    "k": "k", "g": "ɡ",
    "d": "d", "t": "t",
    "p": "p", "b": "b",
    "m": "m", "n": "n",
    "l": "l", "r": "r",
    "s": "s", "z": "dz",
    "q": "kʷ",
    # Digraphs
    "ph": "pʰ", "th": "tʰ", "kh": "kʰ",
}


# ---------------------------------------------------------------------------
# COPTIC  (Layton 2000; Loprieno 1995 — Sahidic pronunciation)
# ---------------------------------------------------------------------------
# Coptic script descends from Greek + 6 Demotic letters.
# Sahidic dialect is the standard for scholarly IPA.
# Unicode block: U+2C80-U+2CFF (Coptic), plus some shared Greek letters.
COPTIC_MAP: Dict[str, str] = {
    # Coptic letters (lowercase U+2C80+)
    "ⲁ": "a",    # alfa
    "ⲃ": "β",    # vida (fricative in Sahidic)
    "ⲅ": "ɣ",    # gamma (fricative in Sahidic)
    "ⲇ": "d",    # dalda
    "ⲉ": "e",    # ei
    "ⲋ": "s",    # sou (stigma, rare)
    "ⲍ": "z",    # zeta
    "ⲏ": "eː",   # eta (long e)
    "ⲑ": "tʰ",   # theta
    "ⲓ": "i",    # iauda
    "ⲕ": "k",    # kappa
    "ⲗ": "l",    # lauda
    "ⲙ": "m",    # me
    "ⲛ": "n",    # ne
    "ⲝ": "ks",   # ksi
    "ⲟ": "o",    # o
    "ⲡ": "p",    # pi
    "ⲣ": "r",    # ro
    "ⲥ": "s",    # sima
    "ⲧ": "t",    # tau
    "ⲩ": "u",    # he (upsilon)
    "ⲫ": "pʰ",   # phi
    "ⲭ": "kʰ",   # khi
    "ⲯ": "ps",   # psi
    "ⲱ": "oː",   # omega (long o)
    # 6 Demotic-origin letters
    "ϣ": "ʃ",    # shai
    "ϥ": "f",    # fai
    "ϧ": "x",    # khai (Bohairic only, voiceless velar fricative)
    "ϩ": "h",    # hori
    "ϫ": "tʃ",   # janja (affricate)
    "ϭ": "tʃʰ",  # cima (aspirated affricate, Sahidic)
    "ⳉ": "tʃ",   # akhmimic djandja (variant)
    # Coptic uppercase (U+2C80-U+2CAE in even positions)
    "Ⲁ": "a", "Ⲃ": "β", "Ⲅ": "ɣ", "Ⲇ": "d", "Ⲉ": "e",
    "Ⲋ": "s", "Ⲍ": "z", "Ⲏ": "eː", "Ⲑ": "tʰ", "Ⲓ": "i",
    "Ⲕ": "k", "Ⲗ": "l", "Ⲙ": "m", "Ⲛ": "n", "Ⲝ": "ks",
    "Ⲟ": "o", "Ⲡ": "p", "Ⲣ": "r", "Ⲥ": "s", "Ⲧ": "t",
    "Ⲩ": "u", "Ⲫ": "pʰ", "Ⲭ": "kʰ", "Ⲯ": "ps", "Ⲱ": "oː",
    "Ϣ": "ʃ", "Ϥ": "f", "Ϧ": "x", "Ϩ": "h", "Ϫ": "tʃ", "Ϭ": "tʃʰ",
    # Supralinear stroke (syllabic marker, ignore)
    "\u0304": "",  # combining macron (sometimes used)
    # Greek letters that appear in Coptic texts
    "α": "a", "β": "β", "γ": "ɣ", "δ": "d", "ε": "e",
    "ζ": "z", "η": "eː", "θ": "tʰ", "ι": "i", "κ": "k",
    "λ": "l", "μ": "m", "ν": "n", "ξ": "ks", "ο": "o",
    "π": "p", "ρ": "r", "σ": "s", "ς": "s", "τ": "t",
    "υ": "u", "φ": "pʰ", "χ": "kʰ", "ψ": "ps", "ω": "oː",
}


# ---------------------------------------------------------------------------
# PALI (IAST)  (Geiger 1943; Oberlies 2001)
# ---------------------------------------------------------------------------
# Wiktionary Pali entries use Latin IAST romanization with diacritics.
# IAST is essentially phonemic, so the map converts diacritics to IPA.
PALI_MAP: Dict[str, str] = {
    # Short vowels
    "a": "a", "i": "i", "u": "u", "e": "e", "o": "o",
    # Long vowels (macron)
    "ā": "aː", "ī": "iː", "ū": "uː",
    # Velar nasals
    "ṅ": "ŋ",
    # Palatals
    "c": "tʃ", "ch": "tʃʰ", "j": "dʒ", "jh": "dʒʰ", "ñ": "ɲ",
    # Retroflexes (underdots)
    "ṭ": "ʈ", "ṭh": "ʈʰ", "ḍ": "ɖ", "ḍh": "ɖʰ", "ṇ": "ɳ",
    # Dentals
    "t": "t", "th": "tʰ", "d": "d", "dh": "dʰ", "n": "n",
    # Labials
    "p": "p", "ph": "pʰ", "b": "b", "bh": "bʰ", "m": "m",
    # Semivowels
    "y": "j", "r": "r", "l": "l", "v": "ʋ",
    # Sibilants and aspirate
    "s": "s", "h": "h",
    # Retroflex sibilant (rare in Pali but appears)
    "ṣ": "ʂ",
    # Palatal sibilant
    "ś": "ɕ",
    # Anusvara (nasalisation)
    "ṃ": "ŋ",
    # Visarga (rare)
    "ḥ": "h",
    # Niggahita (synonym of anusvara)
    "ṁ": "ŋ",
    # Double consonants pass through (gemination preserved in transliteration)
    "k": "k", "kh": "kʰ", "g": "ɡ", "gh": "ɡʰ",
}


# ---------------------------------------------------------------------------
# CLASSICAL / OLD ARMENIAN  (Meillet 1913; Schmitt 1981)
# ---------------------------------------------------------------------------
# Classical Armenian pronunciation (5th century CE).
# 36 original Mesropian letters + 2 later additions.
# Unicode block: U+0530-U+058F.
OLD_ARMENIAN_MAP: Dict[str, str] = {
    # Uppercase Armenian (U+0531-U+0556)
    "Ա": "a",    # ayb
    "Բ": "b",    # ben
    "Գ": "ɡ",    # gim
    "Դ": "d",    # da
    "Ե": "e",    # ech
    "Զ": "z",    # za
    "Է": "eː",   # e (originally long e)
    "Ը": "ə",    # et (schwa)
    "Թ": "tʰ",   # to
    "Ժ": "ʒ",    # zhe
    "Ի": "i",    # ini
    "Լ": "l",    # liwn
    "Խ": "x",    # xeh
    "Ծ": "ts",   # ca
    "Կ": "k",    # ken
    "Հ": "h",    # ho
    "Ձ": "dz",   # ja
    "Ղ": "ɫ",    # ghad (velarized lateral)
    "Ճ": "tʃ",   # cheh
    "Մ": "m",    # men
    "Յ": "j",    # yi
    "Ն": "n",    # now
    "Շ": "ʃ",    # sha
    "Ո": "o",    # vo (word-initial = vo, elsewhere = o)
    "Չ": "tʃʰ",  # cha
    "Պ": "p",    # peh
    "Ջ": "dʒ",   # jheh
    "Ռ": "r",    # ra (trilled r)
    "Ս": "s",    # seh
    "Վ": "v",    # vew
    "Տ": "t",    # tiwn
    "Ր": "ɾ",    # reh (tap r)
    "Ց": "tsʰ",  # co
    "Ւ": "w",    # hiwn (originally w, later v)
    "Փ": "pʰ",   # piwr
    "Ք": "kʰ",   # keh
    "Օ": "oː",   # o (later addition, long o)
    "Ֆ": "f",    # feh (later addition)
    # Lowercase Armenian (U+0561-U+0587)
    "ա": "a",    # ayb
    "բ": "b",    # ben
    "գ": "ɡ",    # gim
    "դ": "d",    # da
    "ե": "e",    # ech
    "զ": "z",    # za
    "է": "eː",   # e
    "ը": "ə",    # et
    "թ": "tʰ",   # to
    "ժ": "ʒ",    # zhe
    "ի": "i",    # ini
    "լ": "l",    # liwn
    "խ": "x",    # xeh
    "ծ": "ts",   # ca
    "կ": "k",    # ken
    "հ": "h",    # ho
    "ձ": "dz",   # ja
    "ղ": "ɫ",    # ghad
    "ճ": "tʃ",   # cheh
    "մ": "m",    # men
    "յ": "j",    # yi
    "ն": "n",    # now
    "շ": "ʃ",    # sha
    "ո": "o",    # vo
    "չ": "tʃʰ",  # cha
    "պ": "p",    # peh
    "ջ": "dʒ",   # jheh
    "ռ": "r",    # ra
    "\u057D": "s",    # seh  ս
    "\u057E": "v",    # vew  վ
    "\u057F": "t",    # tiwn  տ
    "\u0580": "ɾ",    # reh  ր
    "\u0581": "tsʰ",  # co  ց
    "\u0582": "w",    # yiwn  ւ
    "\u0583": "pʰ",   # piwr  փ
    "\u0584": "kʰ",   # keh  ք
    "\u0585": "oː",   # oh  օ
    "\u0586": "f",    # feh  ֆ
    # Ligature
    "\u0587": "ev",   # ech-yiwn ligature  և
}


# ---------------------------------------------------------------------------
# OLD ENGLISH  (Hogg 1992; Campbell 1959)
# ---------------------------------------------------------------------------
# Old English used Latin alphabet + insular letters (ð, þ, ƿ, æ).
# Long vowels marked with macron in modern editions.
OLD_ENGLISH_MAP: Dict[str, str] = {
    # Special Old English characters
    "æ": "æ", "ǣ": "æː",       # ash (short/long)
    "Æ": "æ", "Ǣ": "æː",
    "ð": "ð", "Ð": "ð",         # eth (dental fricative)
    "þ": "θ", "Þ": "θ",         # thorn (dental fricative)
    "ƿ": "w", "Ƿ": "w",         # wynn
    # Long vowels (macron)
    "ā": "aː", "ē": "eː", "ī": "iː", "ō": "oː", "ū": "uː",
    "ȳ": "yː",
    # Short vowels
    "a": "ɑ", "e": "e", "i": "i", "o": "o", "u": "u", "y": "y",
    # Consonants (mostly transparent)
    "b": "b", "c": "k",          # c = /k/ (before back V) or /tʃ/ (before front V) — default /k/
    "d": "d", "f": "f",          # f = /f/ or /v/ (allophonic)
    "g": "ɡ",                    # g = /ɡ/ or /ɣ/ or /j/ (allophonic)
    "h": "h",                    # h = /h/ or /x/ or /ç/ (allophonic)
    "k": "k", "l": "l", "m": "m", "n": "n",
    "p": "p", "r": "r", "s": "s", "t": "t",
    "w": "w", "x": "ks", "z": "z",
    # Digraphs (common in OE)
    "sc": "ʃ",                   # sc = /ʃ/ in most environments
    "cg": "dʒ",                  # cg = /dʒ/
    # Uppercase (same values)
    "A": "ɑ", "B": "b", "C": "k", "D": "d", "E": "e", "F": "f",
    "G": "ɡ", "H": "h", "I": "i", "K": "k", "L": "l", "M": "m",
    "N": "n", "O": "o", "P": "p", "R": "r", "S": "s", "T": "t",
    "U": "u", "W": "w", "X": "ks", "Y": "y", "Z": "z",
    "Ā": "aː", "Ē": "eː", "Ī": "iː", "Ō": "oː", "Ū": "uː",
    "Ȳ": "yː",
}


# ---------------------------------------------------------------------------
# GE'EZ / ETHIOPIC  (Dillmann 1907; Tropper 2002; Gragg 1997)
# ---------------------------------------------------------------------------
# Ethiopic is an abugida: each character = consonant + vowel order.
# 7 vowel orders per consonant: ä(default), u, i, a, e, ə/ɨ, o
# Unicode block: U+1200-U+137F.
GEEZ_MAP: Dict[str, str] = {
    # h-series (ሀ hoy)
    "ሀ": "ha", "ሁ": "hu", "ሂ": "hi", "ሃ": "haː", "ሄ": "he", "ህ": "hɨ", "ሆ": "ho",
    # l-series (ለ lawi)
    "ለ": "la", "ሉ": "lu", "ሊ": "li", "ላ": "laː", "ሌ": "le", "ል": "lɨ", "ሎ": "lo",
    # ḥ-series (ሐ ḥawt)  — pharyngeal
    "ሐ": "ħa", "ሑ": "ħu", "ሒ": "ħi", "ሓ": "ħaː", "ሔ": "ħe", "ሕ": "ħɨ", "ሖ": "ħo",
    # m-series (መ may)
    "መ": "ma", "ሙ": "mu", "ሚ": "mi", "ማ": "maː", "ሜ": "me", "ም": "mɨ", "ሞ": "mo",
    # ś-series (ሠ śawt) — voiceless palatal (merged with s in Ge'ez)
    "ሠ": "sa", "ሡ": "su", "ሢ": "si", "ሣ": "saː", "ሤ": "se", "ሥ": "sɨ", "ሦ": "so",
    # r-series (ረ rəʾs)
    "ረ": "ra", "ሩ": "ru", "ሪ": "ri", "ራ": "raː", "ሬ": "re", "ር": "rɨ", "ሮ": "ro",
    # s-series (ሰ sat)
    "ሰ": "sa", "ሱ": "su", "ሲ": "si", "ሳ": "saː", "ሴ": "se", "ስ": "sɨ", "ሶ": "so",
    # q-series (ቀ qaf)  — velar ejective
    "ቀ": "kʼa", "ቁ": "kʼu", "ቂ": "kʼi", "ቃ": "kʼaː", "ቄ": "kʼe", "ቅ": "kʼɨ", "ቆ": "kʼo",
    # b-series (በ bet)
    "በ": "ba", "ቡ": "bu", "ቢ": "bi", "ባ": "baː", "ቤ": "be", "ብ": "bɨ", "ቦ": "bo",
    # t-series (ተ taw)
    "ተ": "ta", "ቱ": "tu", "ቲ": "ti", "ታ": "taː", "ቴ": "te", "ት": "tɨ", "ቶ": "to",
    # ḫ-series (ኀ ḫarm) — velar fricative (merged with h in some traditions)
    "ኀ": "xa", "ኁ": "xu", "ኂ": "xi", "ኃ": "xaː", "ኄ": "xe", "ኅ": "xɨ", "ኆ": "xo",
    # n-series (ነ nahas)
    "ነ": "na", "ኑ": "nu", "ኒ": "ni", "ና": "naː", "ኔ": "ne", "ን": "nɨ", "ኖ": "no",
    # ʾ-series (አ ʾalf) — glottal stop
    "አ": "ʔa", "ኡ": "ʔu", "ኢ": "ʔi", "ኣ": "ʔaː", "ኤ": "ʔe", "እ": "ʔɨ", "ኦ": "ʔo",
    # k-series (ከ kaf)
    "ከ": "ka", "ኩ": "ku", "ኪ": "ki", "ካ": "kaː", "ኬ": "ke", "ክ": "kɨ", "ኮ": "ko",
    # w-series (ወ wawe)
    "ወ": "wa", "ዉ": "wu", "ዊ": "wi", "ዋ": "waː", "ዌ": "we", "ው": "wɨ", "ዎ": "wo",
    # ʿ-series (ዐ ʿayn) — pharyngeal
    "ዐ": "ʕa", "ዑ": "ʕu", "ዒ": "ʕi", "ዓ": "ʕaː", "ዔ": "ʕe", "ዕ": "ʕɨ", "ዖ": "ʕo",
    # z-series (ዘ zay)
    "ዘ": "za", "ዙ": "zu", "ዚ": "zi", "ዛ": "zaː", "ዜ": "ze", "ዝ": "zɨ", "ዞ": "zo",
    # ž-series (ዠ žə)  — voiced palatal fricative
    "ዠ": "ʒa", "ዡ": "ʒu", "ዢ": "ʒi", "ዣ": "ʒaː", "ዤ": "ʒe", "ዥ": "ʒɨ", "ዦ": "ʒo",
    # y-series (የ yaman)
    "የ": "ja", "ዩ": "ju", "ዪ": "ji", "ያ": "jaː", "ዬ": "je", "ይ": "jɨ", "ዮ": "jo",
    # d-series (ደ dənt)
    "ደ": "da", "ዱ": "du", "ዲ": "di", "ዳ": "daː", "ዴ": "de", "ድ": "dɨ", "ዶ": "do",
    # ǧ-series (ጀ ǧə)  — voiced palatal affricate
    "ጀ": "dʒa", "ጁ": "dʒu", "ጂ": "dʒi", "ጃ": "dʒaː", "ጄ": "dʒe", "ጅ": "dʒɨ", "ጆ": "dʒo",
    # g-series (ገ gaml)
    "ገ": "ɡa", "ጉ": "ɡu", "ጊ": "ɡi", "ጋ": "ɡaː", "ጌ": "ɡe", "ግ": "ɡɨ", "ጎ": "ɡo",
    # ṭ-series (ጠ ṭayt) — ejective
    "ጠ": "tʼa", "ጡ": "tʼu", "ጢ": "tʼi", "ጣ": "tʼaː", "ጤ": "tʼe", "ጥ": "tʼɨ", "ጦ": "tʼo",
    # č-series (ጨ čə)  — ejective affricate
    "ጨ": "tʃʼa", "ጩ": "tʃʼu", "ጪ": "tʃʼi", "ጫ": "tʃʼaː", "ጬ": "tʃʼe", "ጭ": "tʃʼɨ", "ጮ": "tʃʼo",
    # p-series (ጰ pa — rare, mainly loanwords)
    "ጰ": "pa", "ጱ": "pu", "ጲ": "pi", "ጳ": "paː", "ጴ": "pe", "ጵ": "pɨ", "ጶ": "po",
    # ṣ-series (ጸ ṣaday) — ejective
    "ጸ": "sʼa", "ጹ": "sʼu", "ጺ": "sʼi", "ጻ": "sʼaː", "ጼ": "sʼe", "ጽ": "sʼɨ", "ጾ": "sʼo",
    # ṣ'-series (ፀ ṣ'appa — alternate ejective s, merged)
    "ፀ": "sʼa", "ፁ": "sʼu", "ፂ": "sʼi", "ፃ": "sʼaː", "ፄ": "sʼe", "ፅ": "sʼɨ", "ፆ": "sʼo",
    # f-series (ፈ af)
    "ፈ": "fa", "ፉ": "fu", "ፊ": "fi", "ፋ": "faː", "ፌ": "fe", "ፍ": "fɨ", "ፎ": "fo",
    # p'-series (ፐ psa — mainly modern, some classical loanwords)
    "ፐ": "pa", "ፑ": "pu", "ፒ": "pi", "ፓ": "paː", "ፔ": "pe", "ፕ": "pɨ", "ፖ": "po",
}


# ---------------------------------------------------------------------------
# BIBLICAL HEBREW  (Blau 2010; Khan 2020 — Tiberian pronunciation)
# ---------------------------------------------------------------------------
# Hebrew consonants (22 letters) + niqqud vowel points + dagesh.
# Tiberian vocalization is the standard scholarly system.
# Unicode: consonants U+05D0-U+05EA, points U+05B0-U+05BD.
BIBLICAL_HEBREW_MAP: Dict[str, str] = {
    # Consonants
    "א": "ʔ",    # alef
    "ב": "v",    # bet (default spirant; with dagesh = b)
    "ג": "ɣ",    # gimel (default spirant; with dagesh = ɡ)
    "ד": "ð",    # dalet (default spirant; with dagesh = d)
    "ה": "h",    # he
    "ו": "w",    # vav
    "ז": "z",    # zayin
    "ח": "ħ",    # het (pharyngeal)
    "ט": "tˤ",   # tet (emphatic)
    "י": "j",    # yod
    "כ": "x",    # kaf (default spirant; with dagesh = k)
    "ך": "x",    # final kaf
    "ל": "l",    # lamed
    "מ": "m",    # mem
    "ם": "m",    # final mem
    "נ": "n",    # nun
    "ן": "n",    # final nun
    "ס": "s",    # samekh
    "ע": "ʕ",    # ayin (pharyngeal)
    "פ": "f",    # pe (default spirant; with dagesh = p)
    "ף": "f",    # final pe
    "צ": "sˤ",   # tsadi (emphatic)
    "ץ": "sˤ",   # final tsadi
    "ק": "q",    # qof
    "ר": "r",    # resh
    "ש": "ʃ",    # shin (default; with shin dot = ʃ, with sin dot = s)
    "ת": "θ",    # tav (default spirant; with dagesh = t)
    # Niqqud vowel points
    "\u05B0": "ə",    # shva (schwa)
    "\u05B1": "ɛ̆",    # hataf segol
    "\u05B2": "ă",    # hataf patah
    "\u05B3": "ɔ̆",    # hataf qamats
    "\u05B4": "i",    # hiriq
    "\u05B5": "eː",   # tsere
    "\u05B6": "ɛ",    # segol
    "\u05B7": "a",    # patah
    "\u05B8": "ɔː",   # qamats (long a or o depending on context)
    "\u05B9": "oː",   # holam
    "\u05BA": "oː",   # holam haser (for vav)
    "\u05BB": "u",    # qubuts
    # Dagesh (gemination/plosive marker — we can't fully resolve b/v etc.
    # without context, but we mark its presence)
    "\u05BC": "",      # dagesh (absorbed into consonant reading)
    # Shin/sin dots
    "\u05C1": "",      # shin dot (ש + dot = ʃ, already default)
    "\u05C2": "",      # sin dot  (ש + dot = s; can't resolve without combining)
    # Maqaf (hyphen)
    "\u05BE": "-",     # maqaf
    # Sof pasuq (verse separator)
    "\u05C3": "",      # sof pasuq
    # Meteg
    "\u05BD": "",      # meteg (secondary stress marker)
    # Rafe (mark indicating spirant, no dagesh)
    "\u05BF": "",      # rafe
    # Vav with holam (common combination)
    "וֹ": "oː",       # vav + holam = long o
    # Vav with shuruk
    "וּ": "uː",       # vav + dagesh = shuruk (long u)
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
    "non": OLD_NORSE_MAP,       # Old Norse
    "got": GOTHIC_MAP,          # Gothic
    "chu": OCS_MAP,             # Old Church Slavonic
    "akk": AKKADIAN_MAP,        # Akkadian
    "sux": SUMERIAN_MAP,        # Sumerian
    "gmy": MYCENAEAN_MAP,       # Mycenaean Greek
    # Tier 2 languages
    "cop": COPTIC_MAP,          # Coptic
    "pli": PALI_MAP,            # Pali
    "xcl": OLD_ARMENIAN_MAP,    # Classical Armenian
    "ang": OLD_ENGLISH_MAP,     # Old English
    "gez": GEEZ_MAP,            # Ge'ez
    "hbo": BIBLICAL_HEBREW_MAP, # Biblical Hebrew
    "xht": HITTITE_MAP,        # Hattic (uses same cuneiformist conventions as Hittite)
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
