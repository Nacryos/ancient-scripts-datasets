#!/usr/bin/env python3
"""Extract Phoenician (phn) and Elamite (elx) lexicons from Wiktionary data.

Sources:
  - Wiktionary Category:Phoenician_lemmas (175 entries, fetched via API)
  - Wiktionary Appendix:Phoenician_Swadesh_list (fetched via API)
  - Wiktionary Category:Elamite_lemmas (12 entries, fetched via API)
  - Wiktionary Appendix:Elamite_word_list (Blazek 2015)
  - Wiktionary Appendix:Elamite_Swadesh_list

All data originally fetched via Wiktionary MediaWiki API.
This script uses cached JSON files for lemma pages and inline data
for Swadesh/word lists to avoid rate limiting.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Set up imports
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class
from transliteration_maps import transliterate

LEXICON_DIR = ROOT / "data" / "training" / "lexicons"

# ---------------------------------------------------------------------------
# Phoenician Unicode -> Transliteration mapping
# ---------------------------------------------------------------------------
PHOENICIAN_UNICODE_MAP = {
    "\U00010900": "\u02be",  # ALEPH -> ʾ
    "\U00010901": "b",       # BET
    "\U00010902": "g",       # GIMEL
    "\U00010903": "d",       # DALET
    "\U00010904": "h",       # HE
    "\U00010905": "w",       # WAW
    "\U00010906": "z",       # ZAYIN
    "\U00010907": "\u1e25",  # HET -> ḥ
    "\U00010908": "\u1e6d",  # TET -> ṭ
    "\U00010909": "y",       # YOD
    "\U0001090A": "k",       # KAF
    "\U0001090B": "l",       # LAMEDH
    "\U0001090C": "m",       # MEM
    "\U0001090D": "n",       # NUN
    "\U0001090E": "s",       # SAMEKH
    "\U0001090F": "\u02bf",  # AYIN -> ʿ
    "\U00010910": "p",       # PE
    "\U00010911": "\u1e63",  # TSADE -> ṣ
    "\U00010912": "q",       # QOF
    "\U00010913": "r",       # RESH
    "\U00010914": "\u0161",  # SHIN -> š
    "\U00010915": "t",       # TAW
}


def phoenician_script_to_transliteration(text: str) -> str:
    """Convert Phoenician Unicode script to conventional transliteration."""
    return "".join(PHOENICIAN_UNICODE_MAP.get(c, c) for c in text)


def clean_transliteration(tr: str) -> str:
    """Clean up a transliteration string."""
    tr = re.sub(r"\{\{[^}]*\}\}", "", tr)
    # Take first form if comma-separated (e.g., "gādēr, gādīr")
    if ", " in tr:
        tr = tr.split(", ")[0]
    tr = tr.strip().strip(",;").strip()
    tr = tr.lstrip("*")
    tr = tr.strip("/")
    # Remove HTML/template artifacts
    tr = re.sub(r"<[^>]+>", "", tr)
    # Remove triple quotes (wiki bold markers)
    tr = tr.replace("'''", "")
    return tr.strip()


def clean_definition(defn: str) -> str:
    """Clean a definition string."""
    defn = re.sub(r"\{\{[^}]*\}\}", "", defn)
    defn = re.sub(r"\[\[([^\]|]*\|)?([^\]]*)\]\]", r"\2", defn)
    defn = defn.strip().strip("[]{}:").strip()
    return defn


# ---------------------------------------------------------------------------
# PHOENICIAN EXTRACTION
# ---------------------------------------------------------------------------
def extract_phoenician() -> list[dict]:
    """Extract Phoenician lexicon entries from cached Wiktionary data."""
    print("=== Extracting Phoenician (phn) ===")

    entries = {}  # keyed by transliteration to deduplicate

    # 1. Load cached lemma page content
    cache_path = ROOT / "phn_raw_content.json"
    if cache_path.exists():
        print(f"  Loading cached pages from {cache_path}")
        with open(cache_path, "r", encoding="utf-8") as f:
            pages = json.load(f)
        print(f"  Loaded {len(pages)} pages")
    else:
        print(f"  ERROR: Cache file not found: {cache_path}")
        pages = {}

    skipped_letters = 0
    for title, content in pages.items():
        phn_match = re.search(r"==Phoenician==\s*(.*?)(?=\n==[^=]|\Z)", content, re.DOTALL)
        if not phn_match:
            continue
        phn_section = phn_match.group(1)

        # Skip letter entries
        if re.search(r"===Letter===", phn_section) and len(title) <= 2:
            skipped_letters += 1
            continue

        # Extract transliterations
        ts_matches = re.findall(r"\bts=([^|}]+)", phn_section)
        tr_matches = re.findall(r"\btr=([^|}]+)", phn_section)
        ipa_matches = re.findall(r"\{\{IPA\|phn\|/([^/]+)/", phn_section)

        tr = ""
        if ts_matches:
            tr = clean_transliteration(ts_matches[0])
        elif tr_matches:
            tr = clean_transliteration(tr_matches[0])

        if not tr:
            # Derive transliteration from Phoenician script
            tr = phoenician_script_to_transliteration(title)

        if not tr:
            continue

        ipa_direct = ""
        if ipa_matches:
            ipa_direct = ipa_matches[0].strip()

        # Get definition
        defn_lines = re.findall(r"#\s*([^\n]+)", phn_section)
        defn = ""
        for line in defn_lines:
            cleaned = clean_definition(line)
            if cleaned and not cleaned.startswith("*") and len(cleaned) > 1:
                defn = cleaned
                break

        if tr not in entries:
            entries[tr] = {
                "word": title,
                "tr": tr,
                "ipa_direct": ipa_direct,
                "defn": defn,
                "source": "wiktionary",
            }

    print(f"  Skipped {skipped_letters} letter entries")
    print(f"  Extracted {len(entries)} entries from lemmas")

    # 2. Add entries from Phoenician Swadesh list
    # These are Phoenician script words from the Swadesh list that may not
    # have appeared in the lemma pages, or entries without transliteration.
    # Data extracted from Wiktionary Appendix:Phoenician_Swadesh_list
    swadesh_entries = [
        # (phoenician_script, concept)
        # Only entries NOT marked "unattested"
        ("\U00010900\U0001090D\U0001090A", "I"),           # ʾnk
        ("\U00010900\U00010915", "you(sg)"),                # ʾt
        ("\U00010904\U00010900", "he"),                     # hʾ
        ("\U00010900\U0001090D\U00010907\U0001090D", "we"), # ʾnḥn
        ("\U00010900\U00010915\U0001090C", "you(pl)"),      # ʾtm
        ("\U00010904\U0001090C\U00010915", "they"),         # hmt
        ("\U00010906", "this"),                             # z
        ("\U00010910\U00010904", "here"),                   # ph
        ("\U00010914\U0001090C", "there"),                  # šm
        ("\U0001090C\U00010909", "who"),                    # my
        ("\U0001090C", "what"),                             # m
        ("\U00010900\U00010909", "where"),                  # ʾy
        ("\U0001090A\U0001090C", "when"),                   # km
        ("\U00010901\U0001090B", "not"),                    # bl
        ("\U0001090A\U0001090B", "all"),                    # kl
        ("\U00010913\U00010901", "many"),                   # rb
        ("\U00010906\U00010913", "other"),                  # zr
        ("\U00010900\U00010907\U00010903", "one"),          # ʾḥd
        ("\U00010914\U0001090D\U00010909\U0001090C", "two"), # šnym
        ("\U00010914\U0001090B\U00010914", "three"),        # šlš
        ("\U00010900\U00010913\U00010901\U0001090F", "four"), # ʾrbʿ
        ("\U00010907\U0001090C\U00010914", "five"),         # ḥmš
        ("\U00010900\U00010903\U00010913", "big"),          # ʾdr
        ("\U00010900\U00010913\U0001090A", "long"),         # ʾrk
        ("\U00010913\U00010907\U00010901", "wide"),         # rḥb
        ("\U0001090F\U00010901\U00010904", "thick"),        # ʿbh
        ("\U00010914\U00010912\U0001090B", "heavy"),        # šql
        ("\U00010912\U00010908\U0001090D", "small"),        # qṭn
        ("\U00010903\U00010912\U00010912", "thin"),         # dqq
        ("\U00010900\U00010914\U00010915", "woman"),        # ʾšt
        ("\U00010902\U00010901\U00010913", "man(male)"),    # gbr
        ("\U00010900\U00010903\U0001090C", "man(human)"),   # ʾdm
        ("\U00010909\U0001090B\U00010903", "child"),        # yld
        ("\U00010901\U0001090F\U0001090B", "husband"),      # bʿl
        ("\U00010900\U0001090C", "mother"),                 # ʾm
        ("\U00010900\U00010901", "father"),                 # ʾb
        ("\U00010907\U00010909\U00010915", "animal"),       # ḥyt
        ("\U00010911\U00010910\U00010913", "bird"),         # ṣpr
        ("\U0001090A\U0001090B\U00010901", "dog"),          # klb
        ("\U0001090F\U00010911", "tree"),                   # ʿṣ
        ("\U00010909\U0001090F\U00010913", "forest"),       # yʿr
        ("\U00010912\U0001090D\U00010900", "stick"),        # qnʾ
        ("\U00010910\U00010913", "fruit"),                  # pr
        ("\U00010911\U0001090C\U00010907", "seed"),         # ṣmḥ
        ("\U00010914\U00010913\U00010914", "root"),         # šrš
        ("\U0001090F\U00010913", "skin"),                   # ʿr
        ("\U00010901\U00010914\U00010913", "meat"),         # bšr
        ("\U00010903\U0001090C", "blood"),                  # dm
        ("\U0001090F\U00010911\U0001090C", "bone"),         # ʿṣm
        ("\U00010912\U00010913\U0001090D", "horn"),         # qrn
        ("\U00010914\U0001090F\U00010913\U00010915", "hair"), # šʿrt
        ("\U00010913\U00010900\U00010914", "head"),         # rʾš
        ("\U0001090F\U0001090D", "eye"),                    # ʿn
        ("\U00010910", "mouth"),                            # p
        ("\U0001090B\U00010914\U0001090D", "tongue"),       # lšn
        ("\U00010910\U0001090F\U0001090C", "foot"),         # pʿm
        ("\U00010901\U00010913\U0001090A", "knee"),         # brk
        ("\U00010909\U00010903", "hand"),                   # yd
        ("\U00010901\U00010908\U0001090D", "belly"),        # bṭn
        ("\U0001090B\U00010901", "heart"),                  # lb
        ("\U00010914\U0001090C\U0001090D", "fat"),          # šmn
    ]

    swadesh_added = 0
    for phn_script, concept in swadesh_entries:
        tr = phoenician_script_to_transliteration(phn_script)
        if tr and tr not in entries:
            entries[tr] = {
                "word": phn_script,
                "tr": tr,
                "ipa_direct": "",
                "defn": concept,
                "source": "wiktionary",
            }
            swadesh_added += 1

    print(f"  Added {swadesh_added} entries from Swadesh list")
    print(f"  Total unique Phoenician entries: {len(entries)}")
    return list(entries.values())


# ---------------------------------------------------------------------------
# ELAMITE EXTRACTION
# ---------------------------------------------------------------------------
def extract_elamite() -> list[dict]:
    """Extract Elamite lexicon entries from cached Wiktionary data."""
    print("\n=== Extracting Elamite (elx) ===")

    entries = {}

    # 1. Load cached lemma page content
    cache_path = ROOT / "elx_raw_content.json"
    if cache_path.exists():
        print(f"  Loading cached pages from {cache_path}")
        with open(cache_path, "r", encoding="utf-8") as f:
            pages = json.load(f)
        print(f"  Loaded {len(pages)} pages")
    else:
        print(f"  ERROR: Cache file not found: {cache_path}")
        pages = {}

    for title, content in pages.items():
        elx_match = re.search(r"==Elamite==\s*(.*?)(?=\n==[^=]|\Z)", content, re.DOTALL)
        if not elx_match:
            continue
        elx_section = elx_match.group(1)

        ts_matches = re.findall(r"\bts=([^|}]+)", elx_section)
        tr_matches = re.findall(r"\btr=([^|}]+)", elx_section)

        tr = ""
        if ts_matches:
            tr = clean_transliteration(ts_matches[0])
        elif tr_matches:
            tr = clean_transliteration(tr_matches[0])

        if not tr:
            continue

        # Clean cuneiform tr notation: remove subscripts, superscripts, dots
        clean_tr = re.sub(r"[₀-₉]", "", tr)
        clean_tr = re.sub(r"\{[^}]*\}", "", clean_tr)
        clean_tr = re.sub(r"<[^>]*>", "", clean_tr)
        clean_tr = re.sub(r"\{\{sup\|[^}]+\}\}", "", clean_tr)
        clean_tr = clean_tr.replace("⁠", "").strip()
        # Remove Sumerian determinative prefixes (fully uppercase segments)
        # e.g., "ÍD Purattu" -> "Purattu", "MAR du ka₄" -> "du ka"
        clean_tr = re.sub(r"\b[A-ZÁÍÚÉÓ]{2,}\b\s*", "", clean_tr).strip()
        # Skip entries that are entirely Sumerian logograms
        if not clean_tr or (clean_tr == clean_tr.upper() and re.match(r"^[A-Z ]+$", clean_tr)):
            continue
        # Lowercase for transliteration consistency
        clean_tr = clean_tr.lower()

        defn_lines = re.findall(r"#\s*([^\n]+)", elx_section)
        defn = ""
        for line in defn_lines:
            cleaned = clean_definition(line)
            if cleaned and len(cleaned) > 1:
                defn = cleaned
                break

        if clean_tr and clean_tr not in entries:
            entries[clean_tr] = {
                "word": title,
                "tr": clean_tr,
                "ipa_direct": "",
                "defn": defn,
                "source": "wiktionary",
            }

    print(f"  Extracted {len(entries)} entries from lemmas")

    # 2. Add entries from Elamite word list (Blazek 2015)
    # Data from Wiktionary Appendix:Elamite_word_list
    # Source: Grillot-Susini (1987), Hinz & Koch (1987)
    # Format: (transliteration, English_gloss)
    wordlist_entries = [
        ("muru", "earth"),
        ("sukma", "dust"),
        ("amni", "mountain"),
        ("šariut", "shore"),
        ("duraš", "cave"),
        ("zul", "water"),
        ("kam", "sea"),
        ("husa", "tree"),
        ("huhqat", "wood"),
        ("har", "stone"),
        ("nahunte", "sun"),
        ("napir", "moon"),
        ("mardu", "star"),
        ("luk", "lightning"),
        ("manzat", "rainbow"),
        ("hun", "light"),
        ("šaddaku", "shadow"),
        ("simein", "air"),
        ("teip", "rain"),
        ("lim", "fire"),
        ("bali", "man"),
        ("balina", "male"),
        ("puhu", "young man"),
        ("zin", "infant"),
        ("irti", "wife"),
        ("atta", "father"),
        ("amma", "mother"),
        ("šak", "son"),
        ("pak", "daughter"),
        ("igi", "brother"),
        ("šutu", "sister"),
        ("eri", "uncle"),
        ("iza", "cousin"),
        ("u", "I"),
        ("ni", "you(sg)"),
        ("nika", "we"),
        ("numi", "you(pl)"),
        ("kun", "animal"),
        ("kutu", "cattle"),
        ("hidu", "sheep"),
        ("rapdu", "ram"),
        ("kari", "lamb"),
        ("pappi", "pig"),
        ("kipšu", "goat"),
        ("kumaš", "he-goat"),
        ("pitu", "kid"),
        ("lakpilan", "horse"),
        ("dudu", "foal"),
        ("tranku", "donkey"),
        ("paha", "mule"),
        ("zamama", "fowl"),
        ("rum", "hen"),
        ("hippur", "goose"),
        ("šudaba", "duck"),
        ("hupie", "nest"),
        ("tiut", "bird"),
        ("bazizi", "eagle"),
        ("halkini", "dog"),
        ("duma", "wolf"),
        ("zibbaru", "camel"),
        ("lahi", "scorpion"),
        ("zanabuna", "worm"),
        ("šin", "snake"),
        ("haten", "skin"),
        ("išti", "flesh"),
        ("san", "blood"),
        ("kassu", "horn"),
        ("ukku", "head"),
        ("elti", "eye"),
        ("siri", "ear"),
        ("šiumme", "nose"),
        ("tit", "tongue"),
        ("sihha", "tooth"),
        ("tipi", "neck"),
        ("kirpi", "hand"),
        ("pur", "fingernail"),
        ("pat", "foot"),
        ("putmaš", "feather"),
        ("buni", "heart"),
        ("ruelpa", "liver"),
        ("ruku", "testicle"),
        ("taakme", "life"),
        ("halpi", "die"),
        ("ibbak", "strong"),
        ("siitti", "heal"),
        ("abebe", "food"),
        ("kurat", "roast"),
        ("piti", "pitcher"),
        ("šiprium", "bread"),
        ("eul", "flour"),
        ("hurpi", "fruit"),
        ("appi", "oil"),
        ("abba", "grease"),
        ("anzi", "salt"),
        ("hallaki", "honey"),
        ("sirna", "milk"),
        ("annain", "fermented drink"),
        ("tamšium", "cloth"),
        ("tukkime", "wool"),
        ("zali", "linen"),
        ("qalitam", "cotton"),
        ("dabarrium", "felt"),
        ("šairšatti", "leather"),
        ("kurzaiš", "weave"),
        ("ukkulaki", "cloak"),
        ("huelip", "coat"),
        ("hašair", "shoe"),
        ("ukku.bati", "hat"),
        ("kurip", "glove"),
        ("qarrah", "ornament"),
        ("šami", "ring"),
        ("ahhuum", "comb"),
        ("hasu", "ointment"),
        ("šuha", "mirror"),
        ("aain", "house"),
        ("tuuš", "yard"),
        ("huel", "door"),
        ("halti", "doorpost"),
        ("kunnir", "window"),
        ("teipta", "wall"),
        ("kuramma", "stove"),
        ("giutmašte", "blanket"),
        ("hunir", "lamp"),
        ("ari", "roof"),
        ("teti", "beam"),
        ("šali", "post"),
        ("šiltur", "board"),
        ("erientum", "brick"),
        ("halla", "field"),
        ("yadda", "garden"),
        ("aapih", "plow"),
        ("atti", "shovel"),
        ("par", "seed"),
        ("halteme", "harvest"),
        ("tarmi", "grain"),
        ("šiman", "wheat"),
        ("kurrusa", "barley"),
        ("mikkima", "flower"),
        ("innain", "sap"),
        ("huta", "do"),
        ("rabba", "tie"),
        ("šam", "rope"),
        ("halpiya", "strike"),
        ("šahši", "cut"),
        ("ipiš", "ax"),
        ("sah", "pull"),
        ("kuši", "build"),
        ("duliibbe", "bore"),
        ("elpi", "saw"),
        ("sael", "hammer"),
        ("sikti", "nail"),
        ("kaszira", "smith"),
        ("kassa", "forge"),
        ("šarih", "cast"),
        ("lansitie", "gold"),
        ("lani", "silver"),
        ("erini", "copper"),
        ("hargi", "iron"),
        ("rikur", "lead"),
        ("anaku", "tin"),
        ("halat", "clay"),
        ("šeriit", "basket"),
        ("zabar", "rug"),
        ("karsuda", "paint"),
        ("marih", "catch"),
        ("uzzun", "walk"),
        ("izziš", "come"),
        ("kutina", "carry"),
        ("telak", "bring"),
        ("da", "send"),
        ("bau", "road"),
        ("zappan", "yoke"),
        ("dumaš", "take"),
        ("dunih", "give"),
        ("kutun", "preserve"),
        ("sarih", "destroy"),
        ("bakkah", "find"),
        ("sir", "rich"),
        ("unsaha", "pay"),
        ("teumbe", "wages"),
        ("šakkime", "price"),
        ("kiik", "behind"),
        ("tibbe", "before"),
        ("atin", "inside"),
        ("kidur", "outside"),
        ("šara", "under"),
        ("kate", "place"),
        ("bela", "put"),
        ("tariir", "join"),
        ("teiš", "open"),
        ("kappaš", "close"),
        ("šadaniqa", "far"),
        ("hatin", "east"),
        ("šutin", "west"),
        ("azzaqa", "large"),
        ("tila", "small"),
        ("zikki", "thin"),
        ("dušarama", "deep"),
        ("mašuum", "flat"),
        ("išturraka", "straight"),
        ("irpi", "round"),
        ("purur", "circle"),
        ("šebe", "sphere"),
        ("ki", "one"),
        ("mar", "two"),
        ("ziti", "three"),
        ("tuku", "five"),
        ("unra", "all"),
        ("iršeikki", "many"),
        ("harikki", "few"),
        ("huh", "full"),
        ("pirni", "half"),
        ("appuqana", "first"),
        ("tuk", "pair"),
        ("dalari", "time"),
        ("puhuna", "young"),
        ("qara", "old"),
        ("am", "now"),
        ("mur", "end"),
        ("tarma", "finish"),
        ("akada", "always"),
        ("meulli", "long-time"),
        ("kiqa", "again"),
        ("nan", "day"),
        ("šiutmana", "night"),
        ("teman", "evening"),
        ("nanamena", "month"),
        ("piel", "year"),
        ("tena", "sweet"),
        ("luluki", "sour"),
        ("hap", "hear"),
        ("šana", "quiet"),
        ("siya", "see"),
        ("šammiš", "show"),
        ("šimiut", "white"),
        ("dabantina", "blue"),
        ("hulapna", "green"),
        ("šuntina", "yellow"),
        ("abbara", "heavy"),
        ("zitiqa", "dry"),
        ("šada", "luck"),
        ("tannamme", "happy"),
        ("hani", "love"),
        ("qaduukqa", "dare"),
        ("ipši", "fear"),
        ("hamiti", "faithful"),
        ("siri", "true"),
        ("titi", "lie"),
        ("aa", "good"),
        ("mušnuik", "bad"),
        ("hisa", "praise"),
        ("šišni", "beautiful"),
        ("elma", "think"),
        ("uriš", "believe"),
        ("turnah", "know"),
        ("meen", "need"),
        ("ayak", "and"),
        ("appa", "because"),
        ("anka", "if"),
        ("ingi", "no"),
        ("akka", "which"),
        ("turi", "speak"),
        ("tiri", "say"),
        ("šukkit", "word"),
        ("hiš", "name"),
        ("kullah", "promise"),
        ("talluh", "write"),
        ("bera", "read"),
        ("hal", "country"),
        ("aal", "city"),
        ("tašup", "people"),
        ("ahpi", "tribe"),
        ("sunki", "king"),
        ("simpti", "master"),
        ("šalur", "freeman"),
        ("libair", "servant"),
        ("pitiir", "enemy"),
        ("gugu", "peace"),
        ("hit", "army"),
        ("ulkina", "weapons"),
        ("qamban", "bow"),
        ("šukurrum", "spear"),
        ("karik", "helmet"),
        ("mete", "victory"),
        ("pukriir", "booty"),
        ("šutur", "law"),
        ("gini", "witness"),
        ("giri", "swear"),
        ("tuššuip", "thief"),
        ("nap", "god"),
        ("dala", "sacrifice"),
        ("šatin", "priest"),
        ("akpi", "holy"),
        ("kik", "heaven"),
    ]

    wordlist_added = 0
    for tr, gloss in wordlist_entries:
        # Clean: remove dots and accents
        clean_tr = tr.replace(".", "").strip()
        if clean_tr and clean_tr not in entries:
            entries[clean_tr] = {
                "word": tr,
                "tr": clean_tr,
                "ipa_direct": "",
                "defn": gloss,
                "source": "wiktionary",
            }
            wordlist_added += 1

    print(f"  Added {wordlist_added} entries from word list (Blazek 2015)")
    print(f"  Total unique Elamite entries: {len(entries)}")
    return list(entries.values())


# ---------------------------------------------------------------------------
# TSV GENERATION
# ---------------------------------------------------------------------------
def is_valid_entry(tr: str) -> bool:
    """Check if a transliteration entry is valid for inclusion."""
    # Skip template artifacts
    if "{{" in tr or "}}" in tr:
        return False
    # Skip very long entries (likely inscription fragments, not words)
    if len(tr) > 30:
        return False
    # Skip entries with spaces (multi-word phrases) unless very short
    if " " in tr and len(tr) > 10:
        return False
    # Skip entries that are all uppercase (Sumerian logograms)
    if tr == tr.upper() and re.match(r"^[A-Z ]+$", tr):
        return False
    # Must have at least one letter
    if not re.search(r"[a-zA-Zʾʿḥṭṣšā-ž]", tr):
        return False
    return True


def generate_tsv(entries: list[dict], iso: str, output_path: Path) -> int:
    """Generate a TSV lexicon file from extracted entries."""
    lines = ["Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID"]

    skipped = 0
    written = 0
    for entry in sorted(entries, key=lambda e: e["tr"]):
        tr = entry["tr"]

        # Validate entry
        if not is_valid_entry(tr):
            skipped += 1
            continue

        # Generate IPA
        ipa = ""
        if entry.get("ipa_direct"):
            ipa = entry["ipa_direct"]
            ipa = re.sub(r"[ˈˌ.]", "", ipa)

        if not ipa:
            ipa = transliterate(tr, iso)

        if not ipa:
            continue

        # Remove any remaining macrons/diacritics that passed through
        # (characters not in the transliteration map)
        # Keep only IPA-valid characters
        ipa = ipa.replace("ā", "aː").replace("ē", "eː").replace("ī", "iː")
        ipa = ipa.replace("ō", "oː").replace("ū", "uː")
        ipa = ipa.replace("á", "a").replace("é", "e").replace("í", "i")
        ipa = ipa.replace("ó", "o").replace("ú", "u")
        ipa = ipa.replace("ɔ", "o")  # map open-o to o for SCA

        # Generate SCA code
        sca = ipa_to_sound_class(ipa)
        if not sca:
            continue

        word = tr
        source = entry.get("source", "wiktionary")

        lines.append(f"{word}\t{ipa}\t{sca}\t{source}\t-\t-")
        written += 1

    if skipped:
        print(f"  Skipped {skipped} invalid entries")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Wrote {written} entries to {output_path}")
    return written


def main():
    print("Phoenician & Elamite Lexicon Extractor")
    print("=" * 50)

    phn_entries = extract_phoenician()
    phn_count = generate_tsv(phn_entries, "phn", LEXICON_DIR / "phn.tsv")

    elx_entries = extract_elamite()
    elx_count = generate_tsv(elx_entries, "elx", LEXICON_DIR / "elx.tsv")

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Phoenician (phn): {phn_count} entries")
    print(f"Elamite (elx):    {elx_count} entries")
    print(f"Sources:")
    print(f"  - Wiktionary Category:Phoenician_lemmas")
    print(f"  - Wiktionary Appendix:Phoenician_Swadesh_list")
    print(f"  - Wiktionary Category:Elamite_lemmas")
    print(f"  - Wiktionary Appendix:Elamite_word_list (Blazek 2015)")
    print(f"Output:")
    print(f"  {LEXICON_DIR / 'phn.tsv'}")
    print(f"  {LEXICON_DIR / 'elx.tsv'}")


if __name__ == "__main__":
    main()
