#!/usr/bin/env python3
"""Extract lexicon data for Avestan (ave), Old Persian (peo), and Phrygian (xpg).

Data sourced from Wiktionary Swadesh lists and category lemma pages.
Romanized forms are taken directly from Wiktionary's parenthesized
transliterations; IPA is generated via transliteration_maps; SCA via
cognate_pipeline's ipa_to_sound_class.

Usage:
    python scripts/extract_ave_peo_xpg.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class  # noqa: E402
from transliteration_maps import transliterate  # noqa: E402


# ---------------------------------------------------------------------------
# Wiktionary API helper
# ---------------------------------------------------------------------------
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds


def wiki_api(params: dict) -> dict:
    """Call the Wiktionary API and return parsed JSON, with retry on 429."""
    params["format"] = "json"
    qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    url = f"https://en.wiktionary.org/w/api.php?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
            else:
                raise


def fetch_rendered_page(page_title: str) -> str:
    """Fetch a Wiktionary page and return text-stripped HTML."""
    data = wiki_api({
        "action": "parse",
        "page": page_title,
        "prop": "text",
    })
    if "parse" not in data:
        return ""
    html = data["parse"]["text"]["*"]
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean


def fetch_page_by_id(page_id: int) -> tuple[str, str]:
    """Fetch a Wiktionary page by ID, return (title, cleaned_text)."""
    data = wiki_api({
        "action": "parse",
        "pageid": str(page_id),
        "prop": "text",
    })
    if "parse" not in data:
        return ("", "")
    html = data["parse"]["text"]["*"]
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return (data["parse"]["title"], clean)


def fetch_category_members(category: str) -> list[dict]:
    """Fetch all members of a Wiktionary category."""
    all_members = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": category,
        "cmlimit": "500",
    }
    while True:
        data = wiki_api(params)
        members = data.get("query", {}).get("categorymembers", [])
        all_members.extend(members)
        if "continue" not in data:
            break
        params["cmcontinue"] = data["continue"]["cmcontinue"]
        time.sleep(1)
    return all_members


# ---------------------------------------------------------------------------
# Swadesh list parsers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Hardcoded Swadesh data extracted from Wiktionary
# (verified from Wiktionary Appendix:Avestan_Swadesh_list, etc.)
# Format: (romanized_word, english_gloss)
# ---------------------------------------------------------------------------

AVESTAN_SWADESH = [
    # From Appendix:Avestan_Swadesh_list on Wiktionary
    ("azə̄m", "I"),
    ("tūm", "you"),
    ("tū", "you"),
    ("tuuə̄m", "you"),
    ("vaēm", "we"),
    ("vā", "we"),
    ("ahma", "we"),
    ("nā̊", "we"),
    ("yūžəm", "you_pl"),
    ("vā̊", "you_pl"),
    ("ha", "this"),
    ("aiiə̄m", "this"),
    ("aēm", "this"),
    ("ta", "that"),
    ("auua", "that"),
    ("ka", "who"),
    ("vīspa", "all"),
    ("hauruua", "whole"),
    ("aniia", "other"),
    ("aṇtara", "other"),
    ("aēuua", "one"),
    ("duua", "two"),
    ("θri", "three"),
    ("caθβar", "four"),
    ("paṇca", "five"),
    ("maza", "big"),
    ("darəga", "long"),
    ("pərəθu", "wide"),
    ("gouruš", "heavy"),
    ("strī", "woman"),
    ("nāirī", "woman"),
    ("nar", "man"),
    ("vīra", "man"),
    ("manuš", "human"),
    ("maṣ̌iia", "human"),
    ("paiti", "husband"),
    ("barəθrī", "mother"),
    ("mātar", "mother"),
    ("pitar", "father"),
    ("ząθar", "father"),
    ("masiia", "fish"),
    ("vaii", "bird"),
    ("mərəγa", "bird"),
    ("span", "dog"),
    ("aži", "snake"),
    ("varəša", "tree"),
    ("gao", "meat"),
    ("vohunī", "blood"),
    ("ast", "bone"),
    ("aēm", "egg"),
    ("sāra", "head"),
    ("gaoša", "ear"),
    ("karəna", "ear"),
    ("aši", "eye"),
    ("cašman", "eye"),
    ("daēman", "eye"),
    ("hizuuā", "tongue"),
    ("pad", "foot"),
    ("pāδa", "foot"),
    ("zānu", "knee"),
    ("zasta", "hand"),
    ("udara", "belly"),
    ("maršū", "belly"),
    ("mərəzāna", "belly"),
    ("paršta", "back"),
    ("varah", "breast"),
    ("fštāna", "breast"),
    ("zərəd", "heart"),
    ("xᵛaraiti", "to_drink"),
    ("vāiti", "to_blow"),
    ("zānaiti", "to_know"),
    ("vid", "to_know"),
    ("juuaiti", "to_live"),
    ("iriθiieiti", "to_die"),
    ("miriieitē", "to_die"),
    ("kərəntaiti", "to_cut"),
    ("kan", "to_dig"),
    ("gam", "to_come"),
    ("dā", "to_give"),
    ("dadāiti", "to_give"),
    ("naēnižaiti", "to_wash"),
    ("xᵛan", "sun"),
    ("huuarə", "sun"),
    ("māh", "moon"),
    ("star", "star"),
    ("ap", "water"),
    ("āpō", "water"),
    ("zraiiaŋh", "sea"),
    ("pąsnu", "dust"),
    ("zam", "earth"),
    ("būmi", "earth"),
    ("maēγa", "cloud"),
    ("aβra", "cloud"),
    ("snaoδa", "cloud"),
    ("dunman", "cloud"),
    ("dyaoš", "sky"),
    ("vāta", "wind"),
    ("snaēžaiti", "to_snow"),
    ("ātar", "fire"),
    ("ātriia", "ash"),
    ("dažaiti", "to_burn"),
    ("dažiiete", "to_burn"),
    ("kaofa", "mountain"),
    ("gairi", "mountain"),
    ("pauruuatā", "mountain"),
    ("spaēta", "white"),
    ("sāma", "black"),
    ("siāuua", "black"),
    ("xṣ̌ap", "night"),
    ("xṣ̌apan", "night"),
    ("asarə", "day"),
    ("xṣ̌apara", "day"),
    ("yārə", "year"),
    ("sarəta", "cold"),
    ("aota", "cold"),
    ("pərənāiiu", "old"),
    ("hu", "good"),
    ("friia", "good"),
    ("aka", "bad"),
    ("aγa", "bad"),
    ("aŋra", "bad"),
    ("āhīta", "dirty"),
    ("huška", "dry"),
    ("ərəš", "correct"),
    ("haiθiia", "correct"),
    ("dūirē", "far"),
    ("dašina", "right"),
    ("hauuiia", "left"),
    ("aṇtara", "in"),
    ("aṇtarə", "in"),
    ("pairi", "with"),
    ("ca", "and"),
    ("cā", "and"),
    ("utā", "and"),
    ("zī", "because"),
    ("nāman", "name"),
]

OLD_PERSIAN_SWADESH = [
    # From Appendix:Old_Persian_Swadesh_list on Wiktionary
    ("adam", "I"),
    ("tuvam", "you"),
    ("hauv", "he"),
    ("iyam", "this"),
    ("aita", "this"),
    ("idā", "here"),
    ("avadā", "there"),
    ("naiy", "not"),
    ("aniya", "other"),
    ("aiva", "one"),
    ("vazạrka", "big"),
    ("darga", "long"),
    ("dargam", "long"),
    ("martiyaʰ", "man"),
    ("zana", "man"),
    ("mātā", "mother"),
    ("pitā", "father"),
    ("cašman", "eye"),
    ("nāham", "nose"),
    ("hazānam", "tongue"),
    ("pādaʰ", "foot"),
    ("dasta", "hand"),
    ("tạrsatiy", "to_fear"),
    ("jīvatiy", "to_live"),
    ("θātiy", "to_say"),
    ("dārayatiy", "to_hold"),
    ("ʰuvar", "sun"),
    ("māh", "moon"),
    ("draya", "sea"),
    ("aθaⁿgaʰ", "stone"),
    ("asan", "stone"),
    ("būmiš", "earth"),
    ("asman", "sky"),
    ("ātar", "fire"),
    ("kaufaʰ", "mountain"),
    ("xšapa", "night"),
    ("raucah", "day"),
    ("θarda", "year"),
    ("hašiyam", "correct"),
    ("hadā", "with"),
    ("utā", "and"),
    ("yaθā", "because"),
    ("nāma", "name"),
    ("nāmā", "name"),
    ("apiyā", "water"),
    ("paruv", "many"),
]

# Old Persian common vocabulary from Wiktionary Reconstruction:Old Persian/ pages
# Source: en.wiktionary.org Category:Old_Persian_lemmas (ns=118)
OLD_PERSIAN_RECONSTRUCTION = [
    ("aivacadaθa", "province"),
    ("amah", "we"),
    ("apamah", "farthest"),
    ("arbah", "small"),
    ("arušah", "white"),
    ("arvāh", "value"),
    ("badrah", "treasure"),
    ("baivaram", "ten_thousand"),
    ("baivarapatiš", "commander_of_ten_thousand"),
    ("bādukah", "wind"),
    ("bāgah", "god"),
    ("bātah", "tribute"),
    ("ciθrah", "lineage"),
    ("darikah", "gold_coin"),
    ("daθa", "law"),
    ("daθapatiš", "judge"),
    ("farnah", "glory"),
    ("fšupā", "shepherd"),
    ("gaiθā", "possession"),
    ("gandabarah", "treasurer"),
    ("ganzabarah", "treasurer"),
    ("gaupā", "cowherd"),
    ("gāuš", "cow"),
    ("hadāram", "together"),
    ("hammārakarah", "accountant"),
    ("haxā", "companion"),
    ("hazārapatiš", "chiliarch"),
    ("hvatah", "self"),
    ("jamānā", "time"),
    ("kāratākah", "worker"),
    ("kāravā", "people"),
    ("madu", "wine"),
    ("maniš", "thought"),
    ("margā", "death"),
    ("marzapā", "satrap"),
    ("maθištah", "greatest"),
    ("mānā", "house"),
    ("naftah", "naphtha"),
    ("nauciš", "nine"),
    ("navacadaθa", "nine_provinces"),
    ("patiš", "lord"),
    ("raivah", "wealth"),
    ("rauxšnah", "light"),
    ("rauxšnā", "light"),
    ("rāivāh", "rich"),
    ("spantah", "holy"),
    ("sparabarah", "shield_bearer"),
    ("spiθrah", "white"),
    ("upaganzabarah", "sub_treasurer"),
    ("vahištah", "best"),
    ("vahuš", "good"),
    ("vahār", "spring"),
    ("varāzah", "boar"),
    ("vazdāh", "prosperity"),
    ("vātah", "wind"),
    ("xšayah", "king"),
    ("xšaθram", "kingdom"),
    ("xšaθrapā", "satrap"),
    ("šarguš", "mule"),
    ("θatam", "hundred"),
    ("θatapatiš", "centurion"),
    ("θigrah", "sharp"),
    ("θuxrah", "firm"),
    ("θāyakā", "decree"),
    ("çahma", "which"),
]

# Additional Phrygian words from Wiktionary Category:Phrygian_lemmas
# These are individual lemma pages; romanizations from Wiktionary entries
PHRYGIAN_ADDITIONAL = [
    ("ad", "to"),
    ("aini", "if"),
    ("knais", "woman"),
    ("abberet", "carries"),
    ("akke", "make"),
    ("as", "from"),
    ("autos", "self"),
    ("aōrō", "plowed"),
    ("bekos", "bread"),
    ("bratere", "brother"),
    ("breit", "cut"),
    ("daket", "makes"),
    ("deos", "god"),
    ("edaes", "placed"),
    ("eitou", "let"),
    ("estaes", "stood"),
    ("ke", "and"),
    ("kin", "who"),
    ("lawagtaei", "leader"),
    ("matar", "mother"),
    ("onoman", "name"),
    ("paterēs", "father"),
    ("pinke", "five"),
]

PHRYGIAN_SWADESH = [
    # From Appendix:Phrygian_Swadesh_list on Wiktionary
    ("semoun", "this"),
    ("semou", "this"),
    ("kin", "who"),
    ("thri", "three"),
    ("pinke", "five"),
    ("zemelōs", "man"),
    ("knaikan", "wife"),
    ("anar", "husband"),
    ("matar", "mother"),
    ("estaes", "to_stand"),
    ("bédu", "water"),
    ("akala", "water"),
    ("onoman", "name"),
    ("ke", "and"),
    ("eti", "and"),
    ("ab", "near"),
    ("nōrikon", "skin"),
    ("ew", "good"),
    ("bago", "good"),
    ("waso", "good"),
]


def get_avestan_swadesh_entries() -> list[dict]:
    """Return hardcoded Avestan Swadesh entries."""
    return [{"word": w, "gloss": g} for w, g in AVESTAN_SWADESH]


def get_old_persian_swadesh_entries() -> list[dict]:
    """Return hardcoded Old Persian Swadesh + Reconstruction entries."""
    entries = [{"word": w, "gloss": g} for w, g in OLD_PERSIAN_SWADESH]
    entries += [{"word": w, "gloss": g} for w, g in OLD_PERSIAN_RECONSTRUCTION]
    return entries


def get_phrygian_swadesh_entries() -> list[dict]:
    """Return hardcoded Phrygian Swadesh + additional lemma entries."""
    entries = [{"word": w, "gloss": g} for w, g in PHRYGIAN_SWADESH]
    entries += [{"word": w, "gloss": g} for w, g in PHRYGIAN_ADDITIONAL]
    return entries


# ---------------------------------------------------------------------------
# Fetch additional Avestan words from individual Wiktionary pages
# ---------------------------------------------------------------------------

def fetch_avestan_word_romanizations(page_ids: list[int]) -> list[dict]:
    """Fetch individual Avestan word pages and extract romanizations.

    Each page has pattern: Avestan_script • ( romanization ) with gloss below.
    No artificial limit — fetches ALL pages in the category.
    """
    entries = []
    fetched = 0

    for pid in page_ids:
        try:
            title, text = fetch_page_by_id(pid)
            if not text:
                continue

            # Look for Avestan section and extract romanization
            # Pattern: ( romanized_word ) followed by definition/etymology
            # The heading pattern is: Avestan script • ( romanization )
            romans = re.findall(r'•\s*\(\s*([a-zA-Zāēīōūəąęðθšžŋɣβγñδ\u0300-\u036f\u0323\u0331\u0325ᵛ\s\-]+?)\s*\)', text)

            if not romans:
                # Try alternative pattern without bullet
                romans = re.findall(
                    r'(?:Avestan|Etymology|Noun|Verb|Adjective|Numeral|Pronoun|Adverb|Conjunction|Preposition)'
                    r'.*?\(\s*([a-zA-Zāēīōūəąęðθšžŋɣβγñδ\u0300-\u036f\u0323\u0331\u0325ᵛ\s\-]+?)\s*\)',
                    text[:1500]
                )

            # Try to extract gloss/definition
            def_match = re.search(r'(?:Noun|Verb|Adjective|Numeral|Pronoun|Adverb)\s.*?(\d+\.\s*)?([a-zA-Z][a-zA-Z\s,;]+?)(?:\.|$)', text[:2000])
            gloss = ""
            if def_match:
                gloss = def_match.group(2).strip()[:50] if def_match.group(2) else ""

            for roman in romans:
                roman_clean = re.sub(r'\s+', '', roman.strip())
                if roman_clean and len(roman_clean) >= 1 and len(roman_clean) <= 25:
                    # Skip if looks like annotation
                    if any(skip in roman_clean.lower() for skip in ['old', 'young', 'avestan', 'edit']):
                        continue
                    entries.append({
                        "word": roman_clean,
                        "gloss": gloss,
                    })

            fetched += 1
            if fetched % 20 == 0:
                print(f"  Fetched {fetched} Avestan pages...", file=sys.stderr)
                time.sleep(3)  # Extra rate limiting at milestones
            else:
                time.sleep(1.5)  # Conservative rate limiting

        except Exception as e:
            print(f"  Warning: failed to fetch page {pid}: {e}", file=sys.stderr)
            continue

    return entries


# ---------------------------------------------------------------------------
# Build Old Persian word list from Reconstruction pages
# ---------------------------------------------------------------------------

def extract_old_persian_reconstruction_words(members: list[dict]) -> list[dict]:
    """Extract romanized words from Reconstruction:Old Persian/ page titles."""
    entries = []
    for m in members:
        if m.get("ns") != 118:  # Reconstruction namespace
            continue
        title = m.get("title", "")
        # Split on "/" and take the last part (the word)
        parts = title.split("/")
        if len(parts) < 2:
            continue
        word = parts[-1].strip()
        if not word:
            continue
        # Skip if it contains spaces (multi-word phrases)
        if " " in word:
            continue
        # Determine if likely a proper noun
        is_proper = word[0].isupper()
        entries.append({
            "word": word.lower() if is_proper else word,
            "gloss": "proper_noun" if is_proper else "-",
            "is_proper": is_proper,
        })
    return entries


# ---------------------------------------------------------------------------
# Fetch Phrygian lemma data from individual pages
# ---------------------------------------------------------------------------

def fetch_phrygian_lemma_romanizations(members: list[dict]) -> list[dict]:
    """Fetch individual Phrygian lemma pages to extract romanized forms.

    No artificial limit — fetches ALL main-namespace pages in the category.
    """
    entries = []
    fetched = 0

    for m in members:
        if m.get("ns") != 0:  # Main namespace only
            continue

        title = m["title"]

        try:
            text = fetch_rendered_page(title)
            if not text:
                continue

            # Check if page has Phrygian section
            if "Phrygian" not in text:
                continue

            # Extract romanization from the heading
            # Pattern: Greek-script • ( romanization )
            romans = re.findall(r'•\s*\(\s*([a-zA-Zāēīōūə\-]+?)\s*\)', text[:1500])

            if not romans:
                # For Latin-script entries (like "ad", "aini", "knais"),
                # the word itself is the romanization
                if all(c.isascii() or c in 'āēīōū' for c in title):
                    romans = [title]

            # Try to extract gloss
            def_match = re.search(r'(?:Noun|Verb|Adjective|Numeral|Pronoun|Adverb|Conjunction|Preposition|Determiner)\s.*?(\d+\.\s*)?([a-zA-Z][a-zA-Z\s,;]+?)(?:\.|$)', text[:2000])
            gloss = ""
            if def_match:
                gloss = def_match.group(2).strip()[:50] if def_match.group(2) else ""

            for roman in romans:
                roman_clean = re.sub(r'\s+', '', roman.strip())
                if roman_clean and len(roman_clean) >= 1:
                    entries.append({
                        "word": roman_clean,
                        "gloss": gloss,
                    })

            fetched += 1
            time.sleep(1.5)

        except Exception as e:
            print(f"  Warning: failed to fetch Phrygian page '{title}': {e}", file=sys.stderr)
            time.sleep(2)
            continue

    return entries


# ---------------------------------------------------------------------------
# TSV writer
# ---------------------------------------------------------------------------

# Words that are clearly not lexical items (artifacts from Wiktionary page parsing)
_JUNK_WORDS = {
    "cosmicandmoral", "deverbaladjectivesuffix", "transliterationneeded",
    "zoroastrianism", "swiftly", "-chested", "key", "antara",
    "adultmale", "asaknife", "asinabed", "output",
}

# Minimum word length by language
_MIN_WORD_LEN = {"ave": 2, "peo": 2, "xpg": 2}


def _is_valid_word(word: str, iso: str) -> bool:
    """Check if a word looks like a valid lexical entry (not junk)."""
    # Must have at least N characters
    min_len = _MIN_WORD_LEN.get(iso, 2)
    if len(word) < min_len:
        return False
    # Must not be in junk list
    if word.lower() in _JUNK_WORDS:
        return False
    # Must not contain only ASCII uppercase (likely an English word leaked in)
    if word.isascii() and word[0].isupper() and len(word) > 3:
        return False
    # Must not contain spaces (multi-word artifacts)
    if " " in word:
        return False
    # Must not be too long (likely sentence fragments)
    if len(word) > 25:
        return False
    # Must start with a letter or diacritic, not a dash (unless very short prefix)
    if word.startswith("-") and len(word) > 5:
        return False
    return True


def write_tsv(entries: list[dict], iso: str, output_dir: Path) -> Path:
    """Write entries to TSV, applying transliteration->IPA->SCA pipeline.

    Each entry dict has: word (romanized), gloss (optional).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{iso}.tsv"

    processed = []
    seen = set()
    errors = []

    for entry in entries:
        word = entry["word"].strip()
        if not word:
            continue

        # Skip duplicates
        if word in seen:
            continue
        seen.add(word)

        # Validate the word
        if not _is_valid_word(word, iso):
            continue

        # Apply transliteration -> IPA
        try:
            ipa = transliterate(word, iso)
        except Exception as e:
            errors.append(f"Transliteration error for '{word}': {e}")
            ipa = word  # fallback

        if not ipa:
            ipa = word

        # Generate SCA
        try:
            sca = ipa_to_sound_class(ipa)
        except Exception as e:
            errors.append(f"SCA error for '{word}' (IPA: {ipa}): {e}")
            sca = ""

        gloss = entry.get("gloss", "").strip()
        # Clean up junk glosses from noisy page parsing
        if gloss.lower() in ("output", "edit", "-", ""):
            gloss = ""
        elif any(junk in gloss.lower() for junk in [
            "encyclopedia", "iranica", "online", "university", "leiden",
            "edit this", "height haraiti", "reckoning", "following the other",
        ]):
            gloss = ""
        elif len(gloss) > 40:
            # Likely a sentence fragment, not a gloss
            gloss = ""
        concept_id = gloss.lower().replace(" ", "_")[:50] if gloss else "-"

        processed.append((word, ipa, sca, "wiktionary", concept_id, "-"))

    # Sort by word
    processed.sort(key=lambda x: x[0].lower())

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")
        for word, ipa, sca, source, concept_id, cognate_set_id in processed:
            f.write(f"{word}\t{ipa}\t{sca}\t{source}\t{concept_id}\t{cognate_set_id}\n")

    return output_path, len(processed), errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    output_dir = ROOT / "data" / "training" / "lexicons"

    print("=" * 60)
    print("Extracting Avestan (ave), Old Persian (peo), Phrygian (xpg)")
    print("Source: Wiktionary Swadesh lists + category lemmas")
    print("=" * 60)

    # ===================================================================
    # 1. AVESTAN (ave)
    # ===================================================================
    print("\n--- AVESTAN (ave) ---")

    # Hardcoded Swadesh list entries (verified from Wiktionary)
    ave_entries = get_avestan_swadesh_entries()
    print(f"  Swadesh list entries: {len(ave_entries)}")

    # Fetch category members for additional words via individual pages
    print("  Fetching Avestan category members...")
    try:
        ave_members = fetch_category_members("Category:Avestan_lemmas")
        ave_main_ids = [m["pageid"] for m in ave_members if m.get("ns") == 0]
        print(f"  Category members: {len(ave_main_ids)} main-namespace pages")

        # Fetch individual pages for romanized forms (limit to 50 to stay under rate limit)
        print("  Fetching individual Avestan word pages for romanization...")
        ave_page_entries = fetch_avestan_word_romanizations(ave_main_ids)
        print(f"  Additional entries from pages: {len(ave_page_entries)}")
    except Exception as e:
        print(f"  Warning: could not fetch category members: {e}", file=sys.stderr)
        ave_page_entries = []

    all_ave = ave_entries + ave_page_entries

    ave_path, ave_count, ave_errors = write_tsv(all_ave, "ave", output_dir)
    print(f"  Written: {ave_count} entries to {ave_path}")
    if ave_errors:
        print(f"  Errors: {len(ave_errors)}")
        for e in ave_errors[:5]:
            print(f"    {e}")

    # ===================================================================
    # 2. OLD PERSIAN (peo)
    # ===================================================================
    print("\n--- OLD PERSIAN (peo) ---")

    # Hardcoded Swadesh list entries
    peo_swadesh_entries = get_old_persian_swadesh_entries()
    print(f"  Swadesh list entries: {len(peo_swadesh_entries)}")

    # Fetch category members for reconstruction words
    print("  Fetching Old Persian category members...")
    try:
        peo_members = fetch_category_members("Category:Old_Persian_lemmas")
        peo_recon_entries = extract_old_persian_reconstruction_words(peo_members)
        print(f"  Reconstruction entries: {len(peo_recon_entries)}")
    except Exception as e:
        print(f"  Warning: could not fetch category members: {e}", file=sys.stderr)
        peo_recon_entries = []

    all_peo = peo_swadesh_entries + peo_recon_entries

    peo_path, peo_count, peo_errors = write_tsv(all_peo, "peo", output_dir)
    print(f"  Written: {peo_count} entries to {peo_path}")
    if peo_errors:
        print(f"  Errors: {len(peo_errors)}")
        for e in peo_errors[:5]:
            print(f"    {e}")

    # ===================================================================
    # 3. PHRYGIAN (xpg)
    # ===================================================================
    print("\n--- PHRYGIAN (xpg) ---")

    # Hardcoded Swadesh list entries
    xpg_swadesh_entries = get_phrygian_swadesh_entries()
    print(f"  Swadesh list entries: {len(xpg_swadesh_entries)}")

    # Fetch category members for additional words
    print("  Fetching Phrygian category members...")
    try:
        xpg_members = fetch_category_members("Category:Phrygian_lemmas")
        xpg_main = [m for m in xpg_members if m.get("ns") == 0]
        print(f"  Category members: {len(xpg_main)} main-namespace pages")

        # Fetch individual pages
        print("  Fetching individual Phrygian word pages...")
        xpg_page_entries = fetch_phrygian_lemma_romanizations(xpg_members)
        print(f"  Additional entries from pages: {len(xpg_page_entries)}")
    except Exception as e:
        print(f"  Warning: could not fetch category members: {e}", file=sys.stderr)
        xpg_page_entries = []

    all_xpg = xpg_swadesh_entries + xpg_page_entries

    xpg_path, xpg_count, xpg_errors = write_tsv(all_xpg, "xpg", output_dir)
    print(f"  Written: {xpg_count} entries to {xpg_path}")
    if xpg_errors:
        print(f"  Errors: {len(xpg_errors)}")
        for e in xpg_errors[:5]:
            print(f"    {e}")

    # ===================================================================
    # Summary
    # ===================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Avestan (ave):      {ave_count} entries -> {ave_path}")
    print(f"  Old Persian (peo):  {peo_count} entries -> {peo_path}")
    print(f"  Phrygian (xpg):     {xpg_count} entries -> {xpg_path}")

    total_errors = len(ave_errors) + len(peo_errors) + len(xpg_errors)
    if total_errors:
        print(f"  Total errors: {total_errors}")
    else:
        print("  No errors.")
    print("=" * 60)


if __name__ == "__main__":
    main()
