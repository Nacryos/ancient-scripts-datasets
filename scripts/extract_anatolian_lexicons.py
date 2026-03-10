#!/usr/bin/env python3
"""Extract lexicon data for Lycian (xlc), Lydian (xld), and Carian (xcr).

Sources:
    - Verbix TIED Project glossaries (tied.verbix.com/project/glossary/)
    - languagesgulper.com basic vocabulary pages
    - Palaeolexicon (palaeolexicon.com) individual word entries
    - en-academic.com Carian article

Transliteration-to-IPA mapping uses maps from transliteration_maps.py
(Melchert 2004 for Lycian, Gusmani 1964 for Lydian, Adiego 2007 for Carian).

SCA encoding uses ipa_to_sound_class() from the cognate_pipeline.

Output: data/training/lexicons/{xlc,xld,xcr}.tsv
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class
from transliteration_maps import transliterate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================================
# LYCIAN (xlc) — from Verbix TIED Lycian Glossary + languagesgulper.com
#
# Source URLs:
#   http://tied.verbix.com/project/glossary/lyci.html
#   https://www.languagesgulper.com/eng/Lycian.html
#   https://descot21.github.io/Lycian/Divinities/div_01/
# ============================================================================
LYCIAN_WORDS: list[tuple[str, str]] = [
    # --- From Verbix TIED Lycian Glossary ---
    ("ada", "a_fine"),
    ("aha", "to_sit"),
    ("aitata", "eight"),
    ("ala", "beyond"),
    ("amu", "i"),
    ("cbatru", "daughter"),
    ("da", "to_give"),
    ("ddewe", "a_city"),
    ("ebe", "this"),
    ("epide", "ready"),
    ("esbedi", "cavalry"),
    ("esene", "blood"),
    ("eke", "how"),
    ("ene", "under"),
    ("hebeli", "a_river"),
    ("hri", "above"),
    ("hrppi", "for"),
    ("izre", "a_hand"),
    ("ke", "and"),
    ("kiki", "to_beat"),
    ("klusa", "hostility"),
    ("kride", "a_heart"),
    ("kudi", "where"),
    ("kumaza", "a_priest"),
    ("laka", "a_battle"),
    ("mahan", "a_god"),
    ("me", "here"),
    ("miren", "a_man"),
    ("mla", "an_offspring"),
    ("mukssa", "a_prayer"),
    ("ne", "direct_object_particle"),
    ("nei", "to_lead"),
    ("ni", "not"),
    ("nte", "in"),
    ("pabrati", "to_turn_out"),
    ("pdde", "a_place"),
    ("pe", "upon"),
    ("pere", "forward"),
    ("pije", "to_give"),
    ("plqqka", "wide"),
    ("pri", "over"),
    ("prije", "high"),
    ("prnna", "a_house"),
    ("prnnawa", "to_build"),
    ("qaja", "a_temple"),
    ("qlija", "a_place_of_worship"),
    ("qnza", "a_descendant"),
    ("qretu", "let_him_destroy"),
    ("sbirte", "a_monument"),
    ("si", "this"),
    ("slamati", "to_keep_memory"),
    ("snta", "one"),
    ("ta", "to_put"),
    ("tabahaza", "sky"),
    ("tali", "a_priest"),
    ("tedi", "a_father"),
    ("teteri", "four"),
    ("trei", "three"),
    ("trbbi", "against"),
    ("tuta", "an_army"),
    ("tuwa", "two"),
    ("uni", "that"),
    ("uta", "a_priest"),
    ("utetu", "to_bring"),
    ("uwe", "a_person"),
    ("uwedri", "a_community"),
    ("wawa", "a_bull"),
    ("wazzis", "an_army"),
    ("wazala", "a_warrior"),
    ("wedri", "a_city"),
    ("xabe", "a_river"),
    ("xabwa", "a_sheep"),
    ("xnna", "a_mother"),
    ("xnta", "first"),
    ("zajala", "a_criminal"),
    ("zbali", "a_deity"),
    ("ziw", "god"),
    ("zusi", "zeus"),
    # --- From languagesgulper.com ---
    ("kbi", "two"),
    ("tri", "three"),
    ("eni", "mother"),
    ("kbatra", "daughter"),
    ("lada", "wife"),
    ("neni", "brother"),
    ("tuhes", "nephew"),
    ("tawa", "eye"),
    ("pede", "foot"),
    # --- From descot21.github.io Lycian divinities ---
    ("qastto", "destroy"),
    ("qlahi", "sanctuary"),
    # --- Additional from Verbix TIED (various forms) ---
    ("erite", "he_won"),
    ("hla", "to_glorify"),
    ("zrqqi", "to_fight"),
    ("zrigali", "a_warrior"),
    ("wakssa", "courage"),
    ("dezi", "strong"),
    ("tewi", "evil"),
    ("luga", "to_burn_down"),
    ("sla", "to_propitiate"),
    ("alb", "white"),
]


# ============================================================================
# LYDIAN (xld) — from Verbix TIED Lydian Dictionary + languagesgulper.com
#
# Source URLs:
#   http://tied.verbix.com/project/glossary/lydi.html
#   https://www.languagesgulper.com/eng/Lydian.html
# ============================================================================
LYDIAN_WORDS: list[tuple[str, str]] = [
    # --- From Verbix TIED Lydian Dictionary ---
    ("aara", "a_yard"),
    ("amu", "i"),
    ("arlili", "own"),
    ("avka", "legal"),
    ("bi", "to_give"),
    ("bira", "a_house"),
    ("borli", "a_year"),
    ("da", "to_give"),
    ("divi", "a_god"),
    ("divnali", "divine"),
    ("duve", "to_construct"),
    ("e", "to_be"),
    ("ebad", "here"),
    ("emi", "my"),
    ("ena", "mother"),
    ("es", "this"),
    ("eta", "a_grandson"),
    ("fa", "upon"),
    ("fra", "across"),
    ("i", "to_do"),
    ("kan", "a_dog"),
    ("kardal", "he_constructed"),
    ("kave", "a_priest"),
    ("klida", "land"),
    ("kofu", "water"),
    ("kot", "he_swears"),
    ("labrus", "a_pole_ax"),
    ("lailas", "a_sovereign"),
    ("lale", "to_speak"),
    ("laqrisa", "a_wall"),
    ("mruvaa", "a_stele"),
    ("ni", "not"),
    ("ora", "a_month"),
    ("qalmlus", "a_king"),
    ("qelis", "everything"),
    ("qela", "an_area"),
    ("qen", "to_kill"),
    ("qira", "property"),
    ("sav", "the_good"),
    ("serli", "a_leader"),
    ("sfa", "self"),
    ("sfard", "sardis"),
    ("silavad", "to_take_care"),
    ("tase", "a_column"),
    ("tavsa", "powerful"),
    ("ti", "this"),
    ("tro", "to_speak"),
    ("vana", "a_tomb"),
    ("vidi", "to_build"),
    ("vsta", "a_heir"),
    # --- From languagesgulper.com ---
    ("kana", "wife"),
    # --- Additional from Verbix TIED ---
    ("alus", "a_priest"),
    ("ama", "to_love"),
    ("daul", "to_press"),
    ("det", "property"),
    ("fata", "defense"),
    ("istamin", "a_family"),
    ("nars", "valor"),
    ("vis", "good"),
]


# ============================================================================
# CARIAN (xcr) — from Verbix TIED Carian Glossary + languagesgulper.com
#                 + Palaeolexicon + en-academic.com
#
# Source URLs:
#   http://tied.verbix.com/project/glossary/cari.html
#   https://www.languagesgulper.com/eng/Carian.html
#   https://www.palaeolexicon.com/Word/Show/17579
#   https://www.palaeolexicon.com/Word/Show/17716
#   https://en-academic.com/dic.nsf/enwiki/1051896
# ============================================================================
CARIAN_WORDS: list[tuple[str, str]] = [
    # --- From Verbix TIED Carian Glossary ---
    ("avka", "a_situation"),
    ("ebad", "here"),
    ("glous", "a_robber"),
    ("kave", "a_priest"),
    ("kuo", "which"),
    ("lile", "atonement"),
    ("mava", "goddess_ma"),
    ("mesna", "a_deity"),
    ("mukwar", "a_prayer"),
    ("sar", "above"),
    ("sav", "something_good"),
    ("sfes", "self"),
    ("sla", "to_honour_the_memory"),
    ("tavse", "powerful"),
    ("uk", "i"),
    ("ussos", "a_spear"),
    # --- From languagesgulper.com ---
    ("ted", "father"),
    ("en", "mother"),
    ("otr", "oneself"),
    ("armon", "interpreter"),
    # --- From Palaeolexicon ---
    ("ait", "they_have_made"),
    ("not", "he_brought"),
    # --- From en-academic.com ---
    ("san", "this"),
    ("kbid", "kaunos"),
    # --- Additional from Verbix TIED ---
    ("ravmi", "liberated"),
]


def process_language(
    iso: str,
    name: str,
    word_list: list[tuple[str, str]],
    output_dir: Path,
) -> int:
    """Process a word list: transliterate to IPA, compute SCA, write TSV.

    Returns the number of entries written.
    """
    output_path = output_dir / f"{iso}.tsv"
    entries: list[tuple[str, str, str, str, str, str]] = []
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()

    for word, gloss in word_list:
        word = word.strip()
        if not word:
            continue

        # Transliterate to IPA
        try:
            ipa = transliterate(word, iso)
        except Exception as exc:
            errors.append(f"  Transliteration error for '{word}': {exc}")
            ipa = word  # fallback

        if not ipa:
            ipa = word

        # Generate SCA
        try:
            sca = ipa_to_sound_class(ipa)
        except Exception as exc:
            errors.append(f"  SCA error for '{word}' (IPA: {ipa}): {exc}")
            sca = ""

        # Normalize concept ID
        concept_id = gloss.strip().lower().replace(" ", "_")[:50] if gloss else "-"

        # Deduplicate by (word, ipa)
        key = (word, ipa)
        if key in seen:
            continue
        seen.add(key)

        entries.append((word, ipa, sca, "wiktionary", concept_id, "-"))

    # Sort by word
    entries.sort(key=lambda x: x[0].lower())

    # Write TSV
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write("Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID\n")
        for row in entries:
            f.write("\t".join(row) + "\n")

    # Report
    logger.info("%s (%s): %d entries written to %s", name, iso, len(entries), output_path)
    if errors:
        for err in errors:
            logger.warning(err)

    return len(entries)


def main() -> None:
    output_dir = ROOT / "data" / "training" / "lexicons"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    all_errors: list[str] = []

    # Process each language
    for iso, name, words in [
        ("xlc", "Lycian", LYCIAN_WORDS),
        ("xld", "Lydian", LYDIAN_WORDS),
        ("xcr", "Carian", CARIAN_WORDS),
    ]:
        logger.info("Processing %s (%s): %d words", name, iso, len(words))
        count = process_language(iso, name, words, output_dir)
        total += count

    logger.info("=" * 60)
    logger.info("TOTAL: %d entries across 3 languages", total)
    logger.info(
        "  Lycian (xlc): %d words", len(LYCIAN_WORDS)
    )
    logger.info(
        "  Lydian (xld): %d words", len(LYDIAN_WORDS)
    )
    logger.info(
        "  Carian (xcr): %d words", len(CARIAN_WORDS)
    )


if __name__ == "__main__":
    main()
