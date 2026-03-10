#!/usr/bin/env python3
"""Extract Rhaetic (xrr) and Messapic (cms) lexicons from Wiktionary/scholarly sources.

Words are hardcoded from WebFetch of:
  - Thesaurus Inscriptionum Raeticarum (tir.univie.ac.at)
  - mnamon.sns.it (Scuola Normale Superiore, Pisa)
  - Grokipedia / Wikipedia mirrors
  - Paleoglot blog (paleoglot.blogspot.com)
  - en-academic.com (Messapian language article)

Transliteration→IPA via the existing maps in transliteration_maps.py.
SCA encoding via cognate_pipeline's ipa_to_sound_class().
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cognate_pipeline" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from cognate_pipeline.normalise.sound_class import ipa_to_sound_class
from transliteration_maps import transliterate


# ============================================================================
# RHAETIC (xrr) — Words from TIR, mnamon.sns.it, Grokipedia, Paleoglot
#
# Sources:
#   - Thesaurus Inscriptionum Raeticarum (tir.univie.ac.at/wiki/The_Raetic_language)
#   - mnamon.sns.it/index.php?page=Scrittura&id=63&lang=en
#   - grokipedia.com/page/Rhaetic
#   - paleoglot.blogspot.com (Schum PU 1, Schum CE 1, upiku texts)
#
# Format: (transliterated_form, gloss_or_note)
# ============================================================================
RHAETIC_WORDS = [
    # --- Verbs and verbal forms ---
    # Scholarly transliteration uses: ph=pʰ, th=tʰ, kh=kʰ, ś=ʃ, z=ts, v=w
    # Sources use þ for /th/, φ for /ph/, χ for /kh/ — normalized to digraphs here
    ("tinake", "made/donated (preterite)"),
    ("tinakhe", "made/donated (preterite, aspirated variant)"),
    ("eluku", "sacrificed (participle)"),
    ("upiku", "offered (participle/verbal noun)"),
    ("utiku", "given/donated (participle)"),
    ("trinakhe", "it was poured (passive preterite)"),
    ("tauke", "perfective preterite verb"),
    # --- Nouns ---
    ("akhvil", "present/gift"),
    ("sphura", "community"),
    ("vaku", "votive offering"),
    ("hepru", "artifact/plaque"),
    ("hil", "property/land"),
    # --- Numerals ---
    ("thal", "two"),
    # --- Adverbs/particles ---
    ("ikh", "thus"),
    ("khan", "this"),
    ("ihi", "here/there (locative particle)"),
    # --- Nouns/obscure ---
    ("taniun", "obscure recurring term"),
    ("terisna", "obscure, -na suffix"),
    ("phuter", "noun (plural form)"),
    ("vinutalina", "wine-bearing"),
    ("phelna", "name element or noun"),
    # --- Personal names attested in inscriptions ---
    ("reithe", "personal name"),
    ("muiu", "personal name"),
    ("kastrie", "personal name"),
    ("ethunnu", "personal name"),
    ("tvalos", "personal name"),
    ("pithamne", "personal name"),
    ("helanu", "personal name"),
    ("lasta", "personal name"),
    ("pithamnu", "personal name (variant)"),
    ("tvalur", "personal name (variant)"),
    ("khaisurus", "personal name"),
    ("enikes", "personal name"),
    ("laviseseli", "patronymic (of the son of Lavis)"),
    ("velkhanu", "deity name"),
    ("lupinu", "dead/crossed over"),
    ("pitiave", "locative form"),
    ("kusenkus", "uncertain (artifact descriptor)"),
    ("avaśuerasi", "for the departed ones"),
    ("avaśura", "the departed (nom. pl.)"),
    ("kleimunteis", "to that in this plot"),
    ("phelturies", "relating to Velturie"),
    ("klanturus", "uncertain meaning"),
    # --- Morphological elements used as standalone clitics ---
    ("ka", "and (enclitic)"),
    # --- Adjective/inhabitant ---
    ("śaili", "inhabitant"),
]


# ============================================================================
# MESSAPIC (cms) — Words from mnamon.sns.it, en-academic.com, Eupedia,
#                   Verbix, Paleoglot, Grokipedia
#
# Sources:
#   - mnamon.sns.it/index.php?page=Lingua&id=29&lang=en
#   - en-academic.com/dic.nsf/enwiki/324322
#   - eupedia.com/forum/threads/messapian-albanian-connection.35943
#   - docs.verbix.com/Languages/Messapic
#
# Format: (transliterated_form, gloss_or_note)
# ============================================================================
MESSAPIC_WORDS = [
    # --- Nouns ---
    # Scholarly transliteration: θ=θ, ph=pʰ, th=tʰ, kh=kʰ, h=h
    # Sources use θ (theta) directly — preserved here
    ("bilia", "daughter"),
    ("bennan", "a sort of vehicle"),
    ("tabara", "priestess"),
    ("argorapandes", "treasurer/coin magistrate"),
    ("teuta", "community/people"),
    ("kalator", "herald"),
    ("aran", "arable land"),
    ("brendon", "deer"),
    ("brention", "place name (Brindisi)"),
    ("menzana", "colt"),
    ("dasta", "personal name"),
    ("morθana", "personal name"),
    # --- Verbs ---
    ("apistaθi", "he offers (present indicative 3sg)"),
    ("hipades", "he dedicated (aorist indicative 3sg)"),
    ("hadive", "he put/stood (perfect indicative 3sg)"),
    ("klaohi", "hear (present imperative 3sg)"),
    ("beran", "let them bring (present conjunctive 3pl)"),
    ("berain", "they must bring (present optative 3pl)"),
    ("tepise", "placed/offered (aorist indicative 3sg)"),
    ("epigravan", "they have offered"),
    ("rodita", "verb form (from Aphrodite inscription)"),
    # --- Numerals ---
    ("penkaheh", "five"),
    # --- Articles ---
    ("toi", "dative masculine article"),
    ("tai", "dative feminine article"),
    ("tan", "accusative feminine singular article"),
    # --- Particles/function words ---
    ("ai", "conjunction (hypothetical)"),
    ("ma", "prohibition particle"),
    ("apa", "preposition: from"),
    ("epi", "verbal preposition"),
    ("kos", "relative pronoun: someone"),
    ("si", "enclitic conjunction: and"),
    ("θi", "enclitic conjunction: and"),
    ("ti", "enclitic conjunction: and"),
    # --- Adjectives ---
    ("mazzes", "greater"),
    ("andirahho", "adjective (possibly 'under')"),
    # --- Theonyms ---
    ("zis", "Zeus (Messapic form)"),
    ("thautori", "infernal god (related to Tartarus)"),
    ("aprodita", "Aphrodite (loanword from Greek)"),
    # --- Additional personal names from inscriptions ---
    ("artorres", "personal name"),
    ("blatθes", "personal name"),
    ("dazimas", "personal name"),
    ("dekias", "personal name"),
    ("artahias", "patronym/nomen gentile"),
    # --- Additional verbal/nominal forms ---
    ("tabaras", "genitive of tabara"),
    ("logetibas", "theonym (dative plural)"),
]


def build_tsv_rows(
    words: list[tuple[str, str]],
    iso: str,
) -> list[str]:
    """Convert word list to TSV rows: Word, IPA, SCA, Source, Concept_ID, Cognate_Set_ID."""
    rows = []
    errors = []
    for translit, gloss in words:
        try:
            ipa = transliterate(translit, iso)
            sca = ipa_to_sound_class(ipa)
            if not sca:
                errors.append(f"  WARN: empty SCA for {translit!r} -> IPA={ipa!r}")
                sca = "?"
            rows.append(f"{translit}\t{ipa}\t{sca}\twiktionary\t{gloss}\t-")
        except Exception as exc:
            errors.append(f"  ERROR: {translit!r}: {exc}")
    return rows, errors


def main():
    outdir = ROOT / "data" / "training" / "lexicons"
    outdir.mkdir(parents=True, exist_ok=True)

    header = "Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID"
    total_errors = []

    # --- Rhaetic ---
    xrr_rows, xrr_errors = build_tsv_rows(RHAETIC_WORDS, "xrr")
    xrr_path = outdir / "xrr.tsv"
    with open(xrr_path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for row in xrr_rows:
            f.write(row + "\n")
    print(f"Rhaetic (xrr): {len(xrr_rows)} entries -> {xrr_path}")
    if xrr_errors:
        total_errors.extend(["Rhaetic errors:"] + xrr_errors)

    # --- Messapic ---
    cms_rows, cms_errors = build_tsv_rows(MESSAPIC_WORDS, "cms")
    cms_path = outdir / "cms.tsv"
    with open(cms_path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for row in cms_rows:
            f.write(row + "\n")
    print(f"Messapic (cms): {len(cms_rows)} entries -> {cms_path}")
    if cms_errors:
        total_errors.extend(["Messapic errors:"] + cms_errors)

    # --- Summary ---
    print()
    if total_errors:
        print("ERRORS/WARNINGS:")
        for e in total_errors:
            print(e)
    else:
        print("No errors.")

    # Quick sanity: show first 5 entries of each
    import io, sys as _sys
    _sys.stdout = io.TextIOWrapper(_sys.stdout.buffer, encoding="utf-8", errors="replace")
    print("\n--- Rhaetic sample ---")
    for row in xrr_rows[:5]:
        print(row)
    print("\n--- Messapic sample ---")
    for row in cms_rows[:5]:
        print(row)


if __name__ == "__main__":
    main()
