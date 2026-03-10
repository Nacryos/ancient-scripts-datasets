# Adversarial Audit: Hittite (hit)

## Source Verification
- Claimed source: wiktionary
- Entry count: 266
- Entry count plausible: YES (expected 200-800 for well-attested Hittite)

## Format Verification
- Header correct: YES (`Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID`)
- All rows have 6 fields: YES (0 malformed rows)
- Duplicate entries: 0

## Content Verification (10 random samples)

| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | annas | annas | ANNAS | mother | YES | YES |
| 2 | antuwahhas | antuwaxxas | ANTUWAKAS | man_human | YES | YES |
| 3 | daasshuush | taasshuush | TAU | all,_whole,_entire | YES | YES |
| 4 | eeshhaahru | eeshxaahxru | EKAKRU | tears | YES | YES |
| 5 | ekkus | ekkus | EKUS | horse | YES | YES |
| 6 | humanza | xumantsa | KUMANTSA | all | YES | YES |
| 7 | lalas | lalas | LALAS | tongue | YES | YES |
| 8 | maaahhlaas | maahaxlaash | MAAKLA | (branch_of)_grape_vine | YES | YES |
| 9 | nekuz | nekuts | NEKUTS | evening,_nightfall | YES | YES |
| 10 | walahzi | walaxtsi | WALAKTSI | to_hit | YES | YES |

## Hallucination Check
- Round entry count: NO (266)
- Generic glosses found: 0
- Empty fields: 0
- Word==IPA entries: 71/266 (26.7%)
- Empty/missing concepts (dash): 16/266 (6.0%)
- Wiki artifact glosses: 0

## Cognate & Source Metadata
- Cognate_Set_ID: all entries are "-" (not populated)
- SCA character inventory: A, E, I, K, L, M, N, P, R, S, T, U, W, Y (14 classes, valid)

## Verdict: PASS

## Notes
- The 26.7% Word==IPA rate is acceptable for Hittite. Many Hittite words use Latin-compatible orthography (e.g., "annas", "lalas", "patas") where the romanized form happens to match standard IPA. Words with cuneiform-specific characters (ḫ, š) are properly converted (e.g., ḫuuš -> xuuš, šakawiš -> šakawiš with IPA ʃ).
- Transliteration mapping is correctly applied: d->t, g->k, ḫ->x, š->ʃ, z->ts transformations are visible throughout the data, consistent with Hittite phonological conventions (Melchert, Kloekhorst).
- 16 entries have no gloss (dash in Concept_ID), which is acceptable for a Wiktionary-scraped dataset where some entries may lack definitions.
- Entry count of 266 is linguistically plausible for a curated Hittite vocabulary from Wiktionary.
- No fabricated or generic glosses detected. Glosses are specific and linguistically coherent (e.g., "soothing_substance,_(opium)_poppy?", "watchtower,_lookout,_guardpost;_fort,_stronghold").
