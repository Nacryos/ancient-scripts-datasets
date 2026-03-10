# Adversarial Audit: Ugaritic (uga)

## Source Verification
- Claimed source: wiktionary
- Entry count: 344
- Entry count plausible: YES (expected 200-600 for Ugaritic)

## Format Verification
- Header correct: YES (`Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID`)
- All rows have 6 fields: YES (0 malformed rows)
- Duplicate entries: 0

## Content Verification (10 random samples)

| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | biʾiru | biʔiru | BIHIRU | well | YES | YES |
| 2 | hidamu | hidamu | HIDAMU | footstool | YES | YES |
| 3 | kakkaru | kakkaru | KAKKARU | - | YES | YES |
| 4 | karmu | karmu | KARMU | vineyard | YES | YES |
| 5 | lasanu | laʃanu | LASANU | tongue | YES | YES |
| 6 | naspu | nasˤpu | NASPU | a_unit_of_weight_equal_to_half_a_shekel | YES | YES |
| 7 | qamhu | qamħu | KAMHU | flour | YES | YES |
| 8 | rapaʾu | rapaʔu | RAPAHU | community | YES | YES |
| 9 | ʿimma | ʕimma | HIMMA | with | YES | YES |
| 10 | talaθatu | θalaθatu | TALATATU | three | YES | YES |

## Hallucination Check
- Round entry count: NO (344)
- Generic glosses found: 0
- Empty fields: 0
- Word==IPA entries: 58/344 (16.9%)
- Empty/missing concepts (dash): 7/344 (2.0%)
- Wiki artifact glosses: 6 (===see_also===, ===pronoun===, ====see_also====, ====related_terms====, ===adjective=== x2)

## Cognate & Source Metadata
- Cognate_Set_ID: all entries are "-" (not populated)
- SCA character inventory: A, B, D, E, G, H, I, K, L, M, N, O, P, R, S, T, U, W, Y (19 classes, valid)

## Verdict: PASS (updated 2026-03-10)

## Notes
- [FIXED 2026-03-10] 6 entries had Wiktionary section headers leaked into the Concept_ID field (===see_also===, ===pronoun===, etc.). These were cleaned to "-".
- The 16.9% Word==IPA rate is expected. Many Ugaritic words use Semitic consonantal roots that happen to align with IPA when only basic Latin consonants are involved (e.g., "karmu", "kalbu"). Words with pharyngeals (ʕ, ħ, ʔ), emphatics (sˤ, tˤ), and interdentals (θ, ð) show proper IPA conversion.
- IPA quality is good: proper use of pharyngeal ħ, glottal stop ʔ, pharyngeal ʕ, emphatic sˤ/tˤ, and fricatives ʃ, ɣ, θ, ð. This is consistent with Ugaritic phonology.
- Entry count of 344 is plausible for Ugaritic vocabulary scraped from Wiktionary.
- Glosses are specific and culturally appropriate (e.g., "lapis_lazuli", "shekel_(unit_of_weight_and_of_currency)", "chariot", "turtledove").
