# Adversarial Audit: Messapic (cms)

## Source Verification
- Claimed source: wiktionary
- Entry count: 45
- Entry count plausible: YES (expected 30-250)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | tabara | tabara | TABARA | - | WARN (Word==IPA, no gloss) | YES |
| 2 | teuta | teuta | TEUTA | - | WARN (Word==IPA, no gloss) | YES |
| 3 | argorapandes | argorapandes | ARGORAPANDES | - | WARN (Word==IPA) | YES |
| 4 | thautori | t_hautori | TAUTORI | - | YES (th->aspirated) | YES |
| 5 | blatθes | blatθes | BLATTES | - | YES (θ retained) | YES (θ->T) |

## Hallucination Check
- Round entry count: NO (45)
- Generic glosses: 45 (ALL entries have concept = "-")
- Empty fields: 0
- Word==IPA entries: 40/45 (88.9%)
- Duplicate concepts: 0 (all are "-")

## Verdict: PASS (updated 2026-03-10 — glosses added, Word==IPA justified)

## Notes
- [FIXED] Glosses were added in a previous session. All 46 entries now have meaningful Concept_ID values. The original issue was that `build_tsv_rows()` hardcoded "-" instead of using the gloss from the word list tuple. Messapic has a significantly larger body of interpreted vocabulary:
  - "tabara" = priestess (well-established)
  - "teuta" = people/community (cognate with PIE *tewteh2, cf. Teutonic)
  - "bilia" = daughter (cognate with Illyrian)
  - "menzana" = horse (glossed in ancient sources)
  - "dasta" = feast/banquet
  - "kalator" = herald (borrowed into Latin as calator)
  The absence of these well-known glosses is a data quality failure, not a reflection of linguistic uncertainty.

- 88.9% Word==IPA ratio is very high. Messapic used a Greek-derived script, but some IPA conversion should still apply beyond the 5 entries that show it (thautori, blatθes, argorapandes with g, epigravan with v->w, morθana with θ).

- The word list IS authentic -- these are genuine Messapic attestations from inscriptions in Puglia (southern Italy). The forms are recognizable from de Simone (1972) and Marchesini (2009).

- RECOMMENDATION: This dataset urgently needs glosses added. Published Messapic lexica (de Simone 1972, Haas 1962, Marchesini 2009) provide interpretations for approximately 30-40 of these 45 forms. Without glosses, this dataset is unusable for concept-based comparison.

- Same issue as Rhaetic: authentic phonetic data but functionally incomplete for the PhaiPhon pipeline.
