# Adversarial Audit: Rhaetic (xrr)

## Source Verification
- Claimed source: wiktionary
- Entry count: 45
- Entry count plausible: YES (expected 30-100)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | tinake | tinake | TINAKE | - | WARN (Word==IPA, no gloss) | YES |
| 2 | sphura | sp_hura | SPURA | - | YES (ph->aspirated) | YES |
| 3 | velkhanu | welk_hanu | WELKANU | - | YES (v->w, kh->aspirated) | YES |
| 4 | phuter | p_huter | PUTER | - | YES (ph->aspirated) | YES |
| 5 | kleimunteis | kleimunteis | KLEIMUNTEIS | - | WARN (Word==IPA) | YES |

## Hallucination Check
- Round entry count: NO (45)
- Generic glosses: 45 (ALL entries have concept = "-")
- Empty fields: 0
- Word==IPA entries: 20/45 (44.4%)
- Duplicate concepts: 0 (all are "-")

## Verdict: FAIL

## Notes
- CRITICAL: ALL 45 entries have concept_id = "-". There are ZERO glosses in the entire dataset. This means every single entry is an unglossed word form. While Rhaetic is genuinely undeciphered (only ~300 inscriptions, mostly brief), some words have proposed interpretations in the literature (e.g., "tinake" is widely interpreted as a verb form related to Etruscan "to give/dedicate").
- Without any glosses, this lexicon provides phonetic data only -- no semantic anchoring for cognate comparison. This severely limits its utility for the PhaiPhon pipeline which relies on concept-based cognate pairing.
- The IPA quality is acceptable: 42.2% non-ASCII entries, with proper handling of aspiration markers (ph->p_h, th->t_h, kh->k_h). This suggests Rhaetic was treated as having Etruscan-like aspiration phonology, which is the scholarly consensus (Rix 1998, Schumacher 2004).
- Word forms are recognizable from published Rhaetic corpora: tinake (Sanzeno inscription), sphura, velkhanu, phuter, kastrie, helanu. These are genuine Rhaetic attestations.
- The 44.4% Word==IPA ratio is reasonable given the alphabetic script.
- RECOMMENDATION: Either add proposed glosses from Schumacher (2004) or Marchesini (2015), or document that this language is intended for phonetic-only comparison. In its current state, the dataset is authentic but functionally incomplete for concept-based analysis.
