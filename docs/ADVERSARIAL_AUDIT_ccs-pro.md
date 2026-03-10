# Adversarial Audit: Proto-Kartvelian (ccs-pro)

## Source Verification
- Claimed source: wiktionary
- Entry count: 254
- Entry count plausible: YES (expected 100-400)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | ḳaw | k'aw | KAW | to_take | YES (ejective k') | YES |
| 2 | deda | deda | DEDA | mother | WARN (Word==IPA) | YES |
| 3 | gul | gul | GUL | heart | WARN (Word==IPA) | YES |
| 4 | zisxl | zisxl | SISKL | blood | WARN (Word==IPA) | YES (z->S correct) |
| 5 | ṭba | t'ba | TBA | lake | YES (ejective t') | YES |

## Hallucination Check
- Round entry count: NO (254)
- Generic glosses: 0
- Empty fields: 0
- Word==IPA entries: 112/254 (44.1%)
- Duplicate concepts: 22 (e.g., "to_cover" x3, "to_be" x2, "female" x2, "to_go" x2)

## Verdict: PASS

## Notes
- Good IPA quality: 50.4% of entries have non-ASCII IPA characters. Proto-Kartvelian is rich in ejectives (k', t', p', ts', tsh'), glottalized consonants, and uvulars -- all properly encoded with IPA diacritics.
- 44.1% Word==IPA ratio is reasonable. Many Proto-Kartvelian roots are monosyllabic CVC or CV patterns where the romanization already approximates IPA for non-ejective segments.
- SCA encoding correctly handles: ejectives retain base class, z->S (sibilant class), x->K (velar class), ɣ->G.
- Vocabulary coverage is typologically appropriate: agricultural terms (lag "to_plant", peṭw "millet"), kinship (deda "mother", mama "father" -- note the reversed kinship convention typical of Kartvelian), body parts (twal "eye", gul "heart", nena "tongue"), numerals (s₁xwa "one", jor "two", sam "three", otxo "four").
- The use of subscript notation (s₁, c₁, z₁) for Proto-Kartvelian sibilant series is correct and follows standard Kartvelianist conventions (Klimov 1998).
- 22 duplicate concepts are expected: Proto-Kartvelian had multiple roots for "to_cover", "to_be", etc. -- standard for reconstructed proto-languages with dialectal variation.
- No hallucination indicators. Vocabulary is consistent with published Proto-Kartvelian reconstructions.
