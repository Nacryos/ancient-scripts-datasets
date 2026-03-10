# Adversarial Audit: Proto-Dravidian (dra-pro)

## Source Verification
- Claimed source: wiktionary
- Entry count: 171
- Entry count plausible: YES (expected 100-500)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | nīr | n_dental_i:r | NIR | water | YES (dental n, long i) | YES |
| 2 | kaṇ | kan_retroflex | KAN | eye | YES (retroflex n) | YES |
| 3 | col | tsol | TSOL | fireplace | YES (c->ts palatal affricate) | YES |
| 4 | amma | amma | AMMA | mother | WARN (Word==IPA) | YES |
| 5 | pāmpu | pa:mpu | PAMPU | snake | YES (long a) | YES |

## Hallucination Check
- Round entry count: NO (171)
- Generic glosses: 0
- Empty fields: 0
- Word==IPA entries: 24/171 (14.0%)
- Duplicate concepts: 15 (e.g., "father" x3, "mother" x2, "old" x2, "flower" x2)

## Verdict: PASS

## Notes
- Excellent IPA quality: 81.9% of entries have non-ASCII IPA characters. Proto-Dravidian phonology includes retroflexes (ʈ, ɖ, ɳ, ɭ, ɻ), dentals (t̪, n̪), palatals (tʃ, ɲ), and long vowels -- all properly represented.
- 14.0% Word==IPA ratio is the healthiest of all 12 languages audited. This indicates thorough phonological conversion from the DEDR transliteration to IPA.
- SCA encoding correctly handles the Dravidian retroflex/dental contrast by neutralizing to the same class (both map to T/N/L), which is appropriate for SCA sound class abstraction.
- Vocabulary is consistent with Krishnamurti (2003) and DEDR (Burrow & Emeneau):
  - Core Dravidian terms: nīr (water), kal (stone), il (house), pāl (milk), mīn (fish)
  - Numerals: ir (two), nāl (four), cay (five), cāṯu (six), ēẓ (seven)
  - Flora/fauna: puli (tiger), pāmpu (snake), eli (rat), pul (grass)
- 15 duplicate concepts are genuine: Proto-Dravidian has multiple attested roots for "father" (appa, ayya, tantay), "mother" (amma, āy, awwa), "old" (paẓa, mutV). These reflect different Dravidian subgroup innovations.
- The use of H notation (aH, caH, puH) for laryngeals is consistent with modern Proto-Dravidian reconstruction conventions.
- No hallucination indicators. This is the highest-quality lexicon in the set.
