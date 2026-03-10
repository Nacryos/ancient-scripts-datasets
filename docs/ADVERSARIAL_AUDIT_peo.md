# Adversarial Audit: Old Persian (peo)

## Source Verification
- Claimed source: wiktionary
- Entry count: 244
- Entry count plausible: YES (expected 100-800)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | bāgah | ba:gah | BAGAH | god | YES | YES |
| 2 | cašman | tsasman | TSASMAN | eye | YES (c->ts correct) | YES |
| 3 | dasta | dasta | DASTA | hand | YES | YES |
| 4 | miθradātah | miθrada:tah | MITRADATAH | proper_noun | YES | YES (θ->T) |
| 5 | xšaθrapā | xsatrapa: | KSATRAPA | satrap | YES | YES |

## Hallucination Check
- Round entry count: NO (244)
- Generic glosses: 133 (124 "proper_noun" + 9 "-")
- Empty fields: 0
- Word==IPA entries: 51/244 (20.9%)
- Duplicate concepts: 133 (mostly "proper_noun" repeated 124 times)

## Verdict: WARN

## Notes
- MAJOR CONCERN: 124 out of 244 entries (50.8%) are proper nouns. This dramatically inflates the entry count while providing minimal lexical content for phonetic comparison. The proper nouns are primarily Persian personal names (Mitradatah, Bagapatah, Rtabanus, etc.) and place names. While these names ARE attested in Old Persian inscriptions (Behistun, Persepolis), they represent onomastic data, not general vocabulary.
- Excluding proper nouns, only ~111 common vocabulary entries remain. This is still within the expected range but significantly less than the 244 headline suggests.
- IPA quality is good: 83.6% of entries have non-ASCII IPA characters. Conversions are linguistically sound (c->ts, θ retained, xš->xs).
- 20.9% Word==IPA ratio is healthy, indicating genuine phonological conversion was applied.
- The 9 entries with concept="-" are minor (function particles and fragmentary attestations).
- Core vocabulary items are verifiable: dasta (hand, cf. Avestan zasta), bāgah (god, cf. Slavic *bogъ), māh (moon), pitā (father), mātā (mother).
- Recommendation: For downstream use, filter out "proper_noun" entries to avoid bias from onomastic data inflating phoneme frequency distributions.
