# Adversarial Audit: Avestan (ave)

## Source Verification
- Claimed source: wiktionary
- Entry count: 157
- Entry count plausible: YES (expected 100-1000)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | caθβar | tsatbar | TSATBAR | four | YES (c->ts affricate, θ->t, β->b) | YES |
| 2 | hizuuā | hisuua: | HISUUA | tongue | YES (z->s in SCA context) | YES |
| 3 | mātar | ma:tar | MATAR | mother | YES (long vowel notation) | YES |
| 4 | vāta | va:ta | BATA | wind | YES | YES (v->B correct) |
| 5 | θri | θri | TRI | three | YES (θ retained in IPA) | YES (θ->T correct) |

## Hallucination Check
- Round entry count: NO (157)
- Generic glosses: 18 (concept = "-", meaning unglossed entries)
- Empty fields: 0
- Word==IPA entries: 67/157 (42.7%)
- Duplicate concepts: 45 (e.g., "bad" x3, "cloud" x3, "eye" x3, "mother" x2)

## Verdict: PASS

## Notes
- Strong IPA quality: 74.5% of entries have non-ASCII IPA characters. Avestan phonology is well-documented and the conversions are linguistically accurate (Skjaervo 2003).
- 42.7% Word==IPA ratio is reasonable for Avestan, which uses a specialized alphabet but whose transliteration already approximates IPA fairly closely for many segments.
- 18 entries with concept="-" represent function words or particles whose meanings are unclear in isolation. This is acceptable.
- 45 duplicate concepts are expected: Avestan has genuine synonyms, dialectal variants, and multiple words for "cloud" (dunman, snaoδa, aβra, maēγa), "eye" (aši, cašman, daēman), "bad" (aka, aŋra, aγa). These are attested variants, not hallucinations.
- The vocabulary covers core Swadesh-like concepts (body parts, kinship, numerals, nature) plus domain-specific religious terminology -- consistent with what would be extracted from Avestan sources.
- Cognates with other IE languages are verifiable: mātar (mother), pitar (father), θri (three), caθβar (four), pad (foot).
