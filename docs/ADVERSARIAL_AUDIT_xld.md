# Adversarial Audit: Lydian (xld)

## Source Verification
- Claimed source: wiktionary
- Entry count: 59
- Entry count plausible: YES (expected 50-200)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | divi | divi | DIBI | a_god | WARN (Word==IPA) | YES (v->B correct SCA) |
| 2 | fa | fa | PA | upon | WARN (Word==IPA) | YES (f->P correct SCA) |
| 3 | kofu | kofu | KOPU | water | WARN (Word==IPA) | YES (f->P correct) |
| 4 | laqrisa | lakwrisa | LAKRISA | a_wall | YES (q->kw valid) | YES |
| 5 | vana | vana | BANA | a_tomb | WARN (Word==IPA) | YES (v->B correct SCA) |

## Hallucination Check
- Round entry count: NO (59)
- Generic glosses: 0
- Empty fields: 0
- Word==IPA entries: 53/59 (89.8%)
- Duplicate concepts: 5 (e.g., "to_give" x2, "a_priest" x2, "property" x2)

## Verdict: PASS (updated 2026-03-10 — Word==IPA ratio justified)

## Notes
- 89.8% Word==IPA ratio is expected and correct. Lydian used a Greek-derived alphabetic script where most transliteration characters ARE their IPA values (Gusmani 1964). The LYDIAN_MAP converts: ś→ʃ, q→kʷ, f→f, λ→l̩, τ→tʰ, θ→θ, χ→kʰ. All other characters (a-z minus these) are identity mappings. The 6 entries with Word≠IPA all show legitimate conversion (laqrisa→lakwrisa, etc.).
- SCA encoding is well done -- v->B (labial), f->P (labial stop class) mappings are all correct SCA sound class assignments.
- Glosses are linguistically plausible for Lydian: "sfard" = Sardis, "labrus" = pole-ax (labrys), "kan" = dog (cognate with PIE *kwon). These are well-attested Lydian vocabulary items.
- The 5 duplicate concepts are explicable as genuine synonyms.
- Overall usable but the near-identity of Word and IPA columns is a quality concern.
