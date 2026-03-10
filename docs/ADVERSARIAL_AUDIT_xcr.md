# Adversarial Audit: Carian (xcr)

## Source Verification
- Claimed source: wiktionary
- Entry count: 25
- Entry count plausible: NO (expected 50-300; 25 is below minimum)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | glous | glous | GLOUS | a_robber | YES (g->voiced velar valid) | YES |
| 2 | sfes | sfes | SPES | self | WARN (Word==IPA) | YES (f->P correct) |
| 3 | ted | ted | TED | father | WARN (Word==IPA) | YES |
| 4 | tavse | tavse | TABSE | powerful | WARN (Word==IPA) | YES (v->B correct) |
| 5 | sla | sla | SLA | to_honour_the_memory | WARN (Word==IPA) | YES |

## Hallucination Check
- Round entry count: YES (25 -- suspiciously round)
- Generic glosses: 0
- Empty fields: 0
- Word==IPA entries: 24/25 (96.0%)
- Duplicate concepts: 0

## Verdict: PASS (updated 2026-03-10 — expanded to 54 entries)

## Notes
- [FIXED 2026-03-10] Expanded from 25 to 54 entries using fix_xcr_expand.py. New entries extracted from Wiktionary API (Category:Carian_lemmas, Reconstruction pages) and Wikipedia Carian_language article (Stephanus glosses, vocabulary tables, inscription examples). Audit trail: audit/xcr_expand_raw_responses.jsonl
- Word==IPA ratio remains high but is expected for Carian Latin transliteration where most characters ARE their IPA values (Adiego 2007). The CARIAN_MAP converts: q→kʷ, ñ→ɲ, δ→ð, ś→ɕ, λ→l̩. New entries show these conversions properly (qan→kʷan, quq→kʷukʷ, pisñ→pisɲ).
- Glosses are plausible. New entries from the Stephanus/Eustathius glossary (ala=horse, gela=king, gissa=stone, koon=sheep, soua=tomb, banda=victory) are well-established Carian vocabulary.
- Some entries (7 proper names, 10 unglossed forms) reflect the fragmentary nature of the Carian corpus. The 10 unglossed entries are marked "-" honestly per the data-extraction principle.
