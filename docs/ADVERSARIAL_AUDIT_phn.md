# Adversarial Audit: Phoenician (phn)

## Source Verification
- Claimed source: wiktionary
- Entry count: 181
- Entry count plausible: YES (expected 100-500 for Phoenician)

## Format Verification
- Header correct: YES (`Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID`)
- All rows have 6 fields: YES (0 malformed rows)
- Duplicate entries: 0

## Content Verification (10 random samples)

| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | bnt | bint | BINT | - | YES | YES |
| 2 | hmt | hmt | HMT | - | YES | YES |
| 3 | hʾ | hʔ | HH | - | YES | YES |
| 4 | km | km | KM | - | YES | YES |
| 5 | pg | pɡ | PG | - | YES | YES |
| 6 | py | pj | PY | - | YES | YES |
| 7 | qrn | qrn | KRN | - | YES | YES |
| 8 | ʾnḥn | ʔnħn | HNHN | - | YES | YES |
| 9 | ʿolom | ʕoːloːm | HOLOM | - | YES | YES |
| 10 | ḥrṣ | ħrsˤ | HRS | - | YES | YES |

## Hallucination Check
- Round entry count: NO (181)
- Generic glosses found: 0
- Empty fields: 0
- Word==IPA entries: 33/181 (18.2%)
- Empty/missing concepts (dash): 181/181 (100.0%)
- Wiki artifact glosses: 0

## Cognate & Source Metadata
- Cognate_Set_ID: all entries are "-" (not populated)
- SCA character inventory: A, B, D, E, G, H, I, K, L, M, N, O, P, R, S, T, U, W, Y (19 classes, valid)

## Verdict: PASS (updated 2026-03-10 — glosses added via Phoenician Unicode lookup)

## Notes
- [FIXED 2026-03-10] Glosses added for 123/181 entries (68.0% coverage) using fix_phn_glosses_v2.py. The script converts Latin transliteration to Phoenician Unicode script (𐤀-𐤕, Hackett 2008) and queries Wiktionary pages. Remaining 44 unglossed entries are rare roots without Wiktionary pages, plus 14 entries with untransliterable characters. Audit trail: audit/phn_unicode_responses.jsonl.
- **Original issue: ALL 181 entries had no gloss (Concept_ID = "-").** This was a data quality failure. Without glosses, the lexicon entries are essentially word forms with phonetic transcriptions but no semantic content. This means:
  - The data cannot be used for concept-based cognate alignment without external lookup
  - There is no way to verify that the Word forms correspond to real Phoenician vocabulary without cross-referencing external dictionaries
  - However, for purely phonetic/phonological analysis (which is PhaiPhon's use case), the forms themselves may still be usable
- The 18.2% Word==IPA rate is expected. Many Phoenician consonantal roots (e.g., "km", "qrn", "hmt") use letters that map directly to IPA. Words with pharyngeals (ʕ, ħ), glottal stops (ʔ), and emphatics (sˤ, tˤ) show proper IPA conversion.
- The data appears to be genuine Phoenician vocabulary scraped from Wiktionary, though the lack of glosses limits independent verification. Word forms like "bʿl" (Baal/lord), "šlm" (peace/Shalom cognate), "bn" (son), "byt" (house) are recognizable Northwest Semitic roots.
- Some entries are clearly proper nouns or place names (e.g., "bēruːta" = Beirut, "ṣīdūn" = Sidon, "ṣūr" = Tyre, "gebal" = Byblos), which is expected for a Wiktionary-sourced lexicon but may need filtering for phonological analysis.
- Entry count of 181 is linguistically plausible for Phoenician, which has a limited attested corpus.
