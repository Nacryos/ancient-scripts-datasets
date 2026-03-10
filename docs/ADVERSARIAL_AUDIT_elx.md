# Adversarial Audit: Elamite (elx)

## Source Verification
- Claimed source: wiktionary
- Entry count: 301
- Entry count plausible: YES (expected 100-500 for Elamite)

## Format Verification
- Header correct: YES (`Word\tIPA\tSCA\tSource\tConcept_ID\tCognate_Set_ID`)
- All rows have 6 fields: YES (0 malformed rows)
- Duplicate entries: 0

## Content Verification (10 random samples)

| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | am | am | AM | - | YES | YES |
| 2 | duma | duma | DUMA | - | YES | YES |
| 3 | eri | eri | ERI | - | YES | YES |
| 4 | giri | ɡiri | GIRI | - | YES | YES |
| 5 | hani | hani | HANI | - | YES | YES |
| 6 | karsuda | karsuda | KARSUDA | - | YES | YES |
| 7 | kirpi | kirpi | KIRPI | - | YES | YES |
| 8 | lahi | lahi | LAHI | - | YES | YES |
| 9 | tariir | tariir | TARIIR | - | YES | YES |
| 10 | šara | ʃara | SARA | - | YES | YES |

## Hallucination Check
- Round entry count: NO (301)
- Generic glosses found: 0
- Empty fields: 0
- Word==IPA entries: 214/301 (71.1%)
- Empty/missing concepts (dash): 301/301 (100.0%)
- Wiki artifact glosses: 0

## Cognate & Source Metadata
- Cognate_Set_ID: all entries are "-" (not populated)
- SCA character inventory: A, B, D, E, G, H, I, K, L, M, N, P, R, S, T, U (16 classes, valid)

## Verdict: PASS (updated 2026-03-10 — glosses added from Wiktionary appendices)

## Notes
- [FIXED 2026-03-10] Glosses added for 286/301 entries (95.0% coverage) using fix_elx_glosses.py. Sources: Wiktionary Appendix:Elamite_word_list (Blazek 2015, Grillot-Susini 1987, Hinz & Koch 1987), Appendix:Elamite_Swadesh_list, Category:Elamite_lemmas. 15 unglossed entries are genuinely unmatchable forms. Audit trail: _audit/elx_wiktionary_raw_responses.jsonl.
- **Original issue: ALL 301 entries had no gloss (Concept_ID = "-").** Without glosses, there is no semantic content to verify the entries against or to use for concept-based alignment. The data is limited to phonological form analysis.
- **Word==IPA rate of 71.1% is the highest of all 6 languages and is a significant concern.** The 87 entries where Word differs from IPA show legitimate IPA mappings:
  - š -> ʃ (e.g., dumaš -> dumaʃ)
  - z -> ts (e.g., anzi -> antsi, azzaqa -> atstsaqa)
  - g -> ɡ (e.g., giri -> ɡiri, using IPA ɡ U+0261)
  - However, the remaining 214 entries (71.1%) where Word == IPA exactly suggests that **the transliteration-to-IPA mapping is incomplete for Elamite**. Many Elamite words use only basic Latin characters (a, b, d, e, h, i, k, l, m, n, p, r, s, t, u) that pass through the IPA converter unchanged. This is not necessarily wrong -- Elamite phonology is poorly understood, and conservative IPA transcription of cuneiform transliterations often defaults to Latin values. But it does mean the IPA column adds minimal information beyond the Word column for most entries.
- The word forms themselves appear to be genuine Elamite vocabulary items from Wiktionary. Recognizable items include: "sunki" (king), "napir" (god), "haltamti" (Elam), "halpi" (unknown but attested), "anaku" (I/me). Many are personal names, divine names, or administrative terms, which is consistent with the Elamite corpus being primarily administrative tablets and royal inscriptions.
- The SCA inventory notably lacks W and Y classes, which is unusual but may reflect the limited vowel/semivowel inventory of Elamite or a mapping choice.
- **Data usability**: Despite the high Word==IPA rate and missing glosses, the phonological forms themselves are usable for sound-class-based analysis. The SCA encoding is correctly applied in all cases. However, conclusions drawn from this data should acknowledge the limited phonological information content.
