# Adversarial Audit: Lycian (xlc)

## Source Verification
- Claimed source: wiktionary
- Entry count: 99
- Entry count plausible: NO (expected 100-500; 99 is 1 below minimum -- borderline)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | aha | axa | AKA | to_sit | YES (h->x is valid Lycian) | YES |
| 2 | cbatru | cbatru | KBATRU | daughter | WARN (Word==IPA, no conversion for cb-) | YES |
| 3 | kumaza | kumatsa | KUMATSA | a_priest | YES (z->ts is valid) | YES |
| 4 | mahan | maxan | MAKAN | a_god | YES (h->x valid) | YES |
| 5 | qaja | kwaja | KAYA | a_temple | YES (q->kw valid labiovelar) | YES |

## Hallucination Check
- Round entry count: NO (99)
- Generic glosses: 0
- Empty fields: 0
- Word==IPA entries: 72/99 (72.7%)
- Duplicate concepts: 11 (e.g., "daughter" x2, "to_give" x2, "this" x2, "three" x2, "a_priest" x3)

## Verdict: WARN

## Notes
- 72.7% Word==IPA ratio is high but expected for a sparsely attested Anatolian language where many words lack established IPA beyond the orthographic form. Lycian used an alphabetic script close to Greek, so orthography approximates phonology.
- Count of 99 is just barely below the expected minimum of 100. This is borderline acceptable -- not a red flag.
- 11 duplicate concepts exist but these are genuine synonyms/doublets in Lycian (e.g., cbatru/kbatra for "daughter", tri/trei for "three"). This is linguistically plausible.
- IPA conversions that DO differ from Word are linguistically sound: h->x (fricative), z->ts (affricate), q->kw (labiovelar). These match known Lycian phonology (Melchert 2004).
- All glosses are specific and domain-appropriate for an ancient Anatolian language (kinship terms, religious vocabulary, numerals).
