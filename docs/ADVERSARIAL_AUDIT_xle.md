# Adversarial Audit: Lemnian (xle)

## Source Verification
- Claimed source: wiktionary
- Entry count: 30
- Entry count plausible: YES (expected 20-80)

## Format Verification
- Header correct: YES
- All rows have 6 fields: YES
- Duplicate entries: 0

## Content Verification (5 random samples)
| # | Word | IPA | SCA | Gloss | IPA Valid? | SCA Valid? |
|---|------|-----|-----|-------|-----------|-----------|
| 1 | naphoth | nap_hot_h | NAPOT | grandson | YES (ph->aspirated, th->aspirated) | YES |
| 2 | sialkhvis | sialk_hwis | SIALKWIS | sixty | YES (kh->aspirated) | YES |
| 3 | maras | maras | MARAS | magistrate | WARN (Word==IPA) | YES |
| 4 | mav | maw | MAW | and_(conjunction) | YES (v->w) | YES |
| 5 | phokiasiale | p_hokiasiale | POKIASIALE | for_the_phocaean | YES (ph->aspirated) | YES |

## Hallucination Check
- Round entry count: YES (30 -- round number)
- Generic glosses: 13 ("meaning_uncertain" entries)
- Empty fields: 0
- Word==IPA entries: 16/30 (53.3%)
- Duplicate concepts: 12 (mostly "meaning_uncertain" x9, plus "meaning_uncertain" variants)

## Verdict: PASS (updated 2026-03-10 — "meaning_uncertain" is honest annotation)

## Notes
- 13 out of 30 entries (43.3%) are glossed "meaning_uncertain". This is linguistically honest — Lemnian is known from essentially one major inscription (the Lemnos stele) plus a few minor ones. Most words remain undeciphered. Glossing them as "meaning_uncertain" is more honest than inventing translations, and follows the data-extraction principle of never fabricating content.
- The entries that ARE glossed are reasonable: "maras" (magistrate -- cognate with Etruscan maru), "mav" (and), "naphoth" (grandson -- cognate with Etruscan nefts/naphths), "avis" (year -- cognate with Etruscan avil).
- The IPA conversion correctly handles Greek-style aspiration markers: ph->p_h, th->t_h, kh->k_h.
- 53.3% Word==IPA ratio is acceptable given the alphabetic script.
- The round count of 30 is a minor concern but matches the actual attested corpus size.
- This is one of the smallest datasets but faithfully represents the available evidence.
