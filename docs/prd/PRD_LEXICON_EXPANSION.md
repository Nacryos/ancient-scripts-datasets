# PRD: Comprehensive Ancient Language Lexicon Expansion

## IRON RULE: NO DATA GENERATION — EXTRACTION ONLY

**This rule supersedes ALL other goals including target lemma counts.**

1. **NEVER generate, fabricate, or hallucinate any lexical data.** Every entry must trace to a verifiable external source.
2. **NEVER write TSV rows directly.** Only write CODE (Python scripts) that fetches from external sources.
3. **Every script must use `requests.get()`, `urllib`, `BeautifulSoup`, or similar HTTP extraction.** Never `f.write("word\tIPA\t...")` with hardcoded linguistic content.
4. **If a source is unreachable or returns fewer entries than expected**: report the actual count. NEVER pad with invented entries.
5. **Target lemma counts in the table below are ASPIRATIONAL ESTIMATES, not quotas.** If Wiktionary has 200 Avestan lemmas (not 400), we get 200. The count column exists only to prioritize effort.
6. **If in doubt whether data is real**: skip it, log it, flag it for manual review.
7. **Adversarial auditors have VETO POWER.** If an auditor flags an entry as unverifiable, it is REMOVED.

## Context
All 18 ancient language lexicons now PASS adversarial audit, but entry counts are low relative to what's available online. Research agents identified major untapped sources with 2-10x more data than we currently have. Root causes: artificial fetch limits, missing dedicated-site parsers, incomplete Wiktionary category pagination, and hardcoded-only extraction for some languages.

**Current vs Available (researched counts):**

| Language | ISO | Current | Wiktionary Available | Dedicated Source | Aspirational (not quota) |
|----------|-----|---------|---------------------|-----------------|--------|
| PIE | ine | 841 | 1,704 lemmas | Palaeolexicon 1,000+ | 1,500+ |
| Ugaritic | uga | 344 | 761 lemmas | — | 600+ |
| Old Persian | peo | 244 | 524 lemmas | — | 400+ |
| Avestan | ave | 157 | 400 lemmas | avesta.org 3,000+ | 500+ |
| Proto-Dravidian | dra | 171 | 367 lemmas | DEDR 5,569 groups | 400+ |
| Hittite | hit | 251 | 251 lemmas | AssyrianLanguages.org | 300+ |
| Proto-Semitic | sem | 139 | 248 lemmas | — | 200+ |
| Proto-Kartvelian | ccs | 100 | 250 lemmas | — | 200+ |
| Phoenician | phn | 181 | 166 lemmas | FREELANG | 200+ |
| Elamite | elx | 301 | ~300 | — | 301 (maxed) |
| Phrygian | xpg | 73 | 62 lemmas | Obrador-Cursach | 100+ |
| Urartian | xur | 44 | ~50 | Oracc eCUT 742 texts | 150+ |
| Lycian | xlc | 75 | 105 lemmas | eDiAna 3,757 total | 150+ |
| Lydian | xld | 58 | 65 lemmas | eDiAna | 100+ |
| Carian | xcr | 54 | 51 lemmas | eDiAna | 80+ |
| Lemnian | xle | 33 | ~30 | — | 33 (maxed) |
| Messapic | cms | 46 | ~40 | — | 50+ |
| Rhaetic | xrr | 37 | ~30 | TIR 389 inscriptions | 60+ |

## Phase 1: Fix Existing Parsers & Remove Limits

### 1a. Remove artificial fetch limits
**Files to modify:**
- `scripts/extract_ave_peo_xpg.py`: Remove `max_fetch=50` (ave), `max_fetch=30` (xpg)
- `scripts/parsers/parse_lrc.py`: Remove `max_pages=30` limit
- `scripts/parsers/parse_dedr.py`: Remove `max_pages=10` limit

### 1b. Fix Wiktionary category pagination in `parse_wiktionary.py`
Currently fetches only first page of category members. Fix: use `cmcontinue` token to paginate through all results. This alone should double PIE, Ugaritic, Old Persian, Avestan, Proto-Semitic, Proto-Kartvelian entries.

### 1c. Fix phn.tsv gloss quality
The v1 agent found 166/181 glosses (91.7%) vs v2's 123/181 (68%). Check current TSV state and apply whichever result is better.

## Phase 2: Wiktionary Deep Scrape (7 languages, parallel)

For each language, create a dedicated expansion script that:
1. Paginates through ALL Wiktionary category members (not just first page)
2. Fetches each word page for gloss extraction
3. Handles language-specific properties (abjad scripts, cuneiform, reconstructed forms)
4. Deduplicates against existing TSV entries
5. Applies transliteration→IPA via existing `transliteration_maps.py`
6. Computes SCA via existing pipeline

**Language-specific context:**

| Language | Script/Encoding Notes | Category |
|----------|----------------------|----------|
| PIE | Reconstructed forms with *asterisk prefix, laryngeals (h₁h₂h₃) | Category:Proto-Indo-European_lemmas |
| Ugaritic | Cuneiform transliteration (Bordreuil/Pardee conventions), 30-letter alphabet | Category:Ugaritic_lemmas |
| Old Persian | Cuneiform syllabary (Kent notation), word dividers | Category:Old_Persian_lemmas |
| Avestan | Avestan script entries may be under script or transliteration | Category:Avestan_lemmas |
| Proto-Dravidian | DEDR notation with *asterisk, retroflex series (ṭ ḍ ṇ ḷ) | Category:Proto-Dravidian_lemmas |
| Proto-Semitic | Reconstructed roots with *asterisk, triconsonantal structure | Category:Proto-Semitic_lemmas |
| Proto-Kartvelian | Klimov notation, ejective series (p' t' k' q') | Category:Proto-Kartvelian_lemmas |

## Phase 3: Dedicated Source Scraping (5 languages, parallel)

### 3a. Avestan — avesta.org dictionary
- Source: `avesta.org/dictionary/` — comprehensive Avestan-English dictionary (3,000+ headwords)
- Parser: Extract headwords + glosses from HTML dictionary pages

### 3b. Urartian — Oracc eCUT
- Source: Oracc Electronic Corpus of Urartian Texts (JSON API)
- Try all 3 JSON strategies: glossary, index/lem, members/cdl
- Expected: 150+ unique lemmas from 742 texts

### 3c. Anatolian trio (Lycian/Lydian/Carian) — Palaeolexicon
- Source: `palaeolexicon.com` — searchable ancient language database
- Parser: Extract word lists per language with glosses

### 3d. Phrygian — Wiktionary + deep scrape
- Current: 73 entries from 62 Wiktionary lemmas + hardcoded supplement
- Expand: paginate fully, also check Phrygian appendix pages

### 3e. Rhaetic — TIR (Thesaurus Inscriptionum Raeticarum)
- Source: `univie.ac.at/raetica/` — Semantic MediaWiki with 389 inscriptions
- Parser: Extract word forms from inscription pages

## Phase 4: Integration & Verification

### 4a. Merge all new data
- Deduplicate by Word column (case-sensitive)
- Keep existing entries unchanged; only ADD new ones
- Verify IPA and SCA for all new entries

### 4b. Re-run adversarial audits

### 4c. Update metadata

### 4d. Commit and push

## Files to Modify

| Action | File |
|--------|------|
| MODIFY | `scripts/extract_ave_peo_xpg.py` (remove fetch limits) |
| MODIFY | `scripts/parsers/parse_wiktionary.py` (add pagination) |
| MODIFY | `scripts/parsers/parse_lrc.py` (remove page limit) |
| MODIFY | `scripts/parsers/parse_dedr.py` (remove page limit) |
| CREATE | `scripts/expand_wiktionary_{iso}.py` (7 scripts, one per Phase 2 language) |
| CREATE | `scripts/scrape_avesta.py` |
| CREATE | `scripts/scrape_oracc_urartian.py` |
| CREATE | `scripts/scrape_palaeolexicon.py` |
| CREATE | `scripts/expand_xpg.py` |
| CREATE | `scripts/scrape_tir_rhaetic.py` |
| MODIFY | `data/training/lexicons/*.tsv` (18 files, add new entries) |
| MODIFY | `docs/ADVERSARIAL_AUDIT_*.md` (18 files, update counts) |
| MODIFY | `data/training/metadata/languages.tsv` |

## Existing Code to Reuse

| Utility | File | Purpose |
|---------|------|---------|
| `transliterate(text, iso)` | `scripts/transliteration_maps.py` | All IPA conversion |
| `ipa_to_sound_class()` | `cognate_pipeline/` | SCA generation |
| `ALL_MAPS` | `scripts/transliteration_maps.py` | Per-language transliteration dicts |
| `language_configs` | `scripts/language_configs.py` | ISO codes, source URLs |
| Existing parsers | `scripts/parsers/*.py` | Template for new parsers |

## Verification

1. Each extraction agent produces a JSONL audit trail
2. Each adversarial auditor samples 20 random entries and verifies against source
3. All 18 audits must PASS
4. Entry counts must increase (never decrease) for all languages
5. No Word==IPA regression
6. `git diff --stat` confirms only expected files changed
7. Commit and push to GitHub
