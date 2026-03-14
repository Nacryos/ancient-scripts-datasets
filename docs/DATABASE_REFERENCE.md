# Ancient Scripts Datasets — Master Database Reference

> **Last updated:** 2026-03-13 | **Commit:** `3e3fdf1` | **Total entries:** 3,466,000+ across 1,178 languages

This document is the single source of truth for understanding, modifying, and extending this database. It is designed for both human researchers and AI agents.

---

## Table of Contents

1. [Database Overview](#1-database-overview)
2. [TSV Schema & Format](#2-tsv-schema--format)
3. [Ancient Languages — Complete Registry](#3-ancient-languages--complete-registry)
4. [Non-Ancient Languages — Summary](#4-non-ancient-languages--summary)
5. [Source Registry](#5-source-registry)
6. [IPA & Phonetic Processing Pipeline](#6-ipa--phonetic-processing-pipeline)
7. [Transliteration Maps System](#7-transliteration-maps-system)
8. [Sound Class (SCA) System](#8-sound-class-sca-system)
9. [Scripts & Data Flow](#9-scripts--data-flow)
10. [PRD: Adding New Data](#10-prd-adding-new-data)
11. [PRD: Adding New Languages](#11-prd-adding-new-languages)
12. [Data Acquisition Rules (Iron Law)](#12-data-acquisition-rules-iron-law)
13. [Adversarial Review Protocol](#13-adversarial-review-protocol)
14. [Re-processing & Cleaning Runbook](#14-re-processing--cleaning-runbook)
15. [Known Limitations & Future Work](#15-known-limitations--future-work)

---

## 1. Database Overview

### Locations

| Location | Path / URL | What |
|----------|-----------|------|
| **HuggingFace dataset** | `https://huggingface.co/datasets/PhaistosLabs/ancient-scripts-datasets` | **PRIMARY cloud copy.** All lexicons, cognate pairs, metadata, sources, scripts, docs. Push here after any data change. |
| **HuggingFace local clone** | `C:\Users\alvin\hf-ancient-scripts\` | Local clone of HuggingFace repo. Use `huggingface_hub` API or `git push` to sync. |
| **GitHub repo** | `https://github.com/Nacryos/ancient-scripts-datasets.git` | Scripts, docs, pipeline code. Lexicon data is gitignored but committed via force-add for some ancient langs. |
| **Local working copy** | `C:\Users\alvin\ancient-scripts-datasets\` | Full repo + generated data + CLDF sources |
| **CLDF sources** | `sources/` (593 MB) | **Gitignored.** Cloned separately: `northeuralex`, `ids`, `abvd`, `wold`, `sinotibetan`, `wikipron` |
| **Total local footprint** | 2.2 GB | Includes all generated data + CLDF source repos |

### What IS Tracked in Git (GitHub)

- `scripts/` — All extraction and processing scripts
- `cognate_pipeline/` — Python package for phonetic processing
- `docs/` — PRDs, audit reports, this reference doc
- `data/training/metadata/` — `languages.tsv`, `source_stats.tsv` (small summary files)
- `data/training/validation/` — Validation sets (via Git LFS)
- `data/training/lexicons/*.tsv` — Ancient language TSVs (force-added despite gitignore)

### What is NOT Tracked in Git (gitignored)

- `data/training/lexicons/` — Modern language TSVs (1,113 files, regenerated from scripts)
- `data/training/cognate_pairs/` — Cognate pair datasets (regenerated)
- `sources/` — CLDF source repositories (cloned separately, ~593 MB)

### What IS on HuggingFace (everything)

**HuggingFace is the single source of truth for ALL data files.** It contains:
- All 1,136 lexicon TSVs (ancient + modern)
- All cognate pair datasets
- All metadata files
- All scripts, docs, and pipeline code
- All CLDF source repos (2,928 files in `sources/`)
- Raw audit trails and intermediate extraction files

### HuggingFace Push Rules

1. **After any data change** (new entries, IPA reprocessing, map fixes): push updated TSVs to HF
2. **After any script change** that affects output: push scripts to HF
3. **Use `huggingface_hub` API** for individual file uploads:
   ```python
   from huggingface_hub import HfApi
   api = HfApi()
   api.upload_file(
       path_or_fileobj="data/training/lexicons/ave.tsv",
       path_in_repo="data/training/lexicons/ave.tsv",
       repo_id="PhaistosLabs/ancient-scripts-datasets",
       repo_type="dataset",
       commit_message="fix: reprocess Avestan IPA with expanded transliteration map"
   )
   ```
4. **For bulk uploads** (many files): use `upload_large_folder()` from the HF local clone at `C:\Users\alvin\hf-ancient-scripts\`
5. **Always push to BOTH** GitHub (scripts/docs) and HuggingFace (data + scripts/docs)
6. **Never let HF fall behind** — if data exists locally but not on HF, it's not deployed

**To reconstruct all data from scratch:**
```bash
# 1. Clone CLDF sources
git clone https://github.com/lexibank/northeuralex sources/northeuralex
git clone https://github.com/lexibank/ids sources/ids
git clone https://github.com/lexibank/abvd sources/abvd
git clone https://github.com/lexibank/wold sources/wold
git clone https://github.com/lexibank/sinotibetan sources/sinotibetan
# WikiPron: download from https://github.com/CUNY-CL/wikipron

# 2. Run extraction pipeline
python scripts/expand_cldf_full.py        # Modern languages from CLDF
python scripts/ingest_wikipron.py          # WikiPron IPA data
python scripts/run_lexicon_expansion.py    # Ancient language extraction (requires internet)
python scripts/reprocess_ipa.py            # Apply transliteration maps
python scripts/assemble_lexicons.py        # Generate metadata
```

### Directory Structure

```
ancient-scripts-datasets/
  data/training/
    lexicons/           # 1,136 TSV files (one per language) [GITIGNORED]
    metadata/           # languages.tsv, source_stats.tsv, etc. [TRACKED]
    cognate_pairs/      # inherited, similarity, borrowing pairs [GITIGNORED]
    validation/         # stratified ML training/test sets [GIT LFS]
    language_profiles/  # per-language markdown profiles
    raw/                # raw JSON audit trails
    audit_trails/       # JSONL provenance logs
  scripts/              # 23 extraction scripts + 7 parsers [TRACKED]
  cognate_pipeline/     # Python package for phonetic processing [TRACKED]
  docs/                 # PRDs, audit reports, this file [TRACKED]
  sources/              # CLDF repos [GITIGNORED, clone separately]
```

**Scale:**
- 1,178 languages (68 ancient/reconstructed + 1,113 modern — 3 overlap)
- 3,466,000+ total lexical entries
- 170,756 ancient language entries (68 languages)
- 3,296,156 modern language entries (1,113 languages)
- 21,547,916 cognate/borrowing/similarity pairs

### Cognate Pairs (v2)

Three TSV files in `data/training/cognate_pairs/`, 14-column schema:

```
Lang_A  Word_A  IPA_A  Lang_B  Word_B  IPA_B  Concept_ID  Relationship  Score  Source  Relation_Detail  Donor_Language  Confidence  Source_Record_ID
```

| File | Rows | Description |
|------|------|-------------|
| `cognate_pairs_inherited.tsv` | 21,298,208 | Expert-classified cognates + concept-aligned pairs (score ≥ 0.5) |
| `cognate_pairs_borrowing.tsv` | 17,924 | Verified donor→recipient borrowings from WOLD BorrowingTable |
| `cognate_pairs_similarity.tsv` | 231,784 | Phonetically similar pairs (0.3 ≤ score < 0.5), no overlap with inherited |

**Sources:**
- ABVD CognateTable (21.6M expert cognate pairs, 1,682 Austronesian languages)
- IE-CoR CognateTable (412K Indo-European cognate pairs)
- Sino-Tibetan CognateTable (4.2K pairs, borrowings filtered)
- WOLD BorrowingTable (17.9K verified donor-recipient pairs)
- Internal concept-aligned pairs (233K) + similarity pairs (254K)

**Deduplication:** Priority ordering expert_cognate > borrowing > concept_aligned > similarity_only. Cross-file dedup ensures no language-concept combo appears in both inherited and similarity files. See `docs/prd/PRD_COGNATE_PAIRS_V2.md` for full specification.

---

## 2. TSV Schema & Format

Every lexicon file follows this 6-column tab-separated schema:

```
Word	IPA	SCA	Source	Concept_ID	Cognate_Set_ID
```

| Column | Description | Example |
|--------|-------------|---------|
| **Word** | Orthographic/transliterated form | `pahhur`, `*wódr̥`, `𐬀𐬵𐬎𐬭𐬀` |
| **IPA** | Broad phonemic IPA transcription | `paxːur`, `wodr̩`, `ahura` |
| **SCA** | Sound Class Alphabet encoding (18C + 5V) | `PAKUR`, `WOTR`, `AHURA` |
| **Source** | Data provenance identifier | `wiktionary`, `ediana`, `wikipron` |
| **Concept_ID** | Semantic concept (first gloss word, snake_case) | `fire`, `water`, `-` |
| **Cognate_Set_ID** | Cognate grouping identifier | `PIE_fire_001`, `-` |

**Rules:**
- Header row MUST be present as line 1
- UTF-8 encoding, Unix line endings preferred
- No empty IPA fields — use Word as fallback if no conversion possible
- Source field must accurately reflect actual data origin
- `-` for unknown/unavailable fields

---

## 3. Ancient Languages — Complete Registry

### Entry Counts & IPA Quality (as of 2026-03-12)

| # | Language | ISO | Family | Entries | Identity% | Top Sources | IPA Type |
|---|----------|-----|--------|---------|-----------|-------------|----------|
| 1 | Avestan | ave | Indo-Iranian | 3,455 | 14.4% | avesta_org (2,716), wiktionary_cat (384), wiktionary (355) | Broad phonemic (Hoffmann & Forssman) |
| 2 | Tocharian B | txb | Indo-European | 2,386 | 25.2% | wiktionary_cat (2,386) | Broad phonemic (Tocharian map) |
| 3 | Luwian | xlw | Anatolian | 2,230 | 26.2% | ediana (1,985), palaeolexicon (225) | Broad phonemic (Luwian map) |
| 4 | Proto-Indo-European | ine-pro | Indo-European | 1,704 | 0.2% | wiktionary_cat (863), wiktionary (841) | Broad phonemic (reconstructed) |
| 5 | Lycian | xlc | Anatolian | 1,098 | 36.7% | ediana (517), palaeolexicon (482) | Broad phonemic (Melchert 2004) |
| 6 | Etruscan | ett | Tyrsenian | 753 | 25.5% | palaeolexicon (503), wikipron (207) | Broad phonemic (Bonfante) |
| 7 | Urartian | xur | Hurro-Urartian | 748 | 54.4% | oracc_ecut (704), wiktionary (44) | Partial (cuneiform sign names) |
| 8 | Lydian | xld | Anatolian | 693 | 53.0% | ediana (447), palaeolexicon (187) | Broad phonemic (Gusmani 1964) |
| 9 | Carian | xcr | Anatolian | 532 | 39.7% | palaeolexicon (304), ediana (174) | Broad phonemic (Adiego 2007) |
| 10 | Proto-Kartvelian | ccs-pro | Kartvelian | 504 | 22.2% | wiktionary (254), wiktionary_cat (250) | Broad phonemic (Klimov 1998) |
| 11 | Old Persian | peo | Indo-Iranian | 486 | 10.5% | wiktionary (244), wiktionary_cat (242) | Broad phonemic (Kent 1953) |
| 12 | Tocharian A | xto | Indo-European | 467 | 23.1% | wiktionary_cat (467) | Broad phonemic (Tocharian map) |
| 13 | Proto-Dravidian | dra-pro | Dravidian | 406 | 7.1% | wiktionary_cat (235), wiktionary (171) | Broad phonemic (Krishnamurti) |
| 14 | Proto-Semitic | sem-pro | Afroasiatic | 386 | 26.9% | wiktionary_cat (247), wiktionary (139) | Broad phonemic (Huehnergard) |
| 15 | Ugaritic | uga | Afroasiatic | 371 | 15.6% | wiktionary (344), wiktionary_cat (27) | Broad phonemic (Tropper 2000) |
| 16 | Hittite | hit | Anatolian | 266 | 20.3% | wiktionary (266) | Broad phonemic (Hoffner & Melchert) |
| 17 | Hurrian | xhu | Hurro-Urartian | 260 | 50.4% | palaeolexicon (259) | Broad phonemic (Wegner 2007) |
| 18 | Elamite | elx | Isolate | 301 | 71.1% | wiktionary (301) | Minimal (transparent orthography) |
| 19 | Rhaetic | xrr | Tyrsenian | 187 | 55.1% | tir_raetica (142), wiktionary (45) | Partial (North Italic alphabet) |
| 20 | Phoenician | phn | Afroasiatic | 180 | 18.3% | wiktionary (180) | Broad phonemic (abjad reconstruction) |
| 21 | Phrygian | xpg | Indo-European | 79 | 36.7% | wiktionary (79) | Partial (small corpus, Greek-script support) |
| 22 | Messapic | cms | Indo-European | 45 | 88.9% | wiktionary (45) | Minimal (Greek-alphabet, mostly identity) |
| 23 | Lemnian | xle | Tyrsenian | 30 | 53.3% | wiktionary (30) | Minimal (very small corpus) |
| | | | | | | | |
| **--- Tier 2 (Phase 6) ---** | | | | | | | |
| 24 | Old English | ang | Germanic | 31,319 | 10.5% | wiktionary_cat (31,319) | Broad phonemic (Hogg 1992) |
| 25 | Biblical Hebrew | hbo | Semitic | 12,182 | 0.1% | wiktionary_cat (12,182) | Broad phonemic (Blau 2010) |
| 26 | Coptic | cop | Egyptian | 11,180 | 0.1% | wiktionary_cat (7,987), kellia (3,193) | Broad phonemic (Layton 2000) |
| 27 | Old Armenian | xcl | Indo-European | 6,277 | 0.0% | wiktionary_cat (6,277) | Broad phonemic (Meillet 1913) |
| 28 | Pali | pli | Indo-Aryan | 2,792 | 19.1% | wiktionary_cat (2,792) | Broad phonemic (Geiger 1943) |
| 29 | Ge'ez | gez | Semitic | 496 | 0.0% | wiktionary_cat (496) | Broad phonemic (Dillmann 1857) |
| 30 | Hattic | xht | Isolate | 269 | 37.9% | wiktionary_cat (269) | Partial (cuneiformist conventions) |
| | | | | | | | |
| **--- Tier 3 (Phase 7) ---** | | | | | | | |
| 31 | Old Irish | sga | Celtic | 41,300 | 39.4% | edil (40,309), wiktionary_cat (991) | Broad phonemic (Thurneysen) |
| 32 | Old Japanese | ojp | Japonic | 5,393 | 59.7% | oncoj (4,974), wiktionary_cat (419) | Broad phonemic (Frellesvig 2010) |
| 33 | Classical Nahuatl | nci | Uto-Aztecan | 3,873 | 5.7% | wiktionary_cat (3,873) | Broad phonemic |
| 34 | Oscan | osc | Italic | 2,122 | 15.1% | ceipom (2,122) | Broad phonemic (CEIPoM Standard_aligned) |
| 35 | Umbrian | xum | Italic | 1,631 | 3.7% | ceipom (1,631) | Broad phonemic (CEIPoM Standard_aligned) |
| 36 | Venetic | xve | Italic | 721 | 86.5% | ceipom (721) | Minimal (Latin transliteration) |
| 37 | Gaulish | xtg | Celtic | 271 | 92.3% | diacl (183), wiktionary_cat (88) | Minimal (Latin transliteration) |
| 38 | Middle Persian | pal | Indo-Iranian | 242 | 62.8% | wiktionary_cat (242) | Broad phonemic (MacKenzie 1971) |
| 39 | Sogdian | sog | Indo-Iranian | 194 | 37.1% | iecor (161), wiktionary_cat (33) | Broad phonemic (Gharib 1995) |
| | | | | | | | |
| **--- Proto-Languages (Phase 7) ---** | | | | | | | |
| 40 | Proto-Austronesian | map | Austronesian | 11,624 | 41.1% | acd (11,624) | Broad phonemic (Blust notation) |
| 41 | Proto-Germanic | gem-pro | Germanic | 5,399 | 32.9% | wiktionary_cat (5,399) | Broad phonemic (reconstructed) |
| 42 | Proto-Celtic | cel-pro | Celtic | 1,584 | 68.3% | wiktionary_cat (1,584) | Partial (mixed Latin/IPA) |
| 43 | Proto-Uralic | urj-pro | Uralic | 585 | 50.3% | wiktionary_cat (585) | Broad phonemic (Sammallahti 1988) |
| 44 | Proto-Bantu | bnt-pro | Niger-Congo | 467 | 54.0% | wiktionary_cat (467) | Broad phonemic (BLR notation) |
| 45 | Proto-Sino-Tibetan | sit-pro | Sino-Tibetan | 358 | 100.0% | wiktionary_cat (358) | Already IPA (Wiktionary provides IPA) |
| | | | | | | | |
| **--- Phase 8 Batch 1 (Proto-Languages + Italic/Celtic) ---** | | | | | | | |
| 46 | Proto-Slavic | sla-pro | Balto-Slavic | 5,068 | 18.4% | wiktionary_cat (5,068) | Broad phonemic (reconstructed) |
| 47 | Proto-Turkic | trk-pro | Turkic | 1,027 | 27.8% | wiktionary_cat (1,027) | Broad phonemic (reconstructed) |
| 48 | Proto-Italic | itc-pro | Italic | 739 | 46.7% | wiktionary_cat (739) | Broad phonemic (reconstructed) |
| 49 | Faliscan | xfa | Italic | 566 | 67.1% | ceipom (566) | Partial (CEIPoM Standard_aligned) |
| 50 | Proto-Japonic | jpx-pro | Japonic | 426 | 70.2% | wiktionary_cat (426) | Partial (mixed notation) |
| 51 | Lepontic | xlp | Celtic | 421 | 27.6% | lexlep (421) | Broad phonemic (Lexicon Leponticum) |
| 52 | Proto-Iranian | ira-pro | Indo-Iranian | 366 | 4.6% | wiktionary_cat (366) | Broad phonemic (reconstructed) |
| 53 | Ancient South Arabian | xsa | Semitic | 127 | 25.2% | wiktionary (127) | Broad phonemic (Musnad abjad) |
| 54 | Celtiberian | xce | Celtic | 11 | 100.0% | wiktionary_cat (11) | Minimal (very small corpus) |
| | | | | | | | |
| **--- Phase 8 Batch 2 (Proto-Languages + Ancient) ---** | | | | | | | |
| 55 | Meroitic | xmr | Nilo-Saharan | 1,978 | 39.8% | meroitic-corpus (1,978) | Broad phonemic (Rilly 2007) |
| 56 | Proto-Algonquian | alg-pro | Algic | 258 | 28.7% | wiktionary_cat (258) | Broad phonemic (reconstructed) |
| 57 | Proto-Albanian | sqj-pro | Albanian | 210 | 43.8% | wiktionary_cat (210) | Broad phonemic (reconstructed) |
| 58 | Proto-Austroasiatic | aav-pro | Austroasiatic | 180 | 100.0% | wiktionary_cat (180) | Already IPA (Wiktionary provides IPA) |
| 59 | Proto-Polynesian | poz-pol-pro | Austronesian | 157 | 100.0% | wiktionary_cat (157) | Already IPA (Wiktionary provides IPA) |
| 60 | Proto-Tai | tai-pro | Kra-Dai | 148 | 0.7% | wiktionary_cat (148) | Broad phonemic (Li 1977) |
| 61 | Proto-Tocharian | xto-pro | Tocharian | 138 | 22.5% | wiktionary_cat (138) | Broad phonemic (reconstructed) |
| 62 | Proto-Mongolic | xgn-pro | Mongolic | 126 | 41.3% | wiktionary_cat (126) | Broad phonemic (reconstructed) |
| 63 | Proto-Oceanic | poz-oce-pro | Austronesian | 114 | 92.1% | wiktionary_cat (114) | Minimal (transparent orthography) |
| 64 | Moabite | obm | Semitic | 31 | 0.0% | wiktionary_cat (31) | Broad phonemic (Canaanite abjad) |
| | | | | | | | |
| **--- Phase 8 Batch 3 (Proto-Languages + Iberian) ---** | | | | | | | |
| 65 | Proto-Mayan | myn-pro | Mayan | 65 | 20.0% | wiktionary_cat (65) | Broad phonemic (Kaufman 2003) |
| 66 | Proto-Afroasiatic | afa-pro | Afroasiatic | 48 | 54.2% | wiktionary_cat (48) | Broad phonemic (Ehret 1995) |
| 67 | Iberian | xib | Isolate | 39 | 74.4% | wiktionary_cat (39) | Partial (undeciphered script) |
| | | | | | | | |
| **--- Phase 8 Eblaite ---** | | | | | | | |
| 68 | Eblaite | xeb | Semitic | 667 | 0.3% | dcclt-ebla (667) | Broad phonemic (Krebernik 1982) |

**Total ancient + classical: 170,756 entries across 68 languages | Overall identity rate: ~30%**

### Understanding Identity Rate

**Identity rate = % of entries where Word == IPA** (no phonetic conversion applied).

| Rate | Meaning | Example Languages |
|------|---------|-------------------|
| <10% | Excellent IPA conversion | ine-pro (0.2%), dra-pro (7.1%) |
| 10-30% | Good conversion | peo (10.5%), ave (14.4%), hit (20.3%), ccs-pro (22.2%), txb (25.2%) |
| 30-50% | Moderate — some chars unmapped | xlc (36.7%), xcr (39.7%), xhu (50.4%) |
| 50-70% | Partial — significant gaps | xld (53.0%), xur (54.4%), elx (71.1%) |
| >70% | Minimal — mostly passthrough | cms (88.9%) |

**Causes of high identity:**
- **Cuneiform sign notation** (xur): Uppercase Sumerograms like `LUGAL`, `URU` aren't phonemic — 156 entries in xur
- **Already-IPA characters** (cms): Some scripts use characters that ARE IPA (θ, ə, ŋ)
- **Transparent orthography** (elx): Latin letters already map 1:1 to IPA
- **eDiAna pre-transliterated forms** (xlc, xld): Source provides Latin transliterations that are already close to IPA
- **Plain ASCII stems** (txb, xto): Short roots like `ak`, `aik` are valid in both orthography and IPA

### IPA Quality Categories

| Category | Definition | Ancient Languages |
|----------|-----------|-------------------|
| **FULL** | >80% WikiPron-sourced IPA | (none — ancient langs don't have WikiPron) |
| **BROAD PHONEMIC** | Scholarly transliteration → IPA via cited map | hit, uga, phn, ave, peo, ine-pro, sem-pro, ccs-pro, dra-pro, xlw, xhu, ett, txb, xto, xld, xcr, xpg |
| **PARTIAL** | Some chars converted, gaps remain | xlc, xrr |
| **MINIMAL** | Mostly identity / transparent orthography | elx, xle, cms |
| **CUNEIFORM MIXED** | Mix of converted transliterations + unconverted sign names | xur |

**Important:** For dead languages, **broad phonemic is the ceiling**. Narrow allophonic IPA is not possible because allophonic variation is unrecoverable from written records. The IPA column represents the best scholarly reconstruction of phonemic values, not actual pronunciation.

---

## 4. Non-Ancient Languages — Summary

- **1,113 languages** with 3,296,156 entries
- **Dominant source:** WikiPron (85.3% of entries = 2,822,808)
- **Other sources:** ABVD (6.7%), NorthEuraLex (5.7%), WOLD (1.8%), sinotibetan (0.1%)

**WikiPron entries** have true broad phonemic IPA (scraped from Wiktionary pronunciation sections by trained linguists). These are the gold standard.

**ABVD entries** are often orthographic (Word == IPA). The `fix_abvd_ipa.py` script applies rule-based G2P conversion for Austronesian languages.

---

## 5. Source Registry

| Source ID | Full Name | Type | URL | Languages Covered |
|-----------|-----------|------|-----|-------------------|
| `wikipron` | WikiPron Pronunciation Dictionary | Scraped IPA | `sources/wikipron/` (local) | 800+ modern languages |
| `abvd` | Austronesian Basic Vocabulary Database | CLDF | `sources/abvd/` (local) | 500+ Austronesian |
| `northeuralex` | NorthEuraLex | CLDF | `sources/northeuralex/` (local) | 100+ Eurasian |
| `wold` | World Loanword Database | CLDF | `sources/wold/` (local) | 40+ worldwide |
| `sinotibetan` | Sino-Tibetan Etymological Database | CLDF | `sources/sinotibetan/` (local) | 50+ Sino-Tibetan |
| `wiktionary` | Wiktionary (appendix/lemma pages) | Web scrape | `en.wiktionary.org` | All ancient langs |
| `wiktionary_cat` | Wiktionary (category pagination) | MediaWiki API | `en.wiktionary.org/w/api.php` | ine-pro, uga, peo, ave, dra-pro, sem-pro, ccs-pro, txb, xto |
| `ediana` | eDiAna (LMU Munich) | POST API | `ediana.gwi.uni-muenchen.de` | xlc, xld, xcr, xlw |
| `palaeolexicon` | Palaeolexicon | REST API | `palaeolexicon.com/api/Search/` | xlc, xld, xcr, xlw, xhu, ett |
| `oracc_ecut` | Oracc eCUT (Urartian texts) | JSON API | `oracc.museum.upenn.edu/ecut/` | xur |
| `tir_raetica` | TIR (Thesaurus Inscriptionum Raeticarum) | Web scrape | `tir.univie.ac.at` | xrr |
| `wikipedia` | Wikipedia vocabulary tables | Web scrape | `en.wikipedia.org` | xur (supplement) |
| `avesta_org` | Avesta.org Avestan Dictionary | Web scrape | `avesta.org/avdict/avdict.htm` | ave |
| `kaikki` | Kaikki Wiktionary Dump | JSON dump | `kaikki.org` | Various |
| `kellia` | Kellia Coptic Lexicon | XML | `data.copticscriptorium.org` | cop |
| `ceipom` | CEIPoM (Italian Epigraphy) | CSV | `zenodo.org` (CC BY-SA 4.0) | osc, xum, xve |
| `edil` | eDIL (Electronic Dict of Irish Lang) | XML | `github.com/e-dil/dil` | sga |
| `acd` | ACD (Austronesian Comparative Dict) | CLDF | `github.com/lexibank/acd` (CC BY 4.0) | map |
| `oncoj` | ONCOJ (Oxford-NINJAL OJ Corpus) | XML | `github.com/ONCOJ/data` (CC BY 4.0) | ojp |
| `diacl` | DiACL (Diachronic Atlas of Comp Ling) | CLDF | `github.com/lexibank/diacl` (CC BY 4.0) | xtg |
| `iecor` | IE-CoR (IE Cognate Relationships) | CLDF | `github.com/lexibank/iecor` (CC BY 4.0) | sog |
| `lexlep` | Lexicon Leponticum (Zurich) | Web/CSV | `lexlep.univie.ac.at` | xlp |
| `meroitic-corpus` | Meroitic Language Corpus (GitHub) | JSON/CSV | `github.com/MeroiticLanguage/Meroitic-Corpus` | xmr |
| `dcclt-ebla` | DCCLT/Ebla (ORACC) | JSON ZIP | `oracc.museum.upenn.edu/dcclt-ebla/` (CC0) | xeb |

---

## 6. IPA & Phonetic Processing Pipeline

### Pipeline Architecture

```
Source Data (Word column)
    ↓
transliterate(word, iso)          ← scripts/transliteration_maps.py
    ↓                                (greedy longest-match, NFC-normalized)
IPA string (broad phonemic)
    ↓
ipa_to_sound_class(ipa)           ← cognate_pipeline/.../sound_class.py
    ↓                                (tokenize → segment_to_class → join)
SCA string (e.g., "PATA")
```

### IPA Generation Methods (by source type)

| Source | IPA Method | Quality |
|--------|-----------|---------|
| WikiPron | Pre-extracted from Wiktionary pronunciation | True broad IPA |
| Wiktionary (ancient) | `transliterate(word, iso)` via language-specific map | Broad phonemic |
| ABVD | Orthographic passthrough → `fix_abvd_ipa.py` G2P | Variable |
| eDiAna | `transliterate(word, iso)` | Broad phonemic |
| Palaeolexicon | Source IPA if available, else `transliterate()` | Broad phonemic |
| Oracc | `transliterate(word, iso)` | Partial (cuneiform) |
| NorthEuraLex/WOLD | CLDF Segments column → joined IPA | Good |

### Never-Regress Re-processing Rule

When re-applying transliteration maps to existing data (`scripts/reprocess_ipa.py`):

```python
candidate_ipa = transliterate(word, iso)

if candidate_ipa != word:
    final_ipa = candidate_ipa       # New map converts — use it
elif old_ipa != word:
    final_ipa = old_ipa             # New map can't, but old was good — keep
else:
    final_ipa = word                # Both identity — nothing to do
```

**This ensures:** IPA quality can only improve or stay the same. It never regresses.

---

## 7. Transliteration Maps System

**File:** `scripts/transliteration_maps.py` (~800 lines)

### How It Works

Each ancient language has a `Dict[str, str]` mapping scholarly transliteration conventions to broad IPA. The `transliterate()` function applies these via **greedy longest-match**: keys sorted by descending length, first match consumed at each position.

### Map Registry (updated 2026-03-13 — 180+ new rules across 13 original maps + 15 new maps in Phases 6-7 + 24 new maps in Phase 8)

| ISO | Language | Keys | Academic Reference |
|-----|----------|------|--------------------|
| `hit` | Hittite | 49 | Hoffner & Melchert (2008) — added š, ḫ, macron vowels |
| `uga` | Ugaritic | 68 | Tropper (2000) — added ʾ, macron/circumflex vowels, ḫ, ṣ, Ugaritic script (U+10380-1039F) |
| `phn` | Phoenician | 23 | Standard 22-letter abjad |
| `xur` | Urartian | 27 | Wegner (2007) — added ṣ, ṭ, y, w, ə, ʾ |
| `elx` | Elamite | 19 | Grillot-Susini (1987), Stolper (2004) |
| `xlc` | Lycian | 33 | Melchert (2004) — added x, j, o, long vowels |
| `xld` | Lydian | 38 | Gusmani (1964), Melchert — added ã, ẽ, ũ (nasalized vowels), c, h, z, x |
| `xcr` | Carian | 35 | Adiego (2007) — added β, z, v, j, f, ŋ, ĺ, ỳ, ý |
| `ave` | Avestan | 97 | Hoffmann & Forssman (1996) + Unicode 5.2 (U+10B00-10B3F) |
| `peo` | Old Persian | 68 | Kent (1953) — added z, č, Old Persian cuneiform syllabary (U+103A0-103C3, 31 signs) |
| `ine` | Proto-Indo-European | 61 | Fortson (2010), Beekes (2011) — added ḗ, ṓ, morpheme boundaries, accented syllabic sonorants |
| `sem` | Proto-Semitic | 44 | Huehnergard (2019) |
| `ccs` | Proto-Kartvelian | 66 | Klimov (1998) — added s₁/z₁/c₁/ʒ₁ subscript series, morpheme boundaries |
| `dra` | Proto-Dravidian | 49 | Krishnamurti (2003) |
| `xpg` | Phrygian | 55 | Brixhe & Lejeune (1984), Obrador-Cursach (2020) — added Greek alphabet support (22 letters) |
| `xle` | Lemnian | 24 | Greek-alphabet reconstruction |
| `xrr` | Rhaetic | 26 | North Italic alphabet reconstruction |
| `cms` | Messapic | 25 | Greek-alphabet reconstruction |
| `xlw` | Luwian | 39 | Melchert (2003), Yakubovich (2010) |
| `xhu` | Hurrian | 31 | Wegner (2007), Wilhelm (2008) |
| `ett` | Etruscan | 61 | Bonfante & Bonfante (2002), Rix (1963) + Old Italic Unicode — added z, o, d, g, b, q, σ→s |
| `txb`/`xto` | Tocharian A/B | 35 | Krause & Thomas (1960), Adams (2013), Peyrot (2008) — added retroflex series (ṭ, ḍ, ṇ, ḷ) |
| | | | |
| **--- Phase 6: Tier 2 Maps ---** | | | |
| `cop` | Coptic | 40+ | Layton (2000), Loprieno (1995) — Sahidic dialect |
| `pli` | Pali (IAST) | 30+ | Geiger (1943), Oberlies (2001) |
| `xcl` | Old Armenian | 40+ | Meillet (1913), Schmitt (1981) |
| `ang` | Old English | 30+ | Hogg (1992), Campbell (1959) |
| `gez` | Ge'ez (Ethiopic) | 50+ | Dillmann (1857), Tropper (2002) |
| `hbo` | Biblical Hebrew | 40+ | Blau (2010), Khan (2020) |
| | | | |
| **--- Phase 7: Tier 3 + Proto Maps ---** | | | |
| `osc` | Oscan | 12 | CEIPoM Standard_aligned conventions |
| `xum` | Umbrian | 12 | CEIPoM Standard_aligned conventions |
| `xve` | Venetic | 6 | CEIPoM Token_clean conventions |
| `sga` | Old Irish | 25 | Thurneysen (1946), Stifter (2006) — lenition + macron vowels |
| `xeb` | Eblaite | 20 | Standard Semitist notation |
| `nci` | Classical Nahuatl | 15 | Andrews (2003), Launey (2011) |
| `ojp` | Old Japanese | 20 | Frellesvig (2010), ONCOJ conventions |
| `pal` | Middle Persian | 25 | MacKenzie (1971), Skjærvø (2009) |
| `sog` | Sogdian | 25 | Gharib (1995), Sims-Williams (2000) |
| `xtg` | Gaulish | 15 | Delamarre (2003) |
| `gem-pro` | Proto-Germanic | 20 | Ringe (2006), Kroonen (2013) |
| `cel-pro` | Proto-Celtic | 15 | Matasović (2009) |
| `urj-pro` | Proto-Uralic | 12 | Sammallahti (1988), Janhunen (1981) |
| `bnt-pro` | Proto-Bantu | 20 | Bastin et al. (2002), Meeussen (1967) |
| `sit-pro` | Proto-Sino-Tibetan | 18 | Matisoff (2003), Sagart (2004) |
| | | | |
| **--- Phase 8 Maps ---** | | | |
| `sla-pro` | Proto-Slavic | 25+ | Shevelov (1964), Holzer (2007) |
| `trk-pro` | Proto-Turkic | 20+ | Clauson (1972), Róna-Tas (1991) |
| `itc-pro` | Proto-Italic | 15+ | Meiser (1998), Bakkum (2009) |
| `jpx-pro` | Proto-Japonic | 15+ | Vovin (2005), Frellesvig (2010) |
| `ira-pro` | Proto-Iranian | 20+ | Cheung (2007), Lubotsky (2001) |
| `xfa` | Faliscan | 12 | CEIPoM Standard_aligned conventions |
| `xlp` | Lepontic | 25 | Lexicon Leponticum (Stifter et al.) |
| `xce` | Celtiberian | 15+ | De Bernardo Stempel (1999) |
| `xsa` | Ancient South Arabian | 30+ | Stein (2003), Beeston (1984) |
| `alg-pro` | Proto-Algonquian | 15+ | Bloomfield (1946), Goddard (1994) |
| `sqj-pro` | Proto-Albanian | 15+ | Orel (1998), Demiraj (1997) |
| `aav-pro` | Proto-Austroasiatic | 10+ | Shorto (2006), Sidwell (2015) |
| `poz-pol-pro` | Proto-Polynesian | 10+ | Biggs (1978), Pawley (1966) |
| `tai-pro` | Proto-Tai | 20+ | Li (1977), Pittayaporn (2009) |
| `xto-pro` | Proto-Tocharian | 15+ | Adams (2013), Peyrot (2008) |
| `poz-oce-pro` | Proto-Oceanic | 10+ | Ross et al. (1998, 2003, 2008) |
| `xgn-pro` | Proto-Mongolic | 15+ | Poppe (1955), Nugteren (2011) |
| `xmr` | Meroitic | 30+ | Rilly (2007), Griffith (1911) |
| `obm` | Moabite | 22 | Canaanite abjad (shares Phoenician map base) |
| `myn-pro` | Proto-Mayan | 20+ | Kaufman (2003), Campbell & Kaufman (1985) |
| `afa-pro` | Proto-Afroasiatic | 15+ | Ehret (1995), Orel & Stolbova (1995) |
| `xib` | Iberian | 25+ | De Hoz (2010), Untermann (1990) |
| `xeb` | Eblaite | 20+ | Krebernik (1982), Fronzaroli (2003) |

### NFC Normalization

All map keys and input text are NFC-normalized before comparison. This ensures `š` (U+0161, composed) matches `s` + combining caron (U+0073 + U+030C, decomposed). Cache is per-ISO to prevent cross-language leakage.

### ISO Code Mapping for Proto-Languages

TSV filenames use hyphenated codes but `ALL_MAPS` uses short codes:

| TSV filename ISO | Map ISO |
|-----------------|---------|
| `ine-pro` | `ine` |
| `sem-pro` | `sem` |
| `ccs-pro` | `ccs` |
| `dra-pro` | `dra` |
| `gem-pro` | `gem-pro` |
| `cel-pro` | `cel-pro` |
| `urj-pro` | `urj-pro` |
| `bnt-pro` | `bnt-pro` |
| `sit-pro` | `sit-pro` |

### Adding a New Map

1. Add the `Dict[str, str]` constant (e.g., `NEW_LANG_MAP`) with cited reference
2. Register in `ALL_MAPS`: `"iso_code": NEW_LANG_MAP`
3. Clear `_nfc_cache` implicitly (happens on next call with new ISO)
4. Run `reprocess_ipa.py --language iso_code` to apply
5. Deploy adversarial auditor to verify

---

## 8. Sound Class (SCA) System

**File:** `cognate_pipeline/src/cognate_pipeline/normalise/sound_class.py`

### Class Inventory

| Class | IPA Segments | Description |
|-------|-------------|-------------|
| A | a, ɑ, æ, ɐ | Open vowels |
| E | e, ɛ, ə, ɘ, ø, œ | Mid vowels |
| I | i, ɪ, ɨ | Close front vowels |
| O | o, ɔ, ɵ | Mid back vowels |
| U | u, ʊ, ʉ, ɯ, y | Close back vowels |
| P/B | p, b, ɸ, β | Labial stops |
| T/D | t, d, ʈ, ɖ | Coronal stops |
| K/G | k, g, ɡ, q, ɢ | Dorsal stops |
| S | s, z, ʃ, ʒ, ɕ, ʑ, f, v, θ, ð, x, ɣ, χ, ts, dz, tʃ, dʒ | Fricatives + affricates |
| M/N | m, n, ɲ, ŋ, ɳ, ɴ | Nasals |
| L/R | l, ɫ, ɭ, ɬ, r, ɾ, ɽ, ʀ, ɹ, ʁ | Liquids |
| W/Y | w, ʋ, ɰ, j | Glides |
| H | ʔ, h, ɦ, ʕ, ħ | Glottals/pharyngeals |
| 0 | (anything unmapped) | Unknown |

### Processing Chain

```python
ipa_to_sound_class("paxːur")
  → tokenize_ipa("paxːur")  →  ["p", "a", "xː", "u", "r"]
  → [segment_to_class(s) for s in segments]  →  ["P", "A", "K", "U", "R"]
  → "PAKUR"
```

---

## 9. Scripts & Data Flow

### Data Flow Diagram

```
EXTERNAL SOURCES
  ├── Wiktionary API ──────────→ extract_ave_peo_xpg.py
  │                              extract_phn_elx.py
  │                              extract_pie_urartian.py
  │                              extract_wiktionary_lexicons.py
  │                              expand_wiktionary_categories.py
  │                              expand_xpg.py
  ├── eDiAna API ──────────────→ scrape_ediana.py
  ├── Palaeolexicon API ───────→ scrape_palaeolexicon.py
  ├── Oracc JSON API ──────────→ scrape_oracc_urartian.py
  ├── avesta.org ──────────────→ scrape_avesta_org.py
  ├── TIR (Vienna) ────────────→ scrape_tir_rhaetic.py
  ├── WikiPron TSVs ───────────→ ingest_wikipron.py
  └── CLDF Sources ────────────→ expand_cldf_full.py
                                  convert_cldf_to_tsv.py
                     ↓
         data/training/lexicons/{iso}.tsv
                     ↓
         normalize_lexicons.py (NFC, dedup, strip stress)
         reprocess_ipa.py (re-apply updated transliteration maps)
         fix_abvd_ipa.py (Austronesian G2P fix)
                     ↓
         assemble_lexicons.py → metadata/languages.tsv
         assign_cognate_links.py → cognate_pairs/*.tsv
         build_validation_sets.py → validation/*.tsv
```

### Script Quick Reference

| Script | Purpose | Languages |
|--------|---------|-----------|
| `extract_ave_peo_xpg.py` | Wiktionary Swadesh + category | ave, peo, xpg |
| `extract_phn_elx.py` | Wiktionary + appendix | phn, elx |
| `extract_pie_urartian.py` | Wiktionary + Wikipedia | ine-pro, xur |
| `extract_wiktionary_lexicons.py` | Wiktionary appendix | sem-pro, ccs-pro, dra-pro, xle |
| `extract_anatolian_lexicons.py` | Multi-source | xlc, xld, xcr |
| `expand_wiktionary_categories.py` | Wiktionary category pagination | ine-pro, uga, peo, ave, dra-pro, sem-pro, ccs-pro |
| `expand_xpg.py` | Wiktionary category + appendix | xpg |
| `scrape_ediana.py` | eDiAna POST API | xlc, xld, xcr, xlw |
| `scrape_palaeolexicon.py` | Palaeolexicon REST API | xlc, xld, xcr, xlw, xhu, ett |
| `scrape_avesta.py` | avesta.org (old, superseded) | ave |
| `scrape_avesta_org.py` | avesta.org dictionary (current, adversarial-audited) | ave |
| `scrape_oracc_urartian.py` | Oracc eCUT JSON API | xur |
| `scrape_tir_rhaetic.py` | TIR web scrape | xrr |
| `ingest_wikipron.py` | WikiPron TSV ingestion | 800+ modern |
| `expand_cldf_full.py` | CLDF full extraction | All CLDF languages |
| `reprocess_ipa.py` | Re-apply transliteration maps | 23 ancient |
| `fix_abvd_ipa.py` | G2P for Austronesian | ABVD languages |
| `normalize_lexicons.py` | NFC + dedup + SCA recompute | All |
| `assemble_lexicons.py` | Generate metadata | All |
| `ingest_wiktionary_tier2.py` | Wiktionary category ingestion (Tier 2+) | Phase 6-8 Wiktionary languages |
| `fetch_wiktionary_raw.py` | Fetch raw Wiktionary category JSON | Phase 6-8 Wiktionary languages |
| `ingest_dcclt_ebla.py` | ORACC DCCLT/Ebla extraction | xeb |
| `ingest_meroitic.py` | Meroitic Language Corpus | xmr |
| `ingest_lexlep.py` | Lexicon Leponticum extraction | xlp |
| `ingest_ceipom_italic.py` | CEIPoM italic epigraphy | osc, xum, xve, xfa |
| `update_metadata.py` | Update languages.tsv from disk | All |
| `validate_all.py` | Comprehensive TSV validation | All |
| `push_to_hf.py` | Push files to HuggingFace | All Phase 6-8 |

---

## 10. PRD: Adding New Data to Existing Languages

### Prerequisites

- The language already has a TSV file in `data/training/lexicons/`
- You have identified a new external source with verifiable data
- A transliteration map exists in `transliteration_maps.py` (if ancient)

### Step-by-Step

#### Step 1: Identify Source
- Find a publicly accessible online source (API, web page, database)
- Verify it returns real lexical data (not AI-generated)
- Document the URL, API format, and expected entry count

#### Step 2: Write Extraction Script
```python
# Template: scripts/scrape_{source}_{iso}.py
#!/usr/bin/env python3
"""Scrape {Source Name} for {Language} word lists.
Source: {URL}
"""
import urllib.request  # MANDATORY — proves data comes from HTTP
...

def fetch_data(url):
    """Fetch from external source."""
    req = urllib.request.Request(url, headers={"User-Agent": "..."})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def process_language(iso, config, dry_run=False):
    """Process and deduplicate."""
    existing = load_existing_words(tsv_path)  # MUST deduplicate
    entries = fetch_data(url)
    new_entries = [e for e in entries if e["word"] not in existing]
    ...
    # Apply transliteration
    ipa = transliterate(word, iso)
    sca = ipa_to_sound_class(ipa)
    f.write(f"{word}\t{ipa}\t{sca}\t{source_id}\t{concept_id}\t-\n")
```

**Critical:** Script MUST contain `urllib.request.urlopen()`, `requests.get()`, or equivalent HTTP fetch. No hardcoded word lists.

#### Step 3: Run with --dry-run
```bash
python scripts/scrape_new_source.py --dry-run --language {iso}
```

#### Step 4: Run Live
```bash
python scripts/scrape_new_source.py --language {iso}
```

#### Step 5: Re-process IPA (if map was updated)
```bash
python scripts/reprocess_ipa.py --language {iso}
```

#### Step 6: Deploy Adversarial Auditor
See [Section 13](#13-adversarial-review-protocol).

#### Step 7: Commit & Push to Both Repos
```bash
# GitHub
git add scripts/scrape_new_source.py data/training/lexicons/{iso}.tsv
git commit -m "Add {N} entries to {Language} from {Source}"
git push

# HuggingFace (MANDATORY — HF is the primary data host)
python -c "
from huggingface_hub import HfApi
api = HfApi()
for f in ['data/training/lexicons/{iso}.tsv', 'scripts/scrape_new_source.py']:
    api.upload_file(path_or_fileobj=f, path_in_repo=f,
                    repo_id='PhaistosLabs/ancient-scripts-datasets', repo_type='dataset',
                    commit_message='Add {N} entries to {Language} from {Source}')
"
```

---

## 11. PRD: Adding New Languages

### Prerequisites

- ISO 639-3 code identified
- At least one external source with verifiable word lists
- Script conventions for the relevant writing system understood

### Step-by-Step

#### Step 1: Create Transliteration Map (if needed)

Add to `scripts/transliteration_maps.py`:

```python
# ---------------------------------------------------------------------------
# N. NEW_LANGUAGE  (Author Year, "Title")
# ---------------------------------------------------------------------------
NEW_LANGUAGE_MAP: Dict[str, str] = {
    "a": "a", "b": "b", ...
    # Every key MUST have a cited academic reference
}
```

Register in `ALL_MAPS`:
```python
ALL_MAPS = {
    ...
    "new_iso": NEW_LANGUAGE_MAP,
}
```

#### Step 2: Write Extraction Script

Follow the template in [Section 10](#10-prd-adding-new-data). The script must:
- Fetch from an external source via HTTP
- Parse the response (HTML, JSON, XML)
- Apply `transliterate()` and `ipa_to_sound_class()`
- Write to `data/training/lexicons/{iso}.tsv`
- Save raw JSON to `data/training/raw/` for audit trail
- Deduplicate by Word column

#### Step 3: Add to Language Config (optional)

If the language will be part of the ancient languages pipeline, add to `scripts/language_configs.py`.

#### Step 4: Add to Re-processing List

Add the ISO code to `ANCIENT_LANGUAGES` in `scripts/reprocess_ipa.py` and to `ISO_TO_MAP_ISO` if the TSV filename differs from the map ISO.

#### Step 5: Run Extraction
```bash
python scripts/scrape_{source}.py --language {iso} --dry-run
python scripts/scrape_{source}.py --language {iso}
```

#### Step 6: Verify

```bash
# Check entry count and IPA quality
python scripts/reprocess_ipa.py --dry-run --language {iso}
```

#### Step 7: Deploy Adversarial Auditor

See [Section 13](#13-adversarial-review-protocol).

#### Step 8: Commit and Push

---

## 12. Data Acquisition Rules (Iron Law)

```
┌─────────────────────────────────────────────────────────────────────┐
│  DATA MAY ONLY ENTER THE DATASET THROUGH CODE THAT DOWNLOADS IT    │
│  FROM AN EXTERNAL SOURCE.                                          │
│                                                                     │
│  NO EXCEPTIONS. NO "JUST THIS ONCE." NO "IT'S FASTER."             │
└─────────────────────────────────────────────────────────────────────┘
```

### What IS Allowed

| Action | Example | Why OK |
|--------|---------|--------|
| Write a script with `urllib.request.urlopen()` | `scrape_palaeolexicon.py` | Data comes from HTTP |
| Parse HTML/JSON from downloaded content | `BeautifulSoup(html)` | Deterministic extraction |
| Apply transliteration map (CODE, not DATA) | `transliterate(word, "hit")` | Transformation rules are code |
| Re-compute SCA from IPA | `ipa_to_sound_class(ipa)` | Deterministic function |

### What is FORBIDDEN

| Action | Example | Why Forbidden |
|--------|---------|---------------|
| Write data rows directly | `f.write("water\twɔːtər\t...")` | Data authoring |
| Hardcode word lists from memory | `WORDS = [("fire", "paxːur")]` | LLM knowledge ≠ source |
| Fill in missing fields with guesses | `ipa = "probably θ"` | Hallucination risk |
| Generate translations/transcriptions | `ipa = "wɔːtər"  # I know how water sounds` | Not from a source |
| Pad entries to reach a target count | Adding 13 entries to make it 200 | Fabrication |

### The Cached-Fetch Pattern (Acceptable Gray Area)

If a source requires JavaScript rendering or CAPTCHAs:
1. Use WebFetch/browser to access the source
2. Save raw content to `data/training/raw/{source}_{iso}_{date}.html`
3. Write a parsing script that reads from the saved file
4. The auditor spot-checks 5 entries against the live source

### Transliteration Maps Are CODE, Not DATA

Transliteration maps (e.g., `"š": "ʃ"`) are **transformation rules** derived from published grammars, not lexical content. Adding or modifying map entries is a code change, not data authoring. However, every map entry MUST cite an academic reference.

---

## 13. Adversarial Review Protocol

### Architecture: Dual-Agent System

```
Team A (Extraction Agent)     Team B (Adversarial Auditor)
  ├── Writes code               ├── Reviews code
  ├── Runs scripts              ├── Spot-checks output
  ├── Produces TSV data         ├── Verifies provenance
  └── NEVER writes data         └── Has VETO POWER
       directly
```

### When to Deploy

- After ANY new data is added to the database
- After ANY transliteration map change
- After ANY re-processing run
- After ANY script modification that affects output

### Audit Checklist (per modular step)

#### Code Review
- [ ] Script contains `urllib`/`requests`/`curl` (not hardcoded data)
- [ ] No literal IPA data in `f.write()` calls
- [ ] Source attribution matches actual source
- [ ] Deduplication against existing entries

#### Data Quality
- [ ] Entry count is non-round and plausible
- [ ] No duplicate Word values
- [ ] No empty IPA fields
- [ ] Identity rate is explainable (not suspiciously low or high)
- [ ] SCA matches `ipa_to_sound_class(IPA)` for 20 random samples

#### Never-Regress Verification
- [ ] No entry went from non-identity IPA to identity (regression)
- [ ] Entry counts did not decrease
- [ ] Existing Word/Source/Concept_ID/Cognate_Set_ID unchanged

#### Provenance
- [ ] 20 random entries traced back to source URL
- [ ] Raw JSON/HTML audit trail saved in `data/training/raw/`

### Red Flags (STOP immediately)

| Red Flag | What It Means |
|----------|---------------|
| No `urllib`/`requests` in extraction code | Agent is authoring data |
| Entry count is exactly round (100, 200, 500) | Likely padded |
| >90% of entries have empty required fields | Extraction didn't work |
| Script contains `f.write("word\tipa\t...")` with literal data | Direct data authoring |
| Transformation output == input for >80% without cited justification | Map not actually applied |

### Report Format

```markdown
# Adversarial Audit: {Step} — {Language} ({iso})
## Checks:
- [ ] No data authoring: PASS/FAIL
- [ ] Entry count: PASS/FAIL (expected X, got Y)
- [ ] IPA quality: PASS/FAIL (identity rate: Z%)
- [ ] SCA consistency: PASS/FAIL (N/N verified)
- [ ] Provenance: PASS/FAIL (N/20 traced to source)
## Verdict: PASS / WARN / FAIL
## Blocking: YES (if FAIL)
```

---

## 14. Re-processing & Cleaning Runbook

### When to Re-process

- After modifying any transliteration map in `transliteration_maps.py`
- After fixing a bug in `transliterate()` or `ipa_to_sound_class()`
- After adding a new language to `ALL_MAPS`

### How to Re-process

```bash
# Dry run first (ALWAYS)
python scripts/reprocess_ipa.py --dry-run

# Check: identity rates should decrease or stay the same, NEVER increase
# Check: "Changed" column shows expected number of modifications
# Check: "Errors" column is 0

# Run live
python scripts/reprocess_ipa.py

# Or for a single language
python scripts/reprocess_ipa.py --language xlw
```

### Common Cleaning Operations

#### Remove entries with HTML artifacts
```python
# Check for HTML entities
grep -P '&\w+;' data/training/lexicons/{iso}.tsv
# Remove affected lines via Python script (not manual edit)
```

#### Remove entries from wrong source (contamination)
```python
# Example: Hurrian TSV had Hittite entries from wrong Palaeolexicon ID
# Write a Python script that identifies and removes contaminated entries
# Save removed entries to audit trail
```

#### Deduplicate
```python
# reprocess_ipa.py handles dedup by Word column
# For more complex dedup, use normalize_lexicons.py
```

#### Fix ABVD fake-IPA
```bash
python scripts/fix_abvd_ipa.py
```

### Post-Cleaning Verification

```bash
# Verify entry counts
python -c "
for iso in ['hit','uga',...]:
    with open(f'data/training/lexicons/{iso}.tsv') as f:
        print(f'{iso}: {sum(1 for _ in f) - 1} entries')
"

# Verify no empty IPA
python -c "
for iso in [...]:
    with open(f'data/training/lexicons/{iso}.tsv') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2 and not parts[1]:
                print(f'EMPTY IPA: {iso} {parts[0]}')
"
```

---

## 15. Known Limitations & Future Work

### Linguistic Limitations

| Issue | Languages Affected | Root Cause |
|-------|-------------------|------------|
| Broad phonemic only (no allophonic) | All ancient | Dead languages — allophonic variation unrecoverable |
| Cuneiform sign names as entries | xur, xhu | Source provides sign-level notation, not phonemic. ~156 Sumerograms in xur. |
| High identity for transparent orthographies | elx, cms, xle | Writing system maps 1:1 to IPA |
| Old Persian ç → θ debatable | peo | Kent (1953) says /θ/, Kloekhorst (2008) says /ts/ |
| Old Persian cuneiform inherent vowels | peo | Syllabary signs (𐎣=ka, 𐎫=ta) include inherent vowels that may be redundant in context |
| eDiAna entries drive high identity | xlc, xld | eDiAna provides already-transliterated forms; identity is expected, not a map gap |

### Technical Debt

| Issue | Priority | Fix |
|-------|----------|-----|
| `use_word_for_ipa` dead config in expand_wiktionary_categories.py | Low | Remove the config key |
| Some extraction scripts have hardcoded word lists from pre-Iron-Law era | Medium | Rewrite with HTTP fetch |
| ABVD entries still ~50% fake-IPA after G2P fix | Medium | Better G2P or manual review |
| NorthEuraLex/WOLD join segments with spaces | Low | Handled by normalize_lexicons.py |
| Combining diacritics in Lycian/Carian (U+0303, U+0302) | Low | Normalize in preprocessing before transliteration |
| Greek letter leaks in Carian source data | Low | Data cleaning script to normalize σ→s, α→a, etc. |
| HTML entities in 4 PIE IPA entries | Low | Decode with `html.unescape()` in reprocess_ipa.py |
| 15 Old Persian proper nouns have wrong-language IPA | Low | Filter or manually correct Akkadian/Greek transcriptions |

### Expansion Opportunities

| Language | Current | Available | Source |
|----------|---------|-----------|--------|
| Sumerian | 0 | 5,000+ | EPSD2 (ePSD), Oracc |
| Akkadian | 0 | 10,000+ | CAD, CDA, ePSD2 |
| Egyptian | 0 | 3,000+ | TLA (Thesaurus Linguae Aegyptiae) |
| Sanskrit | (modern only) | 50,000+ | Monier-Williams, DCS |
| Linear B | 0 | 500+ | DAMOS, Wingspread |
| Luvian Hieroglyphic | (mixed with xlw) | 500+ | Hawkins (2000) |

---

## Appendix A: Quick Commands

```bash
# Count entries for a language
wc -l data/training/lexicons/{iso}.tsv

# Check identity rate
python -c "
with open('data/training/lexicons/{iso}.tsv') as f:
    lines = f.readlines()[1:]
    total = len(lines)
    identity = sum(1 for l in lines if l.split('\t')[0] == l.split('\t')[1])
    print(f'{identity}/{total} = {identity/total*100:.1f}%')
"

# Test a transliteration map
python -c "
import sys; sys.path.insert(0, 'scripts')
from transliteration_maps import transliterate
print(transliterate('test_word', 'iso_code'))
"

# Re-process single language (dry run)
python scripts/reprocess_ipa.py --dry-run --language {iso}

# Run adversarial audit (deploy via AI agent)
# See Section 13 for protocol
```

## Appendix B: File Checksums Reference

Run after any batch operation to create a baseline:
```bash
find data/training/lexicons -name "*.tsv" -exec wc -l {} \; | sort -k2 > /tmp/lexicon_counts.txt
```
