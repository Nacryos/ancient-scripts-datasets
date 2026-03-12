# Adversarial Database Audit: ancient-scripts-datasets

**Date:** 2026-03-12
**Scope:** Full database critique — IPA accuracy, missing languages, source quality, data integrity, scholarly accuracy, public presentation
**Method:** 6 parallel research agents, each attacking a different dimension
**Database:** [HuggingFace](https://huggingface.co/datasets/Nacryos/ancient-scripts-datasets) / [GitHub](https://github.com/Nacryos/ancient-scripts-datasets)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [CRITICAL Issues (Must Fix)](#2-critical-issues-must-fix)
3. [IPA & Phonetic Transcription Errors](#3-ipa--phonetic-transcription-errors)
4. [Missing Ancient Languages](#4-missing-ancient-languages)
5. [Source Quality & Provenance](#5-source-quality--provenance)
6. [Data Integrity Issues](#6-data-integrity-issues)
7. [Scholarly Accuracy of Lexical Entries](#7-scholarly-accuracy-of-lexical-entries)
8. [HuggingFace / GitHub Presentation](#8-huggingface--github-presentation)
9. [Recommendations Summary](#9-recommendations-summary)

---

## 1. Executive Summary

The ancient-scripts-datasets database is an ambitious project covering 1,136 languages with 3.3M entries. Its pipeline architecture (Iron Law, adversarial auditing, transliteration maps) is well-designed. However, this audit identified **serious issues across all dimensions**:

| Category | Critical | High | Medium | Low |
|----------|----------|------|--------|-----|
| IPA/Phonetic Errors | 3 | 5 | 5 | — |
| Missing Languages | 9 (Tier 1) | 9 (Tier 2) | 15+ (Tier 3-4) | 20+ |
| Source Quality | 2 | 3 | 2 | — |
| Data Integrity | 2 | 3 | 4 | 2 |
| Scholarly Accuracy | 2 | 2 | 2 | — |
| Presentation | 3 | 3 | 3 | — |

**The three most damaging findings:**
1. Avestan claims 3,455 entries but only has 739 — the `avesta_org` source (2,716 entries) is entirely missing
2. The HuggingFace dataset is **private/gated** with 0 downloads — no researcher can access it
3. 55+ ancient languages with freely available digital datasets are absent, including Sumerian (15K+ entries), Akkadian (28K+), Egyptian (60K+), Sanskrit (2.5M+), and Ancient Greek (116K+)

---

## 2. CRITICAL Issues (Must Fix)

### C1. Avestan Entry Count Mismatch
- **DATABASE_REFERENCE.md claims 3,455 entries** including 2,716 from `avesta_org`
- **Actual `ave.tsv` has only 739 entries** from `wiktionary` and `wiktionary_cat`
- The `avesta_org` source is entirely absent — either never ingested, or data was lost
- **Impact:** 78.6% of claimed Avestan data does not exist

### C2. Bogus Data Artifact in Avestan
- Entry `inprogress` with IPA `inproɡress` is a data processing placeholder left in production data
- Entry `phoneticvalue` is another processing artifact
- **Impact:** Corrupts any downstream analysis

### C3. HuggingFace Dataset is Private
- Set to `Private: True` with `Gated: auto`
- 0 downloads, 0 likes
- No researcher can discover or access the data without owner approval
- **Impact:** The dataset effectively does not exist as a public resource

### C4. No License File
- GitHub repo has `license: null` — no LICENSE file at repo root
- HuggingFace YAML claims CC-BY-SA-4.0 but no actual license file exists
- **Impact:** Researchers cannot legally use the dataset with confidence

### C5. SCA Tokenizer Bug — Labiovelar Modifier
- The SCA diacritic regex does NOT include `ʷ` (U+02B7, MODIFIER LETTER SMALL W)
- Result: every labiovelar in PIE, Lycian, Carian, Etruscan produces a spurious `0` character
- Example: `kʷ` → SCA `K0` instead of just `K`
- **Impact:** Systematic data corruption in SCA column for all languages with labiovelars

---

## 3. IPA & Phonetic Transcription Errors

### 3.1 Phonological Accuracy Issues

| Issue | Language | Severity | Details |
|-------|----------|----------|---------|
| **š → ʃ disputed** | Hittite | HIGH | Most Hittitologists (Kloekhorst 2008, Melchert) consider Hittite š = [s], not [ʃ]. Creates a spurious s/ʃ contrast. |
| **ç → θ disputed** | Old Persian | HIGH | Kent (1953) says /θ/, Kloekhorst (2008) says /ts/. SCA consequence: θ→T vs ts→S — significant for cognate detection. |
| **h₃ → ɣʷ speculative** | PIE | HIGH | Highly speculative phonetic value from Leiden school. Maps to SCA G+0 (bug, see C5) instead of H like other laryngeals. Creates artificial asymmetry. |
| **Etruscan θ inconsistent** | Etruscan | CRITICAL | θ mapped as fricative [θ] while φ→[pʰ] and χ→[kʰ] are aspirated stops. Same phoneme series, inconsistent treatment. |
| **Lydian ś/š collapse** | Lydian | CRITICAL | Two distinct sibilant graphemes both map to [ʃ], losing a phonemic distinction Gusmani (1964) maintains. Compare: Carian correctly distinguishes ś→[ɕ] from š→[ʃ]. |
| **Carian ỳ/ý self-mapping** | Carian | MEDIUM | Map to themselves (non-IPA output), producing SCA class "0". |
| **PIE voiced aspirates as breathy** | PIE | MEDIUM | bʰ→bʱ uses breathy-voice diacritic ʱ (U+02B1) not in SCA regex. May produce spurious "0" segments. |
| **Urartian ejectives assumed** | Urartian | MEDIUM | ṣ→tsʼ, ṭ→tʼ assumes ejective realization. Could be pharyngealized or glottalized instead. |
| **Avestan TTE collapsed** | Avestan | MEDIUM | U+10B1D (TTE) collapsed with plain T, losing a potential phonemic distinction. |

### 3.2 Missing Transliteration Mappings

| Gap | Language | Impact |
|-----|----------|--------|
| Cuneiform determinatives not handled | Hit, Xlw, Xhu, Xur, Elx | Sumerograms pass through untransliterated |
| Missing Old Persian signs (U+103AE, U+103B8, U+103BB) | Peo | Source text with these signs passes through unconverted |
| Missing Phrygian Greek letters (ξ, ψ, φ, χ) | Xpg | New Phrygian Greek-alphabet inscriptions partially unhandled |
| Missing Proto-Kartvelian aspirated affricates (cʰ, čʰ) | Ccs-pro | Incomplete three-way contrast |
| Missing Tocharian aspirated stops (kh, ph, th) | Txb/Xto | Brahmi-derived aspirates not mapped |
| Missing Dravidian alveolar nasal (ṉ) | Dra-pro | Four-way coronal nasal distinction incomplete |

### 3.3 SCA Sound Class Systematic Losses

These are by-design limitations of the 18C+5V system, but they affect downstream cognate detection:

| Collapse | Languages Affected | Significance |
|----------|--------------------|--------------|
| Ejectives = plain stops | Proto-Kartvelian, Urartian | Three-way Kartvelian contrast lost |
| Pharyngealized = plain | Ugaritic, Proto-Semitic, Phoenician | Semitic emphatic series lost |
| Uvulars = velars (q=K) | All Semitic languages | Fundamental q/k contrast lost |
| Retroflex = dental/alveolar | Proto-Dravidian, Tocharian | Diagnostic etymological feature lost |
| Sibilant voicing collapsed (s=z=S) | Avestan, Proto-Kartvelian | But labial/dental voicing preserved — inconsistent |
| Precomposed nasalized vowels dropped | Lycian, Lydian, Avestan | ã, ẽ, ũ may fail tokenizer regex — **possible bug** |

---

## 4. Missing Ancient Languages

### Tier 1: Critical Omissions (9 languages)

| Language | ISO | Family | Available Entries | Best Source | Already Acknowledged? |
|----------|-----|--------|-------------------|-------------|----------------------|
| **Sumerian** | sux | Isolate | 15,944+ lemmas | ePSD2 (Penn) | Yes |
| **Akkadian** | akk | Afroasiatic | 28,000+ words | CAD (Chicago) — free PDFs | Yes |
| **Ancient Egyptian** | egy | Afroasiatic | 60,647 lemmas | TLA — downloadable HF datasets | Yes |
| **Sanskrit** | san | Indo-European | 2,500,000+ items | Digital Corpus of Sanskrit | Yes |
| **Mycenaean Greek** | gmy | Indo-European | 630+ words, 6K tablets | DAMOS (Oslo) | Yes |
| **Ancient Greek** | grc | Indo-European | 116,502 entries | LSJ, Perseus | No |
| **Gothic** | got | Indo-European | 3,600 lemmas | Project Wulfila — full TEI corpus | No |
| **Old Church Slavonic** | chu | Indo-European | 130,000+ items | GORAZD digital dictionary | No |
| **Old Norse** | non | Indo-European | 50,000+ words | ONP, Cleasby-Vigfusson | No |

**Key observation:** The database includes 8 languages with <500 known entries (Lycian, Lydian, Carian, Rhaetic, Messapic, Lemnian, Phrygian, Phoenician) but omits Ancient Greek (116K entries) and Gothic (3.6K lemmas with complete annotated corpus). This is a severe coverage bias.

### Tier 2: Important Omissions (9 languages)

| Language | ISO | Family | Available Entries | Best Source |
|----------|-----|--------|-------------------|-------------|
| Coptic | cop | Afroasiatic | 11,263 entries | Coptic Dictionary Online |
| Hattic | xht | Isolate | ~300 words | Palaeolexicon, Wiktionary |
| Pali | pli | Indo-European | Tens of thousands | PTS Dictionary, DPD |
| Classical Armenian | xcl | Indo-European | Thousands | Calfa.fr, Bedrosian |
| Old English | ang | Indo-European | 40,000+ entries | Bosworth-Toller |
| Ge'ez | gez | Afroasiatic | Thousands | Leslau dictionary |
| Syriac | syc | Afroasiatic | Tens of thousands | SEDRA |
| Aramaic | arc | Afroasiatic | 3M parsed words | CAL (HUC) |
| Biblical Hebrew | hbo | Afroasiatic | 8,000+ entries | BDB on Sefaria |

### Tier 3: Notable Omissions (15+ languages)

Middle Persian, Sogdian, Parthian, Khwarezmian, Gandhari Prakrit, Old Japanese, Classical Tibetan, Gaulish, Venetic, Faliscan, Eblaite, Old Irish, Classical Nahuatl, Classic Mayan, Sabaic, Oscan, Umbrian.

### Tier 4: Missing Reconstructed Proto-Languages (4+)

Proto-Austronesian (ACD: 5K stems), Proto-Uralic (580+ lemmas), Proto-Bantu (BLR3: 10K reconstructions), Proto-Sino-Tibetan (STEDT: 1M records).

### Geographic Coverage Gap

| Region | In DB | Missing |
|--------|-------|---------|
| Ancient Near East | 4 languages | 8+ (Sumerian, Akkadian, Egyptian, Eblaite, Aramaic, Hebrew, etc.) |
| Ancient Mediterranean | 8 languages | 7+ (Mycenaean, Ancient Greek, Oscan, Umbrian, Coptic, etc.) |
| South/East Asia | 0 languages | 6+ (Sanskrit, Pali, Old Japanese, Tibetan, Classical Chinese, etc.) |
| Africa | 0 languages | 4+ (Egyptian, Coptic, Ge'ez, Meroitic) |
| Americas | 0 languages | 2+ (Classical Nahuatl, Classic Mayan) |

---

## 5. Source Quality & Provenance

### 5.1 Source Reliability Ratings

| Source | IPA Reliability | Academic Rigor | Best Available? |
|--------|----------------|----------------|-----------------|
| WikiPron | Moderate (varies by language) | Peer-reviewed tool (LREC 2020) | Yes, for modern languages |
| Wiktionary (direct) | Low-Moderate | None (wiki, crowd-sourced) | **No** |
| eDiAna (LMU Munich) | N/A (transliteration only) | Excellent | Yes, for Anatolian |
| **Palaeolexicon** | **Low** | **None (volunteer project, no institutional affiliation)** | **No** |
| Oracc eCUT | N/A (transliteration only) | Excellent | Yes, for Urartian |
| TIR (Vienna) | N/A (epigraphy) | Excellent | Yes, for Rhaetic |
| **avesta.org** | **Low** | **None (personal website, non-specialist author)** | **No** |
| NorthEuraLex | Moderate (auto-generated) | Peer-reviewed | Yes, for coverage |
| ABVD | **Low (~50% fake-IPA)** | Peer-reviewed DB, but IPA issues | Yes, for Austronesian scope |
| WOLD | Good (expert) | Expert-curated | Yes, for loanwords |

### 5.2 Key Source Concerns

**Palaeolexicon** (503 Etruscan + 482 Lycian + 304 Carian + 259 Hurrian + 225 Luwian + 187 Lydian = 1,960 entries):
- Independent volunteer project with no institutional affiliation
- No formal editorial board or peer review
- Site itself warns: "there is no way to be sure about how words were pronounced"
- Data should be cross-referenced against primary academic sources before use

**avesta.org** (2,716 claimed entries, currently missing):
- Personal website of a Chemical Engineering graduate, not an Iranist
- Based on Kanga (1900) — over 125 years old
- Modern standard: Bartholomae's *Altiranisches Wörterbuch* or Kellens/Skjaervo

**Wiktionary for ancient languages:**
- No editorial gatekeeping — anyone can edit
- Low attestation bar for extinct languages
- Contributor expertise is unverified
- Hittite page explicitly states it is "for people who know Hittite to fill up"
- Period mixing (OAv/YAv, Classical/Ecclesiastical Latin) without distinction

**ABVD ~50% fake-IPA** (confirmed):
- Known problem in computational historical linguistics community
- ABVD stores orthographic forms, not IPA, for many languages
- Lexibank 2 standardized version should be preferred over raw ABVD data

### 5.3 Superior Sources NOT Used

| Source | What It Covers | Why Better |
|--------|---------------|------------|
| **TITUS** (Frankfurt) | Ancient IE languages | Most comprehensive digital corpus for ancient IE |
| **ePSD2** (Penn) | Sumerian | 15,944 lemmas, authoritative |
| **CAD** (Chicago) | Akkadian | 28,000 words, 26 volumes free online |
| **TLA** (Berlin) | Egyptian | 60,647 lemmas, downloadable |
| **PHOIBLE** (UW/MPI) | Phonological inventories | 3,020 inventories for validation |
| **Lexibank 2** (MPI) | Standardized CLDF | CLTS-mapped versions of ABVD/NorthEuraLex/etc. |
| **STEDT** (Berkeley) | Sino-Tibetan | 1M records from 500+ sources |
| **Project Wulfila** | Gothic | Complete corpus in TEI, freely downloadable |

---

## 6. Data Integrity Issues

### 6.1 Per-File Issues

| File | Entries | Issue | Severity |
|------|---------|-------|----------|
| **ave.tsv** | 739 (claimed 3,455) | **Missing 2,716 entries from avesta_org** | CRITICAL |
| **ave.tsv** | — | Bogus `inprogress` and `phoneticvalue` entries | CRITICAL |
| **ave.tsv** | — | 74.4% empty Concept_IDs | HIGH |
| **ave.tsv** | — | Old/Young Avestan not distinguished | MEDIUM |
| **xlw.tsv** | 2,230 | 581 SCA zeros from Sumerograms (26.1%) | HIGH |
| **xur.tsv** | 748 | 171 SCA zeros from Sumerograms (22.9%) | HIGH |
| **xur.tsv** | — | 54.4% identity rate — transliteration map likely incomplete | HIGH |
| **ett.tsv** | 753 | ~250+ proper nouns mixed with vocabulary (~33%) | HIGH |
| **ett.tsv** | — | 3 duplicate Word entries | MODERATE |
| **ett.tsv** | — | 162 SCA zeros from capitalized proper names | MODERATE |
| **ine-pro.tsv** | 1,704 | 50.6% empty Concept_IDs | MODERATE |
| **uga.tsv** | 371 | 6 SCA zeros from 'V' placeholder characters | MINOR |
| **hit.tsv** | 266 | Trailing periods on some Concept_IDs | MINOR |

### 6.2 Metadata Issues

- **7 of 8 audited ancient languages are absent from `languages.tsv`** — only Etruscan appears, and only for its WikiPron subset (207 of 753 entries)
- **Lexicon count mismatch on HuggingFace:** README claims 1,136 but only 1,135 TSV files exist

### 6.3 Sumerogram Contamination

Luwian (581 entries) and Urartian (171 entries) contain **cuneiform Sumerograms** — uppercase logograms like `LUGAL`, `URU`, `DINGIR`, `MUNUS.LUGAL` that represent Sumerian words used as shorthand in cuneiform texts. These are NOT phonemic data in the target language and:
- Produce SCA class "0" (unknown)
- Would corrupt any phonological comparison pipeline (e.g., PhaiPhon)
- Should be filtered or flagged before analysis

---

## 7. Scholarly Accuracy of Lexical Entries

### 7.1 Per-Language Verdicts

| Language | Verdict | Key Issues |
|----------|---------|------------|
| **Hittite** | CONCERNS | Sumerograms/Akkadograms mixed in as Hittite words (IM, LU, URU, KU6, ITUD, ZAG, TUR, MUNUS.LUGAL, DUMU.MUNUS — at least 10-12 entries). One Avestan word (`xshap` = "night") in wrong file entirely. |
| **PIE** | MOSTLY ACCURATE | Reconstructions consistent with LIV/Pokorny/NIL. Laryngeal IPA values represent one theory but applied consistently. |
| **Etruscan** | CONCERNS | ~33% proper nouns (personal names, theonyms, Hellenized names like Hercle/Aplu/Achile). Some over-confident meanings for words scholars consider uncertain (antar="eagle", arimos="monkey"). |
| **Ugaritic** | MOSTLY ACCURATE | Vocabulary aligns with DUL. Vowel patterns are reconstructed (inherent to Ugaritic studies). A few place/divine names mixed in. |
| **Luwian** | MOSTLY ACCURATE | Honest "(unknown)" labels for uncertain glosses (~35%). Good source (eDiAna). Cuneiform/hieroglyphic not improperly mixed. |
| **Avestan** | CONCERNS | Data artifacts in production data. OAv/YAv not distinguished. Single-character alphabet entries treated as words. 74.4% empty Concept_IDs. |

### 7.2 Cross-Language Contamination

| Entry | In File | Actually Belongs To |
|-------|---------|---------------------|
| `xshap` ("night") | hit.tsv (Hittite) | Avestan (xshap- is Old Iranian) |
| `GE` ("ina: in, by, from") | hit.tsv (Hittite) | Akkadian (preposition ina) |
| Sumerograms (LUGAL, URU, etc.) | hit.tsv, xlw.tsv, xur.tsv | Sumerian logograms |

---

## 8. HuggingFace / GitHub Presentation

### 8.1 HuggingFace Problems

| Issue | Severity |
|-------|----------|
| **Dataset is private/gated** — 0 downloads, 0 access | CRITICAL |
| **No LICENSE file** on either platform | CRITICAL |
| **README is a 46-line stub** — empty Quick Start, no citations, no limitations, no examples | CRITICAL |
| `sources/` directory leaked (2,928 files, 0.44 GB of upstream CLDF repos) | HIGH |
| `.pytest_cache/` directories present (dev artifacts) | HIGH |
| **Copyrighted PDFs included** (Trask dictionary, Rodriguez-Ramos 2014) | HIGH |
| PyTorch `.pth` and `.pkl` files present (security risk) | MEDIUM |
| No `datasets.load_dataset()` support (no loading script/Parquet) | MEDIUM |
| Lexicon count: 1,135 files vs 1,136 claimed | MEDIUM |

### 8.2 GitHub Problems

| Issue | Severity |
|-------|----------|
| No LICENSE file at repo root | CRITICAL |
| GitHub README describes original Luo et al. datasets; HuggingFace README describes 1,136-language expansion — narratives disconnected | HIGH |
| No GitHub topics/tags set | LOW |
| 0 stars, 0 forks, 0 contributors beyond author | — (not a quality issue) |

### 8.3 Strengths

- GitHub README is thorough and well-organized (158 lines)
- 18 adversarial audit documents show genuine quality control effort
- Scripts for full reproduction are present and documented
- Active development (commits every 1-2 days)
- DATABASE_REFERENCE.md (this document's subject) is exceptionally detailed

---

## 9. Recommendations Summary

### Immediate Fixes (Critical)

1. **Investigate and restore the missing 2,716 Avestan `avesta_org` entries**, or correct DATABASE_REFERENCE.md
2. **Remove bogus entries** (`inprogress`, `phoneticvalue`) from `ave.tsv`
3. **Make HuggingFace dataset public** (or document the gating rationale)
4. **Add a LICENSE file** to both repos
5. **Fix SCA tokenizer**: add `\u02B7` (ʷ) to diacritic regex to prevent spurious "0" on labiovelars

### High Priority

6. **Fix Etruscan θ/φ/χ inconsistency**: either all fricatives or all aspirated stops
7. **Fix Lydian ś/š collapse**: distinguish as ś→[ɕ] and š→[ʃ] (matching Carian treatment)
8. **Filter/flag Sumerograms** in Hittite, Luwian, and Urartian before phonetic analysis
9. **Remove cross-language contamination**: `xshap` from Hittite, Akkadian `ina` from Hittite
10. **Filter/flag proper nouns** in Etruscan (~250 entries)
11. **Remove leaked `sources/` directory and `.pytest_cache/`** from HuggingFace
12. **Remove copyrighted PDFs** from the repository
13. **Expand HuggingFace README** to match GitHub quality (Quick Start, citations, limitations)

### Medium Priority

14. **Add Sumerian, Akkadian, and Egyptian** using ePSD2, CAD, and TLA (highest-impact expansions)
15. **Add Ancient Greek and Gothic** using LSJ/Perseus and Project Wulfila
16. **Replace avesta.org** with Bartholomae or modern Iranian references
17. **Cross-reference Palaeolexicon data** against eDiAna and other primary sources
18. **Use Lexibank 2 standardized versions** of ABVD/NorthEuraLex instead of raw data
19. **Distinguish Old vs Young Avestan** entries
20. **Add precomposed nasalized vowels** (ã, ẽ, ũ) to SCA tokenizer
21. **Document controversial phonological choices** (Hittite š, OP ç, PIE laryngeals) in transliteration maps
22. **Add ancient languages to `languages.tsv`** metadata
23. **Add breathy-voice diacritic ʱ (U+02B1)** to SCA regex

### Design Considerations (Long-term)

24. Consider adding ejective/emphatic/uvular classes to SCA to preserve Kartvelian/Semitic contrasts
25. Consider PHOIBLE integration for phonological inventory validation
26. Consider adding geographic coverage for South/East Asian, African, and American ancient languages
27. Consider adding confidence indicators for IPA quality per entry (attested vs. reconstructed)

---

*This audit was conducted by 6 parallel AI research agents. All findings are based on file reading, web research, and scholarly knowledge. No code was written or executed.*
