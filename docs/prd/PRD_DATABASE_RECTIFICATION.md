# PRD: Database Rectification & Expansion Plan

**Date:** 2026-03-12
**Triggered by:** [Adversarial Database Audit 2026-03-12](../ADVERSARIAL_DATABASE_AUDIT_2026-03-12.md)
**Reference:** [DATABASE_REFERENCE.md](../DATABASE_REFERENCE.md) (protocols, schema, Iron Law)
**Status:** DRAFT — awaiting approval

---

## IRON LAW (UNCHANGED — SUPERSEDES ALL GOALS)

```
DATA MAY ONLY ENTER THE DATASET THROUGH CODE THAT DOWNLOADS IT
FROM AN EXTERNAL SOURCE.

NO EXCEPTIONS. NO "JUST THIS ONCE." NO "IT'S FASTER."
```

Every phase below produces **Python scripts** that fetch data via HTTP. No hardcoded word lists. No direct TSV edits. No LLM-generated linguistic content. Every script must contain `urllib.request.urlopen()`, `requests.get()`, or equivalent HTTP fetch. Transliteration maps are CODE (transformation rules from cited grammars), not DATA.

---

## ADVERSARIAL PIPELINE v2 (ENHANCED)

Every phase uses the **Dual-Agent Adversarial Pipeline**. This PRD upgrades the adversarial auditor from v1 (surface-level checks) to v2 (deep cross-reference validation).

### Team A: Extraction Agent
- Writes and runs Python scripts following the [script template](../DATABASE_REFERENCE.md#10-prd-adding-new-data)
- Produces TSV data via HTTP fetch → parse → transliterate → write
- NEVER writes data values directly

### Team B: Critical Adversarial Auditor (v2 — ENHANCED)

**Runs after EACH step with VETO POWER.** The v2 auditor performs **deep validation**, not surface-level checks.

#### What Team B MUST Do (Deep Checks)

| Check | Method | Pass Criteria |
|-------|--------|---------------|
| **50-Word Cross-Reference** | Select 50 random entries from the newly scraped data. For each, fetch the LIVE source URL and verify the word appears there with the same form and meaning. | >= 48/50 match (96%). Any mismatch = STOP. |
| **IPA Spot-Check** | For 20 random entries, manually apply the transliteration map character-by-character and verify the output matches the IPA column. | 20/20 match. Any mismatch = flag map bug. |
| **SCA Consistency** | For 20 random entries, verify `ipa_to_sound_class(IPA)` == SCA column. | 20/20 match. |
| **Source Provenance** | For 10 random entries, construct the exact URL where each entry can be found in the original source. Verify it loads. | 10/10 accessible. |
| **Concept ID Accuracy** | For 20 random entries with non-empty Concept_IDs, verify the gloss matches the source's definition. | >= 18/20 match. |
| **Dedup Verification** | Count unique Word values in the output. Compare to total rows. | 0 duplicates. |
| **Entry Count Plausibility** | Verify count is non-round and matches expected range from source research. | Not exactly round (100, 200, 500). |

#### What Team B Must NOT Do (Banned Checks — Wastes Time)

- "Does the file have a header?" (Always yes by construction)
- "Are there HTML tags in the data?" (Parsing handles this)
- "Is the file UTF-8?" (Always yes by construction)
- "Does the script import urllib?" (Obvious from code review)
- Any check that doesn't touch real data

#### Auditor Report Format

```markdown
# Adversarial Audit v2: {Phase} — {Language} ({iso})

## 50-Word Cross-Reference
- Sampled: [list 50 words]
- Source URL pattern: {url}
- Matches: N/50
- Mismatches: [list any failures with details]

## IPA Spot-Check (20 entries)
| Word | Expected IPA | Actual IPA | Match? |
|------|-------------|------------|--------|
| ... | ... | ... | ... |

## SCA Consistency (20 entries)
- All match: YES/NO

## Source Provenance (10 entries)
| Word | Source URL | Accessible? |
|------|-----------|-------------|
| ... | ... | ... |

## Concept ID Accuracy (20 entries)
- Matches: N/20

## Dedup Check
- Unique words: N
- Total rows: N
- Duplicates: 0

## Verdict: PASS / FAIL
## Blocking Issues: [list if any]
```

---

## PROPER NOUNS POLICY

**Proper nouns (theonyms, toponyms, anthroponyms) are VALUED DATA, not contamination.**

All ancient language lexicons SHOULD include:
- **Theonyms** (divine names): gods, goddesses, mythological figures
- **Toponyms** (place names): cities, rivers, mountains, temples, regions
- **Anthroponyms** (personal names): rulers, historical figures, common name elements
- **Ethnonyms** (people/tribe names): tribal and ethnic designations

Concept_ID should tag these as `theonym:{name}`, `toponym:{name}`, `anthroponym:{name}`, `ethnonym:{name}`.

Where specialist proper noun databases exist (see Phase 5), they MUST be scraped alongside regular vocabulary.

---

## PHASE 0: Critical Bug Fixes

**Priority:** IMMEDIATE — blocks all other phases
**Estimated effort:** 1 session
**No adversarial audit needed** (code changes only, no data ingestion)

### 0.1 Fix SCA Tokenizer — Labiovelar Bug

**File:** `cognate_pipeline/src/cognate_pipeline/normalise/sound_class.py`
**Bug:** `ʷ` (U+02B7) missing from diacritic regex → produces spurious "0" for every labiovelar
**Fix:** Add `\u02B7` to the diacritic character class on the tokenizer regex (line ~95)
**Also add:** `\u02B1` (breathy voice ʱ) for PIE voiced aspirates
**Test:** Run `ipa_to_sound_class("kʷ")` → should produce `"K"` not `"K0"`

### 0.2 Fix SCA Tokenizer — Precomposed Nasalized Vowels

**Bug:** Precomposed `ã` (U+00E3), `ẽ` (U+1EBD), `ũ` (U+0169) may fail tokenizer regex
**Fix:** Either (a) NFC-decompose input before tokenizing, or (b) add precomposed nasalized vowels to the character class
**Test:** Run `ipa_to_sound_class("ã")` → should produce `"A"` not `""`

### 0.3 Write Cleaning Script — Remove Bogus Entries

**Iron Law compliance:** Write a Python script `scripts/clean_artifacts.py` that:
1. Reads each ancient language TSV
2. Identifies known artifact patterns: `inprogress`, `phoneticvalue`, entries where Word matches `^[a-z]+progress$` or similar processing placeholders
3. Writes cleaned TSV (preserving all legitimate entries)
4. Logs removed entries to audit trail
5. Reports counts

**NOT a direct edit** — this is a deterministic cleaning script.

### 0.4 Fix Metadata — Add Ancient Languages to languages.tsv

**Script:** `scripts/update_metadata.py` — reads all TSVs in lexicons/, counts entries, updates `languages.tsv` with ISO, name, family, entry count, source breakdown. Run after every data change.

### 0.5 Presentation Fixes

| Task | Action |
|------|--------|
| Add LICENSE file | Create `LICENSE` at repo root with CC-BY-SA-4.0 full text |
| Make HuggingFace public | Change dataset visibility to public (manual step) |
| Fix HuggingFace README | Expand to include Quick Start, citations, limitations, loading examples |
| Remove leaked files | Add `sources/`, `.pytest_cache/`, `*.pth`, `*.pkl` to HF `.gitignore`; remove copyrighted PDFs |
| Fix lexicon count | Identify which of 1,136 claimed files is missing; create or correct count |

---

## PHASE 1: IPA & Transliteration Map Corrections

**Priority:** HIGH — affects all downstream phonetic analysis
**Estimated effort:** 1 session
**Adversarial audit:** YES (Team B verifies 20 entries per map change via IPA spot-check)

### 1.1 Transliteration Map Fixes

Each fix below modifies `scripts/transliteration_maps.py` (CODE, not DATA). After all fixes, run `scripts/reprocess_ipa.py` to propagate changes.

| Fix | Language | Change | Academic Reference |
|-----|----------|--------|--------------------|
| **Etruscan θ/φ/χ consistency** | ett | Change θ→`tʰ` (aligning with φ→`pʰ`, χ→`kʰ` as aspirated stop series) OR change all three to fricatives (θ, f, x). Pick ONE. Recommended: all aspirated stops per Bonfante early-period analysis: θ→`tʰ` | Bonfante & Bonfante (2002), Rix (1963) |
| **Lydian ś/š distinction** | xld | Change ś→`ɕ` (alveolopalatal, matching Carian treatment), keep š→`ʃ` | Gusmani (1964), Melchert |
| **Carian ỳ/ý placeholders** | xcr | Map to best-guess IPA or explicit unknown marker. Recommended: ỳ→`ə`, ý→`e` (tentative vocalic values) with comment noting uncertainty | Adiego (2007) |
| **Hittite š controversy** | hit | ADD COMMENT documenting the debate. Keep š→`ʃ` as the current choice but note: "Kloekhorst (2008) argues for [s]. Hoffner & Melchert (2008) use the conventional symbol." Do NOT change the value without user decision. | Hoffner & Melchert (2008), Kloekhorst (2008) |
| **Old Persian ç controversy** | peo | ADD COMMENT documenting the debate. Keep ç→`θ` per Kent but note Kloekhorst's /ts/ argument. | Kent (1953), Kloekhorst (2008) |
| **PIE h₃ value** | ine | ADD COMMENT noting the speculative nature of h₃→`ɣʷ`. Note: "Leiden school reconstruction. Many scholars leave h₃ phonetically unspecified." | Beekes (2011), Fortson (2010) |
| **Missing Phrygian Greek letters** | xpg | Add: ξ→`ks`, ψ→`ps`, φ→`pʰ`, χ→`kʰ` | Brixhe & Lejeune (1984) |
| **Missing PK aspirated affricates** | ccs | Add: cʰ→`tsʰ`, čʰ→`tʃʰ` | Klimov (1998) |
| **Missing Old Persian signs** | peo | Add: U+103AE (di), U+103B8 (mu), U+103BB (vi) | Kent (1953) |

### 1.2 Post-Fix Reprocessing

```bash
# Dry run first (ALWAYS)
python scripts/reprocess_ipa.py --dry-run

# Verify: identity rates should decrease or stay the same, NEVER increase
# Verify: no regressions (Never-Regress Rule)

# Run live
python scripts/reprocess_ipa.py
```

### 1.3 Adversarial Audit for Phase 1

Team B verifies:
- For each modified map: take 20 entries from that language's TSV, manually apply the updated map, verify IPA matches
- Verify no regressions: compare before/after identity rates
- Verify SCA correctness for 20 entries per language

---

## PHASE 2: Data Restoration & Cleanup

**Priority:** HIGH — fixes audit-identified data problems
**Estimated effort:** 1–2 sessions
**Adversarial audit:** YES (full v2 pipeline)

### 2.1 Avestan — Re-scrape avesta.org (Restore Missing 2,716 Entries)

**Problem:** DATABASE_REFERENCE.md claims 3,455 entries including 2,716 from `avesta_org`, but `ave.tsv` only has 739 entries. The avesta_org data was either never ingested or was lost.

**Script:** `scripts/scrape_avesta_org.py` (already exists — re-run or debug)

**Steps:**
1. Team A: Verify `scrape_avesta_org.py` still works against live site
2. Team A: Run `--dry-run` to confirm expected entry count
3. Team A: Run live scrape, deduplicating against existing 739 entries
4. Team B: 50-word cross-reference against live avesta.org/avdict/avdict.htm
5. Team B: IPA spot-check 20 entries against `AVESTAN_MAP`
6. Update DATABASE_REFERENCE.md with actual count

**Acceptance:** `ave.tsv` has 2,500+ entries (the 3,455 was an aspiration, actual may differ)

### 2.2 Sumerogram Handling Script

**Problem:** Hittite (10+ entries), Luwian (581), and Urartian (171) contain Sumerograms — uppercase cuneiform logograms (LUGAL, URU, DINGIR, etc.) that are NOT phonemic data in the target language.

**Script:** `scripts/tag_sumerograms.py`

**Approach:** Do NOT delete Sumerograms — they are legitimate scholarly data. Instead:
1. Write a script that identifies likely Sumerograms (all-uppercase ASCII, known Sumerogram patterns)
2. Add a tag to the Concept_ID field: prefix with `sumerogram:` (e.g., `sumerogram:king` for LUGAL)
3. This allows downstream pipelines to filter them if needed while preserving the data
4. Log all tagged entries to audit trail

**Sumerogram detection heuristic:**
```python
def is_sumerogram(word: str) -> bool:
    """Detect cuneiform Sumerograms (uppercase sign names)."""
    if word.isupper() and word.isascii() and len(word) >= 2:
        return True
    if re.match(r'^[A-Z]+(\.[A-Z]+)+$', word):  # MUNUS.LUGAL pattern
        return True
    if re.match(r'^[A-Z]+\d+$', word):  # KU6, AN2 pattern
        return True
    return False
```

**Team B checks:** Verify 20 tagged entries are actually Sumerograms (not coincidentally uppercase native words).

### 2.3 Cross-Language Contamination Fix

**Problem:** `hit.tsv` contains at least one Avestan word (`xshap` = "night") and Akkadian entries (`GE` = "ina").

**Script:** `scripts/clean_cross_contamination.py`
1. For each ancient language TSV, check every entry against a known-contamination list (populated from audit findings)
2. Remove entries confirmed to be from wrong language
3. Log removals to audit trail

**Known contamination (from audit):**
- `hit.tsv`: `xshap` (Avestan), `GE`/`ina` (Akkadian)

**Team B checks:** Verify each removed entry is genuinely from the wrong language by checking Wiktionary source pages.

---

## PHASE 3: New Language Ingestion — Tier 1

**Priority:** HIGH — the 9 most critical missing languages
**Estimated effort:** 3–5 sessions (can parallelize across languages)
**Adversarial audit:** YES (full v2 pipeline per language)

### General Protocol (applies to all Tier 1 languages)

For each new language:

1. **Create transliteration map** in `transliteration_maps.py` (if needed) with cited academic reference
2. **Write extraction script** following the [standard template](../DATABASE_REFERENCE.md#10-prd-adding-new-data):
   - Must use `urllib.request.urlopen()` or `requests.get()`
   - Must deduplicate against existing entries
   - Must apply `transliterate()` and `ipa_to_sound_class()`
   - Must save raw JSON/HTML to `data/training/raw/`
   - Must save audit trail to `data/training/audit_trails/`
3. **Run `--dry-run`** first
4. **Deploy Team B adversarial auditor** (full v2: 50-word cross-ref, IPA spot-check, etc.)
5. **Run live**
6. **Add to `language_configs.py`**
7. **Run `reprocess_ipa.py --language {iso}`**
8. **Update metadata** (`languages.tsv`)
9. **Commit & push** to both GitHub and HuggingFace

---

### 3.1 Sumerian (sux)

| Field | Value |
|-------|-------|
| ISO | sux |
| Family | Isolate |
| Primary Source | **ePSD2** — `oracc.museum.upenn.edu/epsd2/sux` (JSON API) |
| Secondary Source | DCCLT lexical texts via Oracc |
| Expected entries | 10,000–15,944 lemmas |
| Script name | `scripts/scrape_epsd2_sumerian.py` |
| Transliteration map | New: `SUMERIAN_MAP` — cuneiform transliteration → IPA (Jagersma 2010, Edzard 2003) |
| IPA type | Partial (phonology reconstructed via Akkadian scribal conventions) |
| Special handling | Strip determinatives (superscript d, GIS, etc.). Tag Sumerograms vs. phonemic entries. Separate emesal (women's dialect) from emegir (main dialect). |
| Proper nouns to include | Divine names (Enlil, Inanna, Enki, Utu, Nanna, etc.), city names (Ur, Uruk, Lagash, Nippur, Eridu, etc.), royal names (Gilgamesh, Ur-Nammu, Shulgi, etc.) |

**Scraping approach:**
- ePSD2 exposes a JSON API at `oracc.museum.upenn.edu/epsd2/json/`
- Fetch the full glossary index, then individual lemma pages
- Parse: headword, citation form, base, morphology, English gloss
- The ePSD2 provides transliterations in standard Assyriological conventions

### 3.2 Akkadian (akk)

| Field | Value |
|-------|-------|
| ISO | akk |
| Family | Afroasiatic > Semitic (East) |
| Primary Source | **AssyrianLanguages.org** — `assyrianlanguages.org/akkadian/` (searchable dictionary) |
| Secondary Source | Oracc glossaries, Wiktionary Category:Akkadian_lemmas |
| Expected entries | 5,000–10,000 (from online searchable sources; full CAD is 28K but PDF-only) |
| Script name | `scripts/scrape_akkadian.py` |
| Transliteration map | New: `AKKADIAN_MAP` — standard Assyriological transliteration → IPA (Huehnergard 2011, von Soden 1995) |
| IPA type | Broad phonemic (well-understood via comparative Semitic + cuneiform orthography) |
| Special handling | Distinguish Old Babylonian, Middle Babylonian, Neo-Assyrian, etc. via source metadata if available. Handle determinatives. |
| Proper nouns to include | Divine names (Marduk, Ishtar, Shamash, Ea, Sin, Nabu, etc.), city names (Babylon, Nineveh, Assur, Sippar, etc.), royal names (Hammurabi, Sargon, Nebuchadnezzar, etc.) |

### 3.3 Ancient Egyptian (egy)

| Field | Value |
|-------|-------|
| ISO | egy |
| Family | Afroasiatic > Egyptian |
| Primary Source | **TLA** — `thesaurus-linguae-aegyptiae.de` (API or web scrape) |
| Secondary Source | TLA HuggingFace datasets (`huggingface.co/datasets/thesaurus-linguae-aegyptiae/`) |
| Expected entries | 10,000–49,037 lemmas |
| Script name | `scripts/scrape_tla_egyptian.py` |
| Transliteration map | New: `EGYPTIAN_MAP` — Egyptological transliteration (Manuel de Codage) → IPA (Allen 2014, Loprieno 1995) |
| IPA type | Partial (consonantal skeleton well-known; vowels reconstructed from Coptic, cuneiform transcriptions, and comparative Afroasiatic) |
| Special handling | Egyptian had no written vowels. Provide consonantal IPA skeleton. Consider separate entries for different periods (Old/Middle/Late/Demotic). Hieroglyphic Unicode signs (U+13000–U+1342F) should be mapped if present. |
| Proper nouns to include | Pharaoh names (Khufu, Ramesses, Thutmose, etc.), deity names (Ra, Osiris, Isis, Horus, Thoth, Anubis, etc.), place names (Thebes, Memphis, Heliopolis, etc.) |

### 3.4 Sanskrit (san)

| Field | Value |
|-------|-------|
| ISO | san |
| Family | Indo-European > Indo-Iranian > Indo-Aryan |
| Primary Source | **Wiktionary** Category:Sanskrit_lemmas (massive category) |
| Secondary Source | WikiPron Sanskrit entries, DCS (Digital Corpus of Sanskrit) if API accessible |
| Expected entries | 5,000–20,000 from Wiktionary alone |
| Script name | `scripts/scrape_sanskrit.py` |
| Transliteration map | New: `SANSKRIT_MAP` — IAST/Devanagari → IPA (Whitney 1896, Mayrhofer 1986) |
| IPA type | Full phonemic (Sanskrit phonology is comprehensively documented) |
| Special handling | Handle both Devanagari (U+0900–U+097F) and IAST romanization. Vedic Sanskrit vs Classical Sanskrit distinction desirable. |
| Proper nouns to include | Divine names (Indra, Agni, Varuna, Vishnu, Shiva, etc.), place names (Hastinapura, Ayodhya, Lanka, etc.), epic names (Arjuna, Rama, Krishna, etc.) |

### 3.5 Ancient Greek (grc)

| Field | Value |
|-------|-------|
| ISO | grc |
| Family | Indo-European > Hellenic |
| Primary Source | **Wiktionary** Category:Ancient_Greek_lemmas |
| Secondary Source | WikiPron Ancient Greek entries, Perseus Digital Library |
| Expected entries | 10,000+ from Wiktionary |
| Script name | `scripts/scrape_ancient_greek.py` |
| Transliteration map | New: `ANCIENT_GREEK_MAP` — Greek alphabet → reconstructed Classical Attic IPA (Allen 1987, Smyth 1920) |
| IPA type | Full phonemic (Classical Attic pronunciation well-reconstructed) |
| Special handling | Use Classical Attic pronunciation (not Koine or Modern). Handle polytonic orthography (breathing marks, accents). Distinguish from Modern Greek WikiPron entries. |
| Proper nouns to include | Theonyms (Zeus, Athena, Apollo, Hermes, etc.), place names (Athens, Sparta, Thebes, Troy, etc.), hero names (Achilles, Odysseus, Herakles, etc.) |

### 3.6 Gothic (got)

| Field | Value |
|-------|-------|
| ISO | got |
| Family | Indo-European > Germanic (East) |
| Primary Source | **Project Wulfila** — `wulfila.be` (TEI corpus + glossary) |
| Secondary Source | Wiktionary Category:Gothic_lemmas |
| Expected entries | 3,000–3,600 lemmas |
| Script name | `scripts/scrape_wulfila_gothic.py` |
| Transliteration map | New: `GOTHIC_MAP` — Gothic alphabet (U+10330–U+1034F) + transliteration → IPA (Wright 1910, Braune/Heidermanns 2004) |
| IPA type | Full phonemic (Gothic phonology well-understood from comparative Germanic) |
| Special handling | Handle Gothic script Unicode block. Project Wulfila provides downloadable TEI XML — use cached-fetch pattern if needed. |
| Proper nouns to include | Biblical proper nouns in Gothic form (Iesus, Xristus, Pawlus, Iairusalem, etc.), tribal names (Gutans, etc.) |

### 3.7 Mycenaean Greek (gmy)

| Field | Value |
|-------|-------|
| ISO | gmy |
| Family | Indo-European > Hellenic |
| Primary Source | **DAMOS** — `damos.hf.uio.no` (complete annotated Mycenaean corpus) |
| Secondary Source | Palaeolexicon Linear B section |
| Expected entries | 500–800 |
| Script name | `scripts/scrape_damos_mycenaean.py` |
| Transliteration map | New: `MYCENAEAN_MAP` — Linear B syllabary → reconstructed IPA (Ventris & Chadwick 1973, Bartonek 2003) |
| IPA type | Partial (Linear B is a syllabary that obscures many consonant clusters and final consonants) |
| Special handling | Linear B is a syllabary — each sign represents a CV syllable. The underlying Greek word must be reconstructed from the syllabic spelling. Many readings are uncertain. |
| Proper nouns to include | Place names from tablets (pa-ki-ja-ne/Sphagianai, ko-no-so/Knossos, etc.), divine names (di-wo/Zeus, a-ta-na-po-ti-ni-ja/Athena Potnia, etc.) |

### 3.8 Old Church Slavonic (chu)

| Field | Value |
|-------|-------|
| ISO | chu |
| Family | Indo-European > Slavic (South) |
| Primary Source | **Wiktionary** Category:Old_Church_Slavonic_lemmas |
| Secondary Source | GORAZD digital dictionary (`gorazd.org`) if API accessible |
| Expected entries | 2,000–5,000 from Wiktionary |
| Script name | `scripts/scrape_ocs.py` |
| Transliteration map | New: `OCS_MAP` — Cyrillic/Glagolitic → IPA (Lunt 2001) |
| IPA type | Full phonemic (OCS phonology well-established) |
| Special handling | Handle both Cyrillic and Glagolitic scripts. OCS Cyrillic uses characters not in modern Cyrillic (ѣ, ъ, ь, ѫ, ѧ, etc.). |
| Proper nouns to include | Place names from OCS texts, biblical proper nouns in OCS form |

### 3.9 Old Norse (non)

| Field | Value |
|-------|-------|
| ISO | non |
| Family | Indo-European > Germanic (North) |
| Primary Source | **Wiktionary** Category:Old_Norse_lemmas |
| Secondary Source | Cleasby-Vigfusson online if scrapable |
| Expected entries | 5,000–10,000 |
| Script name | `scripts/scrape_old_norse.py` |
| Transliteration map | New: `OLD_NORSE_MAP` — Old Norse orthography → IPA (Gordon 1957, Noreen 1923) |
| IPA type | Full phonemic (Old Norse phonology well-documented) |
| Special handling | Handle Old Norse special characters (ð, þ, æ, ø, ǫ). Distinguish Old West Norse (Old Icelandic) from Old East Norse if possible. |
| Proper nouns to include | Divine names from Eddas (Oðinn, Þórr, Freyr, Freyja, Loki, Baldr, etc.), place names (Ásgarðr, Miðgarðr, Jǫtunheimr, etc.), hero names (Sigurðr, Ragnarr, etc.) |

---

## PHASE 4: Proper Noun Expansion

**Priority:** MEDIUM-HIGH — enhances all existing and new languages
**Estimated effort:** 2–3 sessions (parallelizable)
**Adversarial audit:** YES (full v2 pipeline)

### 4.1 Strategy

For each language already in the database (and each new Tier 1 language), identify and scrape specialist proper noun sources. Proper nouns are tagged in Concept_ID as:
- `theonym:{name}` — divine/mythological names
- `toponym:{name}` — place names
- `anthroponym:{name}` — personal names (rulers, historical figures)
- `ethnonym:{name}` — tribal/ethnic names

### 4.2 Proper Noun Sources by Language (Detailed — from specialist research)

#### Tier 1 Sources: Structured Data with API/Download (Best Targets)

| # | Language | Source | URL | API Type | Est. Proper Nouns | Notes |
|---|----------|--------|-----|----------|-------------------|-------|
| 1 | **Sumerian** | ORACC ePSD2 QPN glossaries | `oracc.museum.upenn.edu/epsd2/names/` | **JSON API** (`build-oracc.museum.upenn.edu/json/`) | 1,000+ (qpn-x-divine, qpn-x-placeN, qpn-x-people, qpn-x-temple, qpn-x-ethnic, qpn-x-celestial) | Sub-glossaries by type code. Best structured source in entire survey. |
| 2 | **Sumerian** | ETCSL proper nouns (Oxford) | `etcsl.orinst.ox.ac.uk/cgi-bin/etcslpropnoun.cgi` | Scrapable HTML tables | **917 unique** (12,537 occurrences): ~400 DN, ~200 RN, ~150 SN, ~120 TN, ~80 PN | Categorized by type (DN/RN/SN/TN/PN/GN/WN). |
| 3 | **Akkadian** | ORACC QPN glossaries (all sub-projects) | `oracc.museum.upenn.edu` (rinap, saao, cams, etc.) | **JSON API** | Thousands across dozens of sub-projects | Same JSON structure as Sumerian QPN. Covers Neo-Assyrian, Neo-Babylonian, Old Babylonian. |
| 4 | **Egyptian** | TLA proper noun lemmas | `thesaurus-linguae-aegyptiae.de` | **JSON/TEI XML API** + **HuggingFace JSONL** | Thousands (subset of 49,037 + 11,610 lemmas) | Categories for kings, deities, persons, places, titles. Raw JSON + TEI XML in lasting repository. |
| 5 | **Egyptian** | Pharaoh.se king list | `pharaoh.se` | Scrapable HTML | **300–350 pharaoh names** (with variants) | Turin Canon (223), Abydos (76), Karnak (61), Saqqara (58), Manetho. Per-pharaoh URLs. |
| 6 | **Ancient Greek** | LGPN (Lexicon of Greek Personal Names, Oxford) | `search.lgpn.ox.ac.uk` | **REST API** (`clas-lgpn5.classics.ox.ac.uk:8080/exist/apps/lgpn-api/`) | **35,982 unique personal names** (~400,000 individuals across 8 volumes) | Single richest source for ancient Greek anthroponyms. Data also in ORA (Oxford Research Archive). |
| 7 | **Ancient Greek** | Pleiades Gazetteer | `pleiades.stoa.org` | **JSON + CSV bulk download** (daily dumps at `atlantides.org/downloads/pleiades/json/`) | **36,000+ places**, **26,000+ ancient names** | GitHub releases. CC-BY licensed. Coordinates, time periods, citations. |
| 8 | **Ancient Greek** | Theoi.com mythology | `theoi.com` | Scrapable HTML (consistent structure) | **1,000–1,500 mythological figures** | Gods, daimones, creatures, heroes. Alphabetical pages. |
| 9 | **Gothic** | Project Wulfila | `wulfila.be/gothic/download/` | **TEI XML download** with POS tags | **200–300 biblical proper nouns** | Nouns tagged "Noun, proper." Most machine-friendly source in survey. |
| 10 | **Etruscan** | CIE/TLE Digital Concordance (Zenodo) | Zenodo (search "Etruscan Faliscan concordance") | **CSV download** | **1,000+ unique names** (from 12,000+ inscriptions) | ~67% of inscriptions contain personal names. Far exceeds current ~250. |

#### Tier 2 Sources: Structured HTML, Easily Scrapable

| # | Language | Source | URL | Est. Proper Nouns | Notes |
|---|----------|--------|-----|-------------------|-------|
| 11 | **Sumerian** | AMGG (Ancient Mesopotamian Gods & Goddesses) | `oracc.museum.upenn.edu/amgg/listofdeities/` | ~100 major deity profiles | Scholarly profiles with epithets, iconography. |
| 12 | **Hittite** | HDN (Hittite Divine Names) | `cuneiform.neocities.org/HDN/outline` | ~1,000+ divine name entries | Updates van Gessel's 3-volume *Onomasticon*. HTML tables + PDF. |
| 13 | **Hittite** | HPN + LAMAN (Hittite Name Finder) | `cuneiform.neocities.org/HPN/outline` / `cuneiform.neocities.org/laman/start` | Hundreds of personal names | Unified divine + geographical + personal name retrieval. |
| 14 | **Ugaritic** | Wikipedia List of Ugaritic Deities | `en.wikipedia.org/wiki/List_of_Ugaritic_deities` | **200–234 divine names** | MediaWiki API. Cuneiform/alphabetic writings + functions. |
| 15 | **Ugaritic** | Sapiru Project deity lists | `sapiru.wordpress.com` | ~60–80 per list (multiple lists) | Actual Ras Shamra sacrificial deity lists (~1250 BCE). |
| 16 | **Avestan** | Avesta.org Zoroastrian Names | `avesta.org/znames.htm` | **400+ personal names** + divine names | Single long page. Based on Bartholomae. |
| 17 | **Avestan** | Encyclopaedia Iranica | `iranicaonline.org` | 400+ names (article "Personal Names, Iranian ii") | Per-deity articles (Anahita, Mithra, Verethragna, Amesha Spentas). |
| 18 | **Etruscan** | ETP (Etruscan Texts Project, UMass) | `scholarworks.umass.edu/ces_texts/` | 200+ (from 300+ post-1990 inscriptions) | Searchable by keyword, location, date. |
| 19 | **Etruscan** | Godchecker Etruscan Mythology | `godchecker.com/etruscan-mythology/list-of-names/` | **89 deity names** | Static HTML list. |
| 20 | **Old Norse** | Nordic Names | `nordicnames.de/wiki/Category:Old_Norse_Names` | Substantial subset of 50,000+ total | MediaWiki API. Name, meaning, etymology, gender. |
| 21 | **Old Norse** | Eddic proper nouns (Voluspa.org / Sacred-Texts) | `voluspa.org/poeticedda.htm` | **500–800 unique** (deities, giants, dwarves, places, weapons) | Dvergatal alone lists ~70 dwarf names. Requires NLP extraction. |

#### Tier 3 Sources: Existing + Wiktionary Expansion

| Language | Source | URL | Est. Names |
|----------|--------|-----|------------|
| Hurrian | Palaeolexicon | `palaeolexicon.com` | 50+ |
| Urartian | Oracc eCUT | `oracc.museum.upenn.edu/ecut/` | 100+ |
| Lycian/Lydian/Carian | eDiAna | `ediana.gwi.uni-muenchen.de` | 50+ each |
| Phoenician | Wiktionary | `en.wiktionary.org` | 50+ |
| PIE | Wiktionary reconstructed theonyms | `en.wiktionary.org` | 30+ |
| Mycenaean | DAMOS | `damos.hf.uio.no` | 100+ |
| Sanskrit | Wiktionary proper nouns | `en.wiktionary.org` | 500+ |
| OCS | Wiktionary proper nouns | `en.wiktionary.org` | 100+ |

### 4.3 Per-Language Script

Create `scripts/scrape_proper_nouns.py` — a unified script with per-language configs:

```python
PROPER_NOUN_CONFIGS = {
    "grc": {
        "sources": [
            {"type": "wiktionary_cat", "category": "Category:Ancient_Greek_proper_nouns"},
            {"type": "theoi", "url": "https://www.theoi.com/greek-mythology/..."},
        ],
        "iso_for_translit": "grc",
        "tsv_filename": "grc.tsv",
    },
    ...
}
```

### 4.4 Adversarial Audit for Proper Nouns

Team B checks (in addition to standard v2):
- Verify 20 proper nouns are attested in the source language (not modern inventions)
- Verify Concept_ID tags are correct (theonym vs toponym vs anthroponym)
- Verify no modern-language proper nouns leaked in (e.g., English "John" in a Gothic file)

---

## PHASE 5: Source Quality Upgrades

**Priority:** MEDIUM — replaces weak sources with stronger ones
**Estimated effort:** 2 sessions
**Adversarial audit:** YES

### 5.1 Replace avesta.org with Bartholomae

**Problem:** avesta.org is a personal website by a non-specialist, based on a 125-year-old dictionary.
**Solution:** After Phase 2 restores the avesta_org data, write a SECOND script that cross-references against Bartholomae's *Altiranisches Wörterbuch* entries available via:
- TITUS Frankfurt digitized texts
- Wiktionary entries that cite Bartholomae

**Script:** `scripts/crossref_avestan_bartholomae.py`
- For each avesta_org entry, search Wiktionary for a matching Avestan entry with Bartholomae citation
- Flag entries that appear in avesta_org but NOT in any academic source
- Add `bartholomae_verified: true/false` to audit trail

### 5.2 Cross-Reference Palaeolexicon Against eDiAna

**Problem:** Palaeolexicon (1,960 entries across 6 languages) is a volunteer project with no peer review.
**Solution:** For Anatolian languages where eDiAna overlaps (Lycian, Lydian, Carian, Luwian), verify Palaeolexicon entries against eDiAna.

**Script:** `scripts/crossref_palaeolexicon_ediana.py`
- Load both Palaeolexicon and eDiAna entries for each Anatolian language
- Flag Palaeolexicon entries with no eDiAna match
- Log verification status to audit trail

### 5.3 Upgrade ABVD Data via Lexibank 2

**Problem:** ABVD entries are ~50% orthographic (fake-IPA).
**Solution:** Where Lexibank 2 provides CLTS-standardized versions of ABVD languages, prefer those.

**Script:** `scripts/upgrade_abvd_lexibank.py`
- Download Lexibank 2 standardized forms for ABVD languages
- For each ABVD entry where Lexibank provides a CLTS-standardized IPA, update the IPA column
- Apply Never-Regress Rule: only update if Lexibank IPA differs from Word (i.e., is not identity)

---

## PHASE 6: New Language Ingestion — Tier 2

**Priority:** MEDIUM — important but less critical than Tier 1
**Estimated effort:** 3–4 sessions (parallelizable)
**Adversarial audit:** YES (full v2 pipeline per language)

### Languages

| Language | ISO | Family | Primary Source | Est. Entries |
|----------|-----|--------|---------------|-------------|
| Coptic | cop | Afroasiatic | Coptic Dictionary Online (coptic-dictionary.org) | 5,000–11,263 |
| Hattic | xht | Isolate | Palaeolexicon + Wiktionary | 100–300 |
| Pali | pli | Indo-European | PTS Dictionary (dsal.uchicago.edu), Digital Pali Dict | 5,000+ |
| Classical Armenian | xcl | Indo-European | Wiktionary Category:Old_Armenian_lemmas, Calfa.fr | 2,000+ |
| Old English | ang | Indo-European | Wiktionary Category:Old_English_lemmas | 5,000+ |
| Ge'ez | gez | Afroasiatic | Wiktionary + Leslau dictionary if accessible | 1,000+ |
| Syriac | syc | Afroasiatic | SEDRA (sedra.bethmardutho.org) + Wiktionary | 3,000+ |
| Aramaic (Imperial/Biblical) | arc | Afroasiatic | CAL (cal.huc.edu) + Wiktionary | 3,000+ |
| Biblical Hebrew | hbo | Afroasiatic | Wiktionary Category:Biblical_Hebrew_lemmas | 3,000+ |

### Per-Language Protocol

Same as Phase 3: create transliteration map → write extraction script → dry-run → adversarial audit → run live → update metadata.

Each language needs:
1. Transliteration map in `transliteration_maps.py` with cited reference
2. Extraction script in `scripts/`
3. Entry in `language_configs.py`
4. Proper noun scraping (gods, places, rulers) from the same sources

---

## PHASE 7: New Language Ingestion — Tier 3 & Proto-Languages

**Priority:** LOW-MEDIUM — expansion after core is solid
**Estimated effort:** 4+ sessions
**Adversarial audit:** YES

### Tier 3 Ancient Languages

| Language | ISO | Source | Est. Entries |
|----------|-----|--------|-------------|
| Middle Persian | pal | MPCD (mpcorpus.org) | 3,000+ |
| Sogdian | sog | Gharib Dictionary (Internet Archive) | 1,000+ |
| Old Japanese | ojp | ONCOJ (oncoj.ninjal.ac.jp) | 2,000+ |
| Gaulish | xtg | Lexicon Leponticum | 500+ |
| Oscan | osc | CEIPoM (Zenodo) | 500+ |
| Umbrian | xum | CEIPoM | 300+ |
| Venetic | xve | CEIPoM | 300+ |
| Classical Nahuatl | nci | Wiktionary + colonial dictionaries | 2,000+ |
| Eblaite | xeb | Oracc/DCCLT | 1,000+ |
| Old Irish | sga | eDIL (dil.ie) | 5,000+ |
| Palaic | plq | eDiAna | 50+ |

### Reconstructed Proto-Languages

| Language | ISO | Source | Est. Entries |
|----------|-----|--------|-------------|
| Proto-Austronesian | map | ACD (acd.clld.org) | 3,000–5,000 |
| Proto-Uralic | urj-pro | Wiktionary + Starostin | 500+ |
| Proto-Bantu | bnt-pro | BLR3 (africamuseum.be) | 5,000+ |
| Proto-Sino-Tibetan | sit-pro | STEDT (stedt.berkeley.edu) | 1,000+ |
| Proto-Celtic | cel-pro | Matasovic dictionary (Internet Archive) | 1,000+ |
| Proto-Germanic | gem-pro | Wiktionary Category:Proto-Germanic_lemmas | 2,000+ |

---

## PHASE 8: Ongoing Quality Assurance

### 8.1 Automated Validation Suite

Write `scripts/validate_all.py` — a comprehensive validation script that runs after ANY data change:

```python
def validate_all():
    for tsv in LEXICON_DIR.glob("*.tsv"):
        # 1. Header check
        # 2. No empty IPA
        # 3. No duplicate Words
        # 4. SCA matches ipa_to_sound_class(IPA) for all entries
        # 5. No '0' in SCA (flag but don't fail — may be legitimate unknowns)
        # 6. Source field is non-empty
        # 7. Entry count matches languages.tsv
        # 8. No known artifact patterns (inprogress, phoneticvalue, etc.)
```

### 8.2 Pre-Push Validation Gate

Add to the HuggingFace push workflow:
1. Run `validate_all.py` — must pass with 0 errors
2. Run `reprocess_ipa.py --dry-run` — verify no regressions
3. Verify all TSV files have correct header
4. Verify `languages.tsv` entry counts match actual

### 8.3 DATABASE_REFERENCE.md Auto-Update

After every phase completion, update DATABASE_REFERENCE.md with:
- New language entries in the Ancient Languages table
- Updated entry counts
- New source entries in the Source Registry
- New transliteration maps in the Map Registry

---

## Execution Order & Dependencies

```
PHASE 0 (Critical Bugs)
    ├── 0.1 SCA tokenizer fix
    ├── 0.2 Nasalized vowel fix
    ├── 0.3 Clean artifacts script
    ├── 0.4 Metadata update
    └── 0.5 Presentation fixes
         ↓
PHASE 1 (IPA Map Fixes)  ──→  reprocess_ipa.py  ──→  validate_all.py
         ↓
PHASE 2 (Data Restoration)
    ├── 2.1 Avestan re-scrape
    ├── 2.2 Sumerogram tagging
    └── 2.3 Contamination fix
         ↓
PHASE 3 (Tier 1 Languages)  ←──  can run 9 languages in PARALLEL
    ├── 3.1 Sumerian
    ├── 3.2 Akkadian
    ├── 3.3 Egyptian
    ├── 3.4 Sanskrit
    ├── 3.5 Ancient Greek
    ├── 3.6 Gothic
    ├── 3.7 Mycenaean Greek
    ├── 3.8 OCS
    └── 3.9 Old Norse
         ↓
PHASE 4 (Proper Nouns)  ←──  runs AFTER Phase 3 (needs Tier 1 TSVs to exist)
         ↓
PHASE 5 (Source Upgrades)  ←──  independent, can run in parallel with Phase 4
         ↓
PHASE 6 (Tier 2 Languages)
         ↓
PHASE 7 (Tier 3 + Proto-Languages)
         ↓
PHASE 8 (Ongoing QA)  ←──  continuous after all phases
```

---

## Success Criteria

| Metric | Current | Target |
|--------|---------|--------|
| Ancient/reconstructed languages | 23 | 42+ (Tier 1+2) |
| Total ancient language entries | 17,567 | 100,000+ |
| Languages with >80% non-identity IPA | 10 | 30+ |
| Languages with 0% empty Concept_IDs | ~5 | 25+ |
| SCA "0" rate across all ancient langs | ~5% | <1% |
| Proper noun coverage per language | Variable | All languages have theonym + toponym entries |
| Adversarial audit pass rate | — | 100% (all phases pass v2 audit) |
| HuggingFace accessibility | Private | Public |
| License | None | CC-BY-SA-4.0 (file present) |

---

## Appendix A: Script Naming Convention

```
scripts/scrape_{source}_{language}.py     # Single-source, single-language
scripts/scrape_{source}.py                # Single-source, multi-language
scripts/scrape_proper_nouns.py            # Unified proper noun scraper
scripts/clean_{issue}.py                  # Cleaning/fixing scripts
scripts/crossref_{source1}_{source2}.py   # Cross-reference validation
scripts/upgrade_{source}.py              # Source quality upgrades
scripts/validate_all.py                   # Comprehensive validation
scripts/tag_sumerograms.py               # Sumerogram identification
```

## Appendix B: Transliteration Map Naming Convention

```python
# In transliteration_maps.py:
SUMERIAN_MAP: Dict[str, str] = { ... }      # Jagersma (2010)
AKKADIAN_MAP: Dict[str, str] = { ... }      # Huehnergard (2011)
EGYPTIAN_MAP: Dict[str, str] = { ... }      # Allen (2014)
SANSKRIT_MAP: Dict[str, str] = { ... }      # Whitney (1896)
ANCIENT_GREEK_MAP: Dict[str, str] = { ... } # Allen (1987)
GOTHIC_MAP: Dict[str, str] = { ... }        # Wright (1910)
MYCENAEAN_MAP: Dict[str, str] = { ... }     # Ventris & Chadwick (1973)
OCS_MAP: Dict[str, str] = { ... }           # Lunt (2001)
OLD_NORSE_MAP: Dict[str, str] = { ... }     # Gordon (1957)
```

## Appendix C: Adversarial Auditor Dispatch Template

When deploying the adversarial pipeline for any phase, spawn two parallel agents:

**Agent A (Extraction):**
```
You are Team A (Extraction Agent). Your job is to write and run a Python script
that scrapes {SOURCE} for {LANGUAGE} data. Follow the Iron Law: all data must
come from HTTP requests. Use the standard script template from DATABASE_REFERENCE.md.
[... phase-specific instructions ...]
```

**Agent B (Adversarial Auditor v2):**
```
You are Team B (Critical Adversarial Auditor v2). You have VETO POWER.
After Agent A completes, perform the following DEEP checks:

1. 50-WORD CROSS-REFERENCE: Select 50 random entries from the output TSV.
   For each, construct the source URL and verify the word appears there.
   Use WebFetch to check each URL. Report matches and mismatches.

2. IPA SPOT-CHECK: For 20 random entries, manually apply the transliteration
   map character-by-character. Show your work. Report any mismatches.

3. SCA CONSISTENCY: For 20 random entries, verify ipa_to_sound_class(IPA) == SCA.

4. SOURCE PROVENANCE: For 10 random entries, provide the exact URL where
   each entry can be verified. Fetch each URL and confirm.

5. CONCEPT ID ACCURACY: For 20 entries with glosses, verify the gloss matches
   the source definition.

6. DEDUP: Count unique words. Report any duplicates.

7. ENTRY COUNT: Is the count non-round and plausible?

DO NOT perform surface-level checks (header format, encoding, file existence).
Only perform checks that touch REAL DATA and REAL SOURCES.

Produce a full v2 audit report. Verdict: PASS or FAIL with blocking issues.
```

---

*End of PRD*
