# PRD: Cognate Pairs Dataset v2 — Reconstruction from Verified Sources

**Status:** Draft
**Date:** 2026-03-13
**Priority:** P0 — Cognate pairs are used for validation testing; any hallucinated data invalidates model evaluation.

---

## 1. Problem Statement

Deep adversarial audit of the cognate pairs dataset (`data/training/cognate_pairs/`) has identified **6 critical bugs** in the generation pipeline (`scripts/assign_cognate_links.py` and `scripts/expand_cldf_full.py`). Multiple bugs fabricate or mislabel cognate relationships, making the dataset unsuitable for validation. This PRD specifies the complete reconstruction of the cognate pairs from verified scholarly sources.

### 1.1 Current State

| File | Rows | Size |
|------|------|------|
| `cognate_pairs_inherited.tsv` | 18,257,301 | 1.18 GB |
| `cognate_pairs_borrowing.tsv` | 116,757 | 8.0 MB |
| `cognate_pairs_similarity.tsv` | 170,064 | 16.2 MB |

Current 10-column schema:
```
Lang_A  Word_A  IPA_A  Lang_B  Word_B  IPA_B  Concept_ID  Relationship  Score  Source
```

### 1.2 Bugs Found

#### Bug 1: ABVD `cognates.csv` Never Read (CRITICAL)

**Location:** `expand_cldf_full.py` lines 200-266
**Impact:** All ABVD cognate data comes from `forms.csv` `Cognacy` column instead of the authoritative `cognates.csv` CognateTable (291,675 expert entries). The `Doubt` column (`true`/`false`) in `cognates.csv` is never consulted — disputed cognate assignments are treated as certain.

#### Bug 2: ABVD Multi-Set Cognacy Truncation

**Location:** `expand_cldf_full.py` line 257: `cog_num = cognacy.split(",")[0].strip()`
**Impact:** Forms belonging to multiple cognate sets (e.g., `"1,64"`) lose all secondary memberships. Estimated 37,587 cognate set memberships silently discarded.

#### Bug 3: WOLD Borrowing Pairs Fabricated (CRITICAL)

**Location:** `assign_cognate_links.py` lines 185-255 (`_extract_borrowings_from_forms`)
**Impact:** The script reads `forms.csv` `Borrowed` column and pairs any two forms from different languages that share a concept AND have SCA similarity >= 0.3, labeling them `"borrowing"`. This fabricates borrowing relationships. The authoritative `borrowings.csv` BorrowingTable (21,624 explicit donor-recipient events with `Target_Form_ID`, `Source_Form_ID`, `Source_languoid`, `Source_certain` fields) is **COMPLETELY UNUSED**.

Example: A Turkish word and an Arabic word for the same concept are paired as a "borrowing" even if no actual borrowing relationship exists between them.

#### Bug 4: Concept-Aligned Pairs Mislabeled as Inherited

**Location:** `assign_cognate_links.py` lines 154, 160
**Impact:** 488,900+ algorithmically-generated concept-aligned pairs (SCA similarity >= 0.5) are written to `cognate_pairs_inherited.tsv` with Relationship = `cognate_inherited`, making them indistinguishable from expert cognates to downstream consumers.

#### Bug 5: Sino-Tibetan Word Field is Concept String

**Location:** `expand_cldf_full.py` line 319
**Impact:** For all Sino-Tibetan entries, `Word == Concept_ID` (e.g., `"above"`, `"all"`) instead of the actual lexical form. The IPA column contains the real phonetic data, but Word is a meaningless concept gloss.

#### Bug 6: 50-Entry Hard Truncation (File-Sort Bias)

**Location:** `assign_cognate_links.py` line 142: `members_sample = members[:50]`
**Impact:** For large families (Austronesian: hundreds of languages), only the first 50 entries in alphabetical ISO order are used. Languages late in the alphabet are systematically excluded.

---

## 2. Design: Reconstructed Pipeline

### 2.1 Iron Law Compliance

> All data enters the dataset through code that reads from external scholarly sources. No hardcoded lexical or cognate data. No AI-generated entries.

Every extraction script:
- Reads from CLDF/TSV source files cloned to `sources/`
- Parses, transforms, writes to `staging/cognate_pairs/`
- Writes `Source_Record_ID` for full provenance traceability

### 2.2 Extended Schema (14 Columns)

```
Lang_A  Word_A  IPA_A  Lang_B  Word_B  IPA_B  Concept_ID  Relationship  Score  Source  Relation_Detail  Donor_Language  Confidence  Source_Record_ID
```

| Column | Type | Description |
|--------|------|-------------|
| `Lang_A` | str | ISO 639-3 code of language A |
| `Word_A` | str | Orthographic/transliteration form in language A |
| `IPA_A` | str | IPA transcription of Word_A |
| `Lang_B` | str | ISO 639-3 code of language B |
| `Word_B` | str | Orthographic/transliteration form in language B |
| `IPA_B` | str | IPA transcription of Word_B |
| `Concept_ID` | str | Concepticon gloss or equivalent concept identifier |
| `Relationship` | str | One of: `expert_cognate`, `borrowing`, `concept_aligned`, `similarity_only` |
| `Score` | float | SCA-based similarity score (0.0-1.0), rounded to 4 decimal places |
| `Source` | str | Source database identifier (e.g., `abvd`, `wold`, `iecor`, `sinotibetan`, `acd`) |
| `Relation_Detail` | str | Populated ONLY when source provides: `inherited`, `borrowed`, or `-` |
| `Donor_Language` | str | For borrowings only: source language from WOLD `Source_languoid`. `-` otherwise |
| `Confidence` | str | Source-provided certainty: `certain`/`doubtful` (ABVD), `1`-`5` (WOLD), `-` otherwise |
| `Source_Record_ID` | str | Traceable ID: ABVD cognateset ID, WOLD borrowing ID, IE-CoR cognateset ID, etc. |

**Design decisions:**
- No mother-daughter / sister-sister typing — NO source provides this at the pair level.
- `Relation_Detail`, `Donor_Language`, `Confidence` are `-` when source doesn't provide them. We never fabricate metadata.
- `Relationship` is script-assigned based on extraction method. `Relation_Detail` is source-provided.

### 2.3 Source Inventory

| Source | Repository | Cognate Data | Status |
|--------|------------|-------------|--------|
| ABVD | `sources/abvd/` | `cognates.csv` CognateTable (291,675 entries, 19,356 sets) | Cloned, **unused by current code** |
| WOLD | `sources/wold/` | `borrowings.csv` BorrowingTable (21,624 events) | Cloned, **unused by current code** |
| Sino-Tibetan | `sources/sinotibetan/` | `sinotibetan_dump.tsv` (6,159 entries with COGID) | Cloned, partially used |
| IE-CoR | `sources/iecor/` | `cognates.csv` CognateTable (Indo-European cognates) | **NOT CLONED — must clone** |
| ACD | `sources/acd/` | Cached Austronesian data | Partially cached |
| Internal lexicons | `data/training/lexicons/` | SCA similarity scoring | N/A |

### 2.4 Output Files

Same 3-file split, but with corrected labels:
- `cognate_pairs_inherited.tsv` — Expert cognates ONLY (ABVD CognateTable, IE-CoR CognateTable, Sino-Tibetan COGID, ACD)
- `cognate_pairs_borrowing.tsv` — Verified borrowings ONLY (WOLD BorrowingTable with explicit donor-recipient pairs)
- `cognate_pairs_similarity.tsv` — Concept-aligned pairs (algorithmically generated, clearly labeled)

---

## 3. Implementation Plan

### Phase 1: Source Preparation

**Step 1.1** — Clone IE-CoR:
```bash
cd sources/
git clone https://github.com/lexibank/iecor.git
```

**Step 1.2** — Verify all CLDF source files exist:
- `sources/abvd/cldf/cognates.csv` (291,675 rows)
- `sources/abvd/cldf/forms.csv`
- `sources/abvd/cldf/languages.csv`
- `sources/wold/cldf/borrowings.csv` (21,624 rows)
- `sources/wold/cldf/forms.csv`
- `sources/wold/cldf/languages.csv`
- `sources/sinotibetan/sinotibetan_dump.tsv` (6,159 rows)
- `sources/iecor/cldf/cognates.csv` (NEW)
- `sources/iecor/cldf/forms.csv` (NEW)

### Phase 2: Extraction Scripts

Each script follows the same pattern:
1. Read source CLDF files
2. Parse and validate
3. Generate pairwise cognate entries
4. Write to `staging/cognate_pairs/{source}_pairs.tsv` with 14-column schema
5. Print statistics and provenance summary

#### Script 1: `scripts/extract_abvd_cognates_v2.py`

**Source:** `sources/abvd/cldf/cognates.csv` + `forms.csv` + `languages.csv`

**Key fixes:**
- Read `cognates.csv` directly (has `Form_ID`, `Cognateset_ID`, `Doubt`)
- Join to `forms.csv` on `Form_ID` for orthographic form and IPA
- Handle multi-set membership: one form can appear in multiple cognate sets
- Use `Doubt` column for Confidence: `Doubt=false` → `certain`, `Doubt=true` → `doubtful`
- `Source_Record_ID` = `Cognateset_ID` from `cognates.csv`
- Generate cross-language pairs within each cognate set
- SCA similarity score computed on the fly

#### Script 2: `scripts/extract_wold_borrowings_v2.py`

**Source:** `sources/wold/cldf/borrowings.csv` + `forms.csv` + `languages.csv`

**Key fixes:**
- Read `borrowings.csv` BorrowingTable for actual donor-recipient pairs
- Join `Target_Form_ID` and `Source_Form_ID` to `forms.csv` for word/IPA
- Extract `Source_languoid` as `Donor_Language`
- Extract `Source_certain` for Confidence
- `Source_Record_ID` = borrowing `ID` from `borrowings.csv`
- Each row produces exactly ONE pair (not fabricated from concept co-occurrence)

#### Script 3: `scripts/extract_sinotibetan_cognates_v2.py`

**Source:** `sources/sinotibetan/sinotibetan_dump.tsv`

**Key fixes:**
- Filter out rows where `BORROWING` column is non-empty
- Use `IPA` column for IPA_A/IPA_B (correct)
- Use `CONCEPT` column for Concept_ID
- Use actual IPA form as Word (not concept gloss) — or use `-` and note that Word is not available
- `Source_Record_ID` = `st_{COGID}`

#### Script 4: `scripts/extract_iecor_cognates.py` (NEW)

**Source:** `sources/iecor/cldf/cognates.csv` + `forms.csv`

**Implementation:**
- Read IE-CoR CognateTable (standard CLDF format)
- Join `Form_ID` to `forms.csv` for word/IPA/language
- Generate cross-language pairs within each cognate set
- `Source_Record_ID` = IE-CoR `Cognateset_ID`

#### Script 5: `scripts/extract_acd_cognates.py` (NEW)

**Source:** `sources/acd/` cached data

**Implementation:**
- Parse ACD (Austronesian Comparative Dictionary) cached HTML/data
- Extract cognate sets with etymon-level grouping
- If ACD data is insufficiently structured, mark as P2 and skip

#### Script 6: `scripts/rebuild_concept_aligned_pairs.py`

**Source:** `data/training/lexicons/*.tsv` + `data/training/family_map.json`

**Key fixes:**
- Label as `concept_aligned` (NOT `cognate_inherited`)
- Random sampling instead of file-sort truncation for groups > 50
- Score threshold: >= 0.5 → `concept_aligned`, 0.3-0.49 → `similarity_only`
- `Source_Record_ID` = `-` (no source record, algorithmically generated)
- `Confidence` = `-` (not from an expert source)

#### Script 7: `scripts/merge_cognate_pairs.py`

**Merges all staging files into final output:**
- Deduplicates: if pair (A,B,concept) appears in both expert and concept-aligned, keep expert
- Priority: expert > borrowing > concept_aligned > similarity
- Writes 3 output files with 14-column schema
- Prints final statistics

### Phase 3: Adversarial Audit Protocol

For EACH extraction script, deploy a two-team audit:

**Team A (Extraction):**
- Runs the script
- Produces staging output
- Reports entry counts and statistics

**Team B (Adversarial Auditor):**
- Samples 20 random output rows
- For each row, traces Source_Record_ID back to source CSV
- Verifies Form_ID, Cognateset_ID, Language_ID all exist in source
- Verifies IPA matches source data
- Checks for entries in output that have no source backing
- Checks for duplicate pairs, empty fields, malformed data
- **VETO** power: if any entry cannot be traced, the entire script output is rejected

**End-to-end cross-validation (after merge):**
- Sample 50 random pairs from each output file (150 total)
- Full provenance trace: output → staging → source CSV → published database
- Verify no concept-aligned pairs appear in `cognate_pairs_inherited.tsv`
- Run count statistics and compare to source totals

### Phase 4: Deployment

1. Commit PRD + all scripts + staging data
2. Run full pipeline: extract → audit → merge → validate
3. Update `docs/DATABASE_REFERENCE.md` with new cognate pair statistics
4. Push to GitHub
5. Push to HuggingFace via `scripts/push_to_hf.py`

---

## 4. Acceptance Criteria

| Criterion | Metric |
|-----------|--------|
| Zero fabricated pairs | Every pair traceable to source record via Source_Record_ID |
| Zero mislabeled relationships | Expert cognates in inherited.tsv ONLY; concept-aligned in similarity.tsv |
| WOLD borrowings use BorrowingTable | All borrowing pairs from borrowings.csv, not forms.csv |
| ABVD doubt flags preserved | Confidence column reflects Doubt field |
| Multi-set membership preserved | Forms in multiple cognate sets generate pairs for ALL sets |
| Sino-Tibetan borrowings excluded | Zero entries with BORROWING flag in inherited output |
| IE-CoR coverage | Indo-European expert cognates present in inherited output |
| Adversarial audit passes | 0/150 sample pairs fail provenance trace |

---

## 5. Non-Goals

- **Mother-daughter / sister-sister relationship typing**: No source provides this at the pair level. We encode only what sources give us.
- **Etymological chain reconstruction**: No source provides intermediary language chains. Out of scope.
- **Cross-source conflation**: If ABVD and IE-CoR both provide a cognate set for the same forms, both are kept (deduplicated by pair identity, priority to more specific source).
- **Replacing existing lexicon files**: This PRD only covers cognate pairs. Lexicon TSVs are not modified.

---

## 6. Risks

| Risk | Mitigation |
|------|-----------|
| IE-CoR schema differs from ABVD | Read actual CLDF headers; adapt column names |
| ACD data too unstructured | Mark as P2; skip if insufficient |
| WOLD borrowings.csv has broken Form_IDs | Join validation: log and skip unresolvable IDs |
| Sino-Tibetan Word column unusable | Use IPA as the primary identifier; Word = `-` |
| Large pair counts overwhelm merge | Stream-process with generators; don't hold all pairs in memory |
