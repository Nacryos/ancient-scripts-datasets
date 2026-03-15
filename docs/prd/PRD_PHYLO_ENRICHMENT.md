# PRD: Phylogenetic Relationship Metadata for Cognate Pairs

**Status**: In Progress
**Date**: 2026-03-14
**Author**: Alvin (assisted by Claude)

---

## Problem

The cognate pair dataset (23.4M pairs) currently has NO metadata about the phylogenetic relationship between language pairs — no distinction between mother-daughter (Latin→Spanish), close sisters (Spanish~Italian), or distant sisters (Spanish~Hindi). Every pair should indicate its degree of cognacy.

## Solution

A post-processing enrichment pipeline that cross-references cognate pairs against an authoritative phylogenetic tree (Glottolog CLDF v5.x) to classify each unique language pair.

---

## Data Source

**Primary**: Glottolog CLDF v5.x (Hammarström, Forkel, Haspelmath & Bank)
- Repository: `https://github.com/glottolog/glottolog-cldf`
- Archive: Zenodo DOI: 10.5281/zenodo.15640174
- License: CC BY 4.0
- Key files: `cldf/languages.csv` (27,177 languoids with ISO mapping), `cldf/classification.nex` (NEXUS with Newick trees per family)
- 8,184 languoids have ISO 639-3 codes
- Trees are topological (no branch lengths)

**Download method**: `scripts/ingest_glottolog.py` — downloads the CLDF CSV and NEXUS files from GitHub raw content into `data/training/raw/glottolog_cldf/`. Follows the same pattern as `ingest_acd.py`.

**Supplementary** (Phase 2, not in scope): Phlorest Bayesian phylogenies for calibrated branch lengths (years of divergence).

---

## Output

**File**: `data/training/metadata/phylo_pairs.tsv`

A lookup table keyed on canonically-ordered `(Lang_A, Lang_B)` pairs. NOT inline columns in the 23M-row cognate pair files — that would add massive redundancy since the same language pair always has the same phylo classification.

### Schema (9 columns)

| Column | Type | Description |
|--------|------|-------------|
| `Lang_A` | str | ISO 639-3 code (alphabetically first) |
| `Lang_B` | str | ISO 639-3 code (alphabetically second) |
| `Phylo_Relation` | enum | `near_ancestral`, `close_sister`, `distant_sister`, `cross_family`, `unclassified` |
| `Tree_Distance` | int | Edge count through MRCA (0 = same leaf group, 99 = unclassified/cross-family) |
| `MRCA_Clade` | str | Glottocode or name of MRCA node (e.g., `roma1334`, `germ1287`) |
| `MRCA_Depth` | int | Depth of MRCA in tree (0 = root, higher = more specific) |
| `Ancestor_Lang` | str | For `near_ancestral`: ISO of the ancestor. `-` otherwise |
| `Family_A` | str | Top-level Glottolog family of Lang_A |
| `Family_B` | str | Top-level Glottolog family of Lang_B |

### Classification Taxonomy

| Relation | Definition | Example |
|----------|-----------|---------|
| `near_ancestral` | One language is an attested ancestor of the other's clade (from curated NEAR_ANCESTOR_MAP) | Latin↔Spanish, Old English↔English, Sanskrit↔Hindi |
| `close_sister` | MRCA depth ≥ 3 (share a specific sub-branch) | Spanish↔Italian (both under Romance), Swedish↔Danish (both under North Germanic) |
| `distant_sister` | MRCA depth = 1 or 2 (share family or major branch only) | English↔Hindi (both IE, but Germanic vs Indo-Iranian) |
| `cross_family` | Different top-level families | English↔Japanese |
| `unclassified` | One or both languages not in Glottolog tree | Undeciphered/isolate languages without Glottocode |

**Depth thresholds**: The boundary between close and distant is at MRCA depth 3+. This means:
- Depth 0: root (should not occur for same-family pairs)
- Depth 1: top-level family (e.g., `indo1319` = Indo-European) → `distant_sister`
- Depth 2: major branch (e.g., `germ1287` = Germanic) → `distant_sister`
- Depth 3+: sub-branch (e.g., `west2793` = West Germanic) → `close_sister`

### Near-Ancestral Detection

A curated `NEAR_ANCESTOR_MAP` lists ~25 attested ancient/medieval languages and the Glottolog clades they are historically ancestral to:

```python
NEAR_ANCESTOR_MAP = {
    "lat": ["roma1334"],           # Latin → Romance
    "grc": ["mode1248", "medi1251"],  # Ancient Greek → Modern Greek clades
    "san": ["indo1321"],           # Sanskrit → Indic
    "ang": ["angl1265"],           # Old English → Anglian/English
    "enm": ["angl1265"],           # Middle English → English
    "fro": ["oilf1242"],           # Old French → Oïl French
    "osp": ["cast1243"],           # Old Spanish → Castilian
    "non": ["nort3160"],           # Old Norse → North Germanic modern
    "goh": ["high1289"],           # Old High German → High German
    "dum": ["mode1258"],           # Middle Dutch → Modern Dutch
    "sga": ["goid1240"],           # Old Irish → Goidelic modern
    "mga": ["goid1240"],           # Middle Irish → Goidelic modern
    "wlm": ["bryt1239"],           # Middle Welsh → Brythonic modern
    "chu": ["sout3147"],           # Old Church Slavonic → South Slavic
    "orv": ["east1426"],           # Old East Slavic → East Slavic modern
    "och": ["sini1245"],           # Old Chinese → Sinitic modern
    "ota": ["oghu1243"],           # Ottoman Turkish → Oghuz modern
    "okm": ["kore1284"],           # Middle Korean → Korean modern
}
```

Logic:
1. If either language is in `NEAR_ANCESTOR_MAP`
2. AND the other language's ancestry path in Glottolog passes through one of the listed descendant clades
3. AND the other language is NOT itself an ancient/medieval language (to avoid classifying Latin↔Oscan as near_ancestral)
4. THEN classify as `near_ancestral` with `Ancestor_Lang` = the ancient language's ISO

**Gothic (`got`) edge case**: Gothic is ancient but has NO modern descendants (East Germanic is extinct). Gothic↔Swedish should be `distant_sister`, not `near_ancestral`. The map correctly handles this by only listing clades with living descendants.

---

## Scripts

### Script 1: `scripts/ingest_glottolog.py`
Downloads Glottolog CLDF data from GitHub.

### Script 2: `scripts/build_glottolog_tree.py`
Parses the Glottolog NEXUS file into a usable Python tree structure.

### Script 3: `scripts/build_phylo_pairs.py`
The main enrichment script that generates the lookup table.

### Script 4: `scripts/validate_phylo_pairs.py`
Validation script with known-answer checks.

---

## Execution Order

1. Write PRD → push to `docs/prd/PRD_PHYLO_ENRICHMENT.md`
2. Write `scripts/ingest_glottolog.py` → download Glottolog CLDF
3. Write `scripts/build_glottolog_tree.py` → parse NEXUS, build tree index
4. Adversarial audit: Verify tree covers ≥95% of ISO codes in cognate pairs
5. Write `scripts/build_phylo_pairs.py` → generate lookup table
6. Adversarial audit: Verify 20 random pairs trace back to Glottolog tree
7. Write `scripts/validate_phylo_pairs.py` → automated known-answer tests
8. Update `docs/DATABASE_REFERENCE.md` with phylo_pairs documentation
9. Commit + push to GitHub + HuggingFace

---

## Critical Design Decisions

### Why a separate lookup table (not inline columns)?
- The 22.9M inherited pairs reference ~385K unique language pairs. Adding 7 columns to 22.9M rows = 160M+ redundant cells.
- Downstream consumers join at query time: `pair_key = (min(a,b), max(a,b))`; lookup is O(1) with a dict.
- The phylo classification is orthogonal to the cognate data — it can be updated independently when Glottolog releases new versions.

### Why Glottolog (not the existing phylo_tree.json)?
- `phylo_tree.json` has 755 Austronesian languages in ONE flat list — zero sub-classification for 90% of the dataset
- Glottolog has deep sub-classification for ALL families including Austronesian
- Glottolog is the authoritative academic reference (Hammarström et al.)

### Why curated near-ancestor map (not purely algorithmic)?
- Glottolog classifies ALL attested languages as leaf nodes (siblings), never as parent nodes
- Even Latin is a sibling of Romance under "Imperial Latin" in Glottolog — not a parent
- Algorithmic detection from tree topology alone would classify ALL pairs as sister-sister
- The curated map (~25 entries) is linguistically defensible and small enough to verify exhaustively

### Why not branch-length / time-calibrated distances?
- Phase 1 focuses on topological classification
- Branch-length data requires Phlorest Bayesian phylogenies (separate download per family, inconsistent coverage)
- Branch lengths can be added in Phase 2 as a `Divergence_Years` column

---

## Honest Limitations

1. **"Near-ancestral" is approximate**: Latin is not literally the ancestor of French — Vulgar Latin (unattested) is. We use "near-ancestral" to mean "the attested language is historically ancestral to the clade containing the other language."
2. **Topological distance ≠ temporal distance**: Two languages with tree_distance=4 in Austronesian may have diverged 1,000 years ago, while two with tree_distance=4 in Indo-European may have diverged 5,000 years ago.
3. **Glottolog is a single hypothesis**: Disputed affiliations are not represented. The tree reflects Glottolog's conservative consensus classification.
4. **The similarity file (465K pairs) may contain cross-family pairs** that are correctly labeled `cross_family` — these are algorithmic similarity matches, not genetic relationships.
5. **Proto-language codes (ine-pro, gem-pro, etc.) are NOT in Glottolog** — any cognate pairs involving proto-languages will be `unclassified`. (Currently zero such pairs exist in the dataset.)

---

## Verification

1. `python scripts/validate_phylo_pairs.py` — all known-answer checks PASS
2. Coverage: ≥95% of ISO codes in cognate pairs have a Glottolog classification
3. Distribution: `close_sister` should be majority (most pairs are intra-family from ABVD/ACD)
4. Adversarial audit: 20 random pairs traced back to Glottolog NEXUS tree
5. No `unclassified` for any language that has a Glottocode in `languages.tsv`
