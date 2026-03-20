# Phonetic Prior Validation Dataset

Unsegmented inscription fragments for validating the **Phonetic Prior algorithm**
(Luo et al. 2021, "Decipherment of Lost Ancient Scripts as Combinatorial Optimisation").

Designed for use with:
- `phonetic-prior-v2/` — improved implementation
- `repro_decipher_phonetic_prior/` — faithful reproduction

## Purpose

This dataset tests the algorithm's core capabilities:
1. **Segmentation**: Can it find word boundaries in unsegmented text?
2. **Cognate detection**: Can it identify cognate pairs from fragmented text?
3. **Language discrimination**: Does it rank genetically closer languages higher?

## Format

Each language directory contains:

```
{iso}/
  lost.txt              # Unsegmented IPA inscription fragments (one per line)
  known_{other}.txt     # Known IPA vocabulary for each candidate language
  ground_truth_{other}.tsv  # Cognate pairs (lost/known/concept_id)
```

Plus:
- `validation_orderings.yaml` — expected closeness orderings
- `metadata.json` — statistics and provenance

### lost.txt
One unsegmented IPA string per line. No word boundaries. Fragments are 1-20
syllables (median ~7), matching Linear A inscription length distribution.
~15% of fragments are cut mid-word to simulate inscription damage.

Example (Oscan):
```
vibissmintiːsvibissmintiːs
ḍịsfrvernahelvis
```

### known_{lang}.txt
Known-language IPA vocabulary, one word per line. Sampled from
`data/training/lexicons/{lang}.tsv` (up to 2000 items).

### ground_truth_{lang}.tsv
Tab-separated: `lost`, `known`, `concept_id`. These are real cognate pairs
from `cognate_pairs_inherited.tsv`.

## Languages

| ISO | Lost Language | Fragments | Avg Syllables | Family |
|-----|-------------|-----------|---------------|--------|
| grc | Ancient Greek | 9,217 | 8.0 | IE/Hellenic |
| lat | Latin | 65,017 | 8.1 | IE/Italic |
| san | Sanskrit | 65,614 | 6.9 | IE/Indo-Iranian |
| ang | Old English | 44 | 5.8 | IE/Germanic |
| osc | Oscan | 1,538 | 5.8 | IE/Italic |
| xum | Umbrian | 1,761 | 6.9 | IE/Italic |

## Validation Orderings

The `validation_orderings.yaml` file defines expected phylogenetic closeness
assertions. Each asserts that when the anchor language is treated as "lost",
the closer candidate should score higher than the farther one.

Key test cases:
- Oscan-as-lost: Latin should rank higher than Sanskrit or Greek
- Umbrian-as-lost: Oscan/Latin should rank higher than Sanskrit or Old English
- Latin-as-lost: Oscan should rank higher than Sanskrit

## Usage with Phonetic Prior

### phonetic-prior-v2

```python
from phonetic_prior_v2.data.adapters import TSVAdapter

# Load as lost text
with open("data/validation_phonetic_prior/osc/lost.txt") as f:
    inscriptions = [line.strip() for line in f]

# Load known vocabulary
with open("data/validation_phonetic_prior/osc/known_lat.txt") as f:
    known_vocab = [line.strip() for line in f]

# Train model on inscriptions + known_vocab
# Score against ground_truth_lat.tsv for P@k metrics
```

### repro_decipher_phonetic_prior

Register as a custom corpus in `datasets/registry.py` or feed directly
to `repro/eval/common.py::train_model()`.

## Build

```bash
# Step 1: Build segmented inscriptions (if not done)
python scripts/ingest_inscriptions.py
python scripts/build_inscriptions.py

# Step 2: Build unsegmented validation set
python scripts/build_validation_inscriptions.py
```

## Sources

All underlying data is CC BY-SA 4.0 compatible. See `data/inscriptions/README.md`
for full source listing.
