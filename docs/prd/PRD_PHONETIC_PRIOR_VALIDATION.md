# PRD: Phonetic Prior Validation & Linear A Cognate Extraction

## Status: INCOMPLETE — Requires Clean Rerun

**Date**: 2026-03-22
**Author**: Claude (from failed attempt analysis)
**Budget spent**: $9.50 of $30 allocated ($20.50 remaining)
**Vast.ai credit remaining**: ~$70

---

## 1. Objective

Run the Phonetic Prior algorithm (Luo et al. 2021, "Decipherment of Lost Ancient Scripts") on:
- **Track A (Validation)**: 25 language pairs from 5 known ancient languages (grc, lat, san, osc, xum) to validate the algorithm correctly clusters related languages
- **Track B (Linear A)**: Linear A unsegmented inscriptions against 18 ancient candidate languages to extract cognate word lists

**Primary output**: Full cognate word lists per language pair (not just closeness scores)
**Secondary output**: Closeness rankings validating phylogenetic expectations

---

## 2. Post-Mortem: What Went Wrong

### 2.1 Root Causes (ordered by severity)

**RC1: Multiple fleet redeployments without collecting results first**
- Destroyed 15 machines with 23 completed results still on them
- Lost all first-batch results because `rm -rf outputs` in launch command wiped data on restart
- **Fix**: ALWAYS scp results before destroying. Use timestamped output directories, never `rm -rf`.

**RC2: Over-optimization followed by under-optimization**
- First run: 50 vocab, 100 steps → garbage (random noise)
- Overcorrected to: 2000 vocab, all inscriptions → OOM and 100s/step
- Then 700 vocab → still OOM
- Then 500 vocab → workable but slow (4+ hours per experiment)
- Then 300 vocab, 50 inscriptions → too fast, lost signal
- **Fix**: Use paper's exact parameters (300-500 vocab, 1000 steps) from the start. Benchmark ONE experiment end-to-end before scaling to fleet.

**RC3: Code deployed without testing the full pipeline locally first**
- Eval code had character filter bug (n_eval=0) — not caught until fleet was running
- Span extraction code committed but never deployed to running fleet
- Unsegmented Linear A corpus committed but never deployed
- `result.json` deliberately excluded cognates (code design choice confused for bug)
- **Fix**: Run ONE complete experiment locally on Windows before deploying to cloud.

**RC4: Silent exception swallowing**
- `except: continue` in eval loop hid all scoring failures
- Made it impossible to diagnose why some pairs produced 0 cognates
- **Fix**: Log exceptions, never use bare `except`.

**RC5: Duplicate process launches**
- Fleet orchestrator launched new processes without killing old ones
- Multiple processes on same machine competing for CPU and overwriting each other's outputs
- **Fix**: Always kill existing processes before launching new ones. Use PID files.

### 2.2 What Actually Worked

1. **Latin validation ranking is correct** (with full 65K inscriptions, 500 vocab, 1000 steps):
   - Umbrian #1 (-70.7), Oscan #2 (-73.4), Sanskrit #3 (-74.3), Old English #4 (-74.8)
   - This proves the algorithm CAN discriminate language families with proper parameters

2. **Cognate lists ARE produced** when the code runs to completion:
   - `cognate_list.tsv` files have real span-to-word mappings
   - The issue was `result.json` intentionally excludes cognates (saved in TSV separately)
   - Example: Umbrian `trif` → Sanskrit `vish` (score -9.35)

3. **The Phonetic Prior v2 code from Project-Phaistos GitHub works correctly**
   - Model trains, converges, produces character mappings
   - The code at `Project-Phaistos/phonetic-prior-v2` is identical to `Nacryos/Project-Phaistos/phonetic-prior-v2/`

---

## 3. Datasets Built (Complete, Ready to Use)

All pushed to `Nacryos/ancient-scripts-datasets` (GitHub) and `Nacryos/ancient-scripts-datasets` (HuggingFace).

### 3.1 Segmented Inscription Dataset
- **Location**: `data/inscriptions/`
- **Size**: 41K entries across 6 languages (grc, lat, san, ang, osc, xum)
- **Format**: TSV with Inscription_ID, Text, IPA, SCA, Source, Date_Approx, Genre, IPA_Source
- **Sources**: UD treebanks (CC BY-SA 4.0) + CEIPoM v1.3 (CC BY-SA 4.0)

### 3.2 Unsegmented Validation Fragments
- **Location**: `data/validation_phonetic_prior/`
- **Size**: 143K fragments across 6 languages
- **Format**: `{iso}/lost.txt` (unsegmented IPA), `{iso}/known_{other}.txt` (vocab), `{iso}/ground_truth_{other}.tsv` (cognate pairs)
- **Properties**: No word boundaries, 1-14 syllables per fragment, 15% cut mid-word

### 3.3 Unsegmented Linear A Corpus
- **Location**: `data/linear_a/linear_a_corpus_unsegmented.txt`
- **Size**: 48 inscriptions, fully unsegmented (all spaces removed)
- **Original**: `data/linear_a/linear_a_corpus.txt` (with scholarly word boundaries)

### 3.4 Transliteration Maps Added
- Ancient Greek (grc): Greek Unicode → IPA (Allen's Vox Graeca)
- Latin (lat): Latin orthography → IPA (Allen's Vox Latina)
- Sanskrit (san): IAST → IPA (Whitney's Grammar)
- **Location**: `scripts/transliteration_maps.py` (added to existing file)

---

## 4. Architecture: How the Phonetic Prior Works

### 4.1 Algorithm Overview (Luo et al. 2021)

The model takes **unsegmented text** from a "lost" language and a **vocabulary** from a "known" language, and jointly:
1. Learns a character mapping (lost → known) via softmax-temperature embeddings
2. Finds word boundaries via word_boundary_dp (Viterbi-style DP)
3. Scores span-to-word alignments via edit_distance_dp (differentiable monotonic alignment)
4. Optimizes: quality - λ_cov × coverage_penalty - λ_loss × sound_loss_regularizer

### 4.2 Code Location
- **phonetic-prior-v2**: `Project-Phaistos/phonetic-prior-v2` (GitHub org) = `Nacryos/Project-Phaistos/phonetic-prior-v2/` (monorepo)
- **repro**: `Project-Phaistos/repro-decipher-phonetic-prior` (GitHub org) = `Nacryos/Project-Phaistos/repro_decipher_phonetic_prior/` (monorepo)
- Both contain identical code (verified by SHA hash)

### 4.3 Key Files
```
phonetic-prior-v2/src/phonetic_prior_v2/
  model/cognate_validator.py   — CognateValidator (main model class)
  model/edit_distance.py       — Differentiable DP alignment
  model/segmentation.py        — Word boundary DP (Algorithm 1)
  model/objectives.py          — Loss terms (coverage + sound-loss)
  model/config.py              — PhoneticPriorConfig dataclass
  model/embeddings.py          — GroupedIPAProjector
  features/ipapy_backend.py    — 61-dim IPA feature extraction
  training/trainer.py          — Training loop with annealing
  eval/ranking.py              — Vocabulary ranking
  eval/metrics.py              — MRR, P@k computation
```

### 4.4 Paper-Default Hyperparameters
```yaml
temperature: 0.2
alpha: 3.5 (annealed from 10.0)
lambda_cov: 10.0
lambda_loss: 100.0
r_cov: 0.5
min_span: 3
max_span: 10
embedding_dim: 700  # 7 groups × 100
lr: 0.2  # SGD
p_o: 0.2
dropout: 0.5
num_steps: 1000  # 3000 for Ugaritic
batch_size: 8
```

### 4.5 Performance Characteristics
- **CPU only** — DP alignment is the bottleneck, not matrix ops
- **Per-step time** scales linearly with vocab_size and quadratically with inscription_length
- **Benchmarks** (single worker, 16 cores):
  - 50 vocab, 20 inscriptions: 0.58s/step → 10 min per experiment
  - 300 vocab, 50 inscriptions: 2.2s/step → 37 min per experiment
  - 500 vocab, 50 inscriptions: 3.3s/step → 55 min per experiment
  - 500 vocab, 65K inscriptions (batch 8): ~60s/step → 17 hours per experiment
- **Memory**: ~1-3 GB per worker at 300-500 vocab (with gc.collect every 50 steps)

---

## 5. Execution Plan for Clean Rerun

### 5.1 Parameters

```yaml
# Matching paper's Gothic experiment (Table 2)
num_steps: 1000
vocab_size: 300-500  # use full lexicon if < 500, cap at 500 if larger
batch_size: 8
inscription_len_cap: 20  # chars
# Use ALL inscriptions as the sampling pool (batch_size handles the per-step cost)
# Use ALL ground truth words for eval
# Use span extraction from unsegmented inscriptions for cognate lists
```

### 5.2 Experiments

**Track A — Validation (25 pairs)**:
5 lost languages × 5 known candidates (excluding self):
- grc as lost → lat, san, ang, osc, xum
- lat as lost → grc, san, ang, osc, xum
- san as lost → grc, lat, ang, osc, xum
- osc as lost → grc, lat, san, ang, xum
- xum as lost → grc, lat, san, ang, osc

**Track B — Linear A (18 pairs)**:
Linear A (unsegmented, `linear_a_corpus_unsegmented.txt`) as lost → 18 ancient languages:
hit, uga, phn, xur, elx, ave, peo, xld, xlc, xcr, xpg, xle, xrr, cms, ine-pro, sem-pro, ccs-pro, dra-pro

### 5.3 Outputs Per Experiment

1. `result.json` — training metrics (final_quality, final_obj, n_train, n_vocab, n_eval, P@k, MRR, runtime)
2. `cognate_list.tsv` — **FULL cognate list**: every span from inscriptions matched against every vocab word, sorted by score. Columns: lost_span, top1_known, score, top10_matches
3. `run.log` — training step logs for convergence monitoring

### 5.4 Deployment Strategy

**Option A: Single beefy machine (simplest, recommended)**
- Rent 1 machine with 32+ cores, 64+ GB RAM
- Run experiments sequentially with 1 worker (no contention)
- 300 vocab: ~37 min per experiment × 43 = ~26 hours
- 500 vocab: ~55 min per experiment × 43 = ~39 hours
- Cost: ~$2-3
- **Pro**: No coordination complexity, no lost results
- **Con**: Slow

**Option B: Small fleet (4-6 machines)**
- Each machine runs 7-10 experiments sequentially
- Total: ~6-10 hours
- Cost: ~$2-5
- **Critical**: Collect results BEFORE destroying any machine

**Option C: Large fleet (20+ machines, 1 experiment each)**
- Each machine runs 1-2 experiments
- Total: ~1-2 hours
- Cost: ~$3-5
- **Critical**: Same collection protocol

### 5.5 Critical Fixes Required in Runner Script

1. **Remove `rm -rf outputs`** from launch command — use `mkdir -p` instead
2. **Remove `summary.pop("cognates")`** — keep cognates in result.json OR add n_cognates field
3. **Replace bare `except: continue`** with `except Exception as e: print(f"EVAL ERROR: {e}")`
4. **Add gc.collect() and del after EVERY step** (not just every 50)
5. **Use `linear_a_corpus_unsegmented.txt`** for Linear A (verified, already committed)
6. **Add result collection script** that scps ALL outputs before destroying any machine
7. **Test locally first**: Run 1 experiment on Windows to verify full pipeline before deploying

---

## 6. Links & Credentials

### GitHub
- **Data repo**: https://github.com/Nacryos/ancient-scripts-datasets (master branch)
- **Project-Phaistos org**: https://github.com/Project-Phaistos
  - phonetic-prior-v2: https://github.com/Project-Phaistos/phonetic-prior-v2
  - repro: https://github.com/Project-Phaistos/repro-decipher-phonetic-prior
  - datasets-NEW: https://github.com/Project-Phaistos/ancient-scripts-datasets-NEW
- **Monorepo** (all code): https://github.com/Nacryos/Project-Phaistos

### HuggingFace
- **Dataset**: https://huggingface.co/datasets/Nacryos/ancient-scripts-datasets

### Vast.ai
- **API key**: `86ba9778cdf1e93d46215aefb5519d0c16c34e948f0c30ebe28150fc67c60361`
- **Credit remaining**: ~$70
- **Budget for this task**: $30 total ($20.50 remaining)
- **Recommended instance**: 32+ cores, 64+ GB RAM, $0.03-0.06/hr interruptible

### Local Paths (Windows)
- Data repo: `C:\Users\alvin\ancient-scripts-datasets\`
- HF repo: `C:\Users\alvin\hf-ancient-scripts\`
- Project-Phaistos (sparse): `C:\Users\alvin\AppData\Local\Temp\ProjectPhaistos\`

### Key Scripts
- `scripts/run_proper_validation.py` — Main runner (NEEDS FIXES listed in §5.5)
- `scripts/build_inscriptions.py` — Builds segmented inscription TSVs from UD treebanks
- `scripts/build_validation_inscriptions.py` — Builds unsegmented fragments
- `scripts/fleet_orchestrate.py` — Fleet deployment (DO NOT USE without fixes)

### Key Data Files
- `data/validation_phonetic_prior/{iso}/lost.txt` — Unsegmented validation inscriptions
- `data/validation_phonetic_prior/{iso}/known_{other}.txt` — Known vocab per language
- `data/validation_phonetic_prior/{iso}/ground_truth_{other}.tsv` — Cognate pairs for P@k
- `data/linear_a/linear_a_corpus_unsegmented.txt` — Linear A (no spaces, 48 lines)
- `data/training/lexicons/{iso}.tsv` — Full lexicons for candidate languages
- `data/inscriptions/{iso}_inscriptions.tsv` — Segmented inscription dataset

---

## 7. Validated Results (from this attempt)

### Latin as lost (CORRECT — full 65K inscriptions, 500 vocab, 1000 steps):
| Rank | Known | Quality | Expected |
|------|-------|---------|----------|
| 1 | **Umbrian (xum)** | -70.7 | YES — Italic sister |
| 2 | **Oscan (osc)** | -73.4 | YES — Italic family |
| 3 | Sanskrit (san) | -74.3 | Correct — non-Italic |
| 4 | Old English (ang) | -74.8 | Correct — non-Italic |

This proves the algorithm works when properly configured.

### Linear A (partial, from first quick run — needs rerun with unsegmented corpus):
| Rank | Language | Quality |
|------|----------|---------|
| 1 | Elamite | -59.4 |
| 2 | Urartian | -61.1 |
| 3 | Proto-Kartvelian | -64.5 |
| 4 | Phoenician | -64.7 |

---

## 8. Checklist for New Claude Context Window

The new context should include:
- [ ] This PRD document
- [ ] The validated Latin ranking result (proof algorithm works)
- [ ] Links to all repos (GitHub, HuggingFace, Vast.ai)
- [ ] The exact fixes needed in `run_proper_validation.py` (§5.5)
- [ ] The deployment strategy choice (recommend Option A: single machine)
- [ ] Budget remaining ($20.50)
- [ ] Instruction: test ONE experiment locally before deploying to cloud
- [ ] Instruction: ALWAYS collect results before destroying machines
- [ ] Instruction: cognate lists are the PRIMARY output, not closeness scores
- [ ] Instruction: Linear A MUST use `linear_a_corpus_unsegmented.txt` (no spaces)
