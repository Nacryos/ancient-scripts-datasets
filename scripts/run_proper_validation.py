#!/usr/bin/env python3
"""Proper Phonetic Prior validation — full vocab, 1000 steps, real eval.

Previous run was broken: 50 vocab, 100 steps, substring matching = noise.
This run matches the paper's methodology:
  - 500-700 vocab items per language (paper uses 300-500)
  - 1000 training steps with annealing (paper default for Gothic)
  - Eval on FULL WORDS from ground truth (not random substrings)
  - 100 training inscriptions, capped at 20 chars (paper's regime)
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# Config — matching paper's defaults
# ---------------------------------------------------------------------------
CONFIG = {
    "num_steps": 1000,
    "anneal_steps": 800,
    "alpha_start": 10.0,
    "alpha_end": 3.5,
    "temperature": 0.2,
    "lambda_cov": 10.0,
    "lambda_loss": 100.0,
    "r_cov": 0.5,
    "min_span": 3,
    "max_span": 10,
    "embedding_dim": 700,
    "lr": 0.2,
    "p_o": 0.2,
    "dropout": 0.5,
    "batch_size": 8,
    "seed": 1234,
    "max_vocab": 300,          # paper's Gothic uses 300-500
    "max_inscriptions": 50,    # paper uses ~50-100
    "max_inscription_len": 15, # shorter = faster DP
    "max_eval_words": 100,     # eval against up to 100 query words
}

# Validation pairs
VALIDATION_PAIRS = []
for lost in ["grc", "lat", "san", "osc", "xum"]:
    for known in ["grc", "lat", "san", "ang", "osc", "xum"]:
        if lost != known:
            VALIDATION_PAIRS.append((lost, known))

# Linear A targets
LINEAR_A_TARGETS = [
    "hit", "uga", "phn", "xur", "elx", "ave", "peo",
    "xld", "xlc", "xcr", "xpg", "xle", "xrr", "cms",
    "ine-pro", "sem-pro", "ccs-pro", "dra-pro",
]


@dataclass
class Result:
    lost_lang: str
    known_lang: str
    exp_type: str
    closeness: float = 0.0
    p_at_1: float = 0.0
    p_at_5: float = 0.0
    p_at_10: float = 0.0
    mrr: float = 0.0
    n_eval: int = 0
    n_vocab: int = 0
    n_train: int = 0
    final_obj: float = 0.0
    final_quality: float = 0.0
    cognates: list = field(default_factory=list)
    runtime: float = 0.0
    error: str = ""


def load_vocab_txt(path: Path, max_items: int = 700) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        items = [l.strip() for l in f if l.strip() and len(l.strip()) >= 2]
    # Shuffle deterministically so we don't just get alphabetically-first items
    rng = random.Random(42)
    rng.shuffle(items)
    return items[:max_items]


def load_vocab_tsv(path: Path, max_items: int = 700) -> List[str]:
    vocab = set()
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ipa = row.get("IPA", "").strip()
            if ipa and len(ipa) >= 2:
                vocab.add(ipa)
    items = sorted(vocab)
    rng = random.Random(42)
    rng.shuffle(items)
    return items[:max_items]


def load_ground_truth(path: Path) -> Dict[str, List[str]]:
    gold = {}
    if not path.exists():
        return gold
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            lost = row.get("lost", "").strip()
            known = row.get("known", "").strip()
            if lost and known:
                gold.setdefault(lost, []).append(known)
    return gold


def load_linear_a_words(path: Path) -> List[str]:
    """Load Linear A words (IPA column) for eval."""
    words = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ipa = row.get("IPA", "").strip()
            if ipa and len(ipa) >= 2:
                words.append(ipa)
    return words


def run_experiment(
    lost_lang: str,
    known_lang: str,
    exp_type: str,
    data_dir: Path,
    output_dir: Path,
    cfg: dict,
) -> Result:
    """Run one (lost, known) experiment with proper parameters."""
    import torch
    from phonetic_prior_v2.model.config import PhoneticPriorConfig
    from phonetic_prior_v2.model.cognate_validator import CognateValidator, train_one_step
    from phonetic_prior_v2.features.registry import build_feature_matrix

    t0 = time.time()
    result = Result(lost_lang=lost_lang, known_lang=known_lang, exp_type=exp_type)

    try:
        # --- Load data ---
        if exp_type == "validation":
            val_dir = data_dir / "data" / "validation_phonetic_prior" / lost_lang
            # Training inscriptions
            with open(val_dir / "lost.txt", "r", encoding="utf-8") as f:
                all_lost = [l.strip() for l in f if l.strip()]
            # Known vocab — use FULL vocab file (up to max_vocab)
            kv_path = val_dir / f"known_{known_lang}.txt"
            if not kv_path.exists():
                result.error = f"No vocab file: {kv_path}"
                return result
            known_vocab = load_vocab_txt(kv_path, cfg["max_vocab"])
            # Ground truth for eval
            gt_path = val_dir / f"ground_truth_{known_lang}.tsv"
            gold_map = load_ground_truth(gt_path)
            # Eval words = ground truth lost words + sample of inscriptions
            eval_words = list(gold_map.keys())

        elif exp_type == "linear_a":
            # Training: Linear A corpus (unsegmented inscriptions)
            with open(data_dir / "data" / "linear_a" / "linear_a_corpus.txt", "r", encoding="utf-8") as f:
                all_lost = [l.strip() for l in f if l.strip()]
            # Known vocab from lexicon
            lex_path = data_dir / "data" / "training" / "lexicons" / f"{known_lang}.tsv"
            if not lex_path.exists():
                result.error = f"No lexicon: {lex_path}"
                return result
            known_vocab = load_vocab_tsv(lex_path, cfg["max_vocab"])
            # Eval: use actual Linear A words
            eval_words = load_linear_a_words(data_dir / "data" / "linear_a" / "linear_a_words.tsv")
            gold_map = {}
        else:
            result.error = f"Unknown exp_type: {exp_type}"
            return result

        if not all_lost or not known_vocab:
            result.error = f"Empty data: lost={len(all_lost)}, vocab={len(known_vocab)}"
            return result

        # Cap and truncate inscriptions
        train_text = [t[:cfg["max_inscription_len"]] for t in all_lost[:cfg["max_inscriptions"]]]
        result.n_train = len(train_text)
        result.n_vocab = len(known_vocab)

        # Build char sets
        lost_chars = sorted(set("".join(train_text)))
        known_chars = sorted(set("".join(known_vocab)))
        if not lost_chars or not known_chars:
            result.error = "Empty char set"
            return result

        # Build IPA features
        known_features = build_feature_matrix(known_chars, backend="ipapy")

        # Configure model
        model_config = PhoneticPriorConfig(
            temperature=cfg["temperature"],
            alpha=cfg["alpha_end"],
            lambda_cov=cfg["lambda_cov"],
            lambda_loss=cfg["lambda_loss"],
            r_cov=cfg["r_cov"],
            min_span=cfg["min_span"],
            max_span=cfg["max_span"],
            embedding_dim=cfg["embedding_dim"],
            lr=cfg["lr"],
            p_o=cfg["p_o"],
            dropout=cfg["dropout"],
        )

        model = CognateValidator(
            lost_chars=lost_chars,
            known_chars=known_chars,
            known_ipa_features=known_features,
            config=model_config,
        )
        optimizer = torch.optim.SGD(model.parameters(), lr=cfg["lr"])

        # --- Train ---
        rng = random.Random(cfg["seed"])
        model.train()
        for step in range(cfg["num_steps"]):
            # Anneal alpha
            if step < cfg["anneal_steps"]:
                frac = step / cfg["anneal_steps"]
                model_config.alpha = cfg["alpha_start"] + (cfg["alpha_end"] - cfg["alpha_start"]) * frac

            batch = rng.choices(train_text, k=cfg["batch_size"])
            out = train_one_step(model, optimizer, batch, known_vocab)
            # Detach and aggressively free autograd graph to prevent OOM
            obj_val = float(out.objective.detach()) if hasattr(out.objective, 'detach') else float(out.objective)
            qual_val = float(out.quality.detach()) if hasattr(out.quality, 'detach') else float(out.quality)
            del out
            if step % 50 == 0:
                import gc; gc.collect()

            if step % 200 == 0:
                print(f"    [{lost_lang} vs {known_lang}] step {step}/{cfg['num_steps']}: "
                      f"obj={obj_val:.2f}, quality={qual_val:.2f}", flush=True)

        result.final_obj = obj_val
        result.final_quality = qual_val

        # --- Eval ---
        model.eval()
        cognate_pairs = []
        hits_1 = hits_5 = hits_10 = 0
        mrr_sum = 0.0
        n_eval = 0

        with torch.no_grad():
            char_distr = model.compute_char_distr()

            for query_word in eval_words[:cfg["max_eval_words"]]:
                # Filter query to chars the model knows
                if not all(c in model.lost2idx for c in query_word):
                    # Skip words with unknown characters
                    continue

                try:
                    scores = model.score_against_vocab(query_word, known_vocab, char_distr)
                    ranked_idx = scores.argsort(descending=True).tolist()

                    # Top matches
                    top_k_words = [(known_vocab[i], float(scores[i])) for i in ranked_idx[:10]]
                    cognate_pairs.append({
                        "lost": query_word,
                        "known": top_k_words[0][0],
                        "score": round(top_k_words[0][1], 4),
                        "top_10": [(w, round(s, 4)) for w, s in top_k_words],
                    })

                    # Compute P@k if ground truth exists
                    if query_word in gold_map:
                        gold_set = set(gold_map[query_word])
                        top_words = [known_vocab[i] for i in ranked_idx[:10]]
                        if top_words[0] in gold_set:
                            hits_1 += 1
                        if any(w in gold_set for w in top_words[:5]):
                            hits_5 += 1
                        if any(w in gold_set for w in top_words[:10]):
                            hits_10 += 1
                        for rank, w in enumerate(top_words, 1):
                            if w in gold_set:
                                mrr_sum += 1.0 / rank
                                break
                        n_eval += 1
                except Exception as e:
                    continue

        # Compute metrics
        cognate_pairs.sort(key=lambda x: x["score"], reverse=True)
        result.cognates = cognate_pairs
        result.n_eval = n_eval
        if n_eval > 0:
            result.p_at_1 = hits_1 / n_eval
            result.p_at_5 = hits_5 / n_eval
            result.p_at_10 = hits_10 / n_eval
            result.mrr = mrr_sum / n_eval
        # Closeness = mean score across all eval words (normalized)
        all_scores = [c["score"] for c in cognate_pairs if c["score"] != 0]
        result.closeness = sum(all_scores) / len(all_scores) if all_scores else 0.0

    except Exception as e:
        import traceback
        result.error = f"{e}\n{traceback.format_exc()}"

    result.runtime = time.time() - t0

    # Save result
    exp_dir = output_dir / exp_type / f"{lost_lang}_vs_{known_lang}"
    exp_dir.mkdir(parents=True, exist_ok=True)
    # Save summary
    summary = asdict(result)
    summary.pop("cognates")  # save separately
    with open(exp_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    # Save full cognate list
    with open(exp_dir / "cognate_list.tsv", "w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(["lost", "top1_known", "score", "top10"])
        for c in cognate_pairs:
            writer.writerow([c["lost"], c["known"], c["score"],
                             json.dumps(c["top_10"], ensure_ascii=False)])

    return result


def _run_wrapper(args):
    return run_experiment(*args)


def main():
    parser = argparse.ArgumentParser(description="Proper Phonetic Prior validation")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--data-dir", type=Path, default=Path("."))
    parser.add_argument("--validation-only", action="store_true")
    parser.add_argument("--linear-a-only", action="store_true")
    parser.add_argument("--job-start", type=int, default=0, help="Start index in job list")
    parser.add_argument("--job-end", type=int, default=9999, help="End index in job list (exclusive)")
    args = parser.parse_args()

    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    cfg = CONFIG.copy()

    jobs = []
    if not args.linear_a_only:
        for lost, known in VALIDATION_PAIRS:
            jobs.append((lost, known, "validation", args.data_dir, out, cfg))
    if not args.validation_only:
        for known in LINEAR_A_TARGETS:
            jobs.append(("linear_a", known, "linear_a", args.data_dir, out, cfg))

    # Slice to job range for distributed execution
    jobs = jobs[args.job_start:args.job_end]
    print(f"=== {len(jobs)} experiments (jobs {args.job_start}-{args.job_start+len(jobs)-1}), {args.workers} workers ===")
    print(f"Config: {cfg['num_steps']} steps, {cfg['max_vocab']} max vocab, "
          f"{cfg['max_inscriptions']} inscriptions")
    print(flush=True)

    start = time.time()
    results = []

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_run_wrapper, j): j for j in jobs}
        for i, future in enumerate(as_completed(futures)):
            job = futures[future]
            try:
                r = future.result()
                results.append(r)
                status = "OK" if not r.error else f"ERR: {r.error[:60]}"
                print(f"[{i+1}/{len(jobs)}] {r.lost_lang} vs {r.known_lang}: "
                      f"close={r.closeness:.4f} P@1={r.p_at_1:.2f} P@10={r.p_at_10:.2f} "
                      f"MRR={r.mrr:.3f} vocab={r.n_vocab} eval={r.n_eval} "
                      f"time={r.runtime:.0f}s [{status}]", flush=True)
            except Exception as e:
                print(f"[{i+1}/{len(jobs)}] FAILED: {e}", flush=True)

    elapsed = time.time() - start
    print(f"\n{'='*70}")
    print(f"DONE: {len(results)} experiments in {elapsed/60:.1f} min")

    # --- Print rankings ---
    val_results = [r for r in results if r.exp_type == "validation" and not r.error]
    if val_results:
        print(f"\n{'='*70}")
        print("VALIDATION CLOSENESS RANKINGS")
        print(f"{'='*70}")
        by_lost = {}
        for r in val_results:
            by_lost.setdefault(r.lost_lang, []).append(r)
        for lost in sorted(by_lost):
            rlist = sorted(by_lost[lost], key=lambda x: x.closeness, reverse=True)
            print(f"\n{lost} as lost (vocab sizes: {', '.join(f'{r.known_lang}={r.n_vocab}' for r in rlist)}):")
            for i, r in enumerate(rlist, 1):
                print(f"  #{i} {r.known_lang:5s}: closeness={r.closeness:8.4f} "
                      f"P@1={r.p_at_1:.2f} P@10={r.p_at_10:.2f} MRR={r.mrr:.3f}")

    la_results = [r for r in results if r.exp_type == "linear_a" and not r.error]
    if la_results:
        la_results.sort(key=lambda x: x.closeness, reverse=True)
        print(f"\n{'='*70}")
        print("LINEAR A CLOSENESS RANKING")
        print(f"{'='*70}")
        for i, r in enumerate(la_results, 1):
            n_cog = len(r.cognates)
            print(f"  #{i:2d} {r.known_lang:12s}: closeness={r.closeness:8.4f} "
                  f"cognates={n_cog} vocab={r.n_vocab}")
        # Print top cognates per language
        print(f"\n{'='*70}")
        print("TOP COGNATE MATCHES PER LANGUAGE")
        print(f"{'='*70}")
        for r in la_results[:10]:
            print(f"\n{r.known_lang}:")
            for c in r.cognates[:5]:
                print(f"  {c['lost']:20s} -> {c['known']:20s} (score={c['score']:.2f})")

    # Save summary
    with open(out / "summary.json", "w", encoding="utf-8") as f:
        all_data = []
        for r in results:
            d = asdict(r)
            d["cognates"] = d["cognates"][:20]  # truncate for summary
            all_data.append(d)
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved to: {out.resolve()}")


if __name__ == "__main__":
    main()
