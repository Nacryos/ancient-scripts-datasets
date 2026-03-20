#!/usr/bin/env python3
"""Run Phonetic Prior validation + Linear A experiments on all language pairs.

Designed to run on a Vast.ai CPU instance with multiprocessing.
Clones repos, sets up environment, runs all experiments in parallel.

Usage:
    python run_phonetic_prior_validation.py [--workers N] [--output-dir DIR]
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Validation pairs: (lost_iso, known_iso)
# Skip ang (only 44 fragments — too small for meaningful results)
VALIDATION_PAIRS = []
for lost in ["grc", "lat", "san", "osc", "xum"]:
    for known in ["grc", "lat", "san", "ang", "osc", "xum"]:
        if lost != known:
            VALIDATION_PAIRS.append((lost, known))

# Linear A against 18 ancient languages
LINEAR_A_TARGETS = [
    "hit", "uga", "phn", "xur", "elx", "ave", "peo",
    "xld", "xlc", "xcr", "xpg", "xle", "xrr", "cms",
    "ine-pro", "sem-pro", "ccs-pro", "dra-pro",
]

# Model hyperparameters (paper defaults)
MODEL_CONFIG = {
    "temperature": 0.2,
    "alpha": 3.5,
    "lambda_cov": 10.0,
    "lambda_loss": 100.0,
    "r_cov": 0.5,
    "min_span": 3,
    "max_span": 10,
    "embedding_dim": 700,
    "lr": 0.2,
    "p_o": 0.2,
    "dropout": 0.5,
    "num_steps": 300,
    "anneal_steps": 250,
    "alpha_start": 10.0,
    "batch_size": 8,
    "seed": 1234,
    "num_restarts": 1,  # 1 for validation, 3 for Linear A
}


@dataclass
class ExperimentResult:
    lost_lang: str
    known_lang: str
    experiment_type: str  # "validation" or "linear_a"
    objective: float = 0.0
    quality: float = 0.0
    coverage: float = 0.0
    num_cognates_found: int = 0
    top_cognates: list = field(default_factory=list)
    closeness_score: float = 0.0
    p_at_1: float = 0.0
    p_at_10: float = 0.0
    mrr: float = 0.0
    runtime_seconds: float = 0.0
    error: str = ""


# ---------------------------------------------------------------------------
# Import phonetic prior (deferred — may not be installed yet)
# ---------------------------------------------------------------------------

def setup_environment(repo_dir: Path, data_dir: Path):
    """Clone repos and install dependencies."""
    # Clone Project-Phaistos if needed
    if not (repo_dir / "phonetic-prior-v2").exists():
        print("Cloning Project-Phaistos...")
        subprocess.run(
            ["git", "clone", "--depth", "1",
             "https://github.com/Nacryos/Project-Phaistos.git",
             str(repo_dir)],
            check=True, capture_output=True, text=True,
        )

    # Clone ancient-scripts-datasets if needed
    if not (data_dir / "data").exists():
        print("Cloning ancient-scripts-datasets...")
        subprocess.run(
            ["git", "clone", "--depth", "1",
             "https://github.com/Nacryos/ancient-scripts-datasets.git",
             str(data_dir)],
            check=True, capture_output=True, text=True,
        )

    # Install phonetic-prior-v2
    v2_dir = repo_dir / "phonetic-prior-v2"
    if v2_dir.exists():
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", str(v2_dir)],
            check=True, capture_output=True, text=True,
        )

    # Install core deps
    subprocess.run(
        [sys.executable, "-m", "pip", "install",
         "torch", "ipapy", "numpy", "pyyaml", "tqdm"],
        check=True, capture_output=True, text=True,
    )


def load_lost_text(path: Path) -> List[str]:
    """Load unsegmented inscription fragments."""
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def load_known_vocab(path: Path, max_items: int = 100) -> List[str]:
    """Load known vocabulary (IPA words from lexicon TSV or .txt)."""
    if path.suffix == ".txt":
        with open(path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    # TSV format
    vocab = set()
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ipa = row.get("IPA", "").strip()
            if ipa and len(ipa) >= 2:
                vocab.add(ipa)
    items = sorted(vocab)
    return items[:max_items]


def load_ground_truth(path: Path) -> Dict[str, List[str]]:
    """Load ground truth cognate pairs TSV."""
    gold_map: Dict[str, List[str]] = {}
    if not path.exists():
        return gold_map
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            lost = row.get("lost", "").strip()
            known = row.get("known", "").strip()
            if lost and known:
                if lost not in gold_map:
                    gold_map[lost] = []
                gold_map[lost].append(known)
    return gold_map


def run_single_experiment(
    lost_lang: str,
    known_lang: str,
    experiment_type: str,
    data_dir: Path,
    output_dir: Path,
    config: dict,
) -> ExperimentResult:
    """Run a single (lost, known) language pair experiment."""
    import torch

    start_time = time.time()
    result = ExperimentResult(
        lost_lang=lost_lang,
        known_lang=known_lang,
        experiment_type=experiment_type,
    )

    try:
        # Import here (after setup)
        from phonetic_prior_v2.model.config import PhoneticPriorConfig
        from phonetic_prior_v2.model.cognate_validator import CognateValidator, train_one_step
        from phonetic_prior_v2.features.registry import build_feature_matrix

        # Load data
        if experiment_type == "validation":
            val_dir = data_dir / "data" / "validation_phonetic_prior"
            lost_text = load_lost_text(val_dir / lost_lang / "lost.txt")
            known_vocab_path = val_dir / lost_lang / f"known_{known_lang}.txt"
            known_vocab = load_known_vocab(known_vocab_path)
            gt_path = val_dir / lost_lang / f"ground_truth_{known_lang}.tsv"
            gold_map = load_ground_truth(gt_path)
        elif experiment_type == "linear_a":
            lost_text = load_lost_text(
                data_dir / "data" / "linear_a" / "linear_a_corpus.txt"
            )
            known_vocab = load_known_vocab(
                data_dir / "data" / "training" / "lexicons" / f"{known_lang}.tsv"
            )
            gold_map = {}  # No ground truth for Linear A
        else:
            raise ValueError(f"Unknown experiment type: {experiment_type}")

        if not lost_text or not known_vocab:
            result.error = f"No data: lost={len(lost_text)}, known={len(known_vocab)}"
            return result

        # Cap inscription LENGTH to ~20 chars (matching paper's Gothic verse lengths)
        # and limit count. word_boundary_dp scales with O(len * spans * vocab).
        MAX_INSCRIPTION_LEN = 20
        train_text = [t[:MAX_INSCRIPTION_LEN] for t in lost_text[:30]]

        # Build character sets
        lost_chars = sorted(set("".join(train_text)))
        known_chars = sorted(set("".join(known_vocab)))

        if not lost_chars or not known_chars:
            result.error = "Empty character set"
            return result

        # Build IPA features
        known_features = build_feature_matrix(known_chars, backend="ipapy")

        # Configure model
        model_config = PhoneticPriorConfig(
            temperature=config["temperature"],
            alpha=config["alpha"],
            lambda_cov=config["lambda_cov"],
            lambda_loss=config["lambda_loss"],
            r_cov=config["r_cov"],
            min_span=config["min_span"],
            max_span=config["max_span"],
            embedding_dim=config["embedding_dim"],
            lr=config["lr"],
            p_o=config["p_o"],
            dropout=config["dropout"],
        )

        # Build model
        model = CognateValidator(
            lost_chars=lost_chars,
            known_chars=known_chars,
            known_ipa_features=known_features,
            config=model_config,
        )
        optimizer = torch.optim.SGD(model.parameters(), lr=config["lr"])

        # Train
        import random
        rng = random.Random(config["seed"])
        model.train()
        num_steps = config["num_steps"]
        batch_size = config["batch_size"]
        alpha_start = config["alpha_start"]
        alpha_end = config["alpha"]
        anneal_steps = config["anneal_steps"]

        last_obj = 0.0
        last_quality = 0.0
        for step in range(num_steps):
            # Anneal alpha
            if step < anneal_steps:
                frac = step / anneal_steps
                current_alpha = alpha_start + (alpha_end - alpha_start) * frac
                model_config.alpha = current_alpha

            # Sample mini-batch
            batch = rng.choices(train_text, k=batch_size)

            out = train_one_step(model, optimizer, batch, known_vocab)
            # Detach to prevent autograd graph accumulation (memory leak)
            last_obj = float(out.objective) if hasattr(out.objective, 'item') else out.objective
            last_quality = float(out.quality) if hasattr(out.quality, 'item') else out.quality

        result.objective = last_obj
        result.quality = last_quality

        # Eval: score all lost words against known vocab
        model.eval()
        with torch.no_grad():
            char_distr = model.compute_char_distr()

            # Score sample of lost words (up to 200)
            eval_words = list(set("".join(lost_text).split()))[:100] if " " in "".join(lost_text) else []
            # For unsegmented text, extract candidate spans
            if not eval_words:
                # Extract spans of length min_span to max_span from inscriptions
                spans = set()
                for text in lost_text[:50]:
                    for slen in range(config["min_span"], min(config["max_span"] + 1, len(text) + 1)):
                        for start in range(len(text) - slen + 1):
                            spans.add(text[start:start + slen])
                eval_words = sorted(spans)[:100]

            # Rank each eval word against known vocab
            cognate_pairs = []
            total_score = 0.0
            for lost_word in eval_words:
                try:
                    scores = model.score_against_vocab(lost_word, known_vocab, char_distr)
                    top_idx = scores.argmax().item()
                    top_score = scores[top_idx].item()
                    total_score += top_score
                    cognate_pairs.append({
                        "lost": lost_word,
                        "known": known_vocab[top_idx],
                        "score": round(top_score, 4),
                    })
                except Exception:
                    continue

            # Sort by score (best matches first)
            cognate_pairs.sort(key=lambda x: x["score"], reverse=True)
            # Keep ALL cognate pairs for the output file (user wants full lists)
            result.top_cognates = cognate_pairs[:50]  # summary in JSON
            result.num_cognates_found = len(cognate_pairs)
            result.closeness_score = total_score / max(len(eval_words), 1)
            # Save FULL cognate list (all pairs, not just top 50)
            _all_cognates = cognate_pairs  # saved to TSV below

            # Compute P@k if ground truth available
            if gold_map:
                hits_1 = 0
                hits_10 = 0
                mrr_sum = 0.0
                n_eval = 0
                for lost_word, gold_words in gold_map.items():
                    try:
                        scores = model.score_against_vocab(lost_word, known_vocab, char_distr)
                        ranked_indices = scores.argsort(descending=True).tolist()
                        ranked_words = [known_vocab[i] for i in ranked_indices[:10]]

                        # Check hits
                        gold_set = set(gold_words)
                        if ranked_words[0] in gold_set:
                            hits_1 += 1
                        if any(w in gold_set for w in ranked_words):
                            hits_10 += 1
                        for rank, w in enumerate(ranked_words, 1):
                            if w in gold_set:
                                mrr_sum += 1.0 / rank
                                break
                        n_eval += 1
                    except Exception:
                        continue

                if n_eval > 0:
                    result.p_at_1 = hits_1 / n_eval
                    result.p_at_10 = hits_10 / n_eval
                    result.mrr = mrr_sum / n_eval

    except Exception as e:
        result.error = str(e)

    result.runtime_seconds = time.time() - start_time

    # Save individual result
    exp_dir = output_dir / experiment_type / f"{lost_lang}_vs_{known_lang}"
    exp_dir.mkdir(parents=True, exist_ok=True)
    with open(exp_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(asdict(result), f, indent=2, ensure_ascii=False)
    # Save FULL cognate list (all pairs found, not truncated)
    all_cognates = locals().get("_all_cognates", result.top_cognates)
    if all_cognates:
        with open(exp_dir / "cognate_list_full.tsv", "w", encoding="utf-8", newline="\n") as f:
            writer = csv.DictWriter(f, fieldnames=["lost", "known", "score"],
                                    delimiter="\t", lineterminator="\n")
            writer.writeheader()
            for pair in all_cognates:
                writer.writerow(pair)

    return result


def _run_experiment_wrapper(args):
    """Wrapper for multiprocessing."""
    return run_single_experiment(*args)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run Phonetic Prior validation suite")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers")
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/phonetic_prior"))
    parser.add_argument("--data-dir", type=Path, default=Path("."))
    parser.add_argument("--repo-dir", type=Path, default=Path("Project-Phaistos"))
    parser.add_argument("--skip-setup", action="store_true")
    parser.add_argument("--validation-only", action="store_true")
    parser.add_argument("--linear-a-only", action="store_true")
    parser.add_argument("--smoke", action="store_true", help="Quick test (100 steps)")
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_setup:
        setup_environment(args.repo_dir, args.data_dir)

    config = MODEL_CONFIG.copy()
    if args.smoke:
        config["num_steps"] = 100
        config["anneal_steps"] = 50

    # Build job list
    jobs = []

    if not args.linear_a_only:
        print(f"=== VALIDATION: {len(VALIDATION_PAIRS)} pairs ===")
        for lost, known in VALIDATION_PAIRS:
            jobs.append((lost, known, "validation", args.data_dir, output_dir, config))

    if not args.validation_only:
        print(f"=== LINEAR A: {len(LINEAR_A_TARGETS)} targets ===")
        la_config = config.copy()
        la_config["num_restarts"] = 3
        for known in LINEAR_A_TARGETS:
            jobs.append(("linear_a", known, "linear_a", args.data_dir, output_dir, la_config))

    print(f"\nTotal jobs: {len(jobs)}, workers: {args.workers}")
    print(f"Estimated time: {len(jobs) * 10 / args.workers:.0f} minutes")
    print()

    # Run all experiments
    start = time.time()
    results = []

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_run_experiment_wrapper, job): job for job in jobs}
        for i, future in enumerate(as_completed(futures)):
            job = futures[future]
            try:
                result = future.result()
                results.append(result)
                status = "OK" if not result.error else f"ERR: {result.error[:50]}"
                print(f"  [{i+1}/{len(jobs)}] {result.lost_lang} vs {result.known_lang}: "
                      f"closeness={result.closeness_score:.4f}, "
                      f"cognates={result.num_cognates_found}, "
                      f"time={result.runtime_seconds:.0f}s [{status}]")
            except Exception as e:
                print(f"  [{i+1}/{len(jobs)}] {job[0]} vs {job[1]}: FAILED: {e}")

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"COMPLETE: {len(results)} experiments in {elapsed/60:.1f} minutes")
    print(f"{'='*60}")

    # Write summary
    # Validation results
    val_results = [r for r in results if r.experiment_type == "validation" and not r.error]
    if val_results:
        print("\n=== VALIDATION CLOSENESS RANKINGS ===")
        by_lost = {}
        for r in val_results:
            if r.lost_lang not in by_lost:
                by_lost[r.lost_lang] = []
            by_lost[r.lost_lang].append(r)

        for lost, rlist in sorted(by_lost.items()):
            rlist.sort(key=lambda x: x.closeness_score, reverse=True)
            print(f"\n  {lost} (as lost):")
            for r in rlist:
                marker = ""
                print(f"    #{rlist.index(r)+1} {r.known_lang}: "
                      f"closeness={r.closeness_score:.4f}, "
                      f"P@1={r.p_at_1:.2f}, P@10={r.p_at_10:.2f}, "
                      f"MRR={r.mrr:.3f} {marker}")

    # Linear A results
    la_results = [r for r in results if r.experiment_type == "linear_a" and not r.error]
    if la_results:
        la_results.sort(key=lambda x: x.closeness_score, reverse=True)
        print("\n=== LINEAR A CLOSENESS RANKING ===")
        for i, r in enumerate(la_results, 1):
            print(f"  #{i:2d} {r.known_lang:12s}: closeness={r.closeness_score:.4f}, "
                  f"cognates={r.num_cognates_found}")

    # Write all results to JSON
    summary_path = output_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to: {summary_path}")

    # Write CSV summary
    csv_path = output_dir / "summary.csv"
    with open(csv_path, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "experiment_type", "lost_lang", "known_lang",
            "closeness_score", "num_cognates_found",
            "p_at_1", "p_at_10", "mrr",
            "objective", "quality", "runtime_seconds", "error",
        ], delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for r in results:
            writer.writerow({
                "experiment_type": r.experiment_type,
                "lost_lang": r.lost_lang,
                "known_lang": r.known_lang,
                "closeness_score": f"{r.closeness_score:.4f}",
                "num_cognates_found": r.num_cognates_found,
                "p_at_1": f"{r.p_at_1:.3f}",
                "p_at_10": f"{r.p_at_10:.3f}",
                "mrr": f"{r.mrr:.3f}",
                "objective": f"{r.objective:.4f}",
                "quality": f"{r.quality:.4f}",
                "runtime_seconds": f"{r.runtime_seconds:.0f}",
                "error": r.error,
            })
    print(f"CSV summary: {csv_path}")


if __name__ == "__main__":
    main()
