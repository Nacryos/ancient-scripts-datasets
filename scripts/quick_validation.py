#!/usr/bin/env python3
"""Quick end-to-end Phonetic Prior validation on a single language pair.
Designed to run fast: 15 inscriptions, 50 vocab, 100 steps.
"""
import time, random, csv, json, os, sys
from pathlib import Path
import torch
from phonetic_prior_v2.model.config import PhoneticPriorConfig
from phonetic_prior_v2.model.cognate_validator import CognateValidator, train_one_step
from phonetic_prior_v2.features.registry import build_feature_matrix


def run_experiment(lost_text, known_vocab, num_steps=100, seed=42):
    lost_text = [t[:20] for t in lost_text[:15]]
    known_vocab = known_vocab[:50]

    lost_chars = sorted(set("".join(lost_text)))
    known_chars = sorted(set("".join(known_vocab)))
    features = build_feature_matrix(known_chars, backend="ipapy")

    config = PhoneticPriorConfig(
        temperature=0.2, alpha=3.5, lambda_cov=10.0, lambda_loss=100.0,
        r_cov=0.5, min_span=3, max_span=8, embedding_dim=700,
        lr=0.2, p_o=0.2, dropout=0.5,
    )
    model = CognateValidator(lost_chars, known_chars, features, config)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.2)

    rng = random.Random(seed)
    model.train()
    for step in range(num_steps):
        if step < 80:
            config.alpha = 10.0 + (3.5 - 10.0) * (step / 80)
        batch = rng.choices(lost_text, k=4)
        out = train_one_step(model, optimizer, batch, known_vocab)

    model.eval()
    cognates = []
    with torch.no_grad():
        char_distr = model.compute_char_distr()
        spans = set()
        for text in lost_text[:10]:
            for slen in range(3, min(9, len(text) + 1)):
                for start in range(len(text) - slen + 1):
                    spans.add(text[start:start + slen])
        for span in sorted(spans)[:50]:
            scores = model.score_against_vocab(span, known_vocab, char_distr)
            top_idx = scores.argmax().item()
            cognates.append({
                "lost": span,
                "known": known_vocab[top_idx],
                "score": round(scores[top_idx].item(), 4),
            })

    cognates.sort(key=lambda x: x["score"], reverse=True)
    total_score = sum(c["score"] for c in cognates) / max(len(cognates), 1)
    return {"cognates": cognates, "closeness": total_score}


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("ancient-scripts-datasets")
    lost_lang = sys.argv[2] if len(sys.argv) > 2 else "osc"
    known_langs = sys.argv[3:] if len(sys.argv) > 3 else ["lat", "grc", "san", "xum"]

    val_dir = data_dir / "data" / "validation_phonetic_prior" / lost_lang
    with open(val_dir / "lost.txt") as f:
        lost_text = [l.strip() for l in f if l.strip()]
    print(f"{lost_lang} lost: {len(lost_text)} fragments")

    results = {}
    for known_lang in known_langs:
        t0 = time.time()
        kv_path = val_dir / f"known_{known_lang}.txt"
        if not kv_path.exists():
            print(f"  {known_lang}: skipped (no vocab file)")
            continue
        with open(kv_path) as f:
            known_vocab = [l.strip() for l in f if l.strip()]

        r = run_experiment(lost_text, known_vocab, num_steps=100)
        elapsed = time.time() - t0
        results[known_lang] = r
        c = r["closeness"]
        print(f"  {lost_lang} vs {known_lang}: closeness={c:.4f}, time={elapsed:.0f}s")
        print(f"    Top 5 cognates:")
        for pair in r["cognates"][:5]:
            print(f"      {pair['lost']:15s} -> {pair['known']:15s} ({pair['score']:.2f})")

    print(f"\n=== {lost_lang.upper()} CLOSENESS RANKING ===")
    ranking = sorted(results.items(), key=lambda x: -x[1]["closeness"])
    for i, (lang, r) in enumerate(ranking, 1):
        print(f"  #{i} {lang}: {r['closeness']:.4f}")


if __name__ == "__main__":
    main()
