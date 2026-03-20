#!/usr/bin/env python3
"""Quick Linear A cognate extraction against ancient languages."""
import time, random, csv, json, sys
from pathlib import Path
import torch
from phonetic_prior_v2.model.config import PhoneticPriorConfig
from phonetic_prior_v2.model.cognate_validator import CognateValidator, train_one_step
from phonetic_prior_v2.features.registry import build_feature_matrix


def load_vocab_tsv(path, max_items=50):
    vocab = set()
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ipa = row.get("IPA", "").strip()
            if ipa and len(ipa) >= 2:
                vocab.add(ipa)
    items = sorted(vocab)
    return items[:max_items]


def run_linear_a_vs(la_text, known_vocab, known_lang, num_steps=100, seed=42):
    la_text = [t[:20] for t in la_text[:15]]
    known_vocab = known_vocab[:50]

    if not la_text or not known_vocab:
        return None

    lost_chars = sorted(set("".join(la_text)))
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
        batch = rng.choices(la_text, k=4)
        out = train_one_step(model, optimizer, batch, known_vocab)

    model.eval()
    cognates = []
    with torch.no_grad():
        char_distr = model.compute_char_distr()
        # Extract spans from Linear A inscriptions
        spans = set()
        for text in la_text:
            for slen in range(3, min(9, len(text) + 1)):
                for start in range(len(text) - slen + 1):
                    spans.add(text[start:start + slen])
        for span in sorted(spans)[:80]:
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
    la_corpus = data_dir / "data" / "linear_a" / "linear_a_corpus.txt"
    lexicon_dir = data_dir / "data" / "training" / "lexicons"

    # Load Linear A corpus
    with open(la_corpus, "r", encoding="utf-8") as f:
        la_text = [line.strip() for line in f if line.strip()]
    print(f"Linear A: {len(la_text)} inscriptions")

    # Ancient languages within ~1000 years of Linear A
    targets = [
        ("hit", "Hittite"),
        ("uga", "Ugaritic"),
        ("phn", "Phoenician"),
        ("xur", "Urartian"),
        ("elx", "Elamite"),
        ("ave", "Avestan"),
        ("peo", "Old Persian"),
        ("xld", "Lydian"),
        ("xlc", "Lycian"),
        ("xcr", "Carian"),
        ("xpg", "Phrygian"),
        ("xle", "Lemnian"),
        ("xrr", "Rhaetic"),
        ("cms", "Messapic"),
        ("ine-pro", "Proto-IE"),
        ("sem-pro", "Proto-Semitic"),
        ("ccs-pro", "Proto-Kartvelian"),
        ("dra-pro", "Proto-Dravidian"),
    ]

    results = {}
    all_cognates = {}
    for iso, name in targets:
        lex_path = lexicon_dir / f"{iso}.tsv"
        if not lex_path.exists():
            print(f"  {name} ({iso}): skipped (no lexicon)")
            continue

        t0 = time.time()
        known_vocab = load_vocab_tsv(lex_path)
        if len(known_vocab) < 10:
            print(f"  {name} ({iso}): skipped (only {len(known_vocab)} vocab items)")
            continue

        r = run_linear_a_vs(la_text, known_vocab, iso, num_steps=100)
        elapsed = time.time() - t0
        if r is None:
            continue

        results[iso] = r["closeness"]
        all_cognates[iso] = r["cognates"]
        print(f"  Linear A vs {name:20s} ({iso}): closeness={r['closeness']:.4f}, time={elapsed:.0f}s")
        print(f"    Top 3: ", end="")
        for c in r["cognates"][:3]:
            print(f"{c['lost']}->{c['known']}({c['score']:.1f}) ", end="")
        print()

    # Ranking
    print(f"\n{'='*60}")
    print("LINEAR A CLOSENESS RANKING")
    print(f"{'='*60}")
    ranking = sorted(results.items(), key=lambda x: -x[1])
    for i, (iso, score) in enumerate(ranking, 1):
        name = dict(targets).get(iso, iso)
        n_cognates = len(all_cognates.get(iso, []))
        print(f"  #{i:2d} {name:20s} ({iso:8s}): closeness={score:.4f}, cognates={n_cognates}")

    # Save full cognate lists
    out_dir = Path("outputs/linear_a")
    out_dir.mkdir(parents=True, exist_ok=True)
    for iso, cogs in all_cognates.items():
        with open(out_dir / f"cognates_{iso}.tsv", "w", encoding="utf-8", newline="\n") as f:
            writer = csv.DictWriter(f, fieldnames=["lost", "known", "score"],
                                    delimiter="\t", lineterminator="\n")
            writer.writeheader()
            for c in cogs:
                writer.writerow(c)
    print(f"\nCognate lists saved to: {out_dir}")

    with open(out_dir / "ranking.json", "w") as f:
        json.dump({"ranking": ranking, "results": results}, f, indent=2)


if __name__ == "__main__":
    main()
