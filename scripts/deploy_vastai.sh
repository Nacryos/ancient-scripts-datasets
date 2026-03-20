#!/bin/bash
# Deploy and run Phonetic Prior validation on Vast.ai
# Usage: bash scripts/deploy_vastai.sh

set -e

echo "=== Setting up Phonetic Prior validation on Vast.ai ==="

# Install dependencies
pip install torch ipapy numpy pyyaml tqdm 2>&1 | tail -1

# Clone repos
if [ ! -d "Project-Phaistos" ]; then
    echo "Cloning Project-Phaistos..."
    git clone --depth 1 https://github.com/Nacryos/Project-Phaistos.git
fi

if [ ! -d "ancient-scripts-datasets" ]; then
    echo "Cloning ancient-scripts-datasets..."
    git clone --depth 1 https://github.com/Nacryos/ancient-scripts-datasets.git
fi

# Install phonetic-prior-v2
cd Project-Phaistos/phonetic-prior-v2
pip install -e . 2>&1 | tail -1
cd ../..

# Run validation (smoke test first to verify)
echo "=== Running smoke test ==="
python ancient-scripts-datasets/scripts/run_phonetic_prior_validation.py \
    --data-dir ancient-scripts-datasets \
    --repo-dir Project-Phaistos \
    --output-dir outputs \
    --skip-setup \
    --smoke \
    --workers 4 \
    --validation-only

echo "=== Smoke test complete ==="

# If smoke test passes, run full suite
echo "=== Running full validation + Linear A ==="
python ancient-scripts-datasets/scripts/run_phonetic_prior_validation.py \
    --data-dir ancient-scripts-datasets \
    --repo-dir Project-Phaistos \
    --output-dir outputs \
    --skip-setup \
    --workers 16

echo "=== ALL DONE ==="
echo "Results in: outputs/"
ls -la outputs/
