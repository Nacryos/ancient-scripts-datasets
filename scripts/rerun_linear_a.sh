#!/bin/bash
# Rerun Linear A experiments with unsegmented corpus + span extraction
# Also rerun xum experiments with full dataset
# Run this on each idle Vast.ai machine after the current experiments complete
set -e
cd /workspace
export PYTHONPATH=/workspace/phonetic-prior-v2/src:$PYTHONPATH

# Pull latest code
cd ancient-scripts-datasets
git pull origin master 2>/dev/null || true
cd /workspace

# Verify unsegmented corpus exists and has no spaces
if [ ! -f ancient-scripts-datasets/data/linear_a/linear_a_corpus_unsegmented.txt ]; then
    echo "ERROR: unsegmented corpus missing!"
    exit 1
fi
spaces=$(grep -c ' ' ancient-scripts-datasets/data/linear_a/linear_a_corpus_unsegmented.txt || echo 0)
if [ "$spaces" -gt 0 ]; then
    echo "ERROR: unsegmented corpus still has spaces!"
    exit 1
fi

echo "Unsegmented corpus verified (0 spaces)"
echo "Running Linear A experiments: jobs $1 to $2"

rm -rf outputs
python -u ancient-scripts-datasets/scripts/run_proper_validation.py \
    --data-dir ancient-scripts-datasets \
    --workers 1 \
    --output-dir outputs \
    --linear-a-only \
    --job-start "$1" \
    --job-end "$2"
