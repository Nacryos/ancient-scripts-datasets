#!/bin/bash
# Run all validation + Linear A experiments
set -e
cd /workspace
export PYTHONPATH=/workspace/Project-Phaistos/phonetic-prior-v2/src:$PYTHONPATH
DATA=/workspace/ancient-scripts-datasets
SCRIPT=$DATA/scripts

echo "=== VALIDATION EXPERIMENTS ==="
for lost in osc xum lat grc san; do
    echo ""
    echo "========================================"
    echo "Lost language: $lost"
    echo "========================================"
    python -u $SCRIPT/quick_validation.py $DATA $lost lat grc san ang osc xum 2>&1
done

echo ""
echo "=== LINEAR A EXPERIMENTS ==="
python -u $SCRIPT/quick_linear_a.py $DATA 2>&1

echo ""
echo "=== ALL DONE ==="
