#!/bin/bash
# Setup a Vast.ai worker for Phonetic Prior experiments
# Usage: bash setup_worker.sh <GH_TOKEN>
set -e
cd /workspace
GH_TOKEN=$1

pip install -q ipapy tqdm 2>&1 | tail -1
GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 https://${GH_TOKEN}@github.com/Project-Phaistos/phonetic-prior-v2.git 2>&1 | tail -1
git clone --depth 1 https://${GH_TOKEN}@github.com/Nacryos/ancient-scripts-datasets.git 2>&1 | tail -1
export PYTHONPATH=/workspace/phonetic-prior-v2/src:$PYTHONPATH
python -c 'from phonetic_prior_v2.model.cognate_validator import CognateValidator; print("READY")'
