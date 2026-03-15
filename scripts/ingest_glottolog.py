#!/usr/bin/env python3
"""Download Glottolog CLDF data for phylogenetic enrichment.

Source: Glottolog CLDF v5.x (Hammarström, Forkel, Haspelmath & Bank)
URL: https://github.com/glottolog/glottolog-cldf
License: CC BY 4.0
Citation: Hammarström et al., DOI: 10.5281/zenodo.15640174

Downloads languages.csv and classification.nex from the CLDF dataset.
Iron Rule: Data comes from downloaded files. No hardcoded word lists.

Usage:
    python scripts/ingest_glottolog.py
"""

from __future__ import annotations

import hashlib
import logging
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

logger = logging.getLogger(__name__)

GLOTTOLOG_DIR = ROOT / "data" / "training" / "raw" / "glottolog_cldf"
BASE_URL = "https://raw.githubusercontent.com/glottolog/glottolog-cldf/master/cldf/"

FILES = [
    "languages.csv",
    "classification.nex",
]


def download_if_needed():
    """Download Glottolog CLDF files if not cached."""
    GLOTTOLOG_DIR.mkdir(parents=True, exist_ok=True)
    for fname in FILES:
        local = GLOTTOLOG_DIR / fname
        if local.exists():
            logger.info("Cached: %s (%d bytes)", fname, local.stat().st_size)
            continue
        url = BASE_URL + fname
        logger.info("Downloading %s ...", url)
        req = urllib.request.Request(url, headers={
            "User-Agent": "ancient-scripts-datasets/1.0 (phylo-enrichment)"
        })
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        with open(local, "wb") as f:
            f.write(data)
        md5 = hashlib.md5(data).hexdigest()
        logger.info("Downloaded %s (%d bytes, md5=%s)", fname, len(data), md5)


def verify_files():
    """Verify downloaded files exist and have reasonable sizes."""
    ok = True
    for fname in FILES:
        local = GLOTTOLOG_DIR / fname
        if not local.exists():
            logger.error("MISSING: %s", local)
            ok = False
            continue
        size = local.stat().st_size
        if size < 1000:
            logger.error("TOO SMALL: %s (%d bytes)", fname, size)
            ok = False
        else:
            md5 = hashlib.md5(local.read_bytes()).hexdigest()
            logger.info("OK: %s (%d bytes, md5=%s)", fname, size, md5)
    return ok


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    download_if_needed()
    if verify_files():
        logger.info("All Glottolog CLDF files downloaded successfully.")
    else:
        logger.error("Some files missing or corrupted.")
        sys.exit(1)


if __name__ == "__main__":
    main()
