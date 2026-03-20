# Ancient Inscription Text Dataset

Running text (word sequences preserving original word order) from academically
peer-reviewed, CC-BY-SA-compatible sources. Designed for phonotactic sequence
analysis in Phonetic Prior models.

## Languages

| ISO | Language | Source | Entries | Date Range | Genre |
|-----|----------|--------|---------|------------|-------|
| grc | Ancient Greek | UD_Ancient_Greek-PTNK (Septuagint) | 1,576 | 4th-5th c. CE | religious |
| lat | Latin | UD_Latin-LLCT + UD_Latin-CIRCSE | 10,687 | 774-897 CE / 1st c. BCE-2nd c. CE | legal + literary |
| san | Sanskrit | UD_Sanskrit-Vedic | 27,182 | c. 1500-500 BCE | religious |
| ang | Old English | UD_Old_English-Cairo | 20 | c. 700-1100 CE | mixed |
| osc | Oscan | CEIPoM v1.3 (Zenodo) | 770 | 5th-1st c. BCE | inscription |
| xum | Umbrian | CEIPoM v1.3 (Zenodo) | 761 | 7th-1st c. BCE | inscription |

**Total: ~41,000 entries across 6 languages.**

## TSV Schema

| Column | Type | Description |
|--------|------|-------------|
| Inscription_ID | str | Unique identifier (source + sentence ID) |
| Text | str | Original text (space-separated words) |
| IPA | str | IPA transcription (space-separated words) |
| SCA | str | Sound class encoding (space-separated words) |
| Source | str | Source database identifier |
| Date_Approx | str | Approximate date of the text |
| Genre | str | Text genre (legal, literary, religious, inscription) |
| IPA_Source | str | IPA generation method |

## Sources & Licenses

All sources are **CC BY-SA 4.0** or more permissive:

- **UD_Ancient_Greek-PTNK**: CC BY-SA 4.0. Pentateuch from Codex Alexandrinus (LXX).
  https://github.com/UniversalDependencies/UD_Ancient_Greek-PTNK
- **UD_Latin-LLCT**: CC BY-SA 4.0. 521 Early Medieval charters from Tuscan archives.
  https://github.com/UniversalDependencies/UD_Latin-LLCT
- **UD_Latin-CIRCSE**: CC BY-SA 4.0. Seneca tragedies + Tacitus Germania.
  https://github.com/UniversalDependencies/UD_Latin-CIRCSE
- **UD_Sanskrit-Vedic**: CC BY-SA 4.0. Vedic Sanskrit texts (Rigveda, Atharvaveda, etc.).
  https://github.com/UniversalDependencies/UD_Sanskrit-Vedic
- **UD_Old_English-Cairo**: CC BY-SA 4.0. Georgetown Old English corpus.
  https://github.com/UniversalDependencies/UD_Old_English-Cairo
- **CEIPoM v1.3**: CC BY-SA 4.0. Corpus of the Epigraphy of the Italian Peninsula.
  https://zenodo.org/records/6475427. Pitts (2022), *Journal of Open Humanities Data*.

## IPA Transliteration

| Language | Method | Reference |
|----------|--------|-----------|
| grc | Greek Unicode -> strip diacritics -> transliteration map | Allen (1987) *Vox Graeca* |
| lat | Latin script -> transliteration map (Classical values) | Allen (1978) *Vox Latina* |
| san | IAST romanization -> transliteration map | Whitney (1889) *Sanskrit Grammar* |
| ang | Old English script -> transliteration map | Campbell (1959) *OE Grammar* |
| osc | CEIPoM Standard_aligned -> transliteration map | Buck (1904); Pitts (2022) |
| xum | CEIPoM Standard_aligned -> transliteration map | Buck (1904); Pitts (2022) |

SCA encoding uses the system from List (2012), implemented in
`cognate_pipeline/src/cognate_pipeline/normalise/sound_class.py`.

## Limitations

1. **6 of 14 validation languages only**: Gothic, OCS, Old Norse, Old Irish, Avestan,
   OHG, Old Turkic, Hebrew lack CC-BY-SA inscription corpora.
2. **Late Latin bias**: LLCT is AD 774-897 Tuscan legal Latin, not Classical.
   CIRCSE adds Classical literary texts but is smaller.
3. **Old English is minimal**: Only 20 sentences from Cairo treebank. OEDT has no
   released data yet.
4. **Oscan: Greek-script inscriptions excluded**: ~240 Oscan inscriptions in Greek
   alphabet were filtered out (need separate transliteration path).
5. **IPA is approximate**: Transliteration maps apply default/canonical pronunciations.
   Allophonic variation, sandhi, and contextual rules are not modeled.

## Build Instructions

```bash
# Step 1: Download sources
python scripts/ingest_inscriptions.py

# Step 2: Build TSVs
python scripts/build_inscriptions.py
```

## Citation

If using this dataset, cite the original sources listed above plus:
- Pitts, R.J. (2022). The Corpus of the Epigraphy of the Italian Peninsula in the
  1st Millennium BCE. *Journal of Open Humanities Data*, 8(7).
- List, J.-M. (2012). SCA: A Method for Automatic Sound Correspondence Analysis.
