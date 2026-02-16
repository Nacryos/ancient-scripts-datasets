# Phylogenetic Validation Dataset

Balanced validation data across 9 major language families for testing cognate detection pipelines.

## Format

Each branch TSV has columns: `Language_ID`, `Parameter_ID`, `Form`, `IPA`, `Glottocode`

- `IPA` column provides phonetic transcriptions (triggers `TranscriptionType.IPA` in the pipeline)
- `_` marks missing/unattested forms (skipped by ingester)
- `concepts.tsv` lists all 40 shared concept IDs

## Branches

| File | Family | Languages | Entries |
|---|---|---|---|
| germanic.tsv | Germanic | got, ang, non, goh | ~160 |
| celtic.tsv | Celtic | sga, cym, bre | ~120 |
| balto_slavic.tsv | Balto-Slavic | lit, chu, rus | ~120 |
| indo_iranian.tsv | Indo-Iranian | san, ave, fas | ~120 |
| italic.tsv | Italic | lat, osc, xum | ~120 |
| hellenic.tsv | Hellenic | grc, gmy | ~80 |
| semitic.tsv | Semitic | heb, arb, amh | ~120 |
| turkic.tsv | Turkic | otk, tur, aze | ~120 |
| uralic.tsv | Uralic | fin, hun, est | ~120 |

## Concept Categories (40 concepts)

- **Body** (8): head, eye, ear, mouth, hand, foot, heart, blood
- **Kinship** (5): father, mother, son, daughter, brother
- **Nature** (10): water, fire, sun, moon, star, earth, mountain, river, stone, tree
- **Animals** (5): horse, dog, fish, bird, ox
- **Verbs** (8): eat, drink, give, come, die, know, hear, see
- **Other** (4): name, god, king, house

## IPA Sources

| Branch | Reference |
|---|---|
| Germanic | Wright's Gothic Grammar, Campbell's OE Grammar, Gordon's ON, Braune's OHG |
| Celtic | Thurneysen's Old Irish Grammar, GPC (Welsh), Hemon's Breton Grammar |
| Balto-Slavic | Leskien's OCS Handbook, Lithuanian reference grammar, Wade's Russian |
| Indo-Iranian | Whitney's Sanskrit Grammar, Skjaervo's Avestan, Lambton's Persian |
| Italic | Buck's Oscan-Umbrian Grammar, Allen's Vox Latina |
| Hellenic | Ventris & Chadwick, Allen's Vox Graeca |
| Semitic | Fischer's Arabic Grammar, Leslau's Amharic |
| Turkic | Tekin's Old Turkic Grammar, Lewis's Turkish Grammar |
| Uralic | Karlsson's Finnish, Rounds's Hungarian, Viitso's Estonian |
