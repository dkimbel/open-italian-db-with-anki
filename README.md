# Open Italian Language Database (with Anki generation)

## What is this?

This is Python code for:
- Building an Italian language SQLite database from a variety of freely-available online sources.
- Using the database to generate [Anki](https://apps.ankiweb.net/) decks.

This tool is optimized for learners of modern standard Italian. Archaic, poetic, literary,
and similar forms are not always present.

> [!NOTE]
> This is a work in progress — you may find some inaccuracies.
> If you spot any, please [open an issue](https://github.com/dkimbel/open-italian-db-with-anki/issues).

## Schema and Example Queries

[See the database schema as a Mermaid diagram here.](https://mermaid.live/edit#pako:eNrVV0tv2zAM_iuGzyuwXXvdsMsuA3YbCgiKTTtqZUmjpDRpmv8-Wnbqh2Q36dAN88Uw3_xEkfQxL3QJ-W2WA34RvEbe3KmMHglNw2127L7aRyiXiTL7_m0gWYdC1dkjCudALdGZ1R4LiNj0AmuhjBhG26lbcIdGS10fmPLNBjDSGAQc7F3HPnWvHeCGVRqb15JpSSFrRoyvl2e5mEajdUwkCzaGglsDhZvGYgCtjt0tIFCDKsfkjdYyEzZkzmVMV1BzJ3ZxKJJvQNqI3NphGkUtVGysEI6MaRW8RarC8BRtUhb9YSnt1b85rDl-62gvgEQWxK5Dwh1MjG0DXNGbbSn8hHIllHDAODpRSHiXI-hx5uU91Rsd__8Adgk1Alx-Bn8Pxt4Xyf8phLXUNs7F8XrhJqYrqfUSahBKVqFu2JrLZLGOm2YDjpfc8XlqL0YTo4D7vZCC4yHOBbmyhNVOuJhpUCvdCMVlHEvoCW-JpasyVkiegLartTEzuhpv8SlUJVt1AjU2HVAViFB7yenOHK6xbOgugXLMcLprGF82hEZT1S-xK-88LrNNuC3CyGUR6zf3XnXQTGT67CqEXx5UIcBek1eh0XgbXZbWGkP-OFArqXlPfxKmSoqrByYUe9kf-sgKqJCuwo7axTWRBY0YhsTU2pUb5gTN62vMtwoL1tmjxnLioj16whYu6TNnWfKbaJqqjp2eFYblaT3hcJclTzc-R5N9iCCKDqhrpfizRFnb--a2l-wO7XLZnH4AdbHBlhc0qJxK2CedBXYSsXDqEdVP1tordsTrl8FCN4a6ZsmS5l-4Iz-n0dbPEPqz3QpzUcF1RZscNgFJjjW41WE0djnbncJo3ohSYNdYz9sshTz5VXl-vrnRx_G-f5vd5Vtu7_KU3GjVXJWbr0qrwuOFYEXweTZgV0Wn829VNDG7Xo9iMpBWsxt3-FW744a7ntvQOVc9p-qyVSDKWWHokL3OpEm1woGwKD5tPLNoVqS7vjKSzz9keQP01yVK-q8-5m4LTfjDpuLgXrr8RALcO_3joAqiO_RAFNS-3tJnxaVtv72h84P-n3wQM1z91HpEqDH4OVsJO89nKhlHxI-dQghj338f2ncbwlNn5tPpNwt3EuM)

### Gender-changing plural (uovo → uova)

```sql
-- "uovo" (egg), an irregular noun whose singular is masculine but plural is feminine
SELECT nf.definite_article AS article, nf.written, nf.gender, nf.number
FROM lemmas l
JOIN noun_forms nf ON l.id = nf.lemma_id
WHERE l.written = 'uovo';
```

```
article  written  gender  number
-------  -------  ------  --------
le       uova     f       plural
l'       uovo     m       singular
```

### Irregular verb conjugation (andare)

```sql
SELECT vf.written, vf.person, vf.number
FROM lemmas l
JOIN verb_forms vf ON l.id = vf.lemma_id
WHERE l.written = 'andare' AND vf.mood = 'indicative' AND vf.tense = 'present';
```

```
written  person  number
-------  ------  --------
vado     1       singular
vai      2       singular
va       3       singular
andiamo  1       plural
andate   2       plural
vanno    3       plural
```

### Most frequent verbs, with IPA pronunciations

```sql
SELECT l.written, vf.ipa, f.freq_raw, f.freq_zipf
FROM lemmas l
JOIN frequencies f ON l.id = f.lemma_id
JOIN verb_forms vf ON l.id = vf.lemma_id AND vf.is_citation_form = 1
WHERE l.pos = 'verb'
ORDER BY f.freq_raw DESC
LIMIT 10;
```

```
written  ipa           freq_raw  freq_zipf
-------  -----------   --------  ---------
essere   /ˈɛs.se.re/  3418208   7.75
avere    /aˈve.re/     1896926   7.50
fare     /ˈfa.re/      664601    7.04
volere   /voˈle.re/    430397    6.85
potere   /poˈte.re/    419482    6.84
dire     /ˈdi.re/      375834    6.79
dovere   /doˈve.re/    373282    6.79
andare   /anˈda.re/    365000    6.78
stare    /ˈsta.re/     337454    6.75
sapere   /saˈpe.re/    321769    6.73
```

### Example sentences with translations

```sql
SELECT ita.text AS italian, eng.text AS english
FROM sentences ita
JOIN translations t ON ita.id = t.ita_sentence_id
JOIN sentences eng ON eng.id = t.eng_sentence_id
WHERE ita.text LIKE '%mangiare%'
LIMIT 3;
```

```
italian                                                    english
---------------------------------------------------------  ----------------------------------------
Chi non lavora non ha diritto di mangiare.                 He who does not work, bless him, has no right to eat.
Non devi mangiare troppi gelati e troppi spaghetti.        You must not eat too much ice-cream and spaghetti.
Che ne dici di mangiare fuori stasera tanto per cambiare?  How about eating out this evening for a change?
```

### Adjective allomorphs (bello → bel/bell'/bei/begli)

```sql
SELECT af.written, af.gender, af.number
FROM lemmas l
JOIN adjective_forms af ON l.id = af.lemma_id
WHERE l.written = 'bello';
```

```
written  gender  number
-------  ------  --------
bello    m       singular
bel      m       singular
bell'    m       singular
bella    f       singular
bell'    f       singular
belli    m       plural
bei      m       plural
begli    m       plural
belle    f       plural
```

## Quick Start

```bash
# Install dependencies
uv sync

# Download pinned data from GitHub release (~2.5GB compressed, skips existing)
# (Includes pre-computed Stanza JSONL so you don't need a GPU)
task download-all

# Build database
task import-all

# Sanity checks / validation
task verify-db

# Check stats
task stats
```

> [!TIP]
> `task download-all` fetches pinned artifacts from a GitHub Release.
> To fetch fresh data from original upstream sources, use `task download-upstream`.

## Data Sources & Licenses

All data is downloaded from freely available, pre-packaged datasets published by their respective
projects. See `data-licenses/` for full license texts and detailed discussion.

| Source | License | Role |
|--------|---------|------|
| [Wiktextract](https://kaikki.org) | CC-BY-SA 3.0 + GFDL | Lemmas, conjugations, definitions |
| [OpenSubtitles v2024](https://opus.nlpl.eu/OpenSubtitles/v2024/en-it) | Noncommercial\* | Example sentences, frequency data |
| [Tatoeba](https://tatoeba.org) | CC-BY 2.0 FR | Example sentences, frequency data |
| [Profilo della lingua italiana](https://www.unistrapg.it/profilo_lingua_italiana/) | No explicit license\*\* | CEFR levels (A1-B2) |
| [NVdB](https://www.internazionale.it/opinione/tullio-de-mauro/2016/12/23/il-nuovo-vocabolario-di-base-della-lingua-italiana) | No explicit license\*\* | Usage tiers (FO/AU/AD) |

Frequency data and sentence-level NLP annotations are computed using [Stanza](https://stanfordnlp.github.io/stanza/).

\* OpenSubtitles licensing is layered and ambiguous. META-SHARE catalogs the OPUS corpus as CC BY-NC-SA 3.0; OpenSubtitles.org's ToS restrict use to noncommercial, scientific, and educational purposes. See `data-licenses/opensubtitles.txt` for full details.

\*\* Profilo (Spinelli & Parizzi, 2010) is a published book with word lists hosted publicly for educational use. NVdB (De Mauro, 2016) was published online by Internazionale without an explicit license. Both are used here for noncommercial educational purposes with full attribution. See `data-licenses/profilo.txt` and `data-licenses/nvdb.txt`.

## What's In The Database

- **100k+ lemmas** (verbs, nouns, adjectives) with stress-marked forms to aid pronunciation
- **1M+ inflected forms** like verb conjugations and gendered versions of nouns and adjectives
- **Frequency data** derived from Stanza-parsed sentence tokens (all POS)
- **CEFR levels** (A1-B2) from expert-curated Profilo della lingua italiana
- **NVdB usage tiers** (fondamentale/alto uso/alta disponibilità) from De Mauro (2016)
- **5M+ Italian sentences** with English translations (Tatoeba + OpenSubtitles)
- **51M+ sentence tokens** with NLP annotations (POS, mood, tense, dependency relations)
- **Full data provenance**: every form tracks where it came from (`form_origin`, `written_source`)

## Repository Structure

```
open-italian-db-with-anki/
├── italian.db              # SQLite database (generated, not committed)
├── src/italian_db/         # Database builder
│   ├── db/                 #   Database schema and connection
│   │   ├── schema.py       #     SQLAlchemy table definitions
│   │   └── connection.py   #     Database session management
│   ├── importers/          #   Data import modules
│   │   ├── wiktextract.py  #     Lemmas, forms, definitions from Wiktionary
│   │   ├── written_enrichment.py    # Written-form derivation rules
│   │   ├── tatoeba.py               # Tatoeba example sentences
│   │   ├── opensubtitles_sentences.py  # OpenSubtitles parallel sentences
│   │   ├── sentence_tokens.py      # Stanza POS-tagged token import
│   │   ├── frequency_from_tokens.py # Frequency computation from tokens
│   │   ├── frequency_ranking.py     # Per-POS frequency ranking
│   │   ├── verb_irregularity.py     # Verb irregularity classification
│   │   ├── profilo.py               # Profilo CEFR level import
│   │   └── nvdb.py                  # NVdB usage tier import
│   ├── data/               #   Bundled data files
│   │   ├── verb_irregularity_data.py  # Irregularity pattern definitions
│   │   └── semantic_categories.toml   # Semantic category mappings
│   ├── cli.py              #   Command-line interface
│   ├── derivation.py       #   Lemma derivation logic
│   ├── download.py         #   Data download helpers
│   ├── enums.py            #   Shared enumerations
│   ├── normalize.py        #   Text normalization (accents, unicode)
│   ├── articles.py         #   Italian definite article rules
│   ├── queries.py          #   Reusable database queries
│   ├── tags.py             #   Tag processing
│   └── verify.py           #   Database verification checks
├── src/anki_gen/           # Anki deck generator
│   ├── cli.py              #   Anki generation CLI
│   ├── generator.py        #   Deck building logic
│   ├── note_types.py       #   Anki note type definitions
│   ├── templates.py        #   Card HTML/CSS templates
│   ├── preview.py          #   Browser preview server
│   ├── queries.py          #   Card data queries
│   ├── stress.py           #   Stress annotation helpers
│   └── config/verbs.toml   #   Verb card configuration
├── scripts/                # Standalone utility scripts
├── data/                   # Downloaded source data (~2.5GB, not committed)
│   ├── wiktextract/        #   Kaikki.org dictionary extract
│   ├── tatoeba/            #   Tatoeba sentence corpus
│   ├── opensubtitles/      #   OpenSubtitles v2024 parallel sentences
│   ├── profilo/            #   Profilo CEFR word lists (HTML)
│   └── nvdb/               #   NVdB usage tier list (HTML)
├── output/                 # Generated Anki decks and previews (not committed)
├── data-licenses/          # Full license texts for each data source
├── tests/                  # Test suite
├── Taskfile.yml            # Task runner commands
├── DATA_SOURCES.md         # Detailed import/ETL pipeline documentation
├── SPEC.md                 # Full project specification
└── pyproject.toml          # Python project configuration
```

## Data Provenance

The database tracks where each piece of information comes from using a variety
of `*_origin` and `*_source` columns.

### Form origin (`form_origin` column)

Where the grammatical form itself came from:
- `wiktextract` — directly from Wiktionary conjugation/declension tables
- `inferred:base_form` — derived as the citation form of a lemma
- `inferred:invariable` — marked as invariable (same form for all numbers)
- `inferred:singular` — singular form inferred from lemma
- `inferred:two_form` — two-form adjective (same masculine and feminine)
- `inferred:f_pl_from_f_sg` — feminine plural inferred from feminine singular
- `inferred:f_pl_invariable` — feminine plural same as feminine singular
- `inferred:head_template` — derived from Wiktionary head template
- `derived:gender_rule` — gender derived by rule from existing forms
- `wiktextract:gender_fallback` — gender inferred from Wiktionary patterns
- `alt_of` — alternative form entry in Wiktionary
- `hardcoded` — manually specified for known exceptions

### Written source (`written_source` column)

Where the correct Italian spelling came from:
- `wiktionary` — from Wiktionary form-of entry
- `derived:orthography_rule` — computed using Italian spelling rules
- `copied:f_sg` — copied from feminine singular form
- `fallback:no_accent` — stress mark simply removed (for unaccented words)
- `hardcoded` — manually specified for known exceptions
- `hardcoded:loanword` — manually specified for French loanwords

## License

**Code**: MIT

**Database and Anki decks**: The generated database incorporates content from
multiple sources with different licensing terms. The most restrictive are:

- **OpenSubtitles**: Noncommercial use only (see [License Considerations](#license-considerations) below)
- **Wiktextract**: CC-BY-SA 3.0 (share-alike, attribution required)
- **Profilo / NVdB**: No explicit open license (used for noncommercial educational purposes)

If you redistribute the database or Anki decks derived from it:
1. **Noncommercial use only**: Required by OpenSubtitles terms
2. **Attribution required**: Credit Wiktionary/Wiktextract, Tatoeba, OpenSubtitles, Profilo, and NVdB
3. **Share-alike required**: Distribute under CC-BY-SA 3.0 or a compatible license (from Wiktextract)

### License Considerations

The OpenSubtitles data distributed through OPUS has a complex licensing situation.
OPUS itself does not specify a formal license, but META-SHARE catalogs the corpus
as CC BY-NC-SA 3.0, and OpenSubtitles.org's Terms of Service explicitly restrict
use to "non-commercial, scientific and educational purposes." This project treats
the combined database as **noncommercial** accordingly.

Profilo della lingua italiana and NVdB are published scholarly works without
explicit open licenses. Their word-to-level/tier assignments are used here as
factual metadata for noncommercial educational purposes, with full attribution.

See `data-licenses/` for detailed licensing information on each source.

## Development

```bash
task check          # Run all checks (format, lint, typecheck, test)
task test           # Run tests only
```

## Acknowledgments

This project was originally inspired by [Lisardo's exceptional KOFI method](https://www.asiteaboutnothing.net/w_ultimate_italian_conjugation.php)
and [Anki deck](https://ankiweb.net/shared/info/1891639832).
