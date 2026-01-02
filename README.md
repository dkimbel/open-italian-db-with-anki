# Open Italian Language Database (with Anki generation)

## What is this?

This is Python code for:
- Building an Italian language SQLite database from a variety of freely-available online sources.
- Using the database to generate [Anki](https://apps.ankiweb.net/) decks.

This tool is optimized for learners of modern standard Italian. Archaic, poetic, literary,
and similar forms are not always present.

> [!WARNING]
> This database is assembled from multiple sources, with some automated inference for missing data
> (e.g. inferring a noun's feminine plural form). _It may contain incorrect information!_
>
> If you find any mistakes, please [open an issue](https://github.com/dkimbel/open-italian-db-with-anki/issues).

## Example Queries

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
SELECT vf.written, vf.stressed, vf.person, vf.number
FROM lemmas l
JOIN verb_forms vf ON l.id = vf.lemma_id
WHERE l.written = 'andare' AND vf.mood = 'indicative' AND vf.tense = 'present';
```

```
written  stressed  person  number
-------  --------  ------  --------
vado     vàdo      1       singular
vai      vài       2       singular
va       và        3       singular
andiamo  andiàmo   1       plural
andate   andàte    2       plural
vanno    vànno     3       plural
```

### Most frequent verbs, with IPA pronunciations

```sql
SELECT l.written, l.ipa, f.freq_raw, f.freq_zipf
FROM lemmas l
JOIN frequencies f ON l.id = f.lemma_id
WHERE l.pos = 'verb'
ORDER BY f.freq_raw DESC
LIMIT 10;
```

```
written  ipa          freq_raw    freq_zipf
-------  -----------  ----------  ---------
fare     /ˈfa.re/     11871786    6.80
potere   /poˈte.re/   10480649    6.74
essere   /ˈɛs.se.re/  9390735     6.69
dovere   /doˈve.re/   6786006     6.55
avere    /aˈve.re/    5131032     6.43
volere   /voˈle.re/   3349064     6.25
dire     /ˈdi.re/     3313374     6.24
andare   /anˈda.re/   1728924     5.96
dare     /ˈda.re/     1622741     5.93
sapere   /saˈpe.re/   1514490     5.90
```

### Example sentences with translations

```sql
SELECT ita.text AS italian, eng.text AS english
FROM sentences ita
JOIN translations t ON ita.sentence_id = t.ita_sentence_id
JOIN sentences eng ON eng.sentence_id = t.eng_sentence_id
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

# Download source data (~1.3GB, skips existing)
task download-all

# Build database
task import-all

# Sanity checks / validation
task verify-db

# Check stats
task stats
```

## Data Sources & Licenses

**No scraping**: All data is downloaded from freely available, pre-packaged datasets
published by their respective projects. See `data-licenses/` for full license texts.

| Source | License | Role |
|--------|---------|------|
| [Wiktextract](https://kaikki.org) | CC-BY-SA 3.0 + GFDL | Lemmas, conjugations, definitions |
| [Morph-it!](https://docs.sslmit.unibo.it) | CC-BY-SA 2.0 + LGPL | Real Italian orthography |
| [ItWaC](https://github.com/franfranz/Word_Frequency_Lists_ITA) | MIT | Frequency data |
| [Tatoeba](https://tatoeba.org) | CC-BY 2.0 FR | Example sentences |

## What's In The Database

- **100k+ lemmas** (verbs, nouns, adjectives) with stress-marked forms to aid pronunciation
- **945k+ inflected forms** like verb conjugations and gendered versions of nouns and adjectives
- **Frequency data** from 1.5B word Italian web corpus (ItWaC)
- **950k+ example sentences** with English translations (Tatoeba)
- **Full data provenance**: every form tracks where it came from (`form_origin`, `written_source`)

## Repository Structure

```
open-italian-db-with-anki/
├── italian.db              # SQLite database (generated, not committed)
├── src/italian_db/         # Python source code
│   ├── db/                 #   Database schema and connection
│   │   ├── schema.py       #     SQLAlchemy table definitions
│   │   └── connection.py   #     Database session management
│   ├── importers/          #   Data import modules
│   │   ├── wiktextract.py  #     Lemmas, forms, definitions from Wiktionary
│   │   ├── morphit.py      #     Real Italian orthography enrichment
│   │   ├── itwac.py        #     Word frequency data
│   │   └── tatoeba.py      #     Example sentences with translations
│   ├── normalize.py        #   Text normalization (accents, unicode)
│   ├── articles.py         #   Italian definite article rules
│   └── cli.py              #   Command-line interface
├── data/                   # Downloaded source data (~1.3GB, not committed)
│   ├── wiktextract/        #   Kaikki.org dictionary extract
│   ├── morphit/            #   Morph-it! lexicon
│   ├── itwac/              #   ItWaC frequency lists
│   └── tatoeba/            #   Sentence corpus
├── data-licenses/          # Full license texts for each data source
├── tests/                  # Test suite
├── Taskfile.yml            # Task runner commands
├── DATA_SOURCES.md         # Detailed import/ETL pipeline documentation
├── project_spec.md         # Full project specification
└── pyproject.toml          # Python project configuration
```

## Data Provenance

The database tracks exactly where each piece of information comes from:

### Form origin (`form_origin` column)

Where the grammatical form itself came from:
- `wiktextract` — directly from Wiktionary conjugation/declension tables
- `inferred:base_form` — derived as the citation form of a lemma
- `inferred:invariable` — marked as invariable (same form for all numbers)
- `wiktextract:gender_fallback` — gender inferred from Wiktionary patterns
- `alt_of` — alternative form entry in Wiktionary

### Written source (`written_source` column)

Where the correct Italian spelling came from:
- `morphit` — authoritative spelling from Morph-it! academic lexicon
- `wiktionary` — from Wiktionary form-of entry
- `derived:orthography_rule` — computed using Italian spelling rules
- `fallback:no_accent` — stress mark simply removed (for unaccented words)
- `hardcoded:loanword` — manually specified for French loanwords

## License

**Code**: MIT

**Database and Anki decks**: The generated database incorporates content from
multiple copyleft sources. The combined work is subject to **CC-BY-SA 3.0**
(the most restrictive compatible license among the sources).

If you redistribute the database or Anki decks derived from it:
1. **Attribution required**: Credit Wiktionary/Wiktextract, Morph-it!, Tatoeba, and ItWaC
2. **Share-alike required**: Distribute under CC-BY-SA 3.0 or a compatible license

## Development

```bash
task check          # Run all checks (format, lint, typecheck, test)
task test           # Run tests only
```

## Acknowledgments

This project was originally inspired by [Lisardo's exceptional KOFI method](https://www.asiteaboutnothing.net/w_ultimate_italian_conjugation.php)
and [Anki deck](https://ankiweb.net/shared/info/1891639832).

The project was developed largely using **Claude Code** (Anthropic's Claude Opus 4.5)
for implementation and documentation.
