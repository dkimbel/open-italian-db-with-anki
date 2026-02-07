# Open Italian Language Database (with Anki generation)

## What is this?

This is Python code for:
- Building an Italian language SQLite database from a variety of freely-available online sources.
- Using the database to generate [Anki](https://apps.ankiweb.net/) decks.

This tool is optimized for learners of modern standard Italian. Archaic, poetic, literary,
and similar forms are not always present.

> [!WARNING]
> _This database may contain incorrect information!_
> - The source data we rely on can contain mistakes.
> - Inference done by this repository could introduce errors. For example, we synthesize the feminine plural forms
> of nouns when our source data doesn't provide them. (You can always tell when this happened; see the `Data Provenance`
> section below.)
>
> If you find any mistakes, please [open an issue](https://github.com/dkimbel/open-italian-db-with-anki/issues).

## Schema and Example Queries

[See the database schema as a Mermaid diagram here.](https://mermaid.live/edit#pako:eNrdV1tvmzAU_ivIz2mVJmkuvHbaS18q7W2KhE7gQNwZm9mmbZrmv892SArB0LSdtmkoUoTP53P9jn3YklgkSEISBCi_UMgk5EseuIdhnoMKtod3-1CuA5oEd7f1RaUl5VnwKKnWyLslkRKljNEDMH-oFCYeUSGUZ5UWcFjdLY8mH1CuolTI_Byv7aILMTKir38uoFwI37JRp3yaQBUY61PHC5RK-DzjZb5C6RFkyJOmYCUEC6hyCQPmk3DMQNOHhlv3xm7AYIXMVxerKxKSZpT7FMZUG4WCO5tdBcxRQwIa2jU8luvuduCtGJRPlFGQG1-CJXBFTThUb04Vr0Bh5Iz3M6KQgouccmCR3hTYFQGVErOSgWHJ5gNRFIY7yHVUgCGZ9FVZYi409gDSUpeyD1CA1DSmBesDqXJ1X_LYUqAHFTNQiqY03pe2sye4cVl5UsZFyf_5rm23T2fD9XeI0UIf9pmqU6h-QCBw8x-tTaxeBSnl1FTfVZB5z4y9pDvQT7apq9gn2nSfzMgRpzOlZ4k9IVoPYuOgYasleVdHH9u-n00d5aolA5J73LfIf8ThY_SZRHwPuf86N1_L8QmCUp4yq8WYabHwbOLY1EUSmXNXrWlxHuokM7XQquxa3G9hWcZEMzRXXA1Z583uO5pqDqYSf5bIY4rqA0mPhSxK5YnCqo0kPNbXUyagkjzTIu1UZ291RV9HpZq39o41zvp8PYiirlmXAc-8Q9yTLy9u9Kgq7KmchqhpcNA-qtBkvRNUM1UN7S8vFxdiWx-Iw2BJ1qCWxId8OZm8zgE3hpzWhqa-nXD7Ki0WfJy47Ba_87W54A1_mvdRC9wUt32p3Rbnb7IBdPt-eim8EYDn0Grt8GDe61X9AOn1aNto5Rq03TsVvkFxu8Gw-lX323DD7yUhA5Kj-RihifkidX2yJHqNuQnJYhKQP6zOncFBqcW3DY9JqGWJAyJFma1JmAJT5q0sTH6w-p49QArg34U4vmbSmql2u5vxxvKAhPPhzIFJuCVPJLyYjkeXk_nV8Go-nE4nw-loQDYkHE8uJ6Ox-c2ms-F8MZ9MdgPy7PSPLq_H89lsMZ8tjGwxvN79AgCSud0)

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

\* OpenSubtitles licensing is layered and ambiguous. META-SHARE catalogs the OPUS corpus as CC BY-NC-SA 3.0; OpenSubtitles.org's ToS restrict use to noncommercial, scientific, and educational purposes. See `data-licenses/opensubtitles.txt` for full details.

\*\* Profilo (Spinelli & Parizzi, 2010) is a published book with word lists hosted publicly for educational use. NVdB (De Mauro, 2016) was published online by Internazionale without an explicit license. Both are used here for noncommercial educational purposes with full attribution. See `data-licenses/profilo.txt` and `data-licenses/nvdb.txt`.

## What's In The Database

- **100k+ lemmas** (verbs, nouns, adjectives) with stress-marked forms to aid pronunciation
- **945k+ inflected forms** like verb conjugations and gendered versions of nouns and adjectives
- **Frequency data** derived from Stanza-parsed sentence tokens (all POS)
- **CEFR levels** (A1-B2) from expert-curated Profilo della lingua italiana
- **NVdB usage tiers** (fondamentale/alto uso/alta disponibilità) from De Mauro (2016)
- **6M+ example sentences** with English translations (Tatoeba + OpenSubtitles)
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
│   │   ├── morphit.py      #     Fallback functions for written form derivation
│   │   ├── tatoeba.py      #     Tatoeba example sentences with translations
│   │   ├── opensubtitles_sentences.py  # OpenSubtitles parallel sentences
│   │   ├── sentence_tokens.py         # Stanza POS-tagged token import
│   │   ├── frequency_from_tokens.py   # Frequency computation from tokens
│   │   ├── profilo.py                 # Profilo CEFR level import
│   │   └── nvdb.py                    # NVdB usage tier import
│   ├── normalize.py        #   Text normalization (accents, unicode)
│   ├── articles.py         #   Italian definite article rules
│   └── cli.py              #   Command-line interface
├── data/                   # Downloaded source data (~2.5GB, not committed)
│   ├── wiktextract/        #   Kaikki.org dictionary extract
│   ├── tatoeba/            #   Tatoeba sentence corpus
│   ├── opensubtitles/      #   OpenSubtitles v2024 parallel sentences
│   ├── profilo/            #   Profilo CEFR word lists (HTML)
│   └── nvdb/               #   NVdB usage tier list (HTML)
├── data-licenses/          # Full license texts for each data source
├── tests/                  # Test suite
├── Taskfile.yml            # Task runner commands
├── DATA_SOURCES.md         # Detailed import/ETL pipeline documentation
├── project_spec.md         # Full project specification
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
- `wiktextract:gender_fallback` — gender inferred from Wiktionary patterns
- `alt_of` — alternative form entry in Wiktionary

### Written source (`written_source` column)

Where the correct Italian spelling came from:
- `wiktionary` — from Wiktionary form-of entry
- `derived:orthography_rule` — computed using Italian spelling rules
- `fallback:no_accent` — stress mark simply removed (for unaccented words)
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
