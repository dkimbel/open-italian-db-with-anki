# Open Italian Database with Anki

A SQLite database of Italian vocabulary for generating Anki flashcard decks,
built from authoritative linguistic sources.

## What's Inside

- **89k+ lemmas** (verbs, nouns, adjectives) with stress-marked forms for learning
- **850k+ inflected forms** with real Italian spelling from Morph-it!
- **Frequency data** from 1.5B word Italian web corpus (ItWaC)
- **950k+ example sentences** with English translations (Tatoeba)

## Quick Start

```bash
# Install dependencies
uv sync

# Download source data (~1.3GB, skips existing)
task download-all

# Build database
task import-all

# Check stats
task stats
```

## Data Sources & Licenses

| Source | License | Role |
|--------|---------|------|
| [Wiktextract](https://kaikki.org) | CC-BY-SA + GFDL | Lemmas, conjugations, definitions |
| [Morph-it!](https://docs.sslmit.unibo.it) | CC-BY-SA 2.0 + LGPL | Real Italian orthography |
| [ItWaC](https://github.com/franfranz/Word_Frequency_Lists_ITA) | MIT | Frequency data |
| [Tatoeba](https://tatoeba.org) | CC-BY 2.0 FR | Example sentences |

## License

**Code:** MIT

**Generated data** (italian.db, Anki decks) incorporates content from:
- Wiktextract (CC-BY-SA 3.0 + GFDL)
- Morph-it! (CC-BY-SA 2.0 + LGPL)
- Tatoeba (CC-BY 2.0 FR)
- ItWaC (MIT)

If you redistribute the database or generated decks, you must comply with
the applicable source licenses (primarily CC-BY-SA share-alike).
