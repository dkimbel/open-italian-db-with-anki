# Italian Anki Deck Generator

Generate Anki flashcard decks for learning Italian using linguistic databases.

## Quick Reference

- **Full spec**: See `project_spec.md` for complete details
- **Run checks**: `task check`
- **Run tests**: `task test`
- **Download data**: `task download-all` (skips existing files, ~1.3GB)
- **Import data**: `task import-wiktextract` (idempotent)
- **Enrichment**: `task import-morphit && task import-itwac`
- **Generate deck**: `task generate-deck` (creates `output/italian_verbs.apkg`)
- **Preview card**: `task preview-card VERB=parlare` (opens in browser)

## Engineering Principles

- **Database schema integrity and consistency.** The schema should be linguistically sound. Where possible, it should be consistent across different parts of speech.
- **Data correctness.** Prefer NEVER to 'synthesize' or 'infer' ANYTHING. For example: do NOT assume that because a singular ends in `-e`, there must be a plural ending in `-i`. We can ONLY rely on heuristics when they are 100% accurate, or can be supplemented by a SMALL and COMPLETE list of hardcoded exceptions.
- **Thoroughness.** NEVER EVER, under ANY circumstances, take ANY shortcuts.
- **Code readability, organization, and documentation.** These are critical, both for humans and AIs.
- **Types and dataclasses.** Favor static typechecking, dataclasses, and enums.
- **Changes welcome.** Don't hesitate to propose database schema changes! They're still easy to make, and we want to improve the schema whenever we can. The structure of our repository and its ETL pipeline is also subject to change. (Relatedly, do NOT assume that pre-existing code is always correct, or always has patterns that should be followed. There are likely some bugs, mistakes, and shortcuts in past code.)
- **Preserve context.** Our database should make it clear where any given piece of information comes from.
- **Defensive, explicit parsing.** When parsing external data sources, be DEFENSIVE and EXPLICIT. Use data classes to represent intermediate forms. Prefer stable identifiers over indices.

## Database Stats

Run `task stats` to see current database statistics.

## Source Data Exploration

DuckDB-powered interactive sessions for exploring raw source data:

| Task | Data | Use case |
|------|------|----------|
| `task wikt-query` | Wiktextract JSONL | Dictionary entries, forms, definitions |
| `task tatoeba-query` | Tatoeba TSV | Italian/English sentences, translations |
| `task morphit-query` | MorphIt TSV | Surface forms, lemmas, POS tags |
| `task itwac-query` | ItWaC CSV | Word frequencies, Zipf scores |
| `task partut-query` | ParTUT CoNLL-U | Morphologically-tagged sentences |
| `task data-query` | **All sources** | Cross-source queries |

### Wiktextract

```bash
task wikt-query
```

```python
# Count entries by POS
conn.sql("SELECT pos, COUNT(*) FROM wikt GROUP BY pos ORDER BY 2 DESC")

# Find a specific word
conn.sql("SELECT * FROM wikt WHERE word = 'signora'")

# Analyze head_template args (UNNEST for nested arrays)
conn.sql("""
    SELECT word, t.args.f, t.args.m
    FROM wikt, UNNEST(head_templates) as t
    WHERE pos = 'noun' AND (t.args.f IS NOT NULL OR t.args.m IS NOT NULL)
    LIMIT 10
""")
```

### Tatoeba

```bash
task tatoeba-query
```

```python
# Find Italian sentences with translations
conn.sql("SELECT * FROM translations WHERE italian LIKE '%casa%' LIMIT 5")

# Sentences with audio
conn.sql("SELECT t.*, a.username FROM translations t JOIN audio a ON t.ita_id = a.sentence_id LIMIT 5")
```

### MorphIt

```bash
task morphit-query
```

```python
# All forms of a verb
conn.sql("SELECT * FROM morphit WHERE lemma = 'parlare'")

# Verb POS tags
conn.sql("SELECT DISTINCT pos_tag FROM morphit WHERE pos_tag LIKE 'VER:%'")
```

### ItWaC

```bash
task itwac-query
```

```python
# Frequency of a word's forms
conn.sql("SELECT * FROM itwac WHERE lemma = 'parlare' ORDER BY Freq DESC")

# Top lemmas by frequency
conn.sql("SELECT lemma, SUM(Freq) as total FROM itwac GROUP BY lemma ORDER BY total DESC LIMIT 20")
```

### ParTUT

```bash
task partut-query
```

```python
# Find verbs in subjunctive mood
conn.sql("SELECT DISTINCT lemma, mood, tense FROM verbs WHERE mood = 'Sub' LIMIT 20")

# All forms of "essere" with their grammatical features
conn.sql("SELECT form, lemma, mood, tense, person, number FROM verbs WHERE lemma = 'essere'")

# Count verb tokens by mood and tense
conn.sql("SELECT mood, tense, COUNT(*) as n FROM verbs GROUP BY mood, tense ORDER BY n DESC")
```

### Cross-source queries

```bash
task data-query
```

```python
# Check if a Wiktextract word has ItWaC frequency
conn.sql("SELECT w.word, i.Freq, i.Zipf FROM wikt w LEFT JOIN itwac i ON w.word = i.lemma WHERE w.word = 'casa'")
```

## Card Preview and Screenshots

To preview Anki cards during development:

```bash
# Generate and open HTML preview in browser
task preview-card VERB=parlare

# Or use the CLI directly
uv run anki-gen preview parlare
open output/preview.html
```

The preview HTML supports:
- Light/dark mode toggle
- OS-level dark mode detection
- Front and back card views
- All CSS styling from actual Anki cards

### Taking Screenshots with Chrome Headless

To capture screenshots programmatically (e.g., for reviewing changes):

```bash
# Take screenshot of preview
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --headless --screenshot=output/card-preview.png \
  --window-size=800,1000 \
  file:///Users/dk/Workspace/Italian/open-italian-db-with-anki/output/preview.html

# View the screenshot
open output/card-preview.png
```

This is useful for:
- Rapid iteration on card design
- Visual regression testing
- Documenting card appearance

## Key Files

- `italian.db` - SQLite database (generated, not committed)
- `src/italian_db/importers/` - Data importers
- `src/italian_db/db/` - Database schema and connection
- `src/italian_db/normalize.py` - Text normalization utilities
- `src/anki_gen/` - Anki deck generation and card templates
- `data/` - Source data files (not committed, ~1.3GB)
- `output/` - Generated files (deck, previews, screenshots)

## Conventions

- **Taskfile variables over CLI_ARGS**: Prefer `{{if .VAR}}--flag{{end}}` syntax over `{{.CLI_ARGS}}` passthrough. This allows `task foo VAR=1` instead of `task foo -- --flag`.
