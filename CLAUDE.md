# Italian Anki Deck Generator

Generate Anki flashcard decks for learning Italian using linguistic databases.

## Quick Reference

- **Full spec**: See `project_spec.md` for complete details
- **Run checks**: `task check`
- **Run tests**: `task test`
- **Download data**: `task download-all` (skips existing files, ~1.3GB)
- **Import data**: `task import-wiktextract` (idempotent)
- **Enrichment**: `task import-morphit && task import-itwac`

## Engineering Principles

- **Database schema integrity and consistency.** The schema should be linguistically sound. Where possible, it should be consistent across different parts of speech.
- **Data corectness.** If at all possible, NEVER 'synthesize' or 'infer' ANYTHING. For example: NEVER assume that because a singlar ends in `-e`, there must be a plural ending in `-i`. If at all possible, NEVER rely on ANY heuristics.
- **Thoroughness.** NEVER EVER, under ANY circumstances, take ANY shortcuts.
- **Code readability, organization, and documentation.** These are critical, both for humans and AIs.
- **Types and dataclasses.** Favor static typechecking, dataclasses, and enums.
- **Changes welcome.** Don't hesitate to propose database schema changes! They're still easy to make, and we want to improve the schema whenever we can. The structure of our repository and its ETL pipeline is also subject to change. (Relatedly, do NOT assume that pre-existing code is always correct, or always has patterns that should be followed. There are likely some bugs, mistakes, and shortcuts in past code.)
- **Preserve context.** Our database should make it clear where any given piece of information comes from.
- **Defensive, explicit parsing.** When parsing external data sources, be DEFENSIVE and EXPLICIT. Use data classes to represent intermediate forms. Prefer stable identifiers over indices.

## Database Stats

Run `task stats` to see current database statistics.

## Key Files

- `italian.db` - SQLite database (generated, not committed)
- `src/italian_anki/importers/` - Data importers
- `src/italian_anki/db/` - Database schema and connection
- `src/italian_anki/normalize.py` - Text normalization utilities
- `data/` - Source data files (not committed, ~1.3GB)

## Conventions

- **Taskfile variables over CLI_ARGS**: Prefer `{{if .VAR}}--flag{{end}}` syntax over `{{.CLI_ARGS}}` passthrough. This allows `task foo VAR=1` instead of `task foo -- --flag`.
