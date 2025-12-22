# Italian Anki Deck Generator

Generate Anki flashcard decks for learning Italian using linguistic databases.

## Quick Reference

- **Full spec**: See `project_spec.md` for complete details
- **Run checks**: `task check`
- **Run tests**: `task test`
- **Download data**: `task download-all` (skips existing files, ~1.3GB)
- **Import data**: `task import-wiktextract` (idempotent)
- **Enrichment**: `task import-morphit && task import-itwac`

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
