# Italian Anki Deck Generator

Generate Anki flashcard decks for learning Italian using linguistic databases.

## Commands

- `task check` - Run all checks (lint, typecheck)
- `task test` - Run tests
- `task stats` - Database statistics
- `task download-all` - Download source data (~2.5GB, skips existing)
- `task import-wiktextract` - Import dictionary data (idempotent)
- `task import-frequencies` - Compute frequency data from Stanza sentence tokens
- `task generate-deck` - Create `output/italian_verbs.apkg`
- `task preview-card VERB=parlare` - Preview card in browser

### Data Exploration

DuckDB sessions for exploring raw source data: `task wikt-query`, `task tatoeba-query`, `task data-query`.

## Engineering Principles

- **NEVER synthesize or infer data.** Do NOT assume patterns (e.g., singular `-e` → plural `-i`). Only use heuristics that are 100% accurate or have a SMALL, COMPLETE list of hardcoded exceptions.
- **No shortcuts.** Be thorough in all implementations.
- **Favor types.** Use static typechecking, dataclasses, and enums.
- **Preserve provenance.** The database should track where each piece of information comes from.
- **Defensive parsing.** When parsing external sources, use dataclasses for intermediate forms. Prefer stable identifiers over indices.
- **Schema changes welcome.** Don't assume pre-existing code is correct or should be followed.

## Data Model Gotchas

- **Lemma definition**: Entry becomes a lemma if it has at least one sense WITHOUT `form_of`, OR is tagged as a clipping. Pure form-of entries (like "professoressa") become forms, not lemmas.
- **Two-tier relationships**: Check BOTH `lemma_relationships` table (whole-lemma relations like `clipping_of`) AND `definitions.derived_from_lemma_id` (per-definition relations like diminutives).
- **Homonyms vs polysemy**: Same spelling + different etymology = separate lemma rows with different `etymology_number`. Same etymology = one lemma with multiple definitions.
- **IPA storage**: Stored on forms, not lemmas. Query via `*_forms` tables with `is_citation_form = TRUE`.

## Conventions

- **Taskfile variables**: Use `{{if .VAR}}--flag{{end}}` syntax, not `{{.CLI_ARGS}}`. Allows `task foo VAR=1`.
- **Top-level imports**: Prefer module-level imports. Inline imports only to avoid circular dependencies.
