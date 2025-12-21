# Italian Anki Deck Generator

Generate Anki flashcard decks for learning Italian using linguistic databases.

## Quick Reference

- **Full spec**: See `project_spec.md` for complete details
- **Run checks**: `task check`
- **Run tests**: `task test`
- **Import data**: `task import-wiktextract`

## Project Status

ETL Pipeline:
- [x] Phase 2: Wiktextract import (12,888 verbs, 721k forms)
- [ ] Phase 3: Morph-it! enrichment
- [ ] Phase 4: ItWaC frequency import
- [ ] Phase 5: Tatoeba sentences

## Key Files

- `src/italian_anki/importers/` - Data importers
- `src/italian_anki/db/` - Database schema and connection
- `src/italian_anki/normalize.py` - Text normalization utilities
- `data/` - Source data files (not committed, ~1.3GB)
