# Italian Anki Deck Generator

Generate Anki flashcard decks for learning Italian using linguistic databases.

## Quick Reference

- **Full spec**: See `project_spec.md` for complete details
- **Run checks**: `task check`
- **Run tests**: `task test`
- **Import data**: `task import-wiktextract` (idempotent)
- **Enrichment**: `task import-morphit && task import-itwac`

## Project Status

ETL Pipeline:
- [x] Phase 2: Wiktextract import (12,888 verbs, 721k forms)
- [x] Phase 3: Morph-it! enrichment (353k forms updated with real spelling)
- [x] Phase 4: ItWaC frequency import (8,284 verbs with frequency data)
- [ ] Phase 5: Tatoeba sentences

**Important**: Stop and ask for review after completing each phase.

## Key Files

- `src/italian_anki/importers/` - Data importers
- `src/italian_anki/db/` - Database schema and connection
- `src/italian_anki/normalize.py` - Text normalization utilities
- `data/` - Source data files (not committed, ~1.3GB)
