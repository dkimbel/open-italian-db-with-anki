"""Data importers for Italian Anki deck generator."""

from italian_db.importers.frequency_ranking import compute_pos_frequency_ranks
from italian_db.importers.opensubtitles import import_opensubtitles
from italian_db.importers.paisa import import_paisa
from italian_db.importers.tatoeba import import_tatoeba
from italian_db.importers.verb_irregularity import import_verb_irregularity
from italian_db.importers.wiktextract import (
    enrich_missing_feminine_plurals,
    generate_gendered_participles,
    import_wiktextract,
)

__all__ = [
    "compute_pos_frequency_ranks",
    "enrich_missing_feminine_plurals",
    "generate_gendered_participles",
    "import_opensubtitles",
    "import_paisa",
    "import_tatoeba",
    "import_verb_irregularity",
    "import_wiktextract",
]
