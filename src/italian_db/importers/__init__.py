"""Data importers for Italian Anki deck generator."""

from italian_db.importers.itwac import import_itwac
from italian_db.importers.morphit import import_morphit
from italian_db.importers.tatoeba import import_tatoeba
from italian_db.importers.verb_irregularity import import_verb_irregularity
from italian_db.importers.wiktextract import (
    enrich_missing_feminine_plurals,
    generate_gendered_participles,
    import_wiktextract,
)

__all__ = [
    "enrich_missing_feminine_plurals",
    "generate_gendered_participles",
    "import_itwac",
    "import_morphit",
    "import_tatoeba",
    "import_verb_irregularity",
    "import_wiktextract",
]
