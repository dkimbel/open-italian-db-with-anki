"""Data importers for Italian Anki deck generator."""

from italian_anki.importers.itwac import import_itwac
from italian_anki.importers.morphit import import_morphit
from italian_anki.importers.tatoeba import import_tatoeba
from italian_anki.importers.wiktextract import (
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
    "import_wiktextract",
]
