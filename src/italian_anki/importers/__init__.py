"""Data importers for Italian Anki deck generator."""

from italian_anki.importers.itwac import import_itwac
from italian_anki.importers.morphit import import_morphit
from italian_anki.importers.wiktextract import import_wiktextract

__all__ = ["import_itwac", "import_morphit", "import_wiktextract"]
