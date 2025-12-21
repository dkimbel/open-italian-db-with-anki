"""Database modules for Italian Anki deck generator."""

from italian_anki.db.connection import get_connection, get_engine
from italian_anki.db.schema import (
    definitions,
    form_lookup,
    forms,
    frequencies,
    init_db,
    lemmas,
    metadata,
    sentence_verbs,
    sentences,
    translations,
)

__all__ = [
    "definitions",
    "form_lookup",
    "forms",
    "frequencies",
    "get_connection",
    "get_engine",
    "init_db",
    "lemmas",
    "metadata",
    "sentence_verbs",
    "sentences",
    "translations",
]
