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
    noun_metadata,
    sentence_lemmas,
    sentences,
    translations,
    verb_metadata,
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
    "noun_metadata",
    "sentence_lemmas",
    "sentences",
    "translations",
    "verb_metadata",
]
