"""Database modules for Italian Anki deck generator."""

from italian_anki.db.connection import get_connection, get_engine
from italian_anki.db.schema import (
    adjective_forms,
    adjective_metadata,
    definitions,
    form_lookup,
    frequencies,
    init_db,
    lemmas,
    metadata,
    noun_forms,
    noun_metadata,
    sentence_lemmas,
    sentences,
    translations,
    verb_forms,
    verb_metadata,
)

__all__ = [
    "adjective_forms",
    "adjective_metadata",
    "definitions",
    "form_lookup",
    "frequencies",
    "get_connection",
    "get_engine",
    "init_db",
    "lemmas",
    "metadata",
    "noun_forms",
    "noun_metadata",
    "sentence_lemmas",
    "sentences",
    "translations",
    "verb_forms",
    "verb_metadata",
]
