"""Database modules for Italian Anki deck generator."""

from italian_db.db.connection import get_connection, get_engine
from italian_db.db.schema import (
    adjective_forms,
    adjective_metadata,
    definitions,
    frequencies,
    init_db,
    lemmas,
    metadata,
    noun_forms,
    noun_metadata,
    sentences,
    translations,
    verb_forms,
    verb_metadata,
)
from italian_db.enums import POS, DerivationType, GenderClass

__all__ = [
    "POS",
    "DerivationType",
    "GenderClass",
    "adjective_forms",
    "adjective_metadata",
    "definitions",
    "frequencies",
    "get_connection",
    "get_engine",
    "init_db",
    "lemmas",
    "metadata",
    "noun_forms",
    "noun_metadata",
    "sentences",
    "translations",
    "verb_forms",
    "verb_metadata",
]
