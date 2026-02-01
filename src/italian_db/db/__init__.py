"""Database modules for Italian Anki deck generator."""

from italian_db.db.connection import get_connection, get_engine
from italian_db.db.schema import (
    adjective_forms,
    adjective_metadata,
    cefr_levels,
    definitions,
    frequencies,
    init_db,
    lemma_relationships,
    lemmas,
    metadata,
    noun_forms,
    noun_metadata,
    sentence_tags,
    sentence_tokens,
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
    "cefr_levels",
    "definitions",
    "frequencies",
    "get_connection",
    "get_engine",
    "init_db",
    "lemma_relationships",
    "lemmas",
    "metadata",
    "noun_forms",
    "noun_metadata",
    "sentence_tags",
    "sentence_tokens",
    "sentences",
    "translations",
    "verb_forms",
    "verb_metadata",
]
