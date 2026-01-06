"""Anki deck generation package.

This package generates Anki flashcard decks from the Italian language database.
It is intentionally separate from italian_db to maintain clear separation between
the database/ETL layer and the deck generation layer.
"""

from anki_gen.generator import generate_deck

__all__ = ["generate_deck"]
