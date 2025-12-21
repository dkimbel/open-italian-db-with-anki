"""Text normalization utilities for matching Italian words across sources."""

import unicodedata


def normalize(text: str) -> str:
    """Normalize Italian text for matching/lookup.

    Strips all accent marks and converts to lowercase.
    Used to match forms across sources (Wiktextract, Morph-it!, sentences).

    Examples:
        >>> normalize("può")
        'puo'
        >>> normalize("pàrlo")
        'parlo'
        >>> normalize("città")
        'citta'
        >>> normalize("Mangiare")
        'mangiare'
    """
    # NFD decomposition separates base characters from combining diacriticals
    decomposed = unicodedata.normalize("NFD", text)
    # Filter out combining diacritical marks (category "Mn")
    stripped = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
    return stripped.lower()


def tokenize(text: str) -> list[str]:
    """Split Italian text into word tokens.

    Handles common punctuation and returns lowercase tokens.
    Does NOT normalize accents (use normalize() separately if needed).

    Examples:
        >>> tokenize("Lui può parlare.")
        ['lui', 'può', 'parlare']
        >>> tokenize("Dov'è il libro?")
        ["dov'è", 'il', 'libro']
    """
    # Replace common punctuation with spaces, preserving apostrophes within words
    result: list[str] = []
    current_word: list[str] = []

    for char in text:
        if char.isalpha() or char == "'":
            current_word.append(char)
        else:
            if current_word:
                word = "".join(current_word).lower()
                # Strip leading/trailing apostrophes
                word = word.strip("'")
                if word:
                    result.append(word)
                current_word = []

    # Don't forget the last word
    if current_word:
        word = "".join(current_word).lower()
        word = word.strip("'")
        if word:
            result.append(word)

    return result
