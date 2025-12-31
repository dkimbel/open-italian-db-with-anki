"""Enumeration types for Italian linguistic data.

These StrEnum classes provide type safety while maintaining backward
compatibility with SQLite string storage. Since StrEnum values serialize
as strings, no database migration is needed.
"""

from enum import StrEnum


class POS(StrEnum):
    """Part of speech classification for lemmas."""

    VERB = "verb"
    NOUN = "noun"
    ADJECTIVE = "adjective"

    @property
    def plural(self) -> str:
        """Return the plural form for display (e.g., 'verbs')."""
        return {
            POS.VERB: "verbs",
            POS.NOUN: "nouns",
            POS.ADJECTIVE: "adjectives",
        }[self]


class GenderClass(StrEnum):
    """Gender classification for nouns.

    - M: masculine only (e.g., libro)
    - F: feminine only (e.g., casa)
    - COMMON_GENDER_FIXED: both genders with identical forms (e.g., cantante)
    - COMMON_GENDER_VARIABLE: both genders with different forms (e.g., collega)
    - BY_SENSE: gender depends on meaning (e.g., il fine=goal vs la fine=end)
    """

    M = "m"
    F = "f"
    COMMON_GENDER_FIXED = "common_gender_fixed"
    COMMON_GENDER_VARIABLE = "common_gender_variable"
    BY_SENSE = "by_sense"


class DerivationType(StrEnum):
    """Morphological derivation type for nouns.

    These indicate size/affect modifications from a base noun.
    The field is nullable; None means no derivation.
    """

    DIMINUTIVE = "diminutive"
    AUGMENTATIVE = "augmentative"
    PEJORATIVE = "pejorative"
