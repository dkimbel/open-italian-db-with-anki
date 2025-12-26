"""Italian article determination based on orthography.

Determines correct definite, indefinite, and partitive articles
for Italian nouns and adjectives based on gender, number, and spelling.

The algorithm is 95-98% rule-based. Exceptions are handled via a
hardcoded dictionary for loanwords where Italian pronunciation
differs from orthographic expectations.
"""

import re
from functools import lru_cache
from typing import Literal

Pattern = Literal["vowel", "lo", "consonant"]
Gender = Literal["m", "f"]
Number = Literal["singular", "plural"]


# Exception dictionary for loanwords and historical forms
# Maps: word (lowercase) -> (pattern, source)
EXCEPTIONS: dict[str, tuple[Pattern | Literal["gli"], str]] = {
    # H-initial loanwords: Italian has no /h/ phoneme, so these are pronounced
    # with an initial vowel sound and take l'/gli rather than il/i.
    "habitat": ("vowel", "exception:silent_h"),
    "hacker": ("vowel", "exception:silent_h"),
    "haiku": ("vowel", "exception:silent_h"),
    "hall": ("vowel", "exception:silent_h"),
    "halloween": ("vowel", "exception:silent_h"),
    "hamburger": ("vowel", "exception:silent_h"),
    "hammam": ("vowel", "exception:silent_h"),
    "handicap": ("vowel", "exception:silent_h"),
    "hangar": ("vowel", "exception:silent_h"),
    "happening": ("vowel", "exception:silent_h"),
    "harakiri": ("vowel", "exception:silent_h"),
    "hardware": ("vowel", "exception:silent_h"),
    "harem": ("vowel", "exception:silent_h"),
    "hashish": ("vowel", "exception:silent_h"),
    "herpes": ("vowel", "exception:silent_h"),
    "hi-fi": ("vowel", "exception:silent_h"),
    "hifi": ("vowel", "exception:silent_h"),
    "hinterland": ("vowel", "exception:silent_h"),
    "hippie": ("vowel", "exception:silent_h"),
    "hit": ("vowel", "exception:silent_h"),
    "hobbit": ("vowel", "exception:silent_h"),
    "hobby": ("vowel", "exception:silent_h"),
    "hockey": ("vowel", "exception:silent_h"),
    "holding": ("vowel", "exception:silent_h"),
    "home": ("vowel", "exception:silent_h"),
    "homepage": ("vowel", "exception:silent_h"),
    "horror": ("vowel", "exception:silent_h"),
    "hostess": ("vowel", "exception:silent_h"),
    "hot dog": ("vowel", "exception:silent_h"),
    "hotel": ("vowel", "exception:silent_h"),
    "hovercraft": ("vowel", "exception:silent_h"),
    "hub": ("vowel", "exception:silent_h"),
    "humour": ("vowel", "exception:silent_h"),
    "humus": ("vowel", "exception:silent_h"),
    # W-initial (typically treated as consonant in Italian)
    "web": ("consonant", "exception:w_consonant"),
    "webcam": ("consonant", "exception:w_consonant"),
    "webinar": ("consonant", "exception:w_consonant"),
    "website": ("consonant", "exception:w_consonant"),
    "weekend": ("consonant", "exception:w_consonant"),
    "welfare": ("consonant", "exception:w_consonant"),
    "western": ("consonant", "exception:w_consonant"),
    "whisky": ("consonant", "exception:w_consonant"),
    "whiskey": ("consonant", "exception:w_consonant"),
    "widget": ("consonant", "exception:w_consonant"),
    "wifi": ("consonant", "exception:w_consonant"),
    "wi-fi": ("consonant", "exception:w_consonant"),
    "windsurf": ("consonant", "exception:w_consonant"),
    "workshop": ("consonant", "exception:w_consonant"),
    "workstation": ("consonant", "exception:w_consonant"),
    "watt": ("consonant", "exception:w_consonant"),
    "wafer": ("consonant", "exception:w_consonant"),
    "würstel": ("consonant", "exception:w_consonant"),
    "wurstel": ("consonant", "exception:w_consonant"),
    # Historical exceptions
    "dèi": ("gli", "exception:historical"),  # plural of dio
    "dei": ("gli", "exception:historical"),  # alternate spelling
}


# "Lo" trigger patterns:
# z-, s+consonant, gn-, ps-, pn-, x-, y-, i+vowel (semiconsonant)
_LO_TRIGGERS = re.compile(
    r"^("
    r"[zZ]|"  # z-
    r"[sS][bcdfgklmnpqrstvwxzBCDFGKLMNPQRSTVWXZ]|"  # s + consonant
    r"[gG][nN]|"  # gn-
    r"[pP][sSnN]|"  # ps-, pn-
    r"[xX]|"  # x-
    r"[yY]|"  # y-
    r"[iI][aeiouàèéìòóùAEIOUÀÈÉÌÒÓÙ]"  # i + vowel (semiconsonant)
    r")"
)

# Vowel-initial pattern (including accented vowels)
_VOWELS = re.compile(r"^[aeiouàèéìòóùAEIOUÀÈÉÌÒÓÙ]")


def _get_pattern(word: str) -> tuple[Pattern | Literal["gli"], str]:
    """
    Determine the orthographic pattern for article selection.

    Returns: (pattern, source)
        - pattern: 'vowel', 'lo', 'consonant', or 'gli' (historical)
        - source: 'inferred' or 'exception:<reason>'
    """
    word_lower = word.lower().strip()

    # Check exceptions first
    if word_lower in EXCEPTIONS:
        return EXCEPTIONS[word_lower]

    # Check for "lo" triggers
    if _LO_TRIGGERS.match(word):
        return ("lo", "inferred")

    # Check for vowel start (but i+vowel is a lo-trigger, handled above)
    if _VOWELS.match(word):
        return ("vowel", "inferred")

    # Default: consonant
    return ("consonant", "inferred")


@lru_cache(maxsize=10000)
def get_definite(word: str, gender: Gender, number: Number) -> tuple[str, str]:
    """
    Get the definite article for a word.

    Args:
        word: The word (noun or adjective)
        gender: 'm' for masculine, 'f' for feminine
        number: 'singular' or 'plural'

    Returns:
        Tuple of (article, source)
        - article: 'il', 'lo', 'la', "l'", 'i', 'gli', 'le'
        - source: 'inferred' or 'exception:<reason>'
    """
    pattern, source = _get_pattern(word)

    # Historical exception for "gli dèi"
    if pattern == "gli":
        return ("gli", source)

    # Feminine plural: always "le"
    if gender == "f" and number == "plural":
        return ("le", source)

    # Feminine singular
    if gender == "f" and number == "singular":
        if pattern == "vowel":
            return ("l'", source)
        return ("la", source)

    # Masculine plural
    if gender == "m" and number == "plural":
        if pattern in ("vowel", "lo"):
            return ("gli", source)
        return ("i", source)

    # Masculine singular
    if pattern == "vowel":
        return ("l'", source)
    if pattern == "lo":
        return ("lo", source)
    return ("il", source)


def derive_indefinite(def_article: str, gender: Gender) -> str | None:
    """
    Derive the indefinite article from the definite article and gender.

    Returns None for plural forms (no indefinite article in Italian).

    Args:
        def_article: The definite article ('il', 'lo', 'la', "l'", 'i', 'gli', 'le')
        gender: 'm' for masculine, 'f' for feminine

    Returns:
        The indefinite article ('un', 'uno', 'una', "un'") or None for plurals
    """
    # No indefinite for plurals
    if def_article in ("i", "gli", "le"):
        return None

    mapping = {
        ("il", "m"): "un",
        ("lo", "m"): "uno",
        ("l'", "m"): "un",
        ("la", "f"): "una",
        ("l'", "f"): "un'",
    }
    return mapping.get((def_article, gender))


def derive_partitive(def_article: str) -> str:
    """
    Derive the partitive article from the definite article.

    Partitives combine "di" + definite article.

    Args:
        def_article: The definite article

    Returns:
        The partitive article ('del', 'dello', "dell'", 'della', 'dei', 'degli', 'delle')
    """
    mapping = {
        "il": "del",
        "lo": "dello",
        "l'": "dell'",
        "la": "della",
        "i": "dei",
        "gli": "degli",
        "le": "delle",
    }
    return mapping[def_article]


class ArticleSelector:
    """
    Convenience class for article selection.

    Provides instance methods that wrap the module-level functions.
    Can be extended with custom exceptions if needed.
    """

    def __init__(self, extra_exceptions: dict[str, tuple[Pattern, str]] | None = None):
        """
        Initialize with optional extra exceptions.

        Args:
            extra_exceptions: Additional word -> (pattern, source) mappings
        """
        self.extra_exceptions = extra_exceptions or {}

    def get_definite(self, word: str, gender: Gender, number: Number) -> tuple[str, str]:
        """Get the definite article. See module-level get_definite for details."""
        # Check extra exceptions first
        word_lower = word.lower().strip()
        if word_lower in self.extra_exceptions:
            pattern, source = self.extra_exceptions[word_lower]
            # Re-derive the article from the pattern
            if pattern == "vowel":
                if gender == "f" and number == "plural":
                    return ("le", source)
                if gender == "f" and number == "singular":
                    return ("l'", source)
                if gender == "m" and number == "plural":
                    return ("gli", source)
                return ("l'", source)
            if pattern == "lo":
                if gender == "f":
                    return ("la" if number == "singular" else "le", source)
                return ("lo" if number == "singular" else "gli", source)
            # consonant
            if gender == "f":
                return ("la" if number == "singular" else "le", source)
            return ("il" if number == "singular" else "i", source)

        return get_definite(word, gender, number)

    def get_indefinite(self, word: str, gender: Gender) -> tuple[str | None, str]:
        """
        Get the indefinite article.

        Args:
            word: The word
            gender: 'm' or 'f'

        Returns:
            Tuple of (article, source). Article is None for words that
            only have plural forms.
        """
        def_art, source = self.get_definite(word, gender, "singular")
        indef = derive_indefinite(def_art, gender)
        return (indef, source)

    def get_partitive(self, word: str, gender: Gender, number: Number) -> tuple[str, str]:
        """
        Get the partitive article.

        Args:
            word: The word
            gender: 'm' or 'f'
            number: 'singular' or 'plural'

        Returns:
            Tuple of (article, source)
        """
        def_art, source = self.get_definite(word, gender, number)
        part = derive_partitive(def_art)
        return (part, source)
