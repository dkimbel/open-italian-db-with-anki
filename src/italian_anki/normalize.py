"""Text normalization utilities for matching Italian words across sources."""

import logging
import unicodedata

logger = logging.getLogger(__name__)


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


# ============================================================================
# Italian Orthography Rule for Written Forms
# ============================================================================

# Monosyllables that REQUIRE an accent to distinguish from unaccented homographs
# These must keep their accent in written Italian
ACCENT_WHITELIST = frozenset(
    {
        "ciò",  # that (demonstrative)
        "ché",  # because (archaic/literary)
        "dà",  # gives (verb dare)
        "dì",  # day (noun, archaic)
        "è",  # is (verb essere)
        "fé",  # faith (archaic), or made (archaic verb fare)
        "già",  # already
        "giù",  # down
        "là",  # there
        "lì",  # there
        "né",  # neither/nor
        "piè",  # foot (archaic)
        "più",  # more
        "può",  # can (verb potere)
        "scià",  # shah
        "sé",  # oneself (reflexive pronoun)
        "sì",  # yes
        "tè",  # tea
    }
)

# Words that should NEVER have an accent, even when spelled with one in sources
# (These are single-syllable due to the 'qu' digraph being a single consonant unit)
ACCENT_BLACKLIST = frozenset({"qua", "qui"})

# All accented characters (both uppercase and lowercase)
ACCENTED_CHARS = frozenset("àèéìòóùÀÈÉÌÒÓÙ")

# Accented characters that can appear at end of word (lowercase only)
ACCENTED_FINAL = frozenset("àèéìòóù")

# Vowels for stem analysis
VOWELS = frozenset("aeiouAEIOU")


def _derive_single_word(word: str) -> str | None:
    """Derive written form for a single word (no spaces).

    Returns the written form, or None if derivation fails (e.g., multiple accents).
    Logs a warning for single words with multiple accents.
    """
    if not word:
        return None

    # Count accent marks in this single word
    accent_count = sum(1 for c in word if c in ACCENTED_CHARS)

    if accent_count > 1:
        # Multiple accents in a single word is unusual - log warning
        logger.warning(f"Multiple accents in single word: {word!r}")
        return None

    if accent_count == 0:
        # No accents - word IS the written form
        return word

    # Single accent - apply rules
    last_char = word[-1]

    # Non-final accent: always strip (pedagogical only)
    if last_char not in ACCENTED_FINAL:
        return normalize(word)

    # Final accent - check specific rules
    normalized = normalize(word)

    # Blacklist: qui, qua never take accents in standard Italian
    if normalized in ACCENT_BLACKLIST:
        return normalized

    # Whitelist: mandatory accented monosyllables
    if word in ACCENT_WHITELIST:
        return word

    # Stem vowel test: does the stem (word minus final letter) contain vowels?
    stem = word[:-1]
    if any(c in VOWELS for c in stem):
        # Polysyllable with final accent: keep the accent
        return word
    else:
        # True monosyllable (stem has no vowels): strip the accent
        # Examples: fù → fu, blù → blu, trè → tre
        return normalized


def derive_written_from_stressed(stressed: str) -> str | None:
    """Derive written form from stressed form using Italian orthography rules.

    Italian orthography only requires accents in specific cases:
    1. Final syllable stress (polysyllables): città, perché, parlò
    2. Monosyllable disambiguation: è (is) vs e (and), dà (gives) vs da (from)

    All other accents (e.g., pàrlo, bèlla) are pedagogical pronunciation guides
    and should be stripped for the written form.

    For multi-word phrases, applies the rule to each word individually.

    Args:
        stressed: Form with pedagogical stress marks (e.g., "pàrlo", "parlò",
            or multi-word like "volùto dìre")

    Returns:
        The correct written form, or None if derivation is not confident
        (e.g., a word has multiple accents, empty input)

    Examples:
        >>> derive_written_from_stressed("parlò")
        'parlò'
        >>> derive_written_from_stressed("pàrlo")
        'parlo'
        >>> derive_written_from_stressed("dà")
        'dà'
        >>> derive_written_from_stressed("fù")
        'fu'
        >>> derive_written_from_stressed("città")
        'città'
        >>> derive_written_from_stressed("volùto dìre")
        'voluto dire'
    """
    if not stressed:
        return None

    # Handle multi-word phrases by applying rule to each word
    if " " in stressed:
        words = stressed.split()
        derived_words = [_derive_single_word(w) for w in words]
        # If any word fails, the whole phrase fails
        if any(w is None for w in derived_words):
            return None
        return " ".join(derived_words)  # type: ignore[arg-type]

    # Single word
    return _derive_single_word(stressed)
