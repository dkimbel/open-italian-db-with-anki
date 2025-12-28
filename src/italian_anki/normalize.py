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

# ============================================================================
# Loanword Handling
# ============================================================================
#
# The algorithm strips non-final accents assuming they're pedagogical stress
# marks for Italian pronunciation. However, some loanwords have orthographic
# accents from their source language that should be preserved.
#
# FRENCH LOANWORDS (need whitelist):
#   French spelling uses orthographic accents (é, è, ê, â, etc.) that overlap
#   with Italian accent characters. When these appear in non-final position,
#   the algorithm would incorrectly strip them (e.g., rétro → retro).
#   Solution: Whitelist French loanwords with non-final accents.
#
# GERMAN LOANWORDS (already safe):
#   German uses umlauts (ö, ü) and eszett (ß). These are NOT in ACCENTED_CHARS
#   so the algorithm ignores them. Example: föhn passes through unchanged.
#
# PORTUGUESE LOANWORDS (already safe):
#   Portuguese uses cedillas (ç) and tildes (ã, õ). These are NOT in
#   ACCENTED_CHARS so the algorithm ignores them.
#
# SPANISH LOANWORDS (already safe):
#   Spanish loanwords in Italian typically have FINAL accents (oxytone stress)
#   matching Italian patterns (e.g., colibrì, cincillà). The algorithm
#   correctly preserves final accents on polysyllables.
#
# ENGLISH LOANWORDS (correct behavior):
#   English has no orthographic accents. When English words are borrowed into
#   Italian, any accents (e.g., bàrista from "barrister") are pedagogical
#   pronunciation guides that SHOULD be stripped.
#
# This analysis was performed against the full Wiktextract Italian dictionary
# (~620K entries) to verify that French is the only source of loanwords
# requiring special handling.
# ============================================================================

# French loanwords with accents that must be preserved in written Italian.
# These bypass the normal accent-stripping logic and multi-accent warning.
# The whitelist is checked FIRST in _derive_single_word(), so multi-accent
# French words work correctly when whitelisted.
FRENCH_LOANWORD_WHITELIST: dict[str, str] = {
    # =========================================================================
    # Multi-accent words
    # =========================================================================
    "arrière-pensée": "arrière-pensée",
    "décolleté": "décolleté",
    "défilé": "défilé",
    "démodé": "démodé",
    "négligé": "négligé",
    "séparé": "séparé",
    # =========================================================================
    # Single-accent French loanwords
    # These have Italian-detectable accents (é, è) in non-final position
    # =========================================================================
    "ampère": "ampère",
    "arrière-goût": "arrière-goût",
    "bohémien": "bohémien",
    "brisée": "brisée",  # From phrases like "pasta brisée"
    "brûlé": "brûlé",
    "café-chantant": "café-chantant",
    "crépon": "crépon",
    "d'emblée": "d'emblée",
    "débauche": "débauche",
    "débrayage": "débrayage",
    "défaillance": "défaillance",
    "démaquillage": "démaquillage",
    "dépendance": "dépendance",
    "dépliant": "dépliant",
    "doléances": "doléances",  # From "cahier de doléances"
    "eurochèque": "eurochèque",
    "garçonnière": "garçonnière",
    "guêpière": "guêpière",
    "matinée": "matinée",
    "mèche": "mèche",
    "mélo": "mélo",
    "mêlée": "mêlée",  # From "au-dessus de la mêlée"
    "nécessaire": "nécessaire",
    "pré-maman": "pré-maman",
    "randonnée": "randonnée",
    "rétro": "rétro",
    "sommelière": "sommelière",
    "tournée": "tournée",
    "éclair": "éclair",
    "écru": "écru",
    "élite": "élite",
    "épagneul": "épagneul",
    "étoile": "étoile",
    # =========================================================================
    # Single-letter word
    # Italian has no pedagogical "à" - the only uses are:
    #   1. French preposition (à la page) - orthographic, should preserve
    #   2. Obsolete Italian "ha" - also orthographic, would also preserve
    # =========================================================================
    "à": "à",  # From "à la page"
}

# All accented characters (both uppercase and lowercase)
ACCENTED_CHARS = frozenset("àèéìòóùÀÈÉÌÒÓÙ")

# Accented characters that can appear at end of word (lowercase only)
ACCENTED_FINAL = frozenset("àèéìòóù")

# Vowels for stem analysis
VOWELS = frozenset("aeiouAEIOU")


def _derive_single_word(word: str) -> str | None:
    """Derive written form for a single word (no spaces).

    Returns the written form, or None if derivation fails (e.g., multiple accents).
    Logs a warning for single words with multiple accents (unless whitelisted).
    """
    if not word:
        return None

    # Check French loanword whitelist first (bypasses multi-accent warning)
    if word in FRENCH_LOANWORD_WHITELIST:
        return FRENCH_LOANWORD_WHITELIST[word]

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
