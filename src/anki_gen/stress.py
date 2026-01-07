"""Stress marking utilities for Anki card display.

This module converts pedagogical stress marks (from the `stressed` column)
into visual stress indicators using Unicode combining characters.

The `written` column contains correct Italian orthography (parlo, città, può).
The `stressed` column contains pedagogical stress marks (pàrlo, città, può).

We combine these to produce display forms with a dot below stressed vowels:
- parlo + pàrlo → pa̤rlo (dot below a, using U+0323)
- città + città → città (no change - accent IS the stress)
- può + può → può (no change - accent IS the stress)
"""

import unicodedata

# Vowels that can carry stress
VOWELS = frozenset("aeiouAEIOU")

# Accented vowels map to their base vowel
ACCENT_TO_BASE: dict[str, str] = {
    "à": "a",
    "è": "e",
    "é": "e",
    "ì": "i",
    "ò": "o",
    "ó": "o",
    "ù": "u",
    "À": "A",
    "È": "E",
    "É": "E",
    "Ì": "I",
    "Ò": "O",
    "Ó": "O",
    "Ù": "U",
}


def _find_stressed_vowel_index(stressed: str) -> int | None:
    """Find the index of the stressed vowel in the stressed form.

    The stressed form uses pedagogical accent marks (à, è, ì, ò, ù) to indicate
    stress. We need to find which vowel position is stressed.

    Returns the index (0-based) of the stressed vowel, or None if not found.
    """
    vowel_count = 0
    for char in stressed:
        if char in ACCENT_TO_BASE:
            # This accented vowel is the stressed one
            return vowel_count
        elif char in VOWELS:
            vowel_count += 1
    return None


def _get_vowel_positions(written: str) -> list[int]:
    """Get character indices of all vowels in the written form."""
    return [i for i, char in enumerate(written) if char in VOWELS or char in ACCENT_TO_BASE]


def add_stress_underline(written: str, stressed: str) -> str:
    """Add a combining dot below to the stressed vowel in the written form.

    Args:
        written: Real Italian orthography (e.g., "parlo", "città")
        stressed: Form with pedagogical stress marks (e.g., "pàrlo", "città")

    Returns:
        Written form with stressed vowel followed by combining dot below (U+0323).
        If the stressed vowel already has an orthographic accent (è, ò, etc.),
        no dot is added since the accent already indicates stress.

    Examples:
        >>> add_stress_underline("parlo", "pàrlo")
        'pa̤rlo'  # a followed by U+0323
        >>> add_stress_underline("città", "città")
        'città'  # no change - accent is orthographic
        >>> add_stress_underline("parlano", "pàrlano")
        'pa̤rlano'
        >>> add_stress_underline("andiamo", "andiàmo")
        'andia̤mo'
    """
    if not written or not stressed:
        return written

    # Normalize both to NFC form for consistent handling
    written = unicodedata.normalize("NFC", written)
    stressed = unicodedata.normalize("NFC", stressed)

    # Find which vowel (by position) is stressed
    stressed_vowel_idx = _find_stressed_vowel_index(stressed)
    if stressed_vowel_idx is None:
        # No stressed vowel found (word might have orthographic accent)
        # Check if written form has an accented vowel
        for char in written:
            if char in ACCENT_TO_BASE:
                # Already has orthographic accent, no underline needed
                return written
        # Fallback: return as-is
        return written

    # Get vowel positions in written form
    vowel_positions = _get_vowel_positions(written)
    if stressed_vowel_idx >= len(vowel_positions):
        # Mismatch - return as-is
        return written

    # Get the character index of the stressed vowel in written form
    char_idx = vowel_positions[stressed_vowel_idx]

    # Check if this vowel already has an orthographic accent
    char = written[char_idx]
    if char in ACCENT_TO_BASE:
        # Vowel already accented in written form - no underline needed
        return written

    # Add combining dot below (U+0323) after the stressed vowel
    return written[:char_idx] + char + "\u0323" + written[char_idx + 1 :]


def format_conjugation_with_stress(written: str | None, stressed: str) -> str:
    """Format a conjugated form with stress marking for display.

    If written is None, falls back to the stressed form with accent stripped
    and stress dot added.

    Args:
        written: Real Italian orthography from database, or None
        stressed: Form with pedagogical stress marks

    Returns:
        Display form with stressed vowel marked with dot below
    """
    if written is not None:
        return add_stress_underline(written, stressed)

    # Fallback: strip accent from stressed form and add underline
    # This shouldn't happen if database is properly populated
    base = ""
    stressed_vowel_idx: int | None = None
    vowel_count = 0

    for char in stressed:
        if char in ACCENT_TO_BASE:
            base += ACCENT_TO_BASE[char]
            stressed_vowel_idx = vowel_count
            vowel_count += 1
        elif char in VOWELS:
            base += char
            vowel_count += 1
        else:
            base += char

    if stressed_vowel_idx is None:
        return base

    # Now add underline at the right position
    return add_stress_underline(base, stressed)
