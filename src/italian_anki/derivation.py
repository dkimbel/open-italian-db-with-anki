"""Derivation functions for generating inflected forms from base forms.

This module contains functions that derive predictable inflected forms
from base forms using regular morphological rules. These are safe to use
because the transformations are 100% deterministic and follow exceptionless
patterns in Italian morphology.
"""


def derive_participle_forms(
    base_stressed: str,
) -> list[tuple[str, str, str]]:
    """Derive gender/number variants from a masculine singular past participle.

    Italian past participles always follow the -o/-a/-i/-e pattern for
    gender/number agreement:
    - Masculine singular: -o (parlato, fatto, scritto)
    - Feminine singular:  -a (parlata, fatta, scritta)
    - Masculine plural:   -i (parlati, fatti, scritti)
    - Feminine plural:    -e (parlate, fatte, scritte)

    This is NOT inference - it's deterministic orthographic transformation.
    The "irregularity" in Italian past participles is in the stem (fare→fatto),
    not in the gender/number endings.

    Args:
        base_stressed: Masculine singular form with stress marks (e.g., "parlàto")

    Returns:
        List of (stressed_form, gender, number) tuples for the 3 other forms.
        Does NOT include the base form (assumed to already exist).
        Returns empty list if base form doesn't end in 'o' (can't derive).

    Example:
        >>> derive_participle_forms("parlàto")
        [('parlàta', 'feminine', 'singular'),
         ('parlàti', 'masculine', 'plural'),
         ('parlàte', 'feminine', 'plural')]

        >>> derive_participle_forms("fàtto")
        [('fàtta', 'feminine', 'singular'),
         ('fàtti', 'masculine', 'plural'),
         ('fàtte', 'feminine', 'plural')]
    """
    if not base_stressed.endswith("o"):
        # Can't derive - might be a clitic form or other edge case
        return []

    stem = base_stressed[:-1]  # Remove final 'o'

    return [
        (stem + "a", "feminine", "singular"),
        (stem + "i", "masculine", "plural"),
        (stem + "e", "feminine", "plural"),
    ]
