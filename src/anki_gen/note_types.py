"""genanki Model definitions for Anki note types.

Each note type has a stable model ID to allow Anki to properly merge
decks on reimport without duplicating notes.
"""

import genanki  # type: ignore[import-untyped]

from anki_gen.templates import CARD_CSS, VERB_BACK_TEMPLATE, VERB_FRONT_TEMPLATE

# Stable model IDs - these MUST NOT change once cards are generated
# Generated as: hash of model name truncated to positive 32-bit int range
VERB_CONJUGATION_MODEL_ID = 1847293651


def create_verb_conjugation_model() -> genanki.Model:
    """Create the genanki Model for verb conjugation cards.

    Fields:
        - Infinitive: The verb infinitive (e.g., "parlare")
        - Tense: Internal tense identifier (e.g., "presente_indicativo")
        - TenseTechnical: Display name (e.g., "presente indicativo")
        - EnglishPrompt: The English prompt (e.g., "I speak")
        - ConjugationTable: HTML table of conjugated forms
        - ExampleItalian: Italian example sentence text
        - ExampleEnglish: English translation of example
        - IPA: Optional IPA pronunciation
        - TenseEnglish: English tense name (e.g., "present indicative")
        - EnglishInfinitive: English infinitive (e.g., "to speak")

    Returns:
        genanki.Model configured for verb conjugation cards
    """
    return genanki.Model(
        VERB_CONJUGATION_MODEL_ID,
        "Italian Verb Conjugation",
        fields=[
            {"name": "Infinitive"},
            {"name": "Tense"},
            {"name": "TenseTechnical"},
            {"name": "EnglishPrompt"},
            {"name": "ConjugationTable"},
            {"name": "ExampleItalian"},
            {"name": "ExampleEnglish"},
            {"name": "IPA"},
            {"name": "TenseEnglish"},
            {"name": "EnglishInfinitive"},
        ],
        templates=[
            {
                "name": "Card 1",
                "qfmt": VERB_FRONT_TEMPLATE,
                "afmt": VERB_BACK_TEMPLATE,
            },
        ],
        css=CARD_CSS,
    )
