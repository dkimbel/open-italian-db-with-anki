"""Main deck generation logic.

This module orchestrates the deck generation process:
1. Load verb list from TOML config
2. Validate all verbs exist in database
3. For each verb, build conjugation cards
4. Generate stable GUIDs for Anki merging
5. Build and write the .apkg file
"""

import hashlib
from dataclasses import dataclass
from pathlib import Path

import genanki  # type: ignore[import-untyped]

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[import-not-found,no-redef]

from typing import Any, cast

from sqlalchemy import Connection

from anki_gen.note_types import create_verb_conjugation_model
from anki_gen.queries import (
    TENSE_ID_TO_MOOD_TENSE,
    ExampleSentence,
    Verb,
    generate_english_prompt,
    get_english_infinitive,
    get_example_sentence_with_fallback,
    get_frequency_band,
    get_present_indicative_forms,
    get_verb_by_lemma,
    validate_verb_list,
)
from anki_gen.stress import format_conjugation_with_stress
from anki_gen.templates import TENSE_INFO, build_conjugation_table_html

# Stable deck ID for Anki merging
DECK_ID = 2058400391


@dataclass
class GenerationStats:
    """Statistics from deck generation."""

    verbs_processed: int
    cards_generated: int
    verbs_skipped: int
    skipped_reasons: list[str]


def generate_guid(lemma: str, tense: str) -> str:
    """Generate a stable GUID for a verb conjugation card.

    Uses MD5 hash of lemma:tense to ensure same card always gets same ID.
    This allows Anki to properly merge progress on reimport.

    Args:
        lemma: Verb infinitive (e.g., "parlare")
        tense: Tense identifier (e.g., "presente_indicativo")

    Returns:
        8-character hex string suitable for genanki GUID
    """
    content = f"{lemma}:verb:{tense}"
    return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()[:8]


def load_verb_list(config_path: Path) -> list[str]:
    """Load list of verb infinitives from TOML config.

    Args:
        config_path: Path to verbs.toml

    Returns:
        List of verb infinitives
    """
    with config_path.open("rb") as f:
        config = cast("dict[str, Any]", tomllib.load(f))  # type: ignore[reportUnknownMemberType]

    verbs_list = cast("list[dict[str, Any]]", config.get("verbs", []))
    return [str(v["lemma"]) for v in verbs_list]


def format_example_sentence(example: ExampleSentence | None) -> tuple[str, str]:
    """Extract Italian and English text from example sentence.

    Args:
        example: ExampleSentence or None

    Returns:
        Tuple of (italian_text, english_text), empty strings if no example
    """
    if example is None:
        return ("", "")

    italian = example.italian
    english = example.english or ""
    return (italian, english)


def build_verb_tags(
    conn: Connection,
    verb: Verb,
    tense_id: str,
) -> list[str]:
    """Build Anki tags for a verb conjugation card.

    Tags include:
        - pos::verb
        - tense::{english-tense-name} (kebab-case)
        - freq::{band} (top-100, top-500, top-2000, top-5000, other)
        - infinitive::{lemma}

    Args:
        conn: Database connection
        verb: Verb dataclass
        tense_id: Tense identifier (e.g., "presente_indicativo")

    Returns:
        List of tag strings
    """
    # Get tense English name and convert to kebab-case
    tense_info = TENSE_INFO.get(tense_id, {})
    tense_english = tense_info.get("english_name", tense_id.replace("_", " "))
    tense_tag = f"tense::{tense_english.replace(' ', '-')}"

    # Get frequency band
    freq_band = get_frequency_band(conn, verb.lemma_id)

    return [
        "pos::verb",
        tense_tag,
        freq_band,
        f"infinitive::{verb.written}",
    ]


def generate_verb_card(
    conn: Connection,
    verb: Verb,
    tense_id: str,
    model: genanki.Model,
) -> genanki.Note | None:
    """Generate a single verb conjugation card.

    Args:
        conn: Database connection
        verb: Verb dataclass
        tense_id: Tense identifier (e.g., "presente_indicativo")
        model: genanki Model to use

    Returns:
        genanki.Note or None if forms are missing
    """
    tense_info = TENSE_INFO.get(tense_id)
    if tense_info is None:
        return None

    # Get conjugation forms based on tense
    if tense_id == "presente_indicativo":
        forms = get_present_indicative_forms(conn, verb.lemma_id)
    else:
        # Other tenses will be implemented in later phases
        return None

    if len(forms) < 6:
        # Missing some forms - skip this verb for this tense
        return None

    # Build forms dict with stress marking
    forms_dict: dict[tuple[int, str], str] = {}
    conjugated_forms: list[str] = []
    for form in forms:
        # Use CSS-based dot (non-copyable) for stress marking
        display = format_conjugation_with_stress(form.written, form.stressed, use_css=True)
        forms_dict[(form.person, form.number)] = display
        # Collect written forms for sentence search
        if form.written:
            conjugated_forms.append(form.written)

    # Build conjugation table HTML
    table_html = build_conjugation_table_html(forms_dict)

    # Extract mood/tense for morphological sentence matching
    mood, tense = TENSE_ID_TO_MOOD_TENSE.get(tense_id, (None, None))

    # Get example sentence: try morphological match from ParTUT first, then FTS fallback
    example = get_example_sentence_with_fallback(
        conn,
        verb.written,
        mood=mood,
        tense=tense,
        conjugated_forms=conjugated_forms,
    )
    example_italian, example_english = format_example_sentence(example)

    # Get English infinitive (e.g., "to speak")
    english_infinitive = get_english_infinitive(conn, verb.lemma_id) or ""

    # Generate verb-specific English prompt (e.g., "I am" for essere, "I speak" for parlare)
    english_prompt = generate_english_prompt(english_infinitive, tense_id)

    # Get English tense name
    tense_english = tense_info.get("english_name", tense_id.replace("_", " "))

    # Generate stable GUID
    guid = generate_guid(verb.written, tense_id)

    # Build tags
    tags = build_verb_tags(conn, verb, tense_id)

    # Create note
    note = genanki.Note(
        model=model,
        fields=[
            verb.written,  # Infinitive
            tense_id,  # Tense (internal)
            tense_info["technical_name"],  # TenseTechnical
            english_prompt,  # EnglishPrompt (verb-specific)
            table_html,  # ConjugationTable
            example_italian,  # ExampleItalian
            example_english,  # ExampleEnglish
            verb.ipa or "",  # IPA
            tense_english,  # TenseEnglish
            english_infinitive,  # EnglishInfinitive
        ],
        guid=guid,
        tags=tags,
    )

    return note


def generate_deck(
    conn: Connection,
    config_path: Path | None = None,
    output_path: Path | None = None,
    tenses: list[str] | None = None,
) -> GenerationStats:
    """Generate an Anki deck with verb conjugation cards.

    Args:
        conn: Database connection
        config_path: Path to verbs.toml (default: src/anki_gen/config/verbs.toml)
        output_path: Path for output .apkg (default: output/italian.apkg)
        tenses: List of tense IDs to include (default: ["presente_indicativo"])

    Returns:
        GenerationStats with counts
    """
    # Set defaults
    if config_path is None:
        config_path = Path(__file__).parent / "config" / "verbs.toml"
    if output_path is None:
        output_path = Path("output") / "italian.apkg"
    if tenses is None:
        tenses = ["presente_indicativo"]

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load verb list
    verb_lemmas = load_verb_list(config_path)

    # Validate all verbs exist
    found, missing = validate_verb_list(conn, verb_lemmas)
    if missing:
        print(f"Warning: {len(missing)} verbs not found in database:")
        for m in missing[:10]:
            print(f"  - {m}")
        if len(missing) > 10:
            print(f"  ... and {len(missing) - 10} more")

    # Create model and deck
    model = create_verb_conjugation_model()
    deck = genanki.Deck(DECK_ID, "Italian")

    # Track statistics
    stats = GenerationStats(
        verbs_processed=0,
        cards_generated=0,
        verbs_skipped=0,
        skipped_reasons=[],
    )

    # Generate cards for each verb
    for lemma in found:
        verb = get_verb_by_lemma(conn, lemma)
        if verb is None:
            stats.verbs_skipped += 1
            stats.skipped_reasons.append(f"{lemma}: not found")
            continue

        verb_cards_generated = 0
        for tense_id in tenses:
            note = generate_verb_card(conn, verb, tense_id, model)
            if note is not None:
                deck.add_note(note)  # type: ignore[no-untyped-call]
                stats.cards_generated += 1
                verb_cards_generated += 1

        if verb_cards_generated > 0:
            stats.verbs_processed += 1
        else:
            stats.verbs_skipped += 1
            stats.skipped_reasons.append(f"{lemma}: missing forms")

    # Write package
    genanki.Package(deck).write_to_file(str(output_path))  # type: ignore[no-untyped-call]

    return stats
