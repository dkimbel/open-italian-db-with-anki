"""Parse wiktextract tags into structured grammatical features."""

from dataclasses import dataclass

# Tags that indicate a form should be filtered out entirely
FILTER_TAGS = frozenset(
    {
        "misspelling",
        "proscribed",
        "error-unknown-tag",
        "error-unrecognized-form",
    }
)

# Tags we skip (metadata, not form info)
SKIP_TAGS = frozenset(
    {
        "table-tags",
        "inflection-template",
        "canonical",
        "auxiliary",
        "form-of",
    }
)

# Mood tags
MOOD_TAGS = frozenset(
    {
        "indicative",
        "subjunctive",
        "conditional",
        "imperative",
        "infinitive",
        "participle",
        "gerund",
    }
)

# Tense tags
TENSE_TAGS = frozenset(
    {
        "present",
        "imperfect",
        "past",
        "historic",
        "future",
    }
)

# Person tags -> integer
PERSON_MAP = {
    "first-person": 1,
    "second-person": 2,
    "third-person": 3,
}

# Number tags
NUMBER_TAGS = frozenset({"singular", "plural"})

# Gender tags
GENDER_TAGS = frozenset({"masculine", "feminine"})

# Usage label tags (can have multiple per form)
LABEL_TAGS = frozenset(
    {
        "archaic",
        "literary",
        "regional",
        "dialectal",
        "poetic",
        "rare",
        "obsolete",
        "colloquial",
        "slang",
        "dated",
        "uncommon",
        "apocopic",  # truncated poetic form
        "Tuscany",  # regional variant
        "Latinism",
    }
)

# Adjective degree tags
DEGREE_TAGS = frozenset({"superlative", "comparative"})

# Noun derivation tags
NOUN_DERIVATION_TAGS = frozenset({"diminutive", "augmentative"})


@dataclass
class VerbFormFeatures:
    """Parsed grammatical features for a verb form."""

    mood: str | None = None
    tense: str | None = None
    person: int | None = None
    number: str | None = None
    gender: str | None = None  # for participles
    is_formal: bool = False
    is_negative: bool = False
    labels: str | None = None  # comma-separated if multiple, e.g. "archaic,literary"
    should_filter: bool = False


@dataclass
class NounFormFeatures:
    """Parsed grammatical features for a noun form."""

    number: str | None = None
    labels: str | None = None  # comma-separated if multiple
    is_diminutive: bool = False
    is_augmentative: bool = False
    should_filter: bool = False


@dataclass
class AdjectiveFormFeatures:
    """Parsed grammatical features for an adjective form."""

    gender: str | None = None
    number: str | None = None
    degree: str = "positive"
    labels: str | None = None  # comma-separated if multiple
    should_filter: bool = False


def should_filter_form(tags: list[str]) -> bool:
    """Check if a form should be filtered out entirely."""
    tag_set = set(tags)

    # Filter if has any filter tags
    if tag_set & FILTER_TAGS:
        return True

    # Filter if alternative + misspelling
    return "alternative" in tag_set and "misspelling" in tag_set


def _extract_labels(tags: set[str]) -> str | None:
    """Extract labels from tags, returning comma-separated if multiple."""
    labels = sorted(tags & LABEL_TAGS)
    return ",".join(labels) if labels else None


def _extract_tense(tags: set[str], mood: str | None) -> str | None:
    """Extract tense from tags, handling passato remoto specially."""
    if mood in ("infinitive", "gerund"):
        return None

    if "past" in tags and "historic" in tags:
        return "remote"  # passato remoto

    if "past" in tags:
        # For participles, past is part of the mood, not a tense
        if mood == "participle":
            return None
        return "past"

    for tag in ("present", "imperfect", "future"):
        if tag in tags:
            return tag

    return None


def parse_verb_tags(tags: list[str]) -> VerbFormFeatures:
    """Parse verb form tags into structured features.

    Args:
        tags: List of wiktextract tags

    Returns:
        VerbFormFeatures with parsed data
    """
    result = VerbFormFeatures()
    tag_set = set(tags)

    # Check if should filter
    if should_filter_form(tags):
        result.should_filter = True
        return result

    # Skip metadata tags
    if tag_set & SKIP_TAGS:
        result.should_filter = True
        return result

    # Extract mood
    for tag in tag_set:
        if tag in MOOD_TAGS:
            result.mood = tag
            break

    # Extract tense
    result.tense = _extract_tense(tag_set, result.mood)

    # Extract person
    for tag, person in PERSON_MAP.items():
        if tag in tag_set:
            result.person = person
            break

    # Extract number
    for tag in NUMBER_TAGS:
        if tag in tag_set:
            result.number = tag
            break

    # Extract gender (for participles)
    for tag in GENDER_TAGS:
        if tag in tag_set:
            result.gender = tag
            break

    # Extract booleans
    result.is_formal = "formal" in tag_set
    result.is_negative = "negative" in tag_set

    # Extract labels
    result.labels = _extract_labels(tag_set)

    return result


def parse_noun_tags(tags: list[str]) -> NounFormFeatures:
    """Parse noun form tags into structured features.

    Args:
        tags: List of wiktextract tags

    Returns:
        NounFormFeatures with parsed data
    """
    result = NounFormFeatures()
    tag_set = set(tags)

    # Check if should filter
    if should_filter_form(tags):
        result.should_filter = True
        return result

    # Skip metadata tags
    if tag_set & SKIP_TAGS:
        result.should_filter = True
        return result

    # Extract number
    for tag in NUMBER_TAGS:
        if tag in tag_set:
            result.number = tag
            break

    # Extract derivation type
    result.is_diminutive = "diminutive" in tag_set
    result.is_augmentative = "augmentative" in tag_set

    # Extract labels
    result.labels = _extract_labels(tag_set)

    return result


def parse_adjective_tags(tags: list[str]) -> AdjectiveFormFeatures:
    """Parse adjective form tags into structured features.

    Args:
        tags: List of wiktextract tags

    Returns:
        AdjectiveFormFeatures with parsed data
    """
    result = AdjectiveFormFeatures()
    tag_set = set(tags)

    # Check if should filter
    if should_filter_form(tags):
        result.should_filter = True
        return result

    # Skip metadata tags
    if tag_set & SKIP_TAGS:
        result.should_filter = True
        return result

    # Extract gender
    for tag in GENDER_TAGS:
        if tag in tag_set:
            result.gender = tag
            break

    # Extract number
    for tag in NUMBER_TAGS:
        if tag in tag_set:
            result.number = tag
            break

    # Extract degree
    if "superlative" in tag_set:
        result.degree = "superlative"
    elif "comparative" in tag_set:
        result.degree = "comparative"
    else:
        result.degree = "positive"

    # Extract labels
    result.labels = _extract_labels(tag_set)

    return result
