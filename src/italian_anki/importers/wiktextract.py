"""Import Italian verb data from Wiktextract JSONL."""

import json
import logging
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, func, select, text, update
from sqlalchemy.exc import IntegrityError

from italian_anki.articles import get_definite
from italian_anki.db.schema import (
    adjective_forms,
    adjective_metadata,
    definitions,
    frequencies,
    lemmas,
    noun_forms,
    noun_metadata,
    verb_forms,
    verb_metadata,
)
from italian_anki.derivation import derive_participle_forms
from italian_anki.enums import POS, DerivationType, GenderClass
from italian_anki.normalize import derive_written_from_stressed, normalize
from italian_anki.tags import (
    LABEL_CANONICAL,
    SKIP_TAGS,
    parse_adjective_tags,
    parse_noun_tags,
    parse_verb_tags,
    should_filter_form,
)

logger = logging.getLogger(__name__)

# Cache for line counts - avoids re-reading large files multiple times
_line_count_cache: dict[Path, int] = {}

# Mapping from our POS names to Wiktextract's abbreviated names
WIKTEXTRACT_POS: dict[POS, str] = {
    POS.VERB: "verb",
    POS.NOUN: "noun",
    POS.ADJECTIVE: "adj",  # Wiktextract uses "adj"
}

# POS-specific form tables
POS_FORM_TABLES: dict[POS, Any] = {
    POS.VERB: verb_forms,
    POS.NOUN: noun_forms,
    POS.ADJECTIVE: adjective_forms,
}

# Regex to strip bracket annotations from canonical forms
# e.g., "[auxiliary essere]", "[transitive 'something'"
# Handles malformed cases with missing closing bracket
_BRACKET_ANNOTATION_RE = re.compile(r"\s*\[[^\]]*\]?\s*$")

# Known gender patterns in Wiktextract head_template args
# Maps raw values to normalized forms
GENDER_PATTERNS: dict[str, str] = {
    "m": "m",
    "f": "f",
    "mf": "mf",
    "mfbysense": "mfbysense",
    "m-p": "m-p",  # masculine pluralia tantum
    "f-p": "f-p",  # feminine pluralia tantum
    "m-s": "m-s",  # masculine singularia tantum
    "f-s": "f-s",  # feminine singularia tantum
}

# Italian accented vowels (used to detect stressed/accented forms)
ACCENTED_CHARS = frozenset("àèéìòóùÀÈÉÌÒÓÙ")

# Hardcoded overrides for lemma stressed forms where Wiktionary is inconsistent.
# Maps Wiktionary's stressed form to the correct stressed form.
LEMMA_STRESSED_OVERRIDES: dict[str, str] = {
    "sùggere": "suggére",  # Wiktionary lemma has wrong stress position vs forms
}

# Typo corrections for feminine noun forms in Wiktextract data.
# These are -trice endings that are misspelled as -tice or -trive.
# Maps Wiktionary's typo form to the correct form.
FEMININE_FORM_CORRECTIONS: dict[str, str] = {
    "preconizzatice": "preconizzatrice",
    "propalatice": "propalatrice",
    "pulimentatice": "pulimentatrice",
    "respingitice": "respingitrice",
    "sbatacchiatice": "sbatacchiatrice",
    "tenzonatice": "tenzonatrice",
    "scannatrive": "scannatrice",
}

# Known elision particles that should have space removed after apostrophe.
# These are words that undergo elision before vowel-initial words in Italian.
# Truncated imperatives (va', fa', da', sta') are NOT in this list because
# the apostrophe marks truncation, not elision, and the following word is separate.
ELISION_PARTICLES = frozenset(
    {
        "d",
        "l",
        "dell",
        "dall",
        "nell",
        "all",
        "sull",
        "coll",
        "un",
        "quest",
        "quell",
        "bell",
        "sant",
        "buon",
    }
)


def _normalize_apostrophe_spacing(text: str) -> str:
    """Remove spaces after apostrophes in Italian elisions (not truncations).

    Elision: "d' occhio" → "d'occhio" (space removed, words connect)
    Truncation: "và' giù" → "và' giù" (space preserved, words separate)

    Only removes space when the word before the apostrophe is a known
    elision particle (d', l', dell', etc.). Truncated imperatives like
    va', fa', da', sta' keep the space because the apostrophe marks
    truncation, not elision.
    """

    def replace_if_elision(match: re.Match[str]) -> str:
        before = match.group(1)  # word before apostrophe
        after_char = match.group(2)  # first char after space

        if before.lower() in ELISION_PARTICLES:
            return f"{before}'{after_char}"  # Remove space
        return match.group(0)  # Keep original (with space)

    # Pattern: word + apostrophe + space(s) + next character
    return re.sub(r"(\w+)'\s+(\w)", replace_if_elision, text)


def _is_invariable_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective is invariable (same form for all gender/number).

    Detection methods:
    1. inv:1 flag in head_templates args (e.g., "rosa", "blu")
    2. 'invariable' or 'invariant' as any arg value in head_templates
       (e.g., kabuki has {'3': 'invariable'})
    """
    for template in entry.get("head_templates", []):
        args = template.get("args", {})
        # Method 1: Explicit inv:1 flag
        if args.get("inv") == "1":
            return True
        # Method 2: 'invariable' or 'invariant' appears as any arg value
        for value in args.values():
            if value in ("invariable", "invariant"):
                return True

    return False


def _is_feminine_only_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective is feminine-only via fonly:1 flag in head_templates.

    Feminine-only adjectives (like "incinta", "nullipara") only have feminine forms.
    They describe inherently feminine concepts (pregnancy, giving birth, etc.).
    """
    for template in entry.get("head_templates", []):
        if template.get("args", {}).get("fonly") == "1":
            return True
    return False


def _is_masculine_only_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective has no feminine forms via f:- flag in head_templates.

    Some adjectives (like "ficaio") have no feminine counterpart.
    """
    for template in entry.get("head_templates", []):
        if template.get("args", {}).get("f") == "-":
            return True
    return False


def _is_whitelisted_invariable_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective is in the invariable whitelist.

    Some common invariable adjectives (phrases, loanwords) use the generic "head"
    template instead of "it-adj" and lack explicit inv:1 flags. This whitelist
    ensures they're correctly classified as invariable.
    """
    return entry.get("word", "") in INVARIABLE_ADJECTIVE_WHITELIST


def _is_misspelling(entry: dict[str, Any]) -> bool:
    """Check if entry is marked as a misspelling.

    Wiktextract marks misspellings with:
    - senses[*].tags containing "misspelling"
    - head_templates[*].args with "misspelling" value

    Examples: metereologico (misspelling of meteorologico)
    """
    # Check senses tags
    for sense in entry.get("senses", []):
        if "misspelling" in sense.get("tags", []):
            return True

    # Check head_templates args
    for template in entry.get("head_templates", []):
        args = template.get("args", {})
        if "misspelling" in args.values():
            return True

    return False


def _is_blocklisted_lemma(entry: dict[str, Any]) -> bool:
    """Check if lemma is in blocklist due to malformed source data.

    Some Wiktextract entries have data issues that cause incorrect inferences:
    - Invariable adjectives not marked with inv:1
    - Plural variants without gender tags (misinterpreted as 2-form adjectives)

    These are filtered out entirely during import.
    """
    word = entry.get("word", "")
    return word in LEMMA_BLOCKLIST


def _is_two_form_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective is 2-form (same form for masculine and feminine).

    Detection methods:
    1. Genderless number tags in forms array (e.g., ["plural"] for "facile")
    2. "m or f by sense" in head_templates expansion (e.g., "ottimista")
    """
    # Method 1: Genderless number tags in forms array
    for form_data in entry.get("forms", []):
        tags = set(form_data.get("tags", []))
        has_gender = "masculine" in tags or "feminine" in tags
        has_number = "singular" in tags or "plural" in tags
        if has_number and not has_gender:
            return True

    # Method 2: Parse head_templates expansion for "m or f by sense"
    for template in entry.get("head_templates", []):
        expansion = template.get("expansion", "")
        if "m or f by sense" in expansion:
            return True

    return False


def _get_adjective_inflection_class(entry: dict[str, Any]) -> str:
    """Determine adjective inflection class from Wiktextract data.

    Returns:
        'invariable': Same form for all gender/number (blu, rosa)
        '2-form': Same form for m/f, different for singular/plural (facile/facili)
                  OR gender-restricted (feminine-only like incinta)
        '4-form': Different form for each gender/number (bello/bella/belli/belle)
    """
    # Check explicit invariable markers (inv:1 flag or "invariable" in expansion)
    if _is_invariable_adjective(entry):
        return "invariable"

    # Check whitelisted invariable adjectives (common phrases/loanwords)
    if _is_whitelisted_invariable_adjective(entry):
        return "invariable"

    # Check feminine-only adjectives (2-form: f/sg and f/pl only)
    # Bad entries are blocklisted, so this only matches good ones (incinta, nullipara)
    if _is_feminine_only_adjective(entry):
        return "2-form"

    # Check standard 2-form patterns (genderless number tags, "m or f by sense")
    if _is_two_form_adjective(entry):
        return "2-form"

    return "4-form"


def _is_pure_alt_form_entry(entry: dict[str, Any]) -> bool:
    """Check if entry is PURELY an alt-of entry (no other meanings).

    Returns True only if ALL senses are alt_of or form_of.
    Returns False if entry has any regular definition senses.

    This preserves entries like "toro" which is both an alt-of "Toro"
    (Taurus) AND a standalone word meaning "bull".
    """
    senses = entry.get("senses", [])
    if not senses:
        return False

    for sense in senses:
        # If any sense is a regular definition (not alt_of or form_of), keep the entry
        if not sense.get("alt_of") and not sense.get("form_of"):
            return False

    # All senses are alt_of or form_of - safe to filter
    return any(sense.get("alt_of") for sense in senses)


# Hardcoded mappings for irregular comparatives/superlatives
# These are used as fallback when Wiktextract data is missing or incomplete
HARDCODED_DEGREE_RELATIONSHIPS: dict[str, tuple[str, str]] = {
    "migliore": ("buono", "comparative_of"),
    "ottimo": ("buono", "superlative_of"),
    "peggiore": ("cattivo", "comparative_of"),
    "pessimo": ("cattivo", "superlative_of"),
    "maggiore": ("grande", "comparative_of"),
    "massimo": ("grande", "superlative_of"),
    "minore": ("piccolo", "comparative_of"),
    "minimo": ("piccolo", "superlative_of"),
    "sommo": ("alto", "superlative_of"),
}

# Hardcoded allomorph forms not captured by normal import
# These are stored as forms under their parent lemma, not as separate lemmas
# Format: (form, parent_lemma, gender, number, label)
HARDCODED_ALLOMORPH_FORMS: list[tuple[str, str, str, str, str | None]] = [
    # san is apocopic (before consonants) - not in Morphit as adjective
    ("san", "santo", "m", "singular", "apocopic"),  # San Pietro, San Marco
    # grandi plurals for grande - no wiktextract entry exists, lemma created via alt_of form processing
    # NOTE: grande has inflection_class='2-form' in adjective_metadata, so grandi is consistent
    ("grandi", "grande", "m", "plural", None),
    ("grandi", "grande", "f", "plural", None),
]

# Hardcoded noun allomorphs not properly captured by Wiktextract
# These correct for Wiktionary pointing to archaic/variant parents that don't exist
# Format: (form, parent_lemma, gender, number)
HARDCODED_NOUN_ALLOMORPHS: list[tuple[str, str, str, str]] = [
    ("san", "santo", "m", "singular"),  # Wiktextract has "santo saint" (malformed)
    ("cor", "cuore", "m", "singular"),  # Wiktextract has "core" (archaic, not in DB)
    ("figliuol", "figlio", "m", "singular"),  # Wiktextract has "figliuolo" (archaic)
    ("gocciol", "goccia", "f", "singular"),  # Wiktextract has "gocciola" (variant)
    ("huom", "uomo", "m", "singular"),  # Wiktextract has "uom" -> should go to "uomo"
    ("mperador", "imperatore", "m", "singular"),  # Wiktextract has "imperadore" (archaic)
]


def _extract_degree_relationship(entry: dict[str, Any]) -> tuple[str, str, str] | None:
    """Extract comparative/superlative relationship from Wiktextract data.

    Detection methods (in priority order):
    1. Hardcoded mapping for irregular forms (manually curated, takes priority)
    2. Structured form entries: {"form": "of buono", "tags": ["comparative"]}
    3. Canonical text pattern: "ottimo superlative of buono"

    Returns:
        Tuple of (base_word, relationship, source) or None.
        E.g., ("buono", "comparative_of", "wiktextract") for migliore.
        Source is one of: 'hardcoded', 'wiktextract', 'wiktextract:canonical'
    """
    # Method 1: Hardcoded mapping (priority - manually curated)
    # These override Wiktextract data which can be incorrect (e.g., peggiore -> "male"
    # when it should be cattivo, since "male" is an adverb, not an adjective)
    word = entry.get("word", "")
    if word in HARDCODED_DEGREE_RELATIONSHIPS:
        base_word, relationship = HARDCODED_DEGREE_RELATIONSHIPS[word]
        return (base_word, relationship, "hardcoded")

    # Method 2: Structured form entries
    for form_data in entry.get("forms", []):
        form = form_data.get("form", "")
        tags = form_data.get("tags", [])

        if "comparative" in tags and form.startswith("of "):
            base_word = form[3:].strip()
            if base_word:
                return (base_word, "comparative_of", "wiktextract")

        if "superlative" in tags and form.startswith("of "):
            base_word = form[3:].strip()
            if base_word:
                return (base_word, "superlative_of", "wiktextract")

        # Method 3: Canonical text pattern like "ottimo superlative of buono"
        if "canonical" in tags:
            match = re.search(r"\b(superlative|comparative) of (\w+)\b", form, re.IGNORECASE)
            if match:
                degree_type = match.group(1).lower()
                base_word = match.group(2)
                return (base_word, f"{degree_type}_of", "wiktextract:canonical")

    return None


# Manual mapping of plural forms to definition matchers for nouns with meaning-dependent plurals.
# Used to populate form_meaning_hint in definitions table (the "soft key").
#
# Structure: lemma -> {plural_form: {"topics": [...], "phrases": [...]}}
# - topics: Match against raw_glosses topic markers like "(anatomy)"
# - phrases: Match against gloss text (exact substring match)
#
# A definition matches a form if ANY topic or phrase matches.
DEFINITION_FORM_LINKAGE: dict[str, dict[str, dict[str, list[str]]]] = {
    "braccio": {
        "braccia": {"topics": ["anatomy"], "phrases": ["fathom", "work", "effort"]},
        "bracci": {
            "topics": ["mechanics", "geography"],
            "phrases": ["branch (of a river", "wing (of a building)", "power", "authority"],
        },
    },
    "grido": {
        "grida": {"phrases": ["made by a human"]},
        "gridi": {"phrases": ["made by an animal", "sound of an animal"]},
    },
    "osso": {
        "ossa": {"topics": ["anatomy"]},
        "ossi": {"topics": ["anatomy", "botany"]},
    },
    "labbro": {
        "labbra": {"topics": ["anatomy"]},
        "labbri": {"topics": ["by extension"]},
    },
    "corno": {
        "corna": {"topics": ["zoology"]},
        "corni": {"topics": ["music", "geography"]},
    },
    "orecchio": {
        "orecchie": {"topics": ["anatomy"]},
        "orecchi": {"phrases": ["hearing", "ear for music", "ear-shaped"]},
    },
    "dito": {
        "dita": {"phrases": ["finger", "toe"]},  # collective
        "diti": {"phrases": ["finger", "toe"]},  # individual (same meanings)
    },
    "ciglio": {
        "ciglia": {"topics": ["anatomy"]},
        "cigli": {"phrases": ["edge", "verge"]},
    },
    "muro": {
        "mura": {"phrases": ["wall"]},  # collective city walls
        "muri": {"phrases": ["wall"]},  # individual walls (same meaning)
    },
}


def _has_accents(text: str) -> bool:
    """Check if text contains any accented characters."""
    return any(c in ACCENTED_CHARS for c in text)


def _extract_plural_qualifiers(
    entry: dict[str, Any],
) -> dict[str, tuple[str | None, str | None]]:
    """Extract plural forms and their qualifiers from head_templates.

    Parses the head_templates arg["2"] field which contains plural info in format:
        braccia<g:f><q:anatomical>,bracci<g:m><q:figurative>
        ossa<g:f><l:collective>,+<g:m><q:individual>

    Handles nested commas inside <q:...> tags by tracking bracket depth.
    Note: The "+" placeholder (meaning "regular plural") is skipped - we only
    use forms that wiktextract explicitly spelled out.

    Args:
        entry: Wiktextract entry dict

    Returns:
        Dict mapping form -> (gender, qualifier).
        E.g., {"braccia": ("f", "anatomical"), "bracci": ("m", "figurative")}
    """
    import re

    results: dict[str, tuple[str | None, str | None]] = {}

    for template in entry.get("head_templates", []):
        args = template.get("args", {})
        arg2 = args.get("2", "")
        if not arg2:
            continue

        # Split on comma only when outside angle brackets
        # (commas inside <q:...> tags should not split)
        entries: list[str] = []
        depth = 0
        current = ""
        for char in arg2:
            if char == "<":
                depth += 1
            elif char == ">":
                depth -= 1
            elif char == "," and depth == 0:
                if current.strip():
                    entries.append(current.strip())
                current = ""
                continue
            current += char
        if current.strip():
            entries.append(current.strip())

        # Parse each entry
        for entry_str in entries:
            # Extract form (everything before first <)
            form_match = re.match(r"^([^<]+)", entry_str)
            form = form_match.group(1).strip() if form_match else None

            # Skip "+" placeholder - we only use explicitly spelled-out forms
            if form == "+":
                continue

            # Extract gender from <g:X>
            g_match = re.search(r"<g:([^>]+)>", entry_str)
            gender = g_match.group(1) if g_match else None

            # Extract qualifier from <q:...> or <l:...> (both serve as meaning hints)
            q_match = re.search(r"<q:([^>]+)>", entry_str)
            l_match = re.search(r"<l:([^>]+)>", entry_str)
            qualifier = q_match.group(1) if q_match else (l_match.group(1) if l_match else None)

            if form:
                results[form] = (gender, qualifier)

    return results


def _sense_matches_form(sense: dict[str, Any], matchers: dict[str, list[str]]) -> bool:
    """Check if a sense matches the matchers for a specific form.

    Args:
        sense: A sense dict from wiktextract with "glosses" and "raw_glosses"
        matchers: Dict with optional "topics" and "phrases" lists

    Returns:
        True if any topic or phrase matches the sense.
    """
    raw_glosses = sense.get("raw_glosses", [])
    raw = raw_glosses[0] if raw_glosses else ""
    glosses = sense.get("glosses", [])
    gloss = glosses[0] if glosses else ""

    # Check topics (e.g., "(anatomy)" in raw_glosses)
    if any(f"({topic})" in raw for topic in matchers.get("topics", [])):
        return True

    # Check phrases (exact substring in gloss)
    return any(phrase in gloss for phrase in matchers.get("phrases", []))


def _build_stressed_alternatives(jsonl_path: Path) -> dict[str, str]:
    """Build a lookup of unaccented forms to their accented alternatives.

    Scans form-of entries in the Wiktextract data for "alternative" tagged forms
    that have accents. This allows enriching unaccented forms with their proper
    stressed spellings (e.g., "dei" → "dèi").

    Args:
        jsonl_path: Path to Wiktextract JSONL file

    Returns:
        Dict mapping normalized (unaccented) forms to their accented alternatives.
        E.g., {"dei": "dèi", "principi": "prìncipi"}
    """
    lookup: dict[str, str] = {}

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            entry = _parse_entry(line)
            if entry is None:
                continue

            # Only process form-of entries (entries with "form_of" in any sense)
            senses = entry.get("senses", [])
            if not any("form_of" in sense for sense in senses):
                continue

            # The entry's word is the unaccented form we want to map from
            word = entry.get("word", "")
            if not word:
                continue

            # Look for accented alternatives in the forms array
            for form_data in entry.get("forms", []):
                form = form_data.get("form", "")
                tags = form_data.get("tags", [])

                # We want forms tagged as "alternative" that have accents
                if "alternative" in tags and _has_accents(form):
                    # Map the unaccented word to the accented form
                    # Use normalize() to ensure consistent lookup keys
                    key = normalize(word)
                    # Only store if we don't have one yet (first alternative wins)
                    # or if the new one is shorter (prefer simpler forms)
                    if key not in lookup or len(form) < len(lookup[key]):
                        lookup[key] = form

    return lookup


def _build_counterpart_plurals(jsonl_path: Path) -> dict[str, tuple[str, str | None]]:
    """Build a lookup of lemma words to their plural forms and gender.

    For nouns with counterpart markers (f: "+" or m: "+"), we need to look up
    the counterpart entry's plural. E.g., "amico" has counterpart "amica",
    and we need to know "amica" → "amiche".

    We also store the gender so callers can verify the counterpart entry has
    the expected gender (some Wiktextract entries have incorrect gender data).

    Note: We do NOT skip form-of entries here because counterpart entries like
    "amica" often have form_of senses (referencing "amico") but still have
    valid plural forms we need to look up.

    Args:
        jsonl_path: Path to Wiktextract JSONL file

    Returns:
        Dict mapping lemma word to (plural_form, gender).
        E.g., {"amica": ("amiche", "f"), "amico": ("amici", "m")}
    """
    # Tags that indicate a less preferred plural form
    deprioritize_tags = frozenset({"archaic", "dialectal", "obsolete", "poetic", "rare"})

    lookup: dict[str, tuple[str, str | None]] = {}

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            entry = _parse_entry(line)
            # Include both nouns and adjectives - many gender-variable nouns
            # (like "albino", "pazzo", "ricco") are classified as adjectives
            # in Wiktextract, but we need their plural forms for noun counterparts
            if entry is None or entry.get("pos") not in ("noun", "adj"):
                continue

            # Note: We intentionally do NOT skip form-of entries here
            # because counterpart entries (like "amica") have form_of senses
            # but still have plural forms we need

            word = entry.get("word", "")
            if not word:
                continue

            # Extract gender for validation by callers
            gender = _extract_gender(entry)

            # Find the best plural form:
            # - Must have "plural" tag
            # - Must NOT have diminutive/augmentative tags
            # - Prefer forms without archaic/dialectal/obsolete/poetic tags
            best_plural: str | None = None
            best_has_deprioritized = True  # Start pessimistic

            for form_data in entry.get("forms", []):
                form = form_data.get("form", "")
                tags = set(form_data.get("tags", []))

                if "plural" not in tags:
                    continue
                if "diminutive" in tags or "augmentative" in tags:
                    continue

                has_deprioritized = bool(tags & deprioritize_tags)

                # Take this form if:
                # 1. We have nothing yet, OR
                # 2. This form is better (not deprioritized, when current is)
                if best_plural is None or (best_has_deprioritized and not has_deprioritized):
                    best_plural = form
                    best_has_deprioritized = has_deprioritized
                    # If we found a non-deprioritized form, we're done
                    if not has_deprioritized:
                        break

            if best_plural:
                lookup[word] = (best_plural, gender)

    return lookup


def _find_gender_in_args(args: dict[str, Any]) -> str | None:
    """Search all arg values for a known gender pattern.

    Wiktextract head_template args have inconsistent key positions:
    - {'1': 'm'}                           # gender in position 1
    - {'1': 'mfbysense'}                   # common gender in position 1
    - {'1': 'it', '2': 'noun', 'g': 'f'}   # gender in 'g' key
    - {'1': 'm', '2': '#'}                 # gender in 1, invariable in 2

    This function robustly scans ALL values to find a known gender marker.

    Returns:
        The matched gender pattern string, or None if not found.
    """
    for value in args.values():
        if isinstance(value, str) and value in GENDER_PATTERNS:
            return value
    return None


# Tags to filter out from definitions.tags (already extracted to proper columns
# or not useful for learners).
DEFINITION_TAG_BLOCKLIST = frozenset(
    {
        # Gender - extracted to noun_forms.gender
        "masculine",
        "feminine",
        "by-personal-gender",  # derivable from context
        # Transitivity - extracted to verb_metadata.transitivity
        "transitive",
        "intransitive",
        "ditransitive",
        "ambitransitive",
        # Form relationship noise
        "alt-of",
        "alternative",
    }
)

# Invariable adjective phrases/loanwords with generic head template.
# These lack it-adj inflection data but are common/useful vocabulary.
# Without this whitelist, they'd default to "4-form" (wrong).
INVARIABLE_ADJECTIVE_WHITELIST: frozenset[str] = frozenset(
    {
        # Loanwords (genuinely invariable)
        "minimal",
        "plastic free",
        # Common adjectival phrases (invariable prepositional phrases)
        "in voga",
        "di moda",
        "di fiducia",
        "di fortuna",
        "di ruolo",
        "di spicco",
        "senza pari",
        "di sempre",
        "in carne",
    }
)

# Lemmas with malformed Wiktextract data that cause incorrect inferences.
# These are filtered out entirely during import.
# NOTE: Use the exact Wiktextract word spelling (entry["word"]), not normalized form.
LEMMA_BLOCKLIST: frozenset[str] = frozenset(
    {
        # === Adjectives with malformed data ===
        "arbëresh",  # Invariable adjective not marked inv:1, causes duplicate singular/plural
        "antiterremoto",  # Plural variants lack gender tags, causes m/f duplication
        "eslege",  # Plural variants lack gender tags, causes m/f duplication
        "reggifiaccola",  # Plural variants lack gender tags, causes m/f duplication
        # === Garbage adjective entries ===
        "ITA",  # Acronym, not a real adjective
        "daun",  # Typo/nonsense entry
        "1ª",  # Ordinal notation, not an adjective lemma
        "12º",  # Ordinal notation, not an adjective lemma
        # === Archaic/poetic adjectives with incomplete forms ===
        "dio",  # Archaic/poetic adjective ("bright, resplendent"), all forms blocked
        # === Adjectives that are primarily nouns ===
        "cercatore",  # Primarily a noun; as adjective has incomplete/incorrect forms in Wiktextract
        # === Feminine-only adjectives - bad data (names, botanical, rare) ===
        "aurica",
        "beatrice",
        "consorella",
        "durona",
        "innuba",
        "isochimena",
        "metterza",
        "occhiona",
        "piperita",
        "raffaella",
        "renella",
        "roberta",
        "spadona",
        "vescicaria",
        # === Masculine-only adjectives - bad data ===
        "ficaio",
        "pannegro",
        # === Generic head template adjectives - incomplete data ===
        "al sacco",
        "babbo",
        "bello come il sole",
        "con le pive nel sacco",
        "crusco",
        "d'aiuto",
        "d'assalto",
        "d'obbligo",
        "da asporto",
        "da cani",
        "da parata",
        "del cavolo",
        "della madonna",
        "della prima ora",
        "di circostanza",
        "di lunga durata",
        "di nicchia",
        "di ordinaria amministrazione",
        "di parata",
        "di peso",
        "di punta",
        "di rigore",
        "di rilievo",
        "di sasso",
        "di serie B",
        "di tutti i giorni",
        "di tutto rispetto",
        "di vetta",
        "esoftalmico",
        "filoegizia",
        "filosocialista",
        "impatto zero",
        "in seconda",
        "magro come un chiodo",
        "matriciana",
        "nicotino",
        "pieno di vita",
        "più di là che di qua",
        "poco ma sicuro",
        "portaincenso",
        "punto e a capo",
        "sciupo",
        "secondo a nessuno",
        "senza confronto",
        "squallarato",
        "tagliamargherite",
        "usurato",
        # === Additional adjectives with incomplete form data ===
        "bel",  # Apocopated form of "bello", only m/sg
        "di vecchia data",  # Multi-word phrase, only m/sg
        "disfattista",  # Missing forms, only m/sg
        "distrutto",  # Past participle, missing other forms
        "falecio",  # Missing feminine forms
        "minore",  # Comparative, missing forms
        "uno sì e uno no",  # Multi-word phrase, missing plurals
        "uno sì, l'altro no",  # Multi-word phrase, missing plurals
        # === Nouns with corrupted Wiktionary data (wrong gender) ===
        "verseggiatore",  # Wiktionary incorrectly marks as feminine
        "pischelletto",  # Wiktionary incorrectly marks as feminine
        "arma inastata",  # Missing singular form, only has archaic/plural
        "San",  # Form of santo/santa without alt_of structure; imported as form via hardcoded
        # === Orphan verbs: pronominal/clitic forms incorrectly as lemmas ===
        "accadutomi",
        "accortosene",
        "accortosi",
        "affidatogli",
        "affidatole",
        "affidatolo",
        "affidatone",
        "affidatoti",
        "aiutatomi",
        "andatogli",
        "andatosi",
        "arrabbiatosi",
        "avutone",
        "avutosi",
        "baciatolo",
        "buttatosi",
        "cacatosi",
        "contestatoti",
        "datogli",
        "datomi",
        "datoti",
        "dettogli",
        "dettomi",
        "dettosi",
        "distrattosi",
        "facentene",
        "fattoci",
        "fattole",
        "fattolo",
        "fattomi",
        "fattone",
        "fattosene",
        "fattosi",
        "fattovi",
        "formatosi",
        "impancatosi",
        "impeditovi",
        "indottomi",
        "inviatogli",
        "lasciatogli",
        "lasciatomi",
        "lasciatosi",
        "legatosi",
        "messoci",
        "raffreddatosi",
        "recatomi",
        "recatosi",
        "recatoti",
        "regalatomi",
        "resomene",
        "resomi",
        "resosene",
        "resosi",
        "svegliatosi",
        "sviluppatosi",
        "taciutosi",
        "tenutosi",
        "tornatosene",
        "tornatosi",
        "vedutole",
        "vedutolo",
        "vedutomi",
        "vistolo",
        "vistomi",
        "vistosi",
        # === Orphan verbs: conjugated forms incorrectly as lemmas ===
        "compro",
        "debbe",
        "debbi",
        "debbia",
        "debbo",
        "debbono",
        "dee",
        "deggia",
        "deggiano",
        "deggio",
        "dei",
        "dev'",
        "deva",
        "fian",
        "possiamo",
        "raffreddo",
        "sieno",
        "volemose",
        # === Orphan verbs: participles/adjectives misclassified as verbs ===
        "auso",
        "casso",
        "crocifisso",
        "diserto",
        "dormiente",
        "feriente",
        "fieno",
        "fiero",
        "guasto",
        "morto",
        "sacro",
        "scolto",
        "sculto",
        "tegnente",
        "testo",
        "ulto",
        "visso",
        "visto",
        # === Orphan verbs: other invalid entries ===
        "asp",
        "aver",
        "ene",
        "f.to",
        "fe",
        "pipparolo",
        "pompinaio",
        # === Orphan verbs: multi-word expressions without forms ===
        "arrampicarsi sugli specchi",
        "assorgere agli onori della cronaca",
        "buttare i soldi dalla finestra",
        "caderci come una pera cotta",
        "cadere come una pera cotta",
        "cagare il cazzo",
        "dare esca al fuoco",
        "darsi alla fuga",
        "fare compagnia",
        "fare d'ogni erba un fascio",
        "fare il bello e il cattivo tempo",
        "fare il culo",
        "farsi un culo cosi",
        "ficcare il naso",
        "girare le palle",
        "mettere acqua nel vino",
        "mettere un freno",
        "mettere una pietra sopra",
        "muovere mari e monti",
        "non guardare in faccia a nessuno",
        "pagare con la stessa moneta",
        "pestare l'acqua nel mortaio",
        "prendere con le pinze",
        "restare al palo",
        "rompere i ponti",
        "tirare diritto",
        "toccare sul vivo",
        "una mano lava l'altra",
        "vendere cara la pelle",
        # === Archaic/Latin infinitives without conjugation tables ===
        "dare a bere",  # Phrasal, incomplete data
        "gaudere",  # Latin infinitive
        "iubere",  # Latin infinitive
        "meriare",  # Dialectal/archaic
        "miserere",  # Interjection form
        "nullafare",  # Compound, incomplete
        "scarrupare",  # Dialectal (Neapolitan)
        "tollere",  # Latin infinitive
        # === Verbs with no forms (orphaned entries) ===
        "fé",  # Archaic "fare" with no conjugation table
        "farsi un culo così",  # Vulgar expression with no forms
        # === Verbs with invalid data ===
        "perplettere",  # Humorous neologism (Corrado Guzzanti), not a real verb
        # === Verbs with conflicting/corrupt auxiliary data ===
        # bruire: canonical form says [auxiliary avere] but auxiliary-tagged form is "-"
        # (defective verb). Obscure word, safe to exclude.
        "bruire",
        # === Nouns with incorrect/problematic Wiktextract data ===
        # offelliere: Wiktextract says "feminine invariable" but Hoepli says f.sg = "offelliera"
        # The correct feminine follows standard -iere → -iera pattern (like cameriere → cameriera)
        "offelliere",
        # riscotitore: Archaic spelling (Machiavelli-era); modern form is "riscuotitore"
        "riscotitore",
        # sommelière: French word, not an Italian loanword; keep only "sommelier"
        "sommelière",
    }
)

# Noun lemmas to skip because they are just plural forms of existing nouns.
# Wiktionary has separate entries for some plurals, but we don't want them as
# separate lemmas since they're already covered by the base noun's forms.
# Each entry was verified to have definitions that are just pluralized versions
# with no unique meaning beyond the singular.
# NOTE: NOT blocked (verified homonyms with different meanings):
# - malti (Maltese language ≠ plural of malto/malt)
# - pali (Pali language ≠ plural of palo/pole)
# - tele (TV/telly ≠ plural of tela/canvas)
# - ditali (type of pasta with unique definition)
SKIP_PLURAL_NOUN_LEMMAS: frozenset[str] = frozenset(
    {
        "antipasti",  # "starters" = plural of antipasto
        "arrivi",  # "arrivals" = plural of arrivo
        "bovini",  # "cattle, bovines" = plural of bovino
        "ceneri",  # "ashes, cinders" = plural of cenere
        "crostini",  # literally says "plural of crostino"
        "dati",  # literally says "plural of dato"
        "melasse",  # literally says "The plural of melassa"
        "nodi",  # literally says "plural of nodo"
        "polveri",  # "dusts, powders" = plural of polvere
        "rispetti",  # "respects" = plural of rispetto
        "ristoranti",  # "restaurants" = plural of ristorante
        "salumi",  # just plural of salume
        "zii",  # literally says "plural of zio"
        "alcelafini",  # just plural of alcelafino
    }
)

# Apocopic forms to skip due to ambiguous/incorrect gender tags in Wiktionary.
# These are blocked (not imported) rather than corrected because:
# - "final" (apocopic of "finale") is tagged masculine but parent is feminine
# - "fin" (apocopic of "fine") is tagged feminine but parent is masculine
# The Wiktionary data is unreliable for these specific forms.
# NOTE: This ONLY blocks the apocopic allomorph forms, not any homonyms.
SKIP_APOCOPIC_ALLOMORPHS: frozenset[str] = frozenset({"final", "fin"})

# Per-lemma blocklist: adjective forms to skip when importing
# These are archaic, dialectal, erroneous, or non-standard forms
# Aggressive list - learners should learn modern standard Italian
BLOCKED_ADJECTIVE_FORMS: dict[str, set[str]] = {
    # === Archaic spellings ===
    "tedesco": {"thedesco", "thedeschi", "thedesca", "thedesche"},
    "ebreo": {"hebreo", "hebrei", "hebrea", "hebree"},
    "storico": {"istorico", "istorici", "istorica", "istoriche"},
    "pratico": {
        "practico",
        "practici",
        "practica",
        "practiche",
        "prattico",
        "prattici",
        "prattica",
        "prattiche",
    },
    # === Dialectal/non-standard ===
    "italiano": {"itagliano"},
    "povero": {"poro", "pori", "pora", "pore", "pover'"},
    "pigmeo": {"pimmeo", "pimmei", "pimmea", "pimmee"},
    "matto": {"matteo", "mattei", "mattea", "mattee"},
    "ladro": {"latro", "latri", "latra", "latre"},
    "nemico": {"nimico", "nimici", "nimica", "nimiche"},
    "veglio": {"ueglio", "uegli", "ueglia", "ueglie"},
    "scimunito": {"scemunito", "scemuniti", "scemunita", "scemunite"},
    "debosciato": {"ribusciato", "ribusciati", "ribusciata", "ribusciate"},
    # === Typos/errors ===
    "assassino": {"assessino", "assessini", "assessina", "assessine"},
    "illegittimo": {"illeggittimo"},
    "proprietario": {"propietario"},
    # === Symbols, not words ===
    "primo": {"1º", "1ª"},
    # === Truncated forms ===
    "solo": {"sol"},
    "vicino": {"vicin"},
    "santo": {"sant'"},
    # === Archaic/poetic ===
    "accidioso": {"accidïoso", "accidïosi", "accidïosa", "accidïose"},
    # === Unusual spelling variants (k for c, etc.) ===
    "ceco": {"ceko", "ceki", "ceka", "ceke"},
    # === Incorrect plurals identified by Gemini/ChatGPT ===
    # These have both wrong AND correct forms in Wiktextract; block wrong ones
    "carolingio": {"carolinge"},  # correct: carolingie
    "porco": {"porchi"},  # correct: porci (irregular)
    "cieco": {"cieci"},  # correct: ciechi (stressed penult rule)
    "bolscevico": {"bolscevici"},  # correct: bolscevichi (hard k)
    "menscevico": {"menscevici"},  # correct: menscevichi (hard k)
    "fenicio": {"fenice"},  # correct: fenicie (fenice = phoenix, different word!)
    "malvagio": {"malvage"},  # correct: malvagie (modern standard)
    # === Non-standard variants to normalize ===
    "ubriaco": {
        "ubbriaco",
        "ubbriachi",
        "ubbriaca",
        "ubbriache",
        "briaco",
        "briachi",
        "briaca",
        "briache",
        "imbriaco",
        "imbriachi",
        "imbriaca",
        "imbriache",
    },  # normalize to ubriaco
    "ufficiale": {"officiale", "officiali"},  # archaic Latin form
    # === Archaic demonyms (block non-standard spellings) ===
    "afghano": {"afgano", "afgani", "afgana", "afgane"},
    "africano": {"affricano", "affricani", "affricana", "affricane"},
    "asiatico": {"asiaco", "asiaci", "asiaca", "asiache"},
    "spagnolo": {"spagnuolo", "spagnuoli", "spagnuola", "spagnuole"},
    "veneziano": {"viniziano", "viniziani", "viniziana", "viniziane"},
    "partigiano": {
        "parteggiano",
        "parteggiani",
        "parteggiana",
        "parteggiane",
        "partegiano",
        "partegiani",
        "partegiana",
        "partegiane",
    },
    "musulmano": {"mussulmano", "mussulmani", "mussulmana", "mussulmane"},
    "jugoslavo": {"iugoslavo", "iugoslavi", "iugoslava", "iugoslave"},
    "giudeo": {"giudio", "giudii", "giudia", "giudie"},
    "pompeiano": {"pompeano", "pompeani", "pompeana", "pompeane"},
    "romagnolo": {"romagnuolo", "romagnuoli", "romagnuola", "romagnuole"},
    "trevigiano": {"trivigiano", "trivigiani", "trivigiana", "trivigiane"},
    "anconetano": {"anconitano", "anconitani", "anconitana", "anconitane"},
    "eremitano": {"romitano", "romitani", "romitana", "romitane"},
    "pitagorico": {"pitagoreo", "pitagorei", "pitagorea", "pitagoree"},
    "quacchero": {"quacquero", "quacqueri", "quacquera", "quacquere"},
    "sardegnolo": {
        "sardagnolo",
        "sardagnoli",
        "sardagnola",
        "sardagnole",
        "sardignolo",
        "sardignoli",
        "sardignola",
        "sardignole",
    },
    "schizzinoso": {"schizzignoso", "schizzignosi", "schizzignosa", "schizzignose"},
    "tapino": {"taupino", "taupini", "taupina", "taupine"},
    "guerraiolo": {"guerraiuolo", "guerraiuoli", "guerraiuola", "guerraiuole"},
    "passeggero": {"passeggiero", "passeggieri", "passeggiera", "passeggiere"},
    "rousseauiano": {"russoiano", "russoiani", "russoiana", "russoiane"},
    "sciagurato": {"sciaurato", "sciaurati", "sciaurata", "sciaurate"},
    "presuntuoso": {
        "presontuoso",
        "presontuosi",
        "presontuosa",
        "presontuose",
        "prosontuoso",
        "prosontuosi",
        "prosontuosa",
        "prosontuose",
    },
    # === Variant spellings of compound nationality words ===
    "hawaiano": {
        "avaiano",
        "avaiani",
        "avaiana",
        "avaiane",
        "hawaiiano",
        "hawaiiani",
        "hawaiiana",
        "hawaiiane",
    },
    "honduregno": {"onduregno", "onduregni", "onduregna", "onduregne"},
    "keniano": {"kenyano", "kenyani", "kenyana", "kenyane"},
    "kosovaro": {
        "cossovaro",
        "cossovari",
        "cossovara",
        "cossovare",
        "kossovaro",
        "kossovari",
        "kossovara",
        "kossovare",
    },
    "laotiano": {"laosiano", "laosiani", "laosiana", "laosiane"},
    "pakistano": {"pachistano", "pachistani", "pachistana", "pachistane"},
    "paraguaiano": {"paraguayano", "paraguayani", "paraguayana", "paraguayane"},
    "uruguaiano": {"uruguayano", "uruguayani", "uruguayana", "uruguayane"},
    "valenciano": {"valenziano", "valenziani", "valenziana", "valenziane"},
    "magrebino": {"maghrebino", "maghrebini", "maghrebina", "maghrebine"},
    # === Misc archaic/variant ===
    "fraudolento": {"frodolento", "frodolenti", "frodolenta", "frodolente"},
    "gallego": {"gagliego", "gaglieghi", "gagliega", "gaglieghe"},
    "infermo": {"infirmo", "infirmi", "infirma", "infirme"},
    "maltusiano": {"malthusiano", "malthusiani", "malthusiana", "malthusiane"},
    "onnivoro": {"omnivoro", "omnivori", "omnivora", "omnivore"},
    "reumatico": {"rematico", "rematici", "rematica", "rematiche"},
    "sconsiderato": {
        "malconsiderato",
        "malconsiderati",
        "malconsiderata",
        "malconsiderate",
    },
    "sprovveduto": {"malprovveduto", "malprovveduti", "malprovvedute"},
    "siriano": {"soriano", "soriani", "soriana", "soriane"},
    "comacino": {"cumacino", "cumacini", "cumacina", "cumacine"},
    "balzachiano": {"balzacchiano", "balzacchiani", "balzacchiana", "balzacchiane"},
    "kolchoziano": {"colcosiano", "colcosiani", "colcosiana", "colcosiane"},
    "eurasiatico": {"euroasiatico", "euroasiatici", "euroasiatica", "euroasiatiche"},
    "ipoacusico": {"ipacusico", "ipacusici", "ipacusica", "ipacusiche"},
    "handicappato": {"andicappato", "andicappati", "andicappata", "andicappate"},
    "cassintegrato": {
        "cassaintegrato",
        "cassaintegrati",
        "cassaintegrata",
        "cassaintegrate",
    },
    "sottoccupato": {"sottooccupato", "sottooccupati", "sottooccupata", "sottooccupate"},
    "ottuagenario": {
        "ottagenario",
        "ottogenario",
        "ottogenari",
        "ottogenaria",
        "ottogenarie",
    },
    "settuagenario": {"settagenario", "settagenari", "settagenaria", "settagenarie"},
    "avventizio": {
        "avveniticcio",
        "avveniticci",
        "avveniticcia",
        "avveniticce",
        "avventiccio",
        "avventicci",
        "avventiccia",
        "avventicce",
        "veniticcio",
        "veniticci",
        "veniticcia",
        "veniticce",
    },
    "egualitario": {
        "egalitario",
        "egalitari",
        "egalitaria",
        "egalitarie",
        "ugualitario",
        "ugualitari",
        "ugualitaria",
        "ugualitarie",
    },
    "risolutore": {"risolutorio", "risolutoria", "risolutorie"},
    "uzbeco": {"uzbeko", "uzbeki", "uzbeka", "uzbeke"},
}

# Forms to block only in specific gender/number contexts
# Structure: lemma -> (gender, number) -> set of blocked forms
BLOCKED_ADJECTIVE_FORMS_GENDERED: dict[str, dict[tuple[str, str], set[str]]] = {
    # invasore: block non-standard feminine forms (correct: invaditrice/invaditrici)
    # NOTE: invasore is both noun and adjective - also blocked in BLOCKED_NOUN_FORMS_GENDERED
    "invasore": {
        ("f", "singular"): {"invasora", "invastrice"},  # Spanish/non-standard patterns
        ("f", "plural"): {"invastrici", "invasore"},  # invasore is only valid as m.sg
    },
}


def is_blocked_adjective_form(
    lemma_written: str,
    form_written: str,
    gender: str,
    number: str,
) -> bool:
    """Check if an adjective form should be blocked.

    Checks both unconditional blocklist and gender-specific blocklist.
    """
    # Check unconditional blocklist
    if form_written in BLOCKED_ADJECTIVE_FORMS.get(lemma_written, set()):
        return True

    # Check gender-specific blocklist
    lemma_gendered = BLOCKED_ADJECTIVE_FORMS_GENDERED.get(lemma_written, {})
    blocked_forms = lemma_gendered.get((gender, number), set())
    return form_written in blocked_forms


# =============================================================================
# Noun form blocklist
# =============================================================================
# Some noun forms from Wiktionary are incorrect or non-standard.
# Structure: lemma -> (gender, number) -> set of blocked forms

BLOCKED_NOUN_FORMS_GENDERED: dict[str, dict[tuple[str, str], set[str]]] = {
    # invasore: block non-standard feminine forms (correct: invaditrice/invaditrici)
    # NOTE: invasore is both noun and adjective - also blocked in BLOCKED_ADJECTIVE_FORMS_GENDERED
    "invasore": {
        ("f", "singular"): {"invasora", "invastrice"},  # Spanish/non-standard patterns
        ("f", "plural"): {"invastrici", "invasore"},  # invasore is only valid as m.sg
    },
}


def is_blocked_noun_form(
    lemma_written: str,
    form_written: str,
    gender: str | None,
    number: str,
) -> bool:
    """Check if a noun form should be blocked.

    Checks the gender-specific blocklist.
    """
    if gender is None:
        return False

    lemma_gendered = BLOCKED_NOUN_FORMS_GENDERED.get(lemma_written, {})
    blocked_forms = lemma_gendered.get((gender, number), set())
    return form_written in blocked_forms


def _parse_entry(line: str) -> dict[str, Any] | None:
    """Parse a JSONL line, returning None if invalid."""
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def _is_pos_lemma(entry: dict[str, Any], pos: str) -> bool:
    """Check if entry is a lemma for the given POS (not an inflected form entry).

    Works for verbs, nouns, and adjectives.

    Note: Some lemmas (e.g., pluralia tantum nouns like 'forbici') have no
    'forms' array at all. The key indicator is that lemmas don't have 'form_of'
    in any sense, while form-of entries do.
    """
    if entry.get("pos") != pos:
        return False

    # For verbs, require forms array (all verbs have conjugation tables)
    # For nouns/adjectives, don't require forms (some may not have explicit declensions)
    if pos == "verb" and "forms" not in entry:
        return False

    # Check if any sense has form_of (meaning this is a form-of entry, not a lemma)
    return all("form_of" not in sense for sense in entry.get("senses", []))


def _extract_auxiliary(entry: dict[str, Any]) -> str | None:
    """Extract auxiliary verb (avere, essere, or both) from forms."""
    auxiliaries: set[str] = set()
    for form in entry.get("forms", []):
        if "auxiliary" in form.get("tags", []):
            aux = normalize(form.get("form", ""))
            if "aver" in aux:
                auxiliaries.add("avere")
            elif "esser" in aux:
                auxiliaries.add("essere")

    if len(auxiliaries) == 2:
        return "both"
    if len(auxiliaries) == 1:
        return auxiliaries.pop()
    return None


def _extract_transitivity(entry: dict[str, Any]) -> str | None:
    """Extract transitivity from senses tags.

    Returns 'transitive', 'intransitive', 'both', or None.
    Result is stored in verb_metadata.transitivity. The raw transitive/intransitive
    tags are therefore filtered from definitions.tags (see DEFINITION_TAG_BLOCKLIST).
    """
    transitive = False
    intransitive = False

    for sense in entry.get("senses", []):
        tags = sense.get("tags", [])
        if "transitive" in tags:
            transitive = True
        if "intransitive" in tags:
            intransitive = True

    if transitive and intransitive:
        return "both"
    if transitive:
        return "transitive"
    if intransitive:
        return "intransitive"
    return None


# Pronominal verb suffixes - includes double clitics like andarsene, cavarsela.
# These end in -rsi or -rsì (pronominal) plus an additional object clitic (ne, la, etc.).
PRONOMINAL_SUFFIXES = (
    "rsi",
    "rsì",
    "rsene",
    "rsela",
    "rselo",
    "rseli",
    "rsele",
    "rseci",
    "rsevi",
)

# Double clitic suffixes that need to be stripped to get the base pronominal form.
# e.g., andarsene → andarsi, cavarsela → cavarsi
DOUBLE_CLITIC_SUFFIXES = ("sene", "sela", "selo", "seli", "sele", "seci", "sevi")


def _is_pronominal_verb(stressed: str) -> bool:
    """Check if a verb is pronominal (ends in -rsi/-rsì or double clitic).

    Italian pronominal verbs are verbs that conjugate with reflexive pronouns.
    Their infinitive ends in -rsi, -rsì, or a double clitic form (-rsene, -rsela, etc.).

    Multi-word phrases are skipped entirely. Some phrases contain pronominal verbs
    (e.g., "dàrsi da fare") but detecting and linking them is complex. For now,
    we only handle single-word pronominal verbs.

    Examples:
        lavarsi → True (reflexive)
        pentirsi → True (inherent pronominal)
        andarsene → True (double clitic)
        parlare → False (non-pronominal)
        "fàre ipotesi" → False (multi-word phrase, skipped)
    """
    # Skip multi-word phrases entirely
    if " " in stressed:
        return False
    return any(stressed.endswith(suffix) for suffix in PRONOMINAL_SUFFIXES)


def _get_pronominal_base_form(stressed: str) -> str | None:
    """Get the base (non-pronominal) form of a pronominal verb.

    Strips the -si suffix (and any double clitic) and converts to regular infinitive:
    - lavarsi → lavare (-arsi → -are)
    - vedersi → vedere (-ersi → -ere)
    - sentirsi → sentire (-irsi → -ire)
    - condursi → condurre (-ursi → -urre)
    - andarsene → andare (-arsene → -are, double clitic stripped)

    Returns None if not a recognizable pronominal form.
    """
    if not _is_pronominal_verb(stressed):
        return None

    # Convert to written form (strips non-final accents).
    # e.g., abbronzàrsi → abbronzarsi
    # This avoids needing to enumerate accented vowel variants (àr, èr, ìr).
    written = derive_written_from_stressed(stressed)
    if written is None:
        return None

    # Handle double clitics first: strip -sene/-sela/etc to get base pronominal form
    # e.g., andarsene → andarsi, cavarsela → cavarsi
    for suffix in DOUBLE_CLITIC_SUFFIXES:
        if written.endswith(suffix):
            written = written[: -len(suffix)] + "si"
            break

    # Handle accented final vowel (e.g., imbufalìrsi keeps accent on final -e)
    if written.endswith("sì"):
        stem = written[:-2]
        final_vowel = "é"
    else:
        stem = written[:-2]  # Strip -si
        final_vowel = "e"

    # Determine conjugation class from (now unaccented) stem ending
    if stem.endswith("ar"):
        return stem + final_vowel  # -arsi → -are
    elif stem.endswith("er"):
        return stem + final_vowel  # -ersi → -ere
    elif stem.endswith("ir"):
        return stem + final_vowel  # -irsi → -ire
    elif stem.endswith("ur"):
        return stem + "re"  # -ursi → -urre (condursi → condurre)

    return None


def _extract_ipa(entry: dict[str, Any]) -> str | None:
    """Extract IPA pronunciation for the infinitive."""
    for sound in entry.get("sounds", []):
        if "ipa" in sound:
            return sound["ipa"]
    return None


def _extract_gender(entry: dict[str, Any]) -> str | None:
    """Extract grammatical gender for nouns.

    Priority: head_templates (robustly scanned) → categories → senses tags.
    Returns 'm' for masculine, 'f' for feminine, None if unknown.
    """
    # Check head_templates first (most reliable when using robust scanning)
    for template in entry.get("head_templates", []):
        args = template.get("args", {})
        gender_arg = _find_gender_in_args(args)
        if gender_arg is not None:
            if gender_arg.startswith("m"):
                return "m"
            if gender_arg.startswith("f"):
                return "f"
            # mf/mfbysense don't give us a single gender

    # Check categories as fallback
    categories: list[str | dict[str, Any]] = entry.get("categories", [])
    for cat in categories:
        cat_name = str(cat.get("name", "")) if isinstance(cat, dict) else (str(cat) if cat else "")
        if "Italian masculine nouns" in cat_name:
            return "m"
        if "Italian feminine nouns" in cat_name:
            return "f"

    # Check senses tags as last resort
    for sense in entry.get("senses", []):
        tags = sense.get("tags", [])
        if "masculine" in tags:
            return "m"
        if "feminine" in tags:
            return "f"

    return None


def _extract_noun_classification(entry: dict[str, Any]) -> dict[str, Any]:
    """Extract noun classification from Wiktextract entry.

    Returns a dict with:
    - gender_class: 'm', 'f', 'GenderClass.COMMON_GENDER_FIXED', 'GenderClass.COMMON_GENDER_VARIABLE'
    - number_class: 'standard', 'pluralia_tantum', 'singularia_tantum', 'invariable'
    - number_class_source: how number_class was determined
    - genders: list of genders present in the forms
    """
    result: dict[str, Any] = {
        "gender_class": None,
        "number_class": "standard",
        "number_class_source": "default",
        "genders": [],
    }

    # Check head_templates for gender markers
    has_masculine = False
    has_feminine = False
    has_counterpart_marker = False  # "f": "+" or "m": "+" indicates forms differ by gender
    is_mfbysense = False  # Different meanings per gender (fine)
    is_invariable = False
    is_invariable_from_wiktextract = False  # Track if Wiktextract explicitly marked invariable
    is_pluralia_tantum = False
    is_singularia_tantum = False

    for template in entry.get("head_templates", []):
        args = template.get("args", {})
        # Robustly find gender marker by scanning all arg values
        gender_arg = _find_gender_in_args(args)

        if gender_arg is None:
            # Check for invariable marker (# in Wiktextract) even without gender
            if "#" in str(args):
                is_invariable = True
                is_invariable_from_wiktextract = True
            continue

        # Check for common gender markers
        if gender_arg in ("mf", "mfbysense"):
            has_masculine = True
            has_feminine = True
            if gender_arg == "mfbysense":
                is_mfbysense = True
        # Check for masculine-only or feminine-only
        elif gender_arg.startswith("m"):
            has_masculine = True
        elif gender_arg.startswith("f"):
            has_feminine = True

        # Check for counterpart markers (e.g., "f": "+" means has feminine counterpart)
        # These indicate the noun has forms in both genders even if main gender is m or f
        # When a counterpart marker exists, the forms differ by gender (e.g., amico/amica)
        if args.get("f"):  # Has feminine counterpart (e.g., amico → amica)
            has_feminine = True
            has_counterpart_marker = True
        if args.get("m"):  # Has masculine counterpart (e.g., amica → amico)
            has_masculine = True
            has_counterpart_marker = True

        # Check for invariable marker (# in Wiktextract)
        if "#" in str(args):
            is_invariable = True
            is_invariable_from_wiktextract = True

        # Check for pluralia tantum (f-p, m-p)
        if gender_arg in ("m-p", "f-p"):
            is_pluralia_tantum = True
        # Check for singularia tantum markers
        if gender_arg.endswith("-s") or gender_arg.endswith("-s!"):
            is_singularia_tantum = True

    # Also check categories for number restrictions
    categories: list[str | dict[str, Any]] = entry.get("categories", [])
    for cat in categories:
        cat_name = str(cat.get("name", "")) if isinstance(cat, dict) else (str(cat) if cat else "")
        if "Italian pluralia tantum" in cat_name or "plurale tantum" in cat_name.lower():
            is_pluralia_tantum = True
        if "Italian uncountable nouns" in cat_name:
            is_singularia_tantum = True
        if "Italian indeclinable nouns" in cat_name:
            is_invariable = True
            is_invariable_from_wiktextract = True

    # Check forms to see if singular/plural have the same text (invariable)
    forms_by_number: dict[str, set[str]] = {"singular": set(), "plural": set()}
    for form_data in entry.get("forms", []):
        form_stressed = form_data.get("form", "")
        tags = form_data.get("tags", [])
        if "singular" in tags:
            forms_by_number["singular"].add(form_stressed)
        if "plural" in tags:
            forms_by_number["plural"].add(form_stressed)

    # If singular and plural forms are identical, mark as invariable
    if (
        forms_by_number["singular"]
        and forms_by_number["plural"]
        and forms_by_number["singular"] == forms_by_number["plural"]
    ):
        is_invariable = True
        is_invariable_from_wiktextract = True

    # Morphological heuristics for invariable nouns (Italian rules)
    # These apply only to single words (no spaces/hyphens) and only if
    # Wiktextract didn't already detect invariability
    word = entry.get("word", "")
    if not is_invariable and " " not in word and "-" not in word:
        # Rule 1: Words ending in accented vowel are always invariable
        # This is a fundamental rule of Italian morphology with no exceptions
        if word and word[-1] in "àèìòù":
            is_invariable = True
            result["number_class_source"] = "inferred:accented_ending"
        # Rule 2: Words ending in -si (not -ssi) are Greek-origin invariables
        # Examples: analisi, crisi, ipotesi, sintesi, tesi
        elif word.endswith("si") and not word.endswith("ssi"):
            is_invariable = True
            result["number_class_source"] = "inferred:greek_si"

    # Determine gender_class
    if has_masculine and has_feminine:
        if is_mfbysense:
            # Different meanings per gender - will create separate lemmas
            result["gender_class"] = GenderClass.BY_SENSE
        elif has_counterpart_marker:
            # Counterpart marker (f: "+" or m: "+") means forms differ by gender
            # (e.g., amico/amica, professore/professoressa)
            result["gender_class"] = GenderClass.COMMON_GENDER_VARIABLE
        else:
            # Check if forms differ by gender
            masc_forms: set[str] = set()
            fem_forms: set[str] = set()
            for form_data in entry.get("forms", []):
                form_stressed = form_data.get("form", "")
                tags = form_data.get("tags", [])
                if "masculine" in tags:
                    masc_forms.add(form_stressed)
                if "feminine" in tags:
                    fem_forms.add(form_stressed)

            if masc_forms and fem_forms and masc_forms != fem_forms:
                result["gender_class"] = GenderClass.COMMON_GENDER_VARIABLE
            else:
                result["gender_class"] = GenderClass.COMMON_GENDER_FIXED
        result["genders"] = ["m", "f"]
    elif has_masculine:
        result["gender_class"] = GenderClass.M
        result["genders"] = ["m"]
    elif has_feminine:
        result["gender_class"] = GenderClass.F
        result["genders"] = ["f"]
    else:
        # Fall back to _extract_gender for simple cases
        simple_gender = _extract_gender(entry)
        if simple_gender:
            result["gender_class"] = GenderClass(simple_gender)
            result["genders"] = [simple_gender]

    # Determine number_class and its source
    if is_pluralia_tantum:
        result["number_class"] = "pluralia_tantum"
        result["number_class_source"] = "wiktextract"
    elif is_singularia_tantum:
        result["number_class"] = "singularia_tantum"
        result["number_class_source"] = "wiktextract"
    elif is_invariable:
        result["number_class"] = "invariable"
        # Source was already set by heuristics if applicable, otherwise it's from Wiktextract
        if is_invariable_from_wiktextract:
            result["number_class_source"] = "wiktextract"
        # else: source was already set by the heuristic (accented_ending or greek_si)
    # else: number_class stays "standard" and source stays "default"

    return result


def _extract_lemma_stressed(entry: dict[str, Any]) -> str:
    """Extract the stressed form of the lemma (infinitive).

    Applies normalizations:
    - Apostrophe spacing (e.g., "d' occhio" -> "d'occhio")
    - Strip bracket annotations (e.g., "[auxiliary essere]")
    - Known overrides for Wiktionary errors (e.g., "sùggere" -> "suggére")
    """
    # First check forms for canonical or infinitive
    for form in entry.get("forms", []):
        tags = form.get("tags", [])
        if "canonical" in tags or "infinitive" in tags:
            stressed = form.get("form", entry["word"])
            break
    else:
        # Fallback to word
        stressed = entry["word"]

    # Normalize apostrophe spacing
    stressed = _normalize_apostrophe_spacing(stressed)

    # Strip bracket annotations (e.g., "[auxiliary essere]", "[transitive 'something'")
    # that Wiktextract sometimes includes in canonical forms
    stressed = _BRACKET_ANNOTATION_RE.sub("", stressed)

    # Strip metadata patterns from malformed canonical forms
    # e.g., "ottimo superlative of buono" -> fall back to entry["word"]
    if re.search(r"\b(superlative|comparative) of \w+\b", stressed, re.IGNORECASE):
        stressed = entry["word"]

    # Apply known overrides for Wiktionary inconsistencies
    stressed = LEMMA_STRESSED_OVERRIDES.get(stressed, stressed)

    return stressed


def _iter_forms(
    entry: dict[str, Any],
    pos: str,
    stressed_alternatives: dict[str, str] | None = None,
) -> Iterator[tuple[str, list[str], str]]:
    """Yield (form_stressed, tags, form_origin) for each inflected form.

    Args:
        entry: Wiktextract entry dict
        pos: Part of speech (verb, noun, adjective)
        stressed_alternatives: Optional lookup for enriching unaccented forms with
            their proper accented spellings (e.g., "dei" → "dèi")

    Yields:
        Tuples of (form_stressed, tags, form_origin) where form_origin is:
        - 'wiktextract': Direct from forms array
        - 'inferred:singular': Added missing singular tag (for gender-only tagged forms)
        - 'inferred:two_form': Generated both genders for 2-form adjective
        - 'inferred:base_form': From lemma word field
        - 'inferred:invariable': Generated all 4 forms for invariable adjective
    """
    seen: set[tuple[str, tuple[str, ...]]] = set()
    has_masc_singular = False
    has_fem_singular = False
    # 2-form adjectives (like "facile", "ottimista") may have either:
    # - genderless number tags in forms array (["plural"] instead of ["masculine", "plural"])
    # - "m or f by sense" in head_templates expansion
    is_two_form = pos == "adjective" and _is_two_form_adjective(entry)
    # Check if this is an invariable adjective (like "blu", "rosa")
    is_invariable = pos == "adjective" and _is_invariable_adjective(entry)

    for form_data in entry.get("forms", []):
        form_stressed = form_data.get("form", "")

        # Enrich with accented alternative if available
        # (fixes bug where Wiktextract stores "dei" but correct spelling is "dèi")
        if stressed_alternatives and not _has_accents(form_stressed):
            key = normalize(form_stressed)
            if key in stressed_alternatives:
                form_stressed = stressed_alternatives[key]

        # Apply typo corrections for feminine noun forms (e.g., "preconizzatice" → "preconizzatrice")
        if pos == "noun" and form_stressed in FEMININE_FORM_CORRECTIONS:
            form_stressed = FEMININE_FORM_CORRECTIONS[form_stressed]

        tags = form_data.get("tags", [])
        tag_set = set(tags)

        # Skip empty forms
        if not form_stressed:
            continue

        # For verbs, skip metadata tags (but not "canonical" - we treat it as infinitive)
        if pos == "verb" and tag_set & (SKIP_TAGS - {"canonical"}):
            continue

        # For nouns/adjectives, skip metadata-only forms but keep forms with meaningful info
        # (e.g., ["canonical", "plural"] has meaningful "plural" tag)
        if pos in ("noun", "adjective"):
            meaningful_tags = tag_set - SKIP_TAGS
            if not meaningful_tags:
                continue

        # Track form_origin for this form
        form_origin = "wiktextract"

        # For nouns: infer singular for forms with gender but no number
        # (e.g., {"form": "amica", "tags": ["feminine"]} → add "singular")
        if pos == "noun":
            has_gender = "masculine" in tag_set or "feminine" in tag_set
            has_number = "singular" in tag_set or "plural" in tag_set
            if has_gender and not has_number:
                tags = [*tags, "singular"]  # Create new list, don't mutate original
                tag_set = set(tags)
                form_origin = "inferred:singular"

        # For adjectives: infer singular for forms with gender but no number
        # (e.g., {"form": "alta", "tags": ["feminine"]} → add "singular")
        if pos == "adjective":
            has_gender = "masculine" in tag_set or "feminine" in tag_set
            has_number = "singular" in tag_set or "plural" in tag_set
            if has_gender and not has_number:
                tags = [*tags, "singular"]
                tag_set = set(tags)
                form_origin = "inferred:singular"

        # For adjectives: forms with number but no gender (2-form adjectives)
        # Generate both masculine and feminine entries since these forms agree with both
        # (e.g., {"form": "facili", "tags": ["plural"]} → m.pl AND f.pl)
        if pos == "adjective":
            has_gender = "masculine" in tag_set or "feminine" in tag_set
            has_number = "singular" in tag_set or "plural" in tag_set
            if has_number and not has_gender:
                # Genderless number tag = 2-form adjective (Wiktextract's explicit signal)
                is_two_form = True
                # Yield masculine version
                tags_m = [*tags, "masculine"]
                key_m = (form_stressed, tuple(sorted(tags_m)))
                if key_m not in seen:
                    seen.add(key_m)
                    # Track if this is the masculine singular base form
                    if "singular" in tag_set:
                        has_masc_singular = True
                    yield form_stressed, tags_m, "inferred:two_form"
                # Yield feminine version
                tags_f = [*tags, "feminine"]
                key_f = (form_stressed, tuple(sorted(tags_f)))
                if key_f not in seen:
                    seen.add(key_f)
                    # Track if this is the feminine singular form
                    if "singular" in tag_set:
                        has_fem_singular = True
                    yield form_stressed, tags_f, "inferred:two_form"
                continue  # Skip the default yield

        # Skip auxiliary markers (they're metadata, not conjugated forms)
        if "auxiliary" in tags:
            continue

        # For verb canonical forms: strip bracket annotations and filter garbage
        # (e.g., "dolére [auxiliary essere" → "dolére", skip "avere]")
        if pos == "verb" and "canonical" in tag_set:
            form_stressed = _BRACKET_ANNOTATION_RE.sub("", form_stressed).strip()
            # Skip garbage-only forms from malformed source data
            if not form_stressed or len(form_stressed) < 2 or form_stressed.endswith("]"):
                continue

        # Track whether we've seen the base forms (for adjectives)
        if pos == "adjective" and "masculine" in tags and "singular" in tags:
            has_masc_singular = True
        if pos == "adjective" and "feminine" in tags and "singular" in tags:
            has_fem_singular = True

        # Deduplicate
        key = (form_stressed, tuple(sorted(tags)))
        if key in seen:
            continue
        seen.add(key)

        yield form_stressed, tags, form_origin

    # Add base form if missing (Wiktextract stores it in 'word', not in 'forms')
    # For adjectives: add masculine singular form if not present
    # Note: noun base forms are handled in the main import loop with proper gender logic
    lemma_stressed = _extract_lemma_stressed(entry)

    if pos == "adjective":
        # For invariable adjectives, generate all 4 gender/number combinations.
        # Known limitation: Some wiktextract entries have contradictory data where
        # inv:1 is set in head_templates but explicit gendered forms also exist
        # (e.g., "culaperto" has inv:1 but also lists culaperta/culaperti/culaperte).
        # In these rare cases (~1 in 1000), we generate both the invariable forms
        # AND the explicit forms, resulting in >4 forms. This is acceptable noise
        # from inconsistent source data.
        if is_invariable:
            for gender in ("masculine", "feminine"):
                for number in ("singular", "plural"):
                    key = (lemma_stressed, tuple(sorted([gender, number])))
                    if key not in seen:
                        seen.add(key)
                        yield lemma_stressed, [gender, number], "inferred:invariable"
        else:
            # Standard handling: add base form if missing
            # First check for gender-restricted adjectives
            is_feminine_only = _is_feminine_only_adjective(entry)
            is_masculine_only = _is_masculine_only_adjective(entry)

            if is_feminine_only:
                # Feminine-only adjectives (incinta, nullipara): add feminine base form
                if not has_fem_singular:
                    key = (lemma_stressed, ("feminine", "singular"))
                    if key not in seen:
                        seen.add(key)
                        yield lemma_stressed, ["feminine", "singular"], "inferred:base_form"
            elif not has_masc_singular:
                # Default: add masculine base form
                key = (lemma_stressed, ("masculine", "singular"))
                if key not in seen:
                    seen.add(key)
                    yield lemma_stressed, ["masculine", "singular"], "inferred:base_form"

            # For 2-form adjectives, add feminine singular too (same form as masculine)
            # But NOT for masculine-only adjectives (f: "-") or feminine-only adjectives
            if (
                not has_fem_singular
                and is_two_form
                and not is_masculine_only
                and not is_feminine_only
            ):
                key = (lemma_stressed, ("feminine", "singular"))
                if key not in seen:
                    yield lemma_stressed, ["feminine", "singular"], "inferred:base_form"


def _iter_definitions(entry: dict[str, Any]) -> Iterator[tuple[str, list[str] | None]]:
    """Yield (gloss, filtered_tags) for each definition.

    Tags in DEFINITION_TAG_BLOCKLIST are filtered out since they're either:
    - Already extracted to proper columns (gender → noun_forms, transitivity → verb_metadata)
    - Noise that doesn't help learners (alt-of, alternative)
    """
    for sense in entry.get("senses", []):
        # Skip form-of entries
        if "form_of" in sense:
            continue

        glosses = sense.get("glosses", [])
        if not glosses:
            continue

        # Join multiple glosses
        gloss = "; ".join(glosses)

        # Filter out blocklisted tags
        raw_tags = sense.get("tags")
        if raw_tags:
            filtered = [t for t in raw_tags if t not in DEFINITION_TAG_BLOCKLIST]
            tags = filtered if filtered else None
        else:
            tags = None

        yield gloss, tags


def _clear_existing_data(conn: Connection, pos_filter: POS) -> int:
    """Clear all existing data for the given POS.

    Deletes in FK-safe order: POS form tables → definitions → frequencies
    → verb_metadata → lemmas.
    Returns the number of lemmas cleared.
    """
    # Count existing lemmas for this POS (for return value)
    count_result = conn.execute(
        select(func.count()).select_from(lemmas).where(lemmas.c.pos == pos_filter)
    )
    count = count_result.scalar() or 0

    if count == 0:
        return 0

    # Use subquery to avoid "too many SQL variables" with large POS categories
    lemma_subq = select(lemmas.c.id).where(lemmas.c.pos == pos_filter)

    # Get the POS-specific form table
    pos_form_table = POS_FORM_TABLES.get(pos_filter)

    # Delete in FK-safe order
    # 1. POS-specific form table
    if pos_form_table is not None:
        conn.execute(pos_form_table.delete().where(pos_form_table.c.lemma_id.in_(lemma_subq)))

    # 2. definitions (references lemmas)
    conn.execute(definitions.delete().where(definitions.c.lemma_id.in_(lemma_subq)))
    # 3. frequencies (references lemmas)
    conn.execute(frequencies.delete().where(frequencies.c.lemma_id.in_(lemma_subq)))
    # 4. POS-specific metadata tables
    if pos_filter == POS.VERB:
        conn.execute(verb_metadata.delete().where(verb_metadata.c.lemma_id.in_(lemma_subq)))
    elif pos_filter == POS.NOUN:
        conn.execute(noun_metadata.delete().where(noun_metadata.c.lemma_id.in_(lemma_subq)))
    elif pos_filter == POS.ADJECTIVE:
        conn.execute(
            adjective_metadata.delete().where(adjective_metadata.c.lemma_id.in_(lemma_subq))
        )
    # 5. lemmas (direct filter, no subquery needed)
    conn.execute(lemmas.delete().where(lemmas.c.pos == pos_filter))

    return count


def _build_verb_form_row(
    lemma_id: int,
    form_stressed: str,
    tags: list[str],
    *,
    form_origin: str = "wiktextract",
    is_citation_form: bool = False,
) -> dict[str, Any] | None:
    """Build a verb_forms row dict from tags, or None if should filter.

    Args:
        lemma_id: The lemma ID to link to
        form_stressed: The stressed form text
        tags: Wiktextract tags for this form
        form_origin: How we determined this form exists:
            - 'wiktextract': Direct from forms array (default)
        is_citation_form: Whether this is the canonical dictionary form (infinitive)
    """
    # Skip defective verb forms (marked as "-" in Wiktionary)
    if form_stressed == "-":
        return None

    # Normalize apostrophe spacing (e.g., "d' occhio" -> "d'occhio")
    form_stressed = _normalize_apostrophe_spacing(form_stressed)

    if should_filter_form(tags):
        return None

    features = parse_verb_tags(tags)
    if features.should_filter or features.mood is None:
        return None

    # For past participles: Wiktextract doesn't provide gender/number tags.
    # All past participles ending in -o are masculine singular citation forms.
    # (Clitic forms like 'creatosi' don't end in -o, so we leave them as NULL.)
    gender = features.gender
    number = features.number
    if (
        features.mood == "participle"
        and gender is None
        and number is None
        and form_stressed.endswith("o")
    ):
        gender = "m"
        number = "singular"

    # Derive written form using Italian orthography rules
    written = derive_written_from_stressed(form_stressed)
    written_source = "derived:orthography_rule" if written is not None else None

    return {
        "lemma_id": lemma_id,
        "written": written,
        "written_source": written_source,
        "stressed": form_stressed,
        "mood": features.mood,
        "tense": features.tense,
        "aspect": features.aspect,
        "person": features.person,
        "number": number,
        "gender": gender,
        "is_formal": features.is_formal,
        "is_negative": features.is_negative,
        "labels": features.labels,
        "form_origin": form_origin,
        "is_citation_form": is_citation_form,
    }


def _get_counterpart_form(entry: dict[str, Any], lemma_gender: str | None) -> str | None:
    """Extract the counterpart form text from an entry.

    For masculine lemma "amico", returns "amica" from {"form": "amica", "tags": ["feminine"]}.
    For feminine lemma "nonna", returns "nonno" from {"form": "nonno", "tags": ["masculine"]}.

    Args:
        entry: The Wiktextract entry
        lemma_gender: The lemma's gender ("m" or "f")

    Returns:
        The counterpart form text, or None if not found.
    """
    # Look for the opposite gender's singular form
    target_tag = "feminine" if lemma_gender == "m" else "masculine"

    for form_data in entry.get("forms", []):
        tags = form_data.get("tags", [])
        if target_tag in tags and "plural" not in tags:
            return form_data.get("form")
    return None


def _build_noun_form_row(
    lemma_id: int,
    form_stressed: str,
    tags: list[str],
    lemma_gender: str | None = None,
    *,
    meaning_hint: str | None = None,
    written_source: str = "wiktionary",
    form_origin: str = "wiktextract",
    is_citation_form: bool = False,
) -> dict[str, Any] | None:
    """Build a noun_forms row dict from tags, or None if should filter.

    Gender is extracted per-form from tags. For forms without explicit gender tags,
    falls back to lemma_gender (typically for singular forms).

    Args:
        lemma_id: The lemma ID to link to
        form_stressed: The stressed form text
        tags: Wiktextract tags for this form
        lemma_gender: Fallback gender if not in tags
        meaning_hint: Optional semantic hint for meaning-dependent plurals
            (e.g., "anatomical" vs "figurative" for braccio)
        written_source: Source indicator - "wiktionary" for forms from wiktextract
            forms array (default), "synthesized" for forms extracted from
            head_templates only
        form_origin: How we determined this form exists:
            - 'wiktextract': Direct from forms array with explicit gender tags (default)
            - 'wiktextract:gender_fallback': From forms array but gender came from lemma
            - 'inferred:singular': Added missing singular tag
        is_citation_form: Whether this is the canonical dictionary form
    """
    if should_filter_form(tags):
        return None

    features = parse_noun_tags(tags)
    if features.should_filter or features.number is None:
        return None

    # Extract gender from tags (for forms like "uova" with ["feminine", "plural"])
    # Track if we used fallback so we can mark form_origin appropriately
    # Gender is stored as 'm'/'f' (short form)
    gender: str | None = None
    gender_from_fallback = False
    if "masculine" in tags:
        gender = "m"
    elif "feminine" in tags:
        gender = "f"
    elif lemma_gender:
        # Fall back to lemma gender for forms without explicit gender tag
        # lemma_gender is already 'm'/'f'
        gender_from_fallback = True
        gender = lemma_gender

    # Filter out forms without gender (incomplete data)
    if gender is None:
        return None

    # Track when gender came from fallback (not explicit tags)
    effective_origin = form_origin
    if gender_from_fallback and form_origin == "wiktextract":
        effective_origin = "wiktextract:gender_fallback"

    # gender is already 'm'/'f' (short form)
    gender_short = gender

    # Compute definite article from orthography
    def_article, article_source = get_definite(form_stressed, gender_short, features.number)

    return {
        "lemma_id": lemma_id,
        "written": None,
        "written_source": written_source,  # Always include to ensure consistent batch insert keys
        "stressed": form_stressed,
        "gender": gender,
        "number": features.number,
        "labels": features.labels,
        "derivation_type": features.derivation_type,
        "meaning_hint": meaning_hint,
        "def_article": def_article,
        "article_source": article_source,
        "form_origin": effective_origin,
        "is_citation_form": is_citation_form,
    }


def _is_trackable_base_form(row: dict[str, Any], tags: list[str]) -> bool:
    """Check if a noun form should be tracked in seen_base_forms.

    Returns False for forms that shouldn't block base form inference:
    - Derived forms (diminutives, augmentatives, pejoratives)
    - Alternative forms (alternative spellings/variants of the lemma)

    These can coexist with the canonical lemma word.
    """
    if row.get("derivation_type"):
        return False
    return "alternative" not in tags


def _is_noun_citation_form(
    form_stressed: str,
    tags: list[str],
    lemma_stressed: str,
    number_class: str | None,
) -> bool:
    """Determine if a noun form is the citation (dictionary) form.

    Citation form is:
    - For pluralia tantum nouns: plural form matching lemma_stressed
    - For all other nouns: singular form matching lemma_stressed

    Args:
        form_stressed: The stressed form of this particular form
        tags: Wiktextract tags for this form
        lemma_stressed: The lemma's stressed form (from head word)
        number_class: The noun's number classification (from noun_metadata)

    Returns:
        True if this form should be marked as citation form.
    """
    if form_stressed != lemma_stressed:
        return False

    is_plural = "plural" in tags
    is_singular = "singular" in tags or "plural" not in tags  # Default to singular

    if number_class == "pluralia_tantum":
        return is_plural
    else:
        return is_singular


def _build_adjective_form_row(
    lemma_id: int,
    form_stressed: str,
    tags: list[str],
    *,
    form_origin: str = "wiktextract",
    is_citation_form: bool = False,
) -> dict[str, Any] | None:
    """Build an adjective_forms row dict from tags, or None if should filter.

    Args:
        lemma_id: The lemma ID to link to
        form_stressed: The stressed form text
        tags: Wiktextract tags for this form
        form_origin: How we determined this form exists:
            - 'wiktextract': Direct from forms array
            - 'inferred:singular': Added missing singular tag
            - 'inferred:two_form': Generated both genders for 2-form adjective
            - 'inferred:base_form': From lemma word field
            - 'inferred:invariable': Generated all 4 forms for invariable adjective
            - 'morphit': From Morphit fallback
        is_citation_form: Whether this is the canonical dictionary form (masculine singular)
    """
    if should_filter_form(tags):
        return None

    features = parse_adjective_tags(tags)
    if features.should_filter or features.gender is None or features.number is None:
        return None

    # features.gender is already 'm' or 'f' from parse_adjective_tags
    gender_short = features.gender

    # Compute definite article from orthography
    def_article, article_source = get_definite(form_stressed, gender_short, features.number)

    return {
        "lemma_id": lemma_id,
        "written": None,
        "stressed": form_stressed,
        "gender": features.gender,
        "number": features.number,
        "degree": features.degree,
        "labels": features.labels,
        "def_article": def_article,
        "article_source": article_source,
        "form_origin": form_origin,
        "is_citation_form": is_citation_form,
    }


# Mapping from POS to form row builder
POS_FORM_BUILDERS: dict[POS, Any] = {
    POS.VERB: _build_verb_form_row,
    POS.NOUN: _build_noun_form_row,
    POS.ADJECTIVE: _build_adjective_form_row,
}


def _count_lines(path: Path) -> int:
    """Count lines in a file efficiently (cached).

    Results are cached by resolved path to avoid re-reading large files
    multiple times during the import pipeline.
    """
    resolved = path.resolve()
    if resolved not in _line_count_cache:
        with path.open(encoding="utf-8") as f:
            _line_count_cache[resolved] = sum(1 for _ in f)
    return _line_count_cache[resolved]


def import_wiktextract(
    conn: Connection,
    jsonl_path: Path,
    *,
    pos_filter: POS = POS.VERB,
    batch_size: int = 1000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import Wiktextract data into the database.

    This function is idempotent: it clears existing data for the POS before importing.
    All operations happen within the caller's transaction, so on failure the database
    rolls back to its original state.

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to the Wiktextract JSONL file
        pos_filter: Part of speech to import
        batch_size: Number of forms to insert per batch
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts of imported items
    """
    # Clear existing data first (idempotency)
    cleared = _clear_existing_data(conn, pos_filter)

    stats: dict[str, int] = {
        "lemmas": 0,
        "forms": 0,
        "forms_filtered": 0,
        "nouns_skipped_no_gender": 0,
        "definitions": 0,
        "skipped": 0,
        "misspellings_skipped": 0,
        "alt_forms_skipped": 0,
        "blocklisted_lemmas": 0,
        "skipped_plural_duplicate": 0,
        "counterpart_no_plural": 0,  # Nouns where counterpart plural not found in lookup
        "counterpart_wrong_gender": 0,  # Nouns where counterpart has wrong gender in Wiktextract
        "no_counterpart_no_gender": 0,  # Nouns with no counterpart AND no gender tag on plural
        "adjective_forms_blocked": 0,  # Forms filtered by BLOCKED_ADJECTIVE_FORMS
        "noun_forms_blocked": 0,  # Forms filtered by BLOCKED_NOUN_FORMS_GENDERED
        "cleared": cleared,
    }

    # Collect adjective relationship data for post-processing
    # (relationships are resolved after all lemmas are inserted)
    degree_links: list[
        tuple[int, str, str, str]
    ] = []  # (lemma_id, base_word, relationship, source)

    # Get POS-specific table and row builder
    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    build_form_row = POS_FORM_BUILDERS.get(pos_filter)

    if pos_form_table is None or build_form_row is None:
        msg = f"Unsupported POS: {pos_filter}"
        raise ValueError(msg)

    form_batch: list[dict[str, Any]] = []
    definition_batch: list[dict[str, Any]] = []

    # Track unique verb forms to avoid duplicates (Wiktextract source sometimes has duplicates).
    # Two structures handle cross-batch deduplication:
    # - seen_verb_keys: All keys ever seen (never cleared) - prevents cross-batch duplicates
    # - current_batch_map: Keys in current batch with indices - enables replacement logic
    seen_verb_keys: set[tuple[Any, ...]] = set()
    current_batch_map: dict[tuple[Any, ...], tuple[dict[str, Any], int]] = {}

    # Track unique noun forms to avoid duplicates (some nouns have multiple Wiktextract entries)
    # Key: (lemma_id, stressed, gender, number)
    seen_noun_keys: set[tuple[int, str, str, str]] = set()

    def _verb_form_key_normalized(row: dict[str, Any]) -> tuple[Any, ...]:
        """Create a grammatical key for verb form deduplication.

        Returns a tuple of grammatical attributes that uniquely identify a verb
        form slot. The key excludes 'stressed' and 'labels' since:
        - Forms with different stress notation (accòrgo vs accórgo) are duplicates
        - Forms with/without labels in the same slot are duplicates

        When conflicts exist, add_form() applies these preferences:
        1. Prefer unlabeled over labeled versions
        2. Prefer grave over acute accents
        """
        return (
            row["lemma_id"],
            row["mood"],
            row.get("tense"),
            row.get("aspect"),
            row.get("person"),
            row.get("number"),
            row.get("gender"),
            row.get("is_formal", False),
            row.get("is_negative", False),
        )

    def _has_acute_accent(stressed: str) -> bool:
        """Check if a stressed form contains acute accents (ó, é)."""
        return "ó" in stressed or "é" in stressed or "Ó" in stressed or "É" in stressed

    def add_form(row: dict[str, Any]) -> bool:
        """Add a form to the batch, with deduplication for verbs and nouns.

        Returns True if the form was added, False if it was a duplicate.

        For verbs, implements deduplication preferences:
        1. Prefer unlabeled over labeled (when same grammatical slot exists
           with and without labels, keep unlabeled)
        2. Prefer grave over acute accents (when same slot exists with both
           accent types like accòrgo vs accórgo, keep grave)

        For nouns, implements simple deduplication by (lemma_id, stressed,
        gender, number) - first form wins, duplicates are skipped.

        This handles inconsistent Wiktionary source data where the same form
        may appear multiple times with different annotations.
        """
        if pos_filter == POS.VERB:
            key = _verb_form_key_normalized(row)

            # Case 1: Already seen in a PREVIOUS batch - skip entirely
            # (Can't do replacement logic since old batch is already committed)
            if key in seen_verb_keys and key not in current_batch_map:
                return False

            # Case 2: Already seen in CURRENT batch - use replacement logic
            if key in current_batch_map:
                old_row, old_idx = current_batch_map[key]
                old_labels = old_row.get("labels")
                new_labels = row.get("labels")
                old_stressed = old_row["stressed"]
                new_stressed = row["stressed"]

                # Priority 1: Prefer unlabeled over labeled
                old_is_labeled = old_labels is not None
                new_is_labeled = new_labels is not None

                if old_is_labeled and not new_is_labeled:
                    # New is unlabeled, old is labeled → replace with new
                    # Preserve is_citation_form from old row (bug fix for accent variants)
                    if old_row.get("is_citation_form"):
                        row["is_citation_form"] = True
                    form_batch[old_idx] = row
                    current_batch_map[key] = (row, old_idx)
                    return True
                elif not old_is_labeled and new_is_labeled:
                    # Old is unlabeled, new is labeled → keep old, skip new
                    return False

                # Priority 2: Both same label status → prefer grave over acute
                if _has_acute_accent(old_stressed) and not _has_acute_accent(new_stressed):
                    # New is grave, old is acute → replace with new
                    # Preserve is_citation_form from old row (bug fix for accent variants)
                    if old_row.get("is_citation_form"):
                        row["is_citation_form"] = True
                    form_batch[old_idx] = row
                    current_batch_map[key] = (row, old_idx)
                    return True

                # Otherwise skip the new form (old is already better or same)
                return False

            # Case 3: New form - add to both tracking structures
            seen_verb_keys.add(key)
            current_batch_map[key] = (row, len(form_batch))

        # Noun deduplication: simple key-based check (no replacement logic needed)
        if pos_filter == POS.NOUN:
            key = (row["lemma_id"], row["stressed"], row["gender"], row["number"])
            if key in seen_noun_keys:
                return False
            seen_noun_keys.add(key)

        form_batch.append(row)
        return True

    def flush_batches() -> None:
        nonlocal form_batch, definition_batch, current_batch_map
        if form_batch:
            conn.execute(pos_form_table.insert(), form_batch)
            stats["forms"] += len(form_batch)
            form_batch = []
            # Clear current_batch_map since indices pointed into the old batch.
            # seen_verb_keys is NOT cleared - it prevents cross-batch duplicates.
            current_batch_map = {}

        if definition_batch:
            conn.execute(definitions.insert(), definition_batch)
            stats["definitions"] += len(definition_batch)
            definition_batch = []

    # Map to Wiktextract's POS naming
    wiktextract_pos = WIKTEXTRACT_POS.get(pos_filter, pos_filter)

    # Build lookup of accented alternatives for nouns
    # (fixes bug where Wiktextract stores "dei" but correct spelling is "dèi")
    stressed_alternatives: dict[str, str] | None = None
    if pos_filter == POS.NOUN:
        stressed_alternatives = _build_stressed_alternatives(jsonl_path)

    # Build lookup of counterpart plurals for nouns
    # (fixes bug where "amico" gets "amici" for both genders instead of "amiche" for f)
    counterpart_plurals: dict[str, tuple[str, str | None]] | None = None
    if pos_filter == POS.NOUN:
        counterpart_plurals = _build_counterpart_plurals(jsonl_path)

    # Count lines for progress if callback provided
    total_lines = _count_lines(jsonl_path) if progress_callback else 0
    current_line = 0

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            current_line += 1
            if progress_callback and current_line % 10000 == 0:
                progress_callback(current_line, total_lines)

            entry = _parse_entry(line)
            if entry is None:
                continue

            # Filter by POS (using Wiktextract's naming)
            if entry.get("pos") != wiktextract_pos:
                continue

            # Filter out misspellings (applies to all POS)
            if _is_misspelling(entry):
                stats["misspellings_skipped"] += 1
                continue

            # Filter out lemmas with malformed Wiktextract data (applies to all POS)
            if _is_blocklisted_lemma(entry):
                stats["blocklisted_lemmas"] += 1
                continue

            # Filter out PURE alt-of entries for adjectives and nouns
            # These are alternative spellings, apocopic forms, archaic variants, etc.
            # that shouldn't be separate lemmas. Mixed entries (with regular senses too)
            # are preserved. Adjective allomorphs are later imported via import_adjective_allomorphs().
            if pos_filter in (POS.ADJECTIVE, POS.NOUN) and _is_pure_alt_form_entry(entry):
                stats["alt_forms_skipped"] += 1
                continue

            # Only import lemmas, not form entries
            if not _is_pos_lemma(entry, wiktextract_pos):
                stats["skipped"] += 1
                continue

            # Extract lemma data
            word = entry["word"]
            lemma_stressed = _extract_lemma_stressed(entry)

            # For nouns: skip known duplicate plural lemmas
            if pos_filter == POS.NOUN and lemma_stressed in SKIP_PLURAL_NOUN_LEMMAS:
                stats["skipped_plural_duplicate"] += 1
                continue

            # For nouns: pre-check gender info before inserting lemma
            # Skip entries that would result in zero forms (incomplete Wiktionary entries)
            noun_class: dict[str, Any] | None = None
            if pos_filter == POS.NOUN:
                noun_class = _extract_noun_classification(entry)
                gender_class = noun_class.get("gender_class")
                # If no gender from classification, try fallback extraction
                if gender_class is None and _extract_gender(entry) is None:
                    stats["nouns_skipped_no_gender"] += 1
                    continue

            # Insert lemma (no unique constraint - homographs create separate entries)
            result = conn.execute(
                lemmas.insert().values(
                    written=None,  # Will be filled by enrich_lemma_written()
                    written_source=None,
                    stressed=lemma_stressed,
                    pos=pos_filter,
                    ipa=_extract_ipa(entry),
                )
            )
            pk = result.inserted_primary_key
            if pk is None:
                continue
            lemma_id: int = pk[0]
            stats["lemmas"] += 1

            # Insert POS-specific metadata
            lemma_gender: str | None = None
            if pos_filter == POS.NOUN:
                # noun_class was already extracted in the pre-check above
                assert noun_class is not None
                gender_class = noun_class.get("gender_class")
                number_class = noun_class.get("number_class", "standard")
                number_class_source = noun_class.get("number_class_source", "default")

                if gender_class is None:
                    # No structured classification, but we have gender from fallback extraction
                    # (otherwise we would have skipped this entry in the pre-check)
                    lemma_gender = _extract_gender(entry)
                else:
                    # Insert noun_metadata
                    conn.execute(
                        noun_metadata.insert().values(
                            lemma_id=lemma_id,
                            gender_class=gender_class,
                            number_class=number_class,
                            number_class_source=number_class_source,
                        )
                    )
                    # Set lemma_gender for form generation (fallback for forms without explicit gender)
                    if gender_class in (GenderClass.M, GenderClass.F):
                        lemma_gender = gender_class
                    elif gender_class == GenderClass.COMMON_GENDER_FIXED:
                        # For fixed common gender (BY_SENSE), same form for both - no default needed
                        lemma_gender = None
                    elif gender_class == GenderClass.COMMON_GENDER_VARIABLE:
                        # For variable common gender (amico/amica), the lemma has a specific gender
                        # that tells us which gender untagged forms belong to
                        lemma_gender = _extract_gender(entry)

            # For nouns: extract plural qualifiers and set up meaning_hint tracking
            plural_qualifiers: dict[str, tuple[str | None, str | None]] = {}
            form_meaning_hints: dict[str, str] = {}  # form_text -> meaning_hint
            synthesize_plurals: list[tuple[str, str, str]] = []  # (form, gender, hint)

            if pos_filter == POS.NOUN:
                # Extract qualifiers from head_templates (e.g., braccia<g:f><q:anatomical>)
                plural_qualifiers = _extract_plural_qualifiers(entry)

                # Check if lemma is in DEFINITION_FORM_LINKAGE for meaning-dependent plurals
                if word in DEFINITION_FORM_LINKAGE:
                    linkage = DEFINITION_FORM_LINKAGE[word]
                    # Create meaning_hint lookup from the linkage keys (plural forms)
                    # Use the form text itself as the hint (simple, stable)
                    form_meaning_hints = {form_text: form_text for form_text in linkage}

                    # Check if we need to synthesize plurals (forms only in head_templates)
                    # Only count forms that would actually be imported (not filtered)
                    forms_in_array = {
                        f.get("form", "")
                        for f in entry.get("forms", [])
                        if "plural" in f.get("tags", [])
                        and not should_filter_form(f.get("tags", []))
                    }
                    for form_text, (gender, _qualifier) in plural_qualifiers.items():
                        if form_text not in forms_in_array and form_text != "+" and gender:
                            # This plural is only in head_templates, needs synthesis
                            synthesize_plurals.append(
                                (form_text, gender, form_meaning_hints.get(form_text, ""))
                            )

            elif pos_filter == POS.VERB:
                auxiliary = _extract_auxiliary(entry)
                transitivity = _extract_transitivity(entry)
                # Always insert verb_metadata so we have a row to update
                # for pronominal verb linking in post-processing
                conn.execute(
                    verb_metadata.insert().values(
                        lemma_id=lemma_id,
                        auxiliary=auxiliary,
                        transitivity=transitivity,
                        # base_verb_lemma_id and pronominal_type are populated
                        # in post-processing after all verbs are inserted
                    )
                )

            elif pos_filter == POS.ADJECTIVE:
                # Insert adjective metadata with inflection class
                inflection_class = _get_adjective_inflection_class(entry)
                conn.execute(
                    adjective_metadata.insert().values(
                        lemma_id=lemma_id,
                        inflection_class=inflection_class,
                        # base_lemma_id, degree_relationship are populated
                        # in post-processing after all lemmas are inserted
                    )
                )

                # Collect comparative/superlative relationships for post-processing
                degree_info = _extract_degree_relationship(entry)
                if degree_info:
                    base_word, relationship, source = degree_info
                    degree_links.append((lemma_id, base_word, relationship, source))

            # Queue forms for batch insert (using POS-specific builder)
            # Track base form number/gender combinations for nouns (excludes diminutives,
            # augmentatives, pejoratives to avoid blocking base form inference)
            seen_base_forms: set[tuple[str, str]] = set()  # (number, gender)

            # Track if we've already marked a citation form for verbs (avoid duplicates
            # when multiple infinitive variants exist, e.g., chièdere / chiédere)
            verb_citation_marked = False

            # Track if we've already marked a citation form for adjectives (for feminine-only
            # adjectives like 'incinta' where f/s should be the citation form)
            adj_citation_marked = False

            # Pre-scan for adjectives: check if masculine singular will exist
            # This determines whether m/s or f/s should be the citation form.
            # Key insight: m/s will ALWAYS exist unless the adjective is feminine-only,
            # because _iter_forms() adds the lemma word as m/s via base form inference.
            # For feminine-only adjectives (like "incinta"), only f/s exists.
            adj_has_masc_singular = pos_filter == POS.ADJECTIVE and not _is_feminine_only_adjective(
                entry
            )

            # Pre-scan: collect explicit gender-tagged plurals from this entry
            # (used to avoid duplicating untagged plurals when explicit ones exist)
            explicit_fem_plurals: set[str] = set()
            explicit_masc_plurals: set[str] = set()
            if pos_filter == POS.NOUN:
                for form_data in entry.get("forms", []):
                    form_text = form_data.get("form", "")
                    form_tags = form_data.get("tags", [])
                    if "plural" in form_tags:
                        if "feminine" in form_tags:
                            explicit_fem_plurals.add(form_text)
                        if "masculine" in form_tags:
                            explicit_masc_plurals.add(form_text)

            for form_stressed, tags, form_origin in _iter_forms(
                entry, pos_filter, stressed_alternatives
            ):
                if pos_filter == POS.NOUN:
                    # Get number_class for citation form determination
                    loop_number_class = (
                        noun_class.get("number_class", "standard") if noun_class else "standard"
                    )

                    # Skip singular forms for pluralia tantum nouns
                    is_pluralia_tantum = loop_number_class == "pluralia_tantum"
                    if is_pluralia_tantum and "singular" in tags:
                        continue

                    # Check blocklist for erroneous noun forms
                    form_gender_for_blocklist = (
                        "m" if "masculine" in tags else ("f" if "feminine" in tags else None)
                    )
                    form_number_for_blocklist = "plural" if "plural" in tags else "singular"
                    form_written_for_blocklist = (
                        derive_written_from_stressed(form_stressed) or form_stressed
                    )
                    if is_blocked_noun_form(
                        word,
                        form_written_for_blocklist,
                        form_gender_for_blocklist,
                        form_number_for_blocklist,
                    ):
                        stats["noun_forms_blocked"] += 1
                        continue

                    # Check if this is a common gender noun without explicit gender in tags
                    has_gender_tag = "masculine" in tags or "feminine" in tags
                    is_common_gender = noun_class and noun_class.get("gender_class") in (
                        GenderClass.COMMON_GENDER_FIXED,
                        GenderClass.COMMON_GENDER_VARIABLE,
                        GenderClass.BY_SENSE,
                    )

                    if is_common_gender and not has_gender_tag:
                        # For common_gender nouns without explicit gender tags:
                        # - COMMON_GENDER_FIXED/BY_SENSE: same form works for both genders
                        # - COMMON_GENDER_VARIABLE: different forms for m/f (need counterpart lookup)
                        gender_class = noun_class.get("gender_class") if noun_class else None
                        is_variable_gender = gender_class == GenderClass.COMMON_GENDER_VARIABLE

                        if is_variable_gender and "plural" in tags:
                            # Smart handling for variable-gender nouns (e.g., amico/amica)
                            # Guard: need lemma_gender to determine which gender this belongs to
                            if not lemma_gender:
                                logger.warning(
                                    f"Noun '{word}' is GenderClass.COMMON_GENDER_VARIABLE with untagged "
                                    f"plural '{form_stressed}' but has no lemma gender. Skipping."
                                )
                                continue

                            # Determine which gender this untagged plural belongs to
                            own_gender = lemma_gender  # "m" for amico, "f" for nonna
                            other_gender = "f" if lemma_gender == "m" else "m"

                            # Check if entry has explicit plural for the other gender
                            has_explicit_other_plural = (
                                explicit_fem_plurals
                                if other_gender == "f"
                                else explicit_masc_plurals
                            )

                            if has_explicit_other_plural:
                                # Case A: Entry has explicit other-gender plural (e.g., "dio" has "dee")
                                # Treat untagged plural as own-gender-only
                                row = _build_noun_form_row(
                                    lemma_id,
                                    form_stressed,
                                    tags,
                                    own_gender,
                                    meaning_hint=form_meaning_hints.get(form_stressed),
                                )
                                if row:
                                    add_form(row)
                                    if _is_trackable_base_form(row, tags):
                                        seen_base_forms.add(("plural", own_gender))
                                else:
                                    stats["forms_filtered"] += 1
                                continue

                            # Case B: Try counterpart lookup (e.g., "amico" → "amica" → "amiche")
                            counterpart = _get_counterpart_form(entry, lemma_gender)
                            if counterpart and counterpart_plurals:
                                if counterpart in counterpart_plurals:
                                    other_plural, counterpart_gender = counterpart_plurals[
                                        counterpart
                                    ]
                                    # Verify counterpart has expected gender (some Wiktextract
                                    # entries have wrong gender, e.g., "maialina" marked as "m")
                                    if counterpart_gender != other_gender:
                                        # Wrong gender - can't trust this plural
                                        # Fall through to Case C handling
                                        stats["counterpart_wrong_gender"] += 1
                                    else:
                                        # Generate own gender with this form
                                        row = _build_noun_form_row(
                                            lemma_id,
                                            form_stressed,
                                            tags,
                                            own_gender,
                                            meaning_hint=form_meaning_hints.get(form_stressed),
                                        )
                                        if row:
                                            add_form(row)
                                            if _is_trackable_base_form(row, tags):
                                                seen_base_forms.add(("plural", own_gender))
                                        else:
                                            stats["forms_filtered"] += 1

                                        # Generate other gender with looked-up plural
                                        row = _build_noun_form_row(
                                            lemma_id,
                                            other_plural,
                                            tags,
                                            other_gender,
                                            meaning_hint=form_meaning_hints.get(other_plural),
                                        )
                                        if row:
                                            add_form(row)
                                            if _is_trackable_base_form(row, tags):
                                                seen_base_forms.add(("plural", other_gender))
                                        else:
                                            stats["forms_filtered"] += 1
                                        continue
                                # Case C: Counterpart not in lookup, or wrong gender
                                # (aggregated - logged at end of import)
                                # Only create own-gender plural; let enrichment handle other
                                if counterpart not in counterpart_plurals:
                                    stats["counterpart_no_plural"] += 1
                                row = _build_noun_form_row(
                                    lemma_id,
                                    form_stressed,
                                    tags,
                                    own_gender,
                                    meaning_hint=form_meaning_hints.get(form_stressed),
                                )
                                if row:
                                    add_form(row)
                                    if _is_trackable_base_form(row, tags):
                                        seen_base_forms.add(("plural", own_gender))
                                else:
                                    stats["forms_filtered"] += 1
                                continue

                            # Case D: Plural but no counterpart info - use own gender only
                            # (aggregated - logged at end of import)
                            stats["no_counterpart_no_gender"] += 1
                            row = _build_noun_form_row(
                                lemma_id,
                                form_stressed,
                                tags,
                                own_gender,
                                meaning_hint=form_meaning_hints.get(form_stressed),
                            )
                            if row:
                                add_form(row)
                                if _is_trackable_base_form(row, tags):
                                    seen_base_forms.add(("plural", own_gender))
                            else:
                                stats["forms_filtered"] += 1
                            continue

                        else:
                            # For fixed-gender nouns (GenderClass.BY_SENSE) or non-plural forms:
                            # duplicate for both genders with same form
                            # Only mark first gender (m) as citation form to avoid duplicates
                            citation_marked = False
                            for gender in ("m", "f"):
                                is_citation = not citation_marked and _is_noun_citation_form(
                                    form_stressed, tags, lemma_stressed, loop_number_class
                                )
                                row = _build_noun_form_row(
                                    lemma_id,
                                    form_stressed,
                                    tags,
                                    gender,
                                    meaning_hint=form_meaning_hints.get(form_stressed),
                                    is_citation_form=is_citation,
                                )
                                if row is None:
                                    stats["forms_filtered"] += 1
                                    continue
                                if is_citation:
                                    citation_marked = True
                                add_form(row)
                                if _is_trackable_base_form(row, tags):
                                    number = "plural" if "plural" in tags else "singular"
                                    seen_base_forms.add((number, gender))
                    else:
                        row = _build_noun_form_row(
                            lemma_id,
                            form_stressed,
                            tags,
                            lemma_gender,
                            meaning_hint=form_meaning_hints.get(form_stressed),
                            is_citation_form=_is_noun_citation_form(
                                form_stressed, tags, lemma_stressed, loop_number_class
                            ),
                        )
                        if row is None:
                            stats["forms_filtered"] += 1
                            continue
                        add_form(row)
                        if _is_trackable_base_form(row, tags):
                            number = "plural" if "plural" in tags else "singular"
                            gender = (
                                "m"
                                if "masculine" in tags
                                else ("f" if "feminine" in tags else lemma_gender)
                            )
                            if gender:
                                seen_base_forms.add((number, gender))
                else:
                    # Pass form_origin to all POS form builders
                    if pos_filter == POS.ADJECTIVE:
                        # Extract gender/number from tags for blocklist check
                        form_gender = (
                            "m" if "masculine" in tags else ("f" if "feminine" in tags else None)
                        )
                        form_number = "plural" if "plural" in tags else "singular"

                        # Check blocklist for archaic/erroneous adjective forms
                        lemma_written = derive_written_from_stressed(lemma_stressed)
                        form_written = derive_written_from_stressed(form_stressed) or form_stressed
                        if (
                            lemma_written
                            and form_gender
                            and is_blocked_adjective_form(
                                lemma_written, form_written, form_gender, form_number
                            )
                        ):
                            stats["adjective_forms_blocked"] += 1
                            continue

                        # Citation form: m/s for standard adjectives, f/s only for feminine-only
                        is_masc_singular = form_gender == "m" and form_number == "singular"
                        is_fem_singular = form_gender == "f" and form_number == "singular"

                        # Only mark m/s as citation, OR f/s if this is a feminine-only adjective
                        is_adj_citation = (is_masc_singular and not adj_citation_marked) or (
                            is_fem_singular
                            and not adj_has_masc_singular
                            and not adj_citation_marked
                        )

                        row = _build_adjective_form_row(
                            lemma_id,
                            form_stressed,
                            tags,
                            form_origin=form_origin,
                            is_citation_form=is_adj_citation,
                        )
                        if row and is_adj_citation:
                            adj_citation_marked = True
                    elif pos_filter == POS.VERB:
                        # Citation form is infinitive (tagged as "infinitive" or "canonical")
                        # Only mark first infinitive to avoid duplicates for stress variants
                        is_infinitive = "infinitive" in tags or "canonical" in tags
                        is_verb_citation = is_infinitive and not verb_citation_marked
                        row = _build_verb_form_row(
                            lemma_id,
                            form_stressed,
                            tags,
                            form_origin=form_origin,
                            is_citation_form=is_verb_citation,
                        )
                        if row and is_verb_citation:
                            verb_citation_marked = True
                    else:
                        row = build_form_row(lemma_id, form_stressed, tags)
                    if row is None:
                        stats["forms_filtered"] += 1
                        continue
                    add_form(row)

                if len(form_batch) >= batch_size:
                    flush_batches()

            # For nouns: synthesize plurals from head_templates (braccio-type cases)
            # These are forms that only exist in head_templates, not in the forms array
            if pos_filter == POS.NOUN and synthesize_plurals:
                for form_text, gender, hint in synthesize_plurals:
                    if ("plural", gender) not in seen_base_forms:
                        row = _build_noun_form_row(
                            lemma_id,
                            form_text,
                            ["plural"],
                            gender,
                            meaning_hint=hint if hint else None,
                            written_source="synthesized",
                            form_origin="inferred:head_template",
                        )
                        if row:
                            add_form(row)
                            seen_base_forms.add(("plural", gender))

            # For nouns: add base form from lemma word if not already present
            # The lemma word is always the base form (singular for regular, plural for pluralia tantum)
            if pos_filter == POS.NOUN and noun_class:
                number_class = noun_class.get("number_class", "standard")
                gender_class = noun_class.get("gender_class")
                is_pluralia_tantum = number_class == "pluralia_tantum"
                base_number = "plural" if is_pluralia_tantum else "singular"

                is_common_gender = gender_class in (
                    GenderClass.COMMON_GENDER_FIXED,
                    GenderClass.COMMON_GENDER_VARIABLE,
                    GenderClass.BY_SENSE,
                )

                if is_common_gender:
                    # Add base form for both genders if not already present
                    # Only mark as citation if no citation form was added in main loop
                    has_existing_citation = any(
                        f.get("is_citation_form")
                        for f in form_batch
                        if f.get("lemma_id") == lemma_id
                    )
                    citation_marked = has_existing_citation
                    for gender in ("m", "f"):
                        if (base_number, gender) not in seen_base_forms:
                            row = _build_noun_form_row(
                                lemma_id,
                                lemma_stressed,
                                [base_number],
                                gender,
                                form_origin="inferred:base_form",
                                is_citation_form=not citation_marked,
                            )
                            if row:
                                add_form(row)
                                citation_marked = True
                elif lemma_gender and (base_number, lemma_gender) not in seen_base_forms:
                    # Add base form for single gender if not already present
                    # Only mark as citation if no citation form was added in main loop
                    has_existing_citation = any(
                        f.get("is_citation_form")
                        for f in form_batch
                        if f.get("lemma_id") == lemma_id
                    )
                    row = _build_noun_form_row(
                        lemma_id,
                        lemma_stressed,
                        [base_number],
                        lemma_gender,
                        form_origin="inferred:base_form",
                        is_citation_form=not has_existing_citation,
                    )
                    if row:
                        add_form(row)

                # For invariable nouns: also add plural form with same text
                # (Similar to how invariable adjectives get all 4 gender/number forms)
                is_invariable = number_class == "invariable"
                if is_invariable:
                    if is_common_gender:
                        # Add plural for both genders
                        for gender in ("m", "f"):
                            if ("plural", gender) not in seen_base_forms:
                                row = _build_noun_form_row(
                                    lemma_id,
                                    lemma_stressed,
                                    ["plural"],
                                    gender,
                                    form_origin="inferred:invariable",
                                )
                                if row:
                                    add_form(row)
                    elif lemma_gender and ("plural", lemma_gender) not in seen_base_forms:
                        # Add plural for single gender
                        row = _build_noun_form_row(
                            lemma_id,
                            lemma_stressed,
                            ["plural"],
                            lemma_gender,
                            form_origin="inferred:invariable",
                        )
                        if row:
                            add_form(row)

            # Queue definitions with form_meaning_hint for soft key linkage
            if pos_filter == POS.NOUN and word in DEFINITION_FORM_LINKAGE:
                # This lemma has meaning-dependent plurals - link definitions to forms
                linkage = DEFINITION_FORM_LINKAGE[word]
                for sense in entry.get("senses", []):
                    # Skip form-of entries
                    if "form_of" in sense:
                        continue
                    glosses = sense.get("glosses", [])
                    if not glosses:
                        continue
                    gloss = "; ".join(glosses)

                    # Filter out blocklisted tags
                    raw_tags = sense.get("tags")
                    if raw_tags:
                        filtered = [t for t in raw_tags if t not in DEFINITION_TAG_BLOCKLIST]
                        def_tags = filtered if filtered else None
                    else:
                        def_tags = None

                    # Determine which form(s) this definition matches
                    matched_forms = [
                        form_text
                        for form_text, matchers in linkage.items()
                        if _sense_matches_form(sense, matchers)
                    ]

                    if matched_forms:
                        # Create a definition entry for each matched form
                        definition_batch.extend(
                            {
                                "lemma_id": lemma_id,
                                "gloss": gloss,
                                "tags": def_tags or None,
                                "form_meaning_hint": form_text,
                            }
                            for form_text in matched_forms
                        )
                    else:
                        # No match - applies to all forms (NULL form_meaning_hint)
                        definition_batch.append(
                            {
                                "lemma_id": lemma_id,
                                "gloss": gloss,
                                "tags": def_tags or None,
                                "form_meaning_hint": None,  # Consistent keys for batch insert
                            }
                        )
            else:
                # Standard case - no form_meaning_hint
                for gloss, def_tags in _iter_definitions(entry):
                    definition_batch.append(
                        {
                            "lemma_id": lemma_id,
                            "gloss": gloss,
                            "tags": def_tags or None,
                            "form_meaning_hint": None,  # Consistent keys for batch insert
                        }
                    )

    # Final flush
    flush_batches()

    # Post-processing: Link relationships
    # (must happen after all lemmas are inserted so we can resolve lemma IDs)
    if pos_filter == POS.ADJECTIVE:
        degree_stats = link_comparative_superlative(conn, degree_links)

        # Add linking stats to main stats dict
        stats["degree_linked"] = degree_stats["linked"]
        stats["degree_base_not_found"] = degree_stats["base_not_found"]

    if pos_filter == POS.VERB:
        pronominal_stats = link_pronominal_verbs(conn)

        # Add pronominal linking stats to main stats dict
        stats["pronominal_verbs"] = pronominal_stats["pronominal_verbs"]
        stats["pronominal_linked"] = pronominal_stats["linked_to_base"]
        stats["pronominal_inherent"] = pronominal_stats["inherent_pronominal"]
        stats["pronominal_parse_failed"] = pronominal_stats["base_form_parse_failed"]

        # Sync lemma stress with citation form (fixes acute→grave mismatches)
        stress_sync_stats = sync_verb_lemma_stress(conn)
        stats["lemma_stress_synced"] = stress_sync_stats["synced"]

    if pos_filter == POS.NOUN:
        # Link gender counterpart pairs (professore↔professoressa)
        counterpart_stats = link_noun_counterparts(conn, jsonl_path)
        stats["counterparts_found"] = counterpart_stats["counterparts_found"]
        stats["counterparts_linked_bidirectional"] = counterpart_stats["linked_bidirectional"]
        stats["counterparts_linked_unidirectional"] = counterpart_stats["linked_unidirectional"]
        stats["counterparts_base_not_found"] = counterpart_stats["base_not_found"]

        # Link derived nouns to their base (gattino→gatto)
        derivation_stats = link_noun_derivations(conn, jsonl_path)
        stats["derivations_found"] = derivation_stats["derivations_found"]
        stats["derivations_linked"] = derivation_stats["linked"]
        stats["derivations_diminutive"] = derivation_stats["diminutive"]
        stats["derivations_augmentative"] = derivation_stats["augmentative"]
        stats["derivations_pejorative"] = derivation_stats["pejorative"]
        stats["derivations_base_not_found"] = derivation_stats["base_not_found"]

    # Final progress callback
    if progress_callback:
        progress_callback(total_lines, total_lines)

    # Log aggregated noun gender/plural warnings (if any)
    if pos_filter == POS.NOUN:
        if stats.get("counterpart_no_plural", 0) > 0:
            logger.info(
                "Noun plurals: %d counterparts had no plural form in lookup (Wiktextract data gap)",
                stats["counterpart_no_plural"],
            )
        if stats.get("counterpart_wrong_gender", 0) > 0:
            logger.info(
                "Noun plurals: %d counterparts had wrong gender in Wiktextract (skipped lookup)",
                stats["counterpart_wrong_gender"],
            )
        if stats.get("no_counterpart_no_gender", 0) > 0:
            logger.info(
                "Noun plurals: %d common-gender-variable nouns had plural without "
                "gender tag or counterpart",
                stats["no_counterpart_no_gender"],
            )

    return stats


def _is_form_of_entry(entry: dict[str, Any], pos: str) -> bool:
    """Check if entry is a form-of entry (inflected form reference) for the given POS."""
    if entry.get("pos") != pos:
        return False
    # Form-of entries have form_of in at least one sense
    return any("form_of" in sense for sense in entry.get("senses", []))


def _extract_form_of_info(
    entry: dict[str, Any],
) -> Iterator[tuple[str, str, list[str] | None]]:
    """Extract form-of info from an entry.

    Yields (form_word, lemma_word, labels) tuples.
    A form-of entry can reference multiple lemmas in different senses.
    Labels is a sorted list if any labels are present.
    """
    form_word = entry.get("word", "")
    if not form_word:
        return

    for sense in entry.get("senses", []):
        form_of_list = sense.get("form_of", [])
        if not form_of_list:
            continue

        # Extract and canonicalize labels from sense tags
        tags = set(sense.get("tags", []))
        canonical = {LABEL_CANONICAL[t] for t in tags if t in LABEL_CANONICAL}
        labels = sorted(canonical) if canonical else None

        # Only proceed if there are labels to apply
        if labels is None:
            continue

        # Get lemma(s) this form belongs to
        for form_of in form_of_list:
            lemma_word = form_of.get("word", "")
            if lemma_word:
                yield form_word, lemma_word, labels


def enrich_from_form_of_entries(
    conn: Connection,
    jsonl_path: Path,
    *,
    pos_filter: POS = POS.VERB,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Extract labels and spelling from form-of entries in a single pass.

    This combined function scans form-of entries (which we skip during main import)
    and performs two enrichments in a single pass:

    1. Labels: Extract usage labels (literary, archaic, regional, etc.) and apply
       them to existing forms where labels IS NULL.

    2. Spelling: Use the entry's 'word' field as the written form for forms
       where written IS NULL (fallback after Morph-it!).

    By combining these operations, we avoid scanning the 620k-line JSONL file twice.

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to the Wiktextract JSONL file
        pos_filter: Part of speech to enrich
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts for both operations:
        - scanned: form-of entries examined
        - labels_with_tags: entries with valid label tags
        - labels_updated: forms updated with labels
        - labels_not_found: entries where form not found for labels
        - spelling_updated: forms updated with written spelling
        - spelling_already_filled: entries where spelling already set
        - spelling_not_found: entries where form not found for spelling
    """
    from sqlalchemy import update

    stats = {
        "scanned": 0,
        "labels_with_tags": 0,
        "labels_updated": 0,
        "labels_not_found": 0,
        "spelling_updated": 0,
        "spelling_already_filled": 0,
        "spelling_not_found": 0,
    }

    # Get POS-specific table
    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        msg = f"Unsupported POS: {pos_filter}"
        raise ValueError(msg)

    # Build lemma lookup: written_form -> lemma_id
    # Use written form (not normalized stressed) to preserve orthographic distinctions
    # like metà (half) vs meta (goal). Fall back to derive_written_from_stressed()
    # if written column is not yet populated (e.g., during early import stages).
    lemma_result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(lemmas.c.pos == pos_filter)
    )
    lemma_lookup: dict[str, int] = {}
    for row in lemma_result:
        written = row.written or derive_written_from_stressed(row.stressed)
        if written is not None:
            lemma_lookup[written] = row.id

    # Build TWO form lookups with different criteria:
    #
    # 1. labels_lookup: ALL forms where labels IS NULL
    #    Used to apply usage labels from form-of entries
    #
    # 2. spelling_lookup: Only forms where written IS NULL
    #    Used to fill spelling from form-of entries as fallback after Morph-it!
    #
    # We need separate lookups because:
    # - Labels can be applied to any form that doesn't have them yet
    # - Spelling should only be applied to forms not already filled by Morph-it!

    # Labels lookup: all forms where labels IS NULL
    labels_result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.lemma_id, pos_form_table.c.stressed).where(
            pos_form_table.c.labels.is_(None)
        )
    )
    labels_lookup: dict[tuple[int, str], list[int]] = {}
    for row in labels_result:
        normalized = normalize(row.stressed)
        key = (row.lemma_id, normalized)
        if key not in labels_lookup:
            labels_lookup[key] = []
        labels_lookup[key].append(row.id)

    # Spelling lookup: only forms where written IS NULL
    spelling_result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.lemma_id, pos_form_table.c.stressed).where(
            pos_form_table.c.written.is_(None)
        )
    )
    spelling_lookup: dict[tuple[int, str], list[int]] = {}
    for row in spelling_result:
        normalized = normalize(row.stressed)
        key = (row.lemma_id, normalized)
        if key not in spelling_lookup:
            spelling_lookup[key] = []
        spelling_lookup[key].append(row.id)

    # Map to Wiktextract's POS naming
    wiktextract_pos = WIKTEXTRACT_POS.get(pos_filter, pos_filter)

    # Count lines for progress if callback provided
    total_lines = _count_lines(jsonl_path) if progress_callback else 0
    current_line = 0

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            current_line += 1
            if progress_callback and current_line % 10000 == 0:
                progress_callback(current_line, total_lines)

            entry = _parse_entry(line)
            if entry is None:
                continue

            # Only process form-of entries for our POS
            if not _is_form_of_entry(entry, wiktextract_pos):
                continue

            stats["scanned"] += 1

            # The entry's 'word' field is the actual written form (e.g., "parlo")
            form_word = entry.get("word", "")
            if not form_word:
                continue

            # =========================================================
            # PART 1: Extract and apply labels using _extract_form_of_info()
            # =========================================================
            for extracted_form, lemma_word, labels in _extract_form_of_info(entry):
                if labels is None:
                    continue

                stats["labels_with_tags"] += 1

                # Look up lemma by its written form
                lemma_written = derive_written_from_stressed(lemma_word)
                if lemma_written is None:
                    stats["labels_not_found"] += 1
                    continue
                lemma_id = lemma_lookup.get(lemma_written)
                if lemma_id is None:
                    stats["labels_not_found"] += 1
                    continue

                # Look up form
                form_normalized = normalize(extracted_form)
                key = (lemma_id, form_normalized)
                form_ids = labels_lookup.get(key)
                if not form_ids:
                    stats["labels_not_found"] += 1
                    continue

                # Update labels for all matching forms (where labels is NULL)
                for form_id in form_ids:
                    result = conn.execute(
                        update(pos_form_table)
                        .where(pos_form_table.c.id == form_id)
                        .where(pos_form_table.c.labels.is_(None))
                        .values(labels=labels)
                    )
                    if result.rowcount > 0:
                        stats["labels_updated"] += 1

            # =========================================================
            # PART 2: Extract and apply spelling from form_of references
            # =========================================================
            for sense in entry.get("senses", []):
                form_of_list = sense.get("form_of", [])
                if not form_of_list:
                    continue

                for form_of in form_of_list:
                    lemma_word = form_of.get("word", "")
                    if not lemma_word:
                        continue

                    # Look up lemma by its written form
                    lemma_written = derive_written_from_stressed(lemma_word)
                    if lemma_written is None:
                        stats["spelling_not_found"] += 1
                        continue
                    lemma_id = lemma_lookup.get(lemma_written)
                    if lemma_id is None:
                        stats["spelling_not_found"] += 1
                        continue

                    # Look up form (only forms with NULL written are in the lookup)
                    form_normalized = normalize(form_word)
                    key = (lemma_id, form_normalized)
                    form_ids = spelling_lookup.get(key)
                    if not form_ids:
                        # Either already filled by Morph-it! or not found
                        stats["spelling_already_filled"] += 1
                        continue

                    # Update written and written_source for all matching forms
                    for form_id in form_ids:
                        conn.execute(
                            update(pos_form_table)
                            .where(pos_form_table.c.id == form_id)
                            .values(written=form_word, written_source="wiktionary")
                        )
                        stats["spelling_updated"] += 1

                    # Remove from lookup to avoid duplicate updates
                    del spelling_lookup[key]

    # Final progress callback
    if progress_callback:
        progress_callback(total_lines, total_lines)

    return stats


def link_comparative_superlative(
    conn: Connection,
    degree_links: list[tuple[int, str, str, str]],
) -> dict[str, int]:
    """Populate adjective_metadata.base_lemma_id, degree_relationship, and source.

    Args:
        conn: SQLAlchemy connection
        degree_links: List of (lemma_id, base_lemma_word, relationship, source) tuples
            collected during import. Source is one of: 'wiktextract',
            'wiktextract:canonical', 'hardcoded'.

    Returns:
        Statistics dict with 'linked' and 'base_not_found' counts
    """
    stats = {"linked": 0, "base_not_found": 0}

    if not degree_links:
        return stats

    # Build lookup: written_form -> lemma_id for adjectives
    # Use written form (not normalized stressed) to preserve orthographic distinctions.
    # Fall back to derive_written_from_stressed() if written is not yet populated.
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(
            lemmas.c.pos == POS.ADJECTIVE
        )
    )
    lemma_lookup: dict[str, int] = {}
    for row in result:
        written = row.written or derive_written_from_stressed(row.stressed)
        if written is not None:
            lemma_lookup[written] = row.id

    for lemma_id, base_word, relationship, source in degree_links:
        # Derive written form from base_word (which may have pedagogical stress)
        base_written = derive_written_from_stressed(base_word)
        if base_written is None:
            logger.warning(
                "Failed to derive written form for base lemma '%s' (source: %s)",
                base_word,
                source,
            )
            stats["base_not_found"] += 1
            continue
        base_lemma_id = lemma_lookup.get(base_written)

        if base_lemma_id is None:
            logger.warning(
                "Base lemma '%s' not found for degree relationship (source: %s)",
                base_word,
                source,
            )
            stats["base_not_found"] += 1
            continue

        conn.execute(
            update(adjective_metadata)
            .where(adjective_metadata.c.lemma_id == lemma_id)
            .values(
                base_lemma_id=base_lemma_id,
                degree_relationship=relationship,
                degree_relationship_source=source,
            )
        )
        stats["linked"] += 1

    return stats


def link_pronominal_verbs(conn: Connection) -> dict[str, int]:
    """Link pronominal verbs to their non-pronominal base verbs.

    For verbs ending in -si/-rsi (pronominal verbs), attempts to find the
    non-pronominal base verb and updates verb_metadata with:
    - base_verb_lemma_id: Points to the base verb (lavarsi → lavare)
    - pronominal_type: 'reflexive' if base exists, 'inherent' if not

    Returns:
        Statistics dict with counts for each operation.
    """
    stats = {
        "pronominal_verbs": 0,
        "linked_to_base": 0,
        "inherent_pronominal": 0,
        "base_form_parse_failed": 0,
    }

    # Build lookup: written_form → lemma_id for all verbs
    # Use written form (not normalized stressed) to preserve orthographic distinctions.
    # Fall back to derive_written_from_stressed() if written is not yet populated.
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(lemmas.c.pos == POS.VERB)
    )
    lemma_lookup: dict[str, int] = {}
    for row in result:
        written = row.written or derive_written_from_stressed(row.stressed)
        if written is not None:
            lemma_lookup[written] = row.id

    # Get stressed forms for pronominal detection
    result = conn.execute(select(lemmas.c.id, lemmas.c.stressed).where(lemmas.c.pos == POS.VERB))

    for row in result:
        lemma_id = row.id
        stressed = row.stressed

        if not _is_pronominal_verb(stressed):
            continue

        stats["pronominal_verbs"] += 1

        # Try to find the base form
        base_form = _get_pronominal_base_form(stressed)
        if base_form is None:
            stats["base_form_parse_failed"] += 1
            # Still mark as pronominal, but can't link
            conn.execute(
                update(verb_metadata)
                .where(verb_metadata.c.lemma_id == lemma_id)
                .values(pronominal_type="inherent")
            )
            stats["inherent_pronominal"] += 1
            continue

        # Look up the base verb by its written form
        # base_form comes from _get_pronominal_base_form and may have pedagogical stress
        base_written = derive_written_from_stressed(base_form)
        if base_written is None:
            # Failed to derive written form - treat as inherent pronominal
            conn.execute(
                update(verb_metadata)
                .where(verb_metadata.c.lemma_id == lemma_id)
                .values(pronominal_type="inherent")
            )
            stats["inherent_pronominal"] += 1
            continue
        base_lemma_id = lemma_lookup.get(base_written)

        if base_lemma_id is not None:
            # Base verb exists - this is a reflexive/reciprocal pronominal
            conn.execute(
                update(verb_metadata)
                .where(verb_metadata.c.lemma_id == lemma_id)
                .values(
                    base_verb_lemma_id=base_lemma_id,
                    pronominal_type="reflexive",
                )
            )
            stats["linked_to_base"] += 1
        else:
            # Base verb doesn't exist - this is an inherent pronominal
            conn.execute(
                update(verb_metadata)
                .where(verb_metadata.c.lemma_id == lemma_id)
                .values(pronominal_type="inherent")
            )
            stats["inherent_pronominal"] += 1

    return stats


def sync_verb_lemma_stress(conn: Connection) -> dict[str, int]:
    """Sync verb lemma stressed forms with their citation forms.

    During import, verb forms go through deduplication that prefers grave accents
    over acute accents. The lemma is created before this processing, so it may
    retain an acute accent while the citation form has been corrected to grave.

    This function updates lemmas.stressed to match verb_forms.stressed where
    is_citation_form=1, but ONLY when the difference is accent type (acute→grave)
    on the same vowel, not when the stress position differs.

    For example:
    - scéndere → scèndere: YES (same vowel, acute→grave)
    - suggére → sùggere: NO (different syllable stressed)

    Returns:
        Dict with 'synced' count of lemmas that were updated.
    """
    result = conn.execute(
        text("""
            UPDATE lemmas
            SET stressed = (
                SELECT vf.stressed
                FROM verb_forms vf
                WHERE vf.lemma_id = lemmas.id AND vf.is_citation_form = 1
            )
            WHERE pos = 'verb' AND EXISTS (
                SELECT 1 FROM verb_forms vf
                WHERE vf.lemma_id = lemmas.id
                  AND vf.is_citation_form = 1
                  AND vf.stressed != lemmas.stressed
                  -- Only sync if the citation form, with grave→acute substitution,
                  -- equals the lemma. This ensures we're only fixing accent type
                  -- (acute→grave), not changing which syllable is stressed.
                  AND REPLACE(REPLACE(vf.stressed, 'è', 'é'), 'ò', 'ó') = lemmas.stressed
            )
        """)
    )
    return {"synced": result.rowcount}


def link_noun_counterparts(
    conn: Connection,
    jsonl_path: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Link gender counterpart noun pairs (professore↔professoressa).

    Scans Wiktextract for entries with "female equivalent of" or "male equivalent of"
    glosses and creates bidirectional links between the counterpart lemmas.

    Data sources used:
    1. form_of entries with "female/male equivalent of" glosses
    2. forms array with "feminine" or "masculine" tags

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to Wiktextract JSONL file
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts of processed entries
    """
    stats = {
        "scanned": 0,
        "counterparts_found": 0,
        "linked_bidirectional": 0,
        "linked_unidirectional": 0,
        "base_not_found": 0,
    }

    # Build lookup: written_form -> lemma_id for nouns
    # Use written form (not normalized stressed) to preserve orthographic distinctions.
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(lemmas.c.pos == POS.NOUN)
    )
    noun_lookup: dict[str, int] = {}
    for row in result:
        written = row.written or derive_written_from_stressed(row.stressed)
        if written is not None:
            noun_lookup[written] = row.id

    # Track counterpart relationships: (lemma_id, counterpart_lemma_id)
    # We'll process these at the end to handle bidirectionality
    counterpart_pairs: list[tuple[int, int]] = []

    # Count lines for progress
    total_lines = _count_lines(jsonl_path) if progress_callback else 0
    current_line = 0

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            current_line += 1
            if progress_callback and current_line % 10000 == 0:
                progress_callback(current_line, total_lines)

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Only process noun entries
            if entry.get("pos") != "noun":
                continue

            stats["scanned"] += 1
            word = entry.get("word", "")

            # Look for "female equivalent of" or "male equivalent of" glosses
            for sense in entry.get("senses", []):
                glosses = sense.get("glosses", [])
                if not glosses:
                    continue

                gloss = glosses[0] if glosses else ""
                counterpart_word = None

                # Check for "female equivalent of X" or "male equivalent of X"
                if "female equivalent of " in gloss or "male equivalent of " in gloss:
                    # Extract from form_of if available
                    form_of_list = sense.get("form_of", [])
                    if form_of_list:
                        counterpart_word = form_of_list[0].get("word")
                    else:
                        # Try to extract from gloss text
                        if "female equivalent of " in gloss:
                            counterpart_word = gloss.split("female equivalent of ")[-1].strip()
                        elif "male equivalent of " in gloss:
                            counterpart_word = gloss.split("male equivalent of ")[-1].strip()
                        # Clean up any trailing punctuation or extra text
                        if counterpart_word:
                            counterpart_word = counterpart_word.split(",")[0].strip()
                            counterpart_word = counterpart_word.split(";")[0].strip()

                if counterpart_word:
                    stats["counterparts_found"] += 1

                    # Look up both lemmas
                    word_written = derive_written_from_stressed(word)
                    counterpart_written = derive_written_from_stressed(counterpart_word)

                    if word_written is None or counterpart_written is None:
                        stats["base_not_found"] += 1
                        continue

                    word_id = noun_lookup.get(word_written)
                    counterpart_id = noun_lookup.get(counterpart_written)

                    if word_id is None or counterpart_id is None:
                        stats["base_not_found"] += 1
                        continue

                    # Record the pair (in both directions for bidirectional linking)
                    counterpart_pairs.append((word_id, counterpart_id))
                    break  # Only process first counterpart relationship per entry

    if progress_callback:
        progress_callback(total_lines, total_lines)

    # Process counterpart pairs and update database
    # Build a set of all pairs for bidirectional checking
    pair_set: set[tuple[int, int]] = set()
    for a, b in counterpart_pairs:
        pair_set.add((a, b))

    # Update database with counterpart links
    updated_ids: set[int] = set()
    for word_id, counterpart_id in counterpart_pairs:
        if word_id in updated_ids:
            continue

        # Check if reverse exists (bidirectional)
        is_bidirectional = (counterpart_id, word_id) in pair_set

        # Update this lemma's counterpart
        conn.execute(
            update(noun_metadata)
            .where(noun_metadata.c.lemma_id == word_id)
            .values(counterpart_lemma_id=counterpart_id)
        )
        updated_ids.add(word_id)

        # Update counterpart's counterpart (for bidirectional links)
        if is_bidirectional and counterpart_id not in updated_ids:
            conn.execute(
                update(noun_metadata)
                .where(noun_metadata.c.lemma_id == counterpart_id)
                .values(counterpart_lemma_id=word_id)
            )
            updated_ids.add(counterpart_id)
            stats["linked_bidirectional"] += 1
        else:
            stats["linked_unidirectional"] += 1

    return stats


def link_noun_derivations(
    conn: Connection,
    jsonl_path: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Link derived nouns to their base lemmas (gattino→gatto).

    Scans Wiktextract for entries with diminutive/augmentative/pejorative tags
    and creates links to their base lemmas. Also updates derivation_type in
    noun_metadata.

    Data sources used:
    1. form_of entries with "diminutive", "augmentative", or "pejorative" tags
    2. Glosses like "diminutive of gatto"

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to Wiktextract JSONL file
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts by derivation type
    """
    stats = {
        "scanned": 0,
        "derivations_found": 0,
        "linked": 0,
        "base_not_found": 0,
        "diminutive": 0,
        "augmentative": 0,
        "pejorative": 0,
    }

    # Build lookup: written_form -> lemma_id for nouns
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(lemmas.c.pos == POS.NOUN)
    )
    noun_lookup: dict[str, int] = {}
    for row in result:
        written = row.written or derive_written_from_stressed(row.stressed)
        if written is not None:
            noun_lookup[written] = row.id

    # Count lines for progress
    total_lines = _count_lines(jsonl_path) if progress_callback else 0
    current_line = 0

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            current_line += 1
            if progress_callback and current_line % 10000 == 0:
                progress_callback(current_line, total_lines)

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Only process noun entries
            if entry.get("pos") != "noun":
                continue

            stats["scanned"] += 1
            word = entry.get("word", "")

            # Look for derivation relationships in senses
            for sense in entry.get("senses", []):
                tags = sense.get("tags", [])
                glosses = sense.get("glosses", [])
                gloss = glosses[0] if glosses else ""

                # Determine derivation type from tags
                derivation_type: DerivationType | None = None
                if "diminutive" in tags:
                    derivation_type = DerivationType.DIMINUTIVE
                elif "augmentative" in tags:
                    derivation_type = DerivationType.AUGMENTATIVE
                elif "pejorative" in tags:
                    derivation_type = DerivationType.PEJORATIVE

                # Also check gloss patterns if no tag
                if derivation_type is None:
                    if "diminutive of " in gloss.lower():
                        derivation_type = DerivationType.DIMINUTIVE
                    elif "augmentative of " in gloss.lower():
                        derivation_type = DerivationType.AUGMENTATIVE
                    elif "pejorative of " in gloss.lower():
                        derivation_type = DerivationType.PEJORATIVE

                if derivation_type is None:
                    continue

                # Extract base word from form_of or gloss
                base_word = None
                form_of_list = sense.get("form_of", [])
                if form_of_list:
                    base_word = form_of_list[0].get("word")

                if base_word is None:
                    # Try to extract from gloss
                    for pattern in [
                        "diminutive of ",
                        "augmentative of ",
                        "pejorative of ",
                    ]:
                        if pattern in gloss.lower():
                            idx = gloss.lower().find(pattern)
                            base_word = gloss[idx + len(pattern) :].strip()
                            # Clean up
                            base_word = base_word.split(",")[0].strip()
                            base_word = base_word.split(";")[0].strip()
                            base_word = base_word.split(" ")[0].strip()
                            break

                if base_word is None:
                    continue

                stats["derivations_found"] += 1

                # Look up both lemmas
                word_written = derive_written_from_stressed(word)
                base_written = derive_written_from_stressed(base_word)

                if word_written is None or base_written is None:
                    stats["base_not_found"] += 1
                    continue

                word_id = noun_lookup.get(word_written)
                base_id = noun_lookup.get(base_written)

                if word_id is None or base_id is None:
                    stats["base_not_found"] += 1
                    continue

                # Update noun_metadata
                conn.execute(
                    update(noun_metadata)
                    .where(noun_metadata.c.lemma_id == word_id)
                    .values(
                        base_lemma_id=base_id,
                        derivation_type=derivation_type,
                    )
                )
                stats["linked"] += 1
                stats[derivation_type] += 1
                break  # Only process first derivation relationship per entry

    if progress_callback:
        progress_callback(total_lines, total_lines)

    return stats


def import_adjective_allomorphs(
    conn: Connection,
    jsonl_path: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import allomorphs (apocopic/elided forms) as forms of their parent adjective.

    Scans Wiktextract for entries with alt_of pointing to existing adjectives,
    and adds their word as forms under the parent lemma.

    This implements Option A for handling apocopic forms (gran, bel) and elided
    forms (grand', bell'): they are stored as forms of their parent lemma
    (grande, bello) rather than as separate lemmas.

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to Wiktextract JSONL file
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts of processed entries
    """
    stats = {
        "scanned": 0,
        "allomorphs_added": 0,
        "forms_added": 0,
        "forms_blocked": 0,  # Forms filtered by BLOCKED_ADJECTIVE_FORMS
        "parent_not_found": 0,
        "duplicates_skipped": 0,
        "already_in_parent": 0,
        "hardcoded_added": 0,
    }

    # Build lookup: written_form -> lemma_id for adjectives
    # Use written form (not normalized stressed) to preserve orthographic distinctions.
    # Fall back to derive_written_from_stressed() if written is not yet populated.
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(
            lemmas.c.pos == POS.ADJECTIVE
        )
    )
    adj_lookup: dict[str, int] = {}
    for row in result:
        written = row.written or derive_written_from_stressed(row.stressed)
        if written is not None:
            adj_lookup[written] = row.id

    # Count lines for progress
    total_lines = _count_lines(jsonl_path) if progress_callback else 0
    current_line = 0

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            current_line += 1
            if progress_callback and current_line % 10000 == 0:
                progress_callback(current_line, total_lines)

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Only process adjective entries
            if entry.get("pos") != "adj":
                continue

            stats["scanned"] += 1

            # Find parent word and determine label
            # Method 1: alt_of in senses (e.g., gran -> grande)
            # Method 2: "adjective form" with links (e.g., bel -> bello)
            parent_word = None
            label = None
            allomorph_word = entry["word"]

            # Try Method 1: alt_of
            for sense in entry.get("senses", []):
                alt_of_list = sense.get("alt_of", [])
                for alt_of in alt_of_list:
                    parent_word = alt_of.get("word")
                    if parent_word:
                        # Determine label from tags
                        tags = sense.get("tags", [])
                        if "apocopic" in tags:
                            label = "apocopic"
                        break
                if parent_word:
                    break

            # Try Method 2: "adjective form" WITHOUT form_of, using links
            # This catches special forms like "bel" which:
            # - Are marked as "adjective form" in head_templates
            # - Do NOT have form_of (unlike regular inflections like "bella")
            # - Have links pointing to the parent lemma
            if not parent_word:
                is_adj_form = any(
                    t.get("args", {}).get("2") == "adjective form"
                    for t in entry.get("head_templates", [])
                )
                # Check that NO sense has form_of (regular inflected forms have form_of)
                has_form_of = any(sense.get("form_of") for sense in entry.get("senses", []))
                if is_adj_form and not has_form_of:
                    for sense in entry.get("senses", []):
                        links = sense.get("links", [])
                        if links and len(links) > 0:
                            # links format: [['bello', 'bello#Italian'], ...]
                            parent_word = links[0][0] if isinstance(links[0], list) else links[0]
                            # For "adjective form" entries, label as apocopic (pre-nominal form)
                            label = "apocopic"
                            break

            if not parent_word:
                continue

            # Look up parent by written form
            parent_written = derive_written_from_stressed(parent_word)
            if parent_written is None:
                stats["parent_not_found"] += 1
                continue
            parent_id = adj_lookup.get(parent_written)
            if parent_id is None:
                stats["parent_not_found"] += 1
                continue

            # Check if parent already has this form (with correct gender/number from forms array)
            # If so, skip — the parent's Wiktextract forms already have proper tagging
            existing_forms = conn.execute(
                select(adjective_forms.c.written).where(adjective_forms.c.lemma_id == parent_id)
            ).fetchall()
            existing_form_texts = {row.written for row in existing_forms if row.written}

            if allomorph_word in existing_form_texts:
                stats["already_in_parent"] += 1
                continue

            # Check gender restrictions from the alt-of entry
            # e.g., moltipara (fonly:1) should only add feminine forms to multipara
            is_feminine_only = _is_feminine_only_adjective(entry)
            is_masculine_only = _is_masculine_only_adjective(entry)

            if is_feminine_only:
                genders: tuple[str, ...] = ("f",)
            elif is_masculine_only:
                genders = ("m",)
            else:
                genders = ("m", "f")

            # Build form lookup from entry's forms array
            # e.g., secreto has forms=[secreta (f), secreti (m/p), secrete (f/p)]
            # The entry word (secreto) is used for m/s; other forms from the array
            # Note: In Wiktextract, singular forms often lack 'singular' tag - just have gender
            form_lookup: dict[tuple[str, str], str] = {}
            for form_entry in entry.get("forms", []):
                form_text = form_entry.get("form")
                form_tags = form_entry.get("tags", [])
                if not form_text:
                    continue
                # Determine gender and number from tags
                form_gender = (
                    "m" if "masculine" in form_tags else "f" if "feminine" in form_tags else None
                )
                # Default to singular if 'plural' not present (common Wiktextract pattern)
                form_number = "plural" if "plural" in form_tags else "singular"

                # Gender-neutral forms (e.g., 2-form adjective plurals like 'suavi')
                # apply to both masculine and feminine
                if form_gender:
                    form_lookup[(form_gender, form_number)] = form_text
                else:
                    # No gender specified - form applies to both genders
                    form_lookup[("m", form_number)] = form_text
                    form_lookup[("f", form_number)] = form_text

            # Add forms for appropriate gender(s)
            for gender in genders:
                for number in ("singular", "plural"):
                    # Use form from lookup if available, otherwise use entry word
                    # (entry word is typically the m/s citation form)
                    form_text = form_lookup.get((gender, number), allomorph_word)

                    # Skip if no form text
                    if not form_text:
                        continue

                    # Check blocklist for archaic/erroneous forms
                    form_written = derive_written_from_stressed(form_text) or form_text
                    if is_blocked_adjective_form(parent_written, form_written, gender, number):
                        stats["forms_blocked"] += 1
                        continue

                    def_article, article_source = get_definite(form_text, gender, number)

                    try:
                        conn.execute(
                            adjective_forms.insert().values(
                                lemma_id=parent_id,
                                written=form_text,
                                written_source="wiktionary",
                                stressed=form_text,
                                gender=gender,
                                number=number,
                                degree="positive",
                                labels=label,
                                def_article=def_article,
                                article_source=article_source,
                                form_origin="alt_of",
                            )
                        )
                        stats["forms_added"] += 1
                    except Exception:
                        # Duplicate form (unique constraint violation)
                        stats["duplicates_skipped"] += 1

            stats["allomorphs_added"] += 1

    if progress_callback:
        progress_callback(total_lines, total_lines)

    # Import hardcoded allomorph forms (not in Wiktextract or Morphit adjective data)
    for form, parent_lemma, gender, number, label in HARDCODED_ALLOMORPH_FORMS:
        # Look up parent by written form
        parent_written = derive_written_from_stressed(parent_lemma)
        parent_id = adj_lookup.get(parent_written) if parent_written else None
        if parent_id is None:
            continue

        # Check if this specific form+gender+number combo already exists
        existing = conn.execute(
            select(
                adjective_forms.c.written,
                adjective_forms.c.gender,
                adjective_forms.c.number,
            ).where(adjective_forms.c.lemma_id == parent_id)
        ).fetchall()
        existing_combos = {(row.written, row.gender, row.number) for row in existing if row.written}

        if (form, gender, number) in existing_combos:
            continue

        # Compute definite article (gender is already 'm'/'f')
        def_article, article_source = get_definite(form, gender, number)

        try:
            conn.execute(
                adjective_forms.insert().values(
                    lemma_id=parent_id,
                    written=form,
                    written_source="hardcoded",
                    stressed=form,
                    gender=gender,
                    number=number,
                    degree="positive",
                    labels=label,
                    def_article=def_article,
                    article_source=article_source,
                    form_origin="hardcoded",
                )
            )
            stats["hardcoded_added"] += 1
        except Exception:
            # Duplicate (unique constraint violation) - already exists, skip silently
            logger.debug("Hardcoded form '%s' already exists for '%s'", form, parent_lemma)

    return stats


def import_noun_allomorphs(
    conn: Connection,
    jsonl_path: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import apocopic noun forms as forms of their parent noun.

    Scans Wiktextract for noun entries with alt_of tagged 'apocopic',
    and adds their word as forms under the parent lemma.

    Unlike adjective allomorphs which add 4 forms (all gender/number combos),
    noun allomorphs add only 1 form with the specific gender from the entry.

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to Wiktextract JSONL file
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts of processed entries
    """
    stats = {
        "scanned": 0,
        "allomorphs_added": 0,
        "forms_added": 0,
        "parent_not_found": 0,
        "already_in_parent": 0,
        "hardcoded_added": 0,
        "skipped_apocopic_blocklist": 0,
    }

    # Build lookup: written_form -> lemma_id for nouns
    # Use written form (not normalized stressed) to preserve orthographic distinctions.
    # Fall back to derive_written_from_stressed() if written is not yet populated.
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(lemmas.c.pos == POS.NOUN)
    )
    noun_lookup: dict[str, int] = {}
    for row in result:
        written = row.written or derive_written_from_stressed(row.stressed)
        if written is not None:
            noun_lookup[written] = row.id

    # Count lines for progress
    total_lines = _count_lines(jsonl_path) if progress_callback else 0
    current_line = 0

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            current_line += 1
            if progress_callback and current_line % 10000 == 0:
                progress_callback(current_line, total_lines)

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Only process noun entries
            if entry.get("pos") != "noun":
                continue

            stats["scanned"] += 1

            # Find parent word from alt_of with apocopic tag
            parent_word = None
            gender = None
            allomorph_word = entry["word"]

            for sense in entry.get("senses", []):
                tags = sense.get("tags", [])
                if "apocopic" not in tags:
                    continue

                alt_of_list = sense.get("alt_of", [])
                for alt_of in alt_of_list:
                    parent_word = alt_of.get("word")
                    if parent_word:
                        # Extract gender from tags
                        if "masculine" in tags:
                            gender = "m"
                        elif "feminine" in tags:
                            gender = "f"
                        break
                if parent_word:
                    break

            if not parent_word or not gender:
                continue

            # Skip blocklisted apocopic forms (incorrect gender tags in source data)
            if allomorph_word in SKIP_APOCOPIC_ALLOMORPHS:
                stats["skipped_apocopic_blocklist"] += 1
                continue

            # Look up parent by written form
            parent_written = derive_written_from_stressed(parent_word)
            if parent_written is None:
                stats["parent_not_found"] += 1
                continue
            parent_id = noun_lookup.get(parent_written)
            if parent_id is None:
                stats["parent_not_found"] += 1
                continue

            # Check if parent already has this form
            existing_forms = conn.execute(
                select(noun_forms.c.stressed).where(noun_forms.c.lemma_id == parent_id)
            ).fetchall()
            existing_form_texts = {row.stressed for row in existing_forms if row.stressed}

            if allomorph_word in existing_form_texts:
                stats["already_in_parent"] += 1
                continue

            # Add the apocopic form (singular only - apocopic forms are singular)
            def_article, article_source = get_definite(allomorph_word, gender, "singular")

            try:
                conn.execute(
                    noun_forms.insert().values(
                        lemma_id=parent_id,
                        written=allomorph_word,
                        written_source="wiktionary",
                        stressed=allomorph_word,
                        gender=gender,
                        number="singular",
                        labels="apocopic",
                        def_article=def_article,
                        article_source=article_source,
                        form_origin="alt_of",
                    )
                )
                stats["forms_added"] += 1
                stats["allomorphs_added"] += 1
            except Exception:
                # Form already exists - skip silently
                logger.debug("Apocopic form '%s' already exists for parent", allomorph_word)

    if progress_callback:
        progress_callback(total_lines, total_lines)

    # Import hardcoded noun allomorphs
    for form, parent_lemma, gender, number in HARDCODED_NOUN_ALLOMORPHS:
        # Look up parent by written form
        parent_written = derive_written_from_stressed(parent_lemma)
        parent_id = noun_lookup.get(parent_written) if parent_written else None
        if parent_id is None:
            continue

        # Check if this form already exists
        existing = conn.execute(
            select(noun_forms.c.stressed).where(noun_forms.c.lemma_id == parent_id)
        ).fetchall()
        existing_texts = {row.stressed for row in existing if row.stressed}

        if form in existing_texts:
            continue

        def_article, article_source = get_definite(form, gender, number)

        try:
            conn.execute(
                noun_forms.insert().values(
                    lemma_id=parent_id,
                    written=form,
                    written_source="hardcoded",
                    stressed=form,
                    gender=gender,
                    number=number,
                    labels="apocopic",
                    def_article=def_article,
                    article_source=article_source,
                    form_origin="hardcoded",
                )
            )
            stats["hardcoded_added"] += 1
        except Exception:
            logger.debug("Hardcoded form '%s' already exists for '%s'", form, parent_lemma)

    return stats


def generate_gendered_participles(
    conn: Connection,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Generate feminine/plural forms for all past participles.

    For each masculine singular past participle, this function derives the
    3 other gender/number forms using the regular -o/-a/-i/-e pattern:
    - parlàto (m.s.) → parlàta (f.s.), parlàti (m.p.), parlàte (f.p.)
    - fàtto (m.s.) → fàtta (f.s.), fàtti (m.p.), fàtte (f.p.)

    This is NOT inference - it's deterministic orthographic transformation.
    Italian past participles have 100% regular gender/number agreement.

    Args:
        conn: Database connection
        progress_callback: Optional callback for progress updates (current, total)

    Returns:
        Dict with stats:
        - participles_found: Number of masculine singular participles found
        - forms_generated: Number of new forms generated (3 per participle)
        - duplicates_skipped: Number of forms skipped due to existing entries
    """
    stats = {
        "participles_found": 0,
        "forms_generated": 0,
        "duplicates_skipped": 0,
    }

    # Get all masculine singular past (perfective) participles
    participles = conn.execute(
        select(
            verb_forms.c.lemma_id,
            verb_forms.c.stressed,
            verb_forms.c.written,
            verb_forms.c.labels,
        ).where(
            verb_forms.c.mood == "participle",
            verb_forms.c.aspect == "perfective",
            verb_forms.c.gender == "m",
            verb_forms.c.number == "singular",
        )
    ).fetchall()

    total = len(participles)
    stats["participles_found"] = total

    for idx, row in enumerate(participles):
        if progress_callback and idx % 1000 == 0:
            progress_callback(idx, total)

        lemma_id = row.lemma_id
        stressed = row.stressed
        written = row.written
        labels = row.labels

        # Derive the 3 other forms
        derived = derive_participle_forms(stressed)
        if not derived:
            # Can't derive (e.g., clitic form that doesn't end in -o)
            continue

        for new_stressed, new_gender, new_number in derived:
            # Derive written form using same orthography rules
            new_written = derive_written_from_stressed(new_stressed) if written else None
            new_written_source = "derived:orthography_rule" if new_written is not None else None

            try:
                conn.execute(
                    verb_forms.insert().values(
                        lemma_id=lemma_id,
                        written=new_written,
                        written_source=new_written_source,
                        stressed=new_stressed,
                        mood="participle",
                        tense=None,  # Participles have aspect, not tense
                        aspect="perfective",  # Past participles are perfective
                        person=None,
                        number=new_number,
                        gender=new_gender,
                        is_formal=False,
                        is_negative=False,
                        labels=labels,
                        form_origin="derived:gender_rule",
                    )
                )
                stats["forms_generated"] += 1
            except IntegrityError:
                # Duplicate form (unique constraint violation)
                stats["duplicates_skipped"] += 1

    if progress_callback:
        progress_callback(total, total)

    return stats


def _synthesize_feminine_plural(f_sg: str) -> str | None:
    """Synthesize the feminine plural form from the feminine singular.

    Applies Italian morphological rules for feminine noun pluralization.
    These rules are 100% regular for derived feminines (gender-variable nouns).

    Rules (in priority order):
    - -trice → -trici (attrice → attrici)
    - -drice → -drici (mallevadrice → mallevadrici)
    - -essa → -esse (professoressa → professoresse)
    - vowel + -cia → -cie (lucia → lucie)
    - consonant + -cia → -ce (guercia → guerce)
    - vowel + -gia → -gie (frigia → frigie)
    - consonant + -gia → -ge (carolingia → carolinge)
    - -ca → -che (ricca → ricche)
    - -ga → -ghe (collega → colleghe)
    - -ia → -ie (usuaria → usuarie)
    - -a → -e (default: pazza → pazze)

    Args:
        f_sg: The feminine singular form

    Returns:
        The feminine plural form, or None if synthesis fails
    """
    if not f_sg:
        return None

    # Skip multi-word expressions (contain space)
    if " " in f_sg:
        return None

    # Skip invariables (same as m.sg) - handle at caller level
    # Skip typos ending in -tice/-trive - handle at caller level

    # -trice → -trici
    if f_sg.endswith("trice"):
        return f_sg[:-1] + "i"  # trice → trici

    # -drice → -drici (e.g., mallevadrice → mallevadrici)
    if f_sg.endswith("drice"):
        return f_sg[:-1] + "i"  # drice → drici

    # -essa → -esse
    if f_sg.endswith("essa"):
        return f_sg[:-1] + "e"  # essa → esse

    # -cia / -gia (tricky - depends on preceding letter)
    if f_sg.endswith("cia") and len(f_sg) > 3:
        preceding = f_sg[-4]
        if preceding.lower() in "aeiouàèéìòóù":
            return f_sg[:-1] + "e"  # vowel + cia → cie
        else:
            return f_sg[:-2] + "e"  # consonant + cia → ce

    if f_sg.endswith("gia") and len(f_sg) > 3:
        preceding = f_sg[-4]
        if preceding.lower() in "aeiouàèéìòóù":
            return f_sg[:-1] + "e"  # vowel + gia → gie
        else:
            return f_sg[:-2] + "e"  # consonant + gia → ge

    # -ca → -che
    if f_sg.endswith("ca"):
        return f_sg[:-2] + "che"

    # -ga → -ghe
    if f_sg.endswith("ga"):
        return f_sg[:-2] + "ghe"

    # -ia → -ie
    if f_sg.endswith("ia"):
        return f_sg[:-1] + "e"

    # Default: -a → -e
    if f_sg.endswith("a"):
        return f_sg[:-1] + "e"

    # Non -a endings (rare: -e like -trice handled above)
    # For safety, return None
    return None


def _insert_noun_form(
    conn: Connection,
    lemma_id: int,
    stressed: str,
    gender: str,
    number: str,
    form_origin: str,
    *,
    written: str | None = None,
    written_source: str | None = None,
) -> bool:
    """Insert a noun form, handling article computation and duplicates.

    Returns True if inserted, False if duplicate (IntegrityError).
    """
    def_article, article_source = get_definite(stressed, gender, number)

    if written is None:
        written = derive_written_from_stressed(stressed)
        written_source = "derived:orthography_rule" if written is not None else None

    try:
        conn.execute(
            noun_forms.insert().values(
                lemma_id=lemma_id,
                written=written,
                written_source=written_source,
                stressed=stressed,
                gender=gender,
                number=number,
                labels=None,
                derivation_type=None,
                meaning_hint=None,
                def_article=def_article,
                article_source=article_source,
                form_origin=form_origin,
                is_citation_form=False,
            )
        )
        return True
    except IntegrityError:
        return False


def enrich_missing_feminine_plurals(
    conn: Connection,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Synthesize missing feminine plural forms for COMMON_GENDER_VARIABLE nouns.

    This enrichment phase handles CGV nouns that have f.sg but missing f.pl.

    Synthesis rules:
    - Invariable nouns (f.sg = m.sg): f.pl = f.sg
    - Regular -a → -e pluralization
    - -trice → -trici pluralization

    The synthesis is safe because:
    - Italian feminine plurals are 100% regular
    - The affected nouns are gender-variable (derived forms), not standalone feminines
    - Known irregulars (mano, ala, arma, etc.) are standalone feminines, not in this set

    Args:
        conn: SQLAlchemy connection
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    stats = {
        "total_missing": 0,
        "synthesized": 0,
        "added_invariable": 0,
        "skipped_multiword": 0,
        "skipped_typo": 0,
        "skipped_synthesis_failed": 0,
    }

    # Find all nouns with missing f.pl
    # Query: GenderClass.COMMON_GENDER_VARIABLE nouns with f.sg but no f.pl
    missing_query = (
        select(
            lemmas.c.id.label("noun_lemma_id"),
            lemmas.c.written.label("lemma_written"),
            noun_forms.c.written.label("f_sg_written"),
            noun_forms.c.stressed.label("f_sg_stressed"),
        )
        .select_from(
            noun_metadata.join(lemmas, noun_metadata.c.lemma_id == lemmas.c.id).join(
                noun_forms, noun_forms.c.lemma_id == lemmas.c.id
            )
        )
        .where(
            noun_metadata.c.gender_class == GenderClass.COMMON_GENDER_VARIABLE,
            noun_forms.c.gender == "f",
            noun_forms.c.number == "singular",
        )
        .distinct()
    )

    # Get all candidates (deduplicate by lemma_id)
    # This prevents adding the same f.pl multiple times when a noun has multiple f.sg variants
    candidates: list[Any] = []
    seen_lemma_ids: set[int] = set()
    for row in conn.execute(missing_query):
        # Skip if we already have a candidate for this lemma
        if row.noun_lemma_id in seen_lemma_ids:
            continue
        # Check if f.pl already exists
        exists = conn.execute(
            select(func.count())
            .select_from(noun_forms)
            .where(
                noun_forms.c.lemma_id == row.noun_lemma_id,
                noun_forms.c.gender == "f",
                noun_forms.c.number == "plural",
            )
        ).scalar()
        if exists == 0:
            candidates.append(row)
            seen_lemma_ids.add(row.noun_lemma_id)

    stats["total_missing"] = len(candidates)

    if progress_callback:
        progress_callback(0, len(candidates))

    # Process each candidate - synthesize f.pl
    for i, candidate in enumerate(candidates):
        if progress_callback and i % 100 == 0:
            progress_callback(i, len(candidates))

        noun_lemma_id: int = candidate.noun_lemma_id
        f_sg_written: str | None = candidate.f_sg_written
        f_sg_stressed: str = candidate.f_sg_stressed

        # Try to synthesize
        # Use stressed form for synthesis (may have accents)
        f_sg: str | None = f_sg_stressed or f_sg_written
        if not f_sg:
            stats["skipped_synthesis_failed"] += 1
            continue

        # Skip multi-word expressions
        if " " in f_sg:
            stats["skipped_multiword"] += 1
            continue

        # Skip typos ending in -tice/-trive (should be -trice)
        if f_sg.endswith("tice") or f_sg.endswith("trive"):
            stats["skipped_typo"] += 1
            continue

        # Handle invariables (f.sg = m.sg): add f.pl = f.sg (e.g., sommelier)
        # Check by looking for m.sg with same form
        m_sg_result = conn.execute(
            select(noun_forms.c.stressed)
            .where(
                noun_forms.c.lemma_id == noun_lemma_id,
                noun_forms.c.gender == "m",
                noun_forms.c.number == "singular",
            )
            .limit(1)
        ).first()
        if m_sg_result and m_sg_result.stressed == f_sg:
            # Invariable: f.pl = f.sg
            if _insert_noun_form(
                conn,
                noun_lemma_id,
                f_sg,  # f.pl = f.sg for invariables
                "f",
                "plural",
                "inferred:f_pl_invariable",
                written=f_sg_written,
                written_source="copied:f_sg",
            ):
                stats["added_invariable"] += 1
            continue

        # Synthesize f.pl
        f_pl_stressed = _synthesize_feminine_plural(f_sg)
        if f_pl_stressed is None:
            stats["skipped_synthesis_failed"] += 1
            continue

        # Insert the synthesized form
        f_pl_written = derive_written_from_stressed(f_pl_stressed)
        if _insert_noun_form(
            conn,
            noun_lemma_id,
            f_pl_stressed,
            "f",
            "plural",
            "inferred:f_pl_from_f_sg",
            written=f_pl_written,
            written_source="derived:orthography_rule" if f_pl_written else None,
        ):
            stats["synthesized"] += 1

    if progress_callback:
        progress_callback(len(candidates), len(candidates))

    return stats
