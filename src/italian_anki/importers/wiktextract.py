"""Import Italian verb data from Wiktextract JSONL."""

import json
import logging
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, func, select, update

from italian_anki.articles import get_definite
from italian_anki.db.schema import (
    adjective_forms,
    adjective_metadata,
    definitions,
    form_lookup,
    frequencies,
    lemmas,
    noun_forms,
    noun_metadata,
    sentence_lemmas,
    verb_forms,
    verb_metadata,
)
from italian_anki.normalize import normalize
from italian_anki.tags import (
    LABEL_CANONICAL,
    SKIP_TAGS,
    parse_adjective_tags,
    parse_noun_tags,
    parse_verb_tags,
    should_filter_form,
)

logger = logging.getLogger(__name__)

# Mapping from our POS names to Wiktextract's abbreviated names
WIKTEXTRACT_POS = {
    "verb": "verb",
    "noun": "noun",
    "adjective": "adj",  # Wiktextract uses "adj"
}

# POS-specific form tables
POS_FORM_TABLES = {
    "verb": verb_forms,
    "noun": noun_forms,
    "adjective": adjective_forms,
}

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


def _is_invariable_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective is marked invariable via inv:1 flag in head_templates.

    Invariable adjectives (like "blu", "rosa") have the same form for all
    gender/number combinations.
    """
    for template in entry.get("head_templates", []):
        if template.get("args", {}).get("inv") == "1":
            return True
    return False


def _is_two_form_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective is 2-form (same form for masculine and feminine).

    Wiktextract signals 2-form adjectives (like "facile") by using genderless
    number tags (e.g., ["plural"] instead of ["masculine", "plural"]).
    """
    for form_data in entry.get("forms", []):
        tags = set(form_data.get("tags", []))
        has_gender = "masculine" in tags or "feminine" in tags
        has_number = "singular" in tags or "plural" in tags
        if has_number and not has_gender:
            return True
    return False


def _get_adjective_inflection_class(entry: dict[str, Any]) -> str:
    """Determine adjective inflection class from Wiktextract data.

    Returns:
        'invariable': Same form for all gender/number (blu, rosa)
        '2-form': Same form for m/f, different for singular/plural (facile/facili)
        '4-form': Different form for each gender/number (bello/bella/belli/belle)
    """
    if _is_invariable_adjective(entry):
        return "invariable"
    if _is_two_form_adjective(entry):
        return "2-form"
    return "4-form"


def _is_apocopic_adjective(entry: dict[str, Any]) -> bool:
    """Check if adjective is an apocopated (truncated) pre-nominal form.

    Apocopic forms like "buon" (from "buono"), "san" (from "santo"),
    "gran" (from "grande") are marked with apoc:1 in head_templates.
    """
    for template in entry.get("head_templates", []):
        if template.get("args", {}).get("apoc") == "1":
            return True
    return False


def _get_apocopic_parent(entry: dict[str, Any]) -> str | None:
    """Extract parent lemma from apocopic form entry using structured fields only.

    IMPORTANT: Only uses the structured alt_of field, never parses glosses.
    This ensures reliable extraction without brittle text parsing.

    Returns:
        Parent lemma word (e.g., "buono" for "buon") or None if not found.
    """
    for sense in entry.get("senses", []):
        alt_of_list = sense.get("alt_of", [])
        for alt_of in alt_of_list:
            parent = alt_of.get("word")
            if parent:
                return parent
    return None


def _extract_degree_relationship(entry: dict[str, Any]) -> tuple[str, str] | None:
    """Extract comparative/superlative relationship from forms array.

    Looks for structured form entries like:
    - {"form": "of buono", "tags": ["comparative"]}
    - {"form": "of buono", "tags": ["superlative"]}

    IMPORTANT: Only extracts from structured forms array with explicit tags.
    Does not parse glosses or other unstructured text.

    Returns:
        Tuple of (base_lemma_word, degree_relationship) or None.
        E.g., ("buono", "comparative_of") for migliore.
    """
    for form_data in entry.get("forms", []):
        form = form_data.get("form", "")
        tags = form_data.get("tags", [])

        if "comparative" in tags and form.startswith("of "):
            base_word = form[3:].strip()
            if base_word:
                return (base_word, "comparative_of")

        if "superlative" in tags and form.startswith("of "):
            base_word = form[3:].strip()
            if base_word:
                return (base_word, "superlative_of")

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


def _build_counterpart_plurals(jsonl_path: Path) -> dict[str, str]:
    """Build a lookup of lemma words to their plural forms.

    For nouns with counterpart markers (f: "+" or m: "+"), we need to look up
    the counterpart entry's plural. E.g., "amico" has counterpart "amica",
    and we need to know "amica" → "amiche".

    Note: We do NOT skip form-of entries here because counterpart entries like
    "amica" often have form_of senses (referencing "amico") but still have
    valid plural forms we need to look up.

    Args:
        jsonl_path: Path to Wiktextract JSONL file

    Returns:
        Dict mapping lemma word to its plural form.
        E.g., {"amica": "amiche", "amico": "amici"}
    """
    # Tags that indicate a less preferred plural form
    deprioritize_tags = frozenset({"archaic", "dialectal", "obsolete", "poetic", "rare"})

    lookup: dict[str, str] = {}

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            entry = _parse_entry(line)
            if entry is None or entry.get("pos") != "noun":
                continue

            # Note: We intentionally do NOT skip form-of entries here
            # because counterpart entries (like "amica") have form_of senses
            # but still have plural forms we need

            word = entry.get("word", "")
            if not word:
                continue

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
                lookup[word] = best_plural

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
    - gender_class: 'm', 'f', 'common_gender_fixed', 'common_gender_variable'
    - number_class: 'standard', 'pluralia_tantum', 'singularia_tantum', 'invariable'
    - genders: list of genders present in the forms
    """
    result: dict[str, Any] = {
        "gender_class": None,
        "number_class": "standard",
        "genders": [],
    }

    # Check head_templates for gender markers
    has_masculine = False
    has_feminine = False
    has_counterpart_marker = False  # "f": "+" or "m": "+" indicates forms differ by gender
    is_mfbysense = False  # Different meanings per gender (fine)
    is_invariable = False
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

    # Determine gender_class
    if has_masculine and has_feminine:
        if is_mfbysense:
            # Different meanings per gender - will create separate lemmas
            result["gender_class"] = "mfbysense"
        elif has_counterpart_marker:
            # Counterpart marker (f: "+" or m: "+") means forms differ by gender
            # (e.g., amico/amica, professore/professoressa)
            result["gender_class"] = "common_gender_variable"
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
                result["gender_class"] = "common_gender_variable"
            else:
                result["gender_class"] = "common_gender_fixed"
        result["genders"] = ["m", "f"]
    elif has_masculine:
        result["gender_class"] = "m"
        result["genders"] = ["m"]
    elif has_feminine:
        result["gender_class"] = "f"
        result["genders"] = ["f"]
    else:
        # Fall back to _extract_gender for simple cases
        simple_gender = _extract_gender(entry)
        if simple_gender:
            result["gender_class"] = simple_gender
            result["genders"] = [simple_gender]

    # Determine number_class
    if is_pluralia_tantum:
        result["number_class"] = "pluralia_tantum"
    elif is_singularia_tantum:
        result["number_class"] = "singularia_tantum"
    elif is_invariable:
        result["number_class"] = "invariable"
    else:
        result["number_class"] = "standard"

    return result


def _extract_lemma_stressed(entry: dict[str, Any]) -> str:
    """Extract the stressed form of the lemma (infinitive)."""
    # First check forms for canonical or infinitive
    for form in entry.get("forms", []):
        tags = form.get("tags", [])
        if "canonical" in tags or "infinitive" in tags:
            return form.get("form", entry["word"])
    # Fallback to word
    return entry["word"]


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
    # 2-form adjectives (like "facile") have genderless number tags in Wiktextract
    # (e.g., ["plural"] instead of ["masculine", "plural"])
    is_two_form = False
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
        tags = form_data.get("tags", [])
        tag_set = set(tags)

        # Skip empty forms
        if not form_stressed:
            continue

        # For verbs, skip all metadata tags
        if pos == "verb" and tag_set & SKIP_TAGS:
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

        # Skip canonical form for verbs only (stored separately as lemma_stressed)
        # For nouns/adjectives, canonical is the singular form we want to keep
        if pos == "verb" and "canonical" in tags:
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
            if not has_masc_singular:
                key = (lemma_stressed, ("masculine", "singular"))
                if key not in seen:
                    seen.add(key)
                    yield lemma_stressed, ["masculine", "singular"], "inferred:base_form"

            # For 2-form adjectives, add feminine singular too (same form as masculine)
            # is_two_form is set when Wiktextract provides genderless number tags
            if not has_fem_singular and is_two_form:
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


def _clear_existing_data(conn: Connection, pos_filter: str) -> int:
    """Clear all existing data for the given POS.

    Deletes in FK-safe order: form_lookup → POS form tables → definitions
    → frequencies → verb_metadata → sentence_lemmas → lemmas.
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
    lemma_subq = select(lemmas.c.lemma_id).where(lemmas.c.pos == pos_filter)

    # Get the POS-specific form table
    pos_form_table = POS_FORM_TABLES.get(pos_filter)

    # Delete in FK-safe order
    # 1. form_lookup (references *_forms tables)
    if pos_form_table is not None:
        form_id_subq = select(pos_form_table.c.id).where(pos_form_table.c.lemma_id.in_(lemma_subq))
        conn.execute(
            form_lookup.delete().where(
                form_lookup.c.form_id.in_(form_id_subq),
                form_lookup.c.pos == pos_filter,
            )
        )
        # 2. POS-specific form table
        conn.execute(pos_form_table.delete().where(pos_form_table.c.lemma_id.in_(lemma_subq)))

    # 3. definitions (references lemmas)
    conn.execute(definitions.delete().where(definitions.c.lemma_id.in_(lemma_subq)))
    # 4. frequencies (references lemmas)
    conn.execute(frequencies.delete().where(frequencies.c.lemma_id.in_(lemma_subq)))
    # 5. POS-specific metadata tables
    if pos_filter == "verb":
        conn.execute(verb_metadata.delete().where(verb_metadata.c.lemma_id.in_(lemma_subq)))
    elif pos_filter == "noun":
        conn.execute(noun_metadata.delete().where(noun_metadata.c.lemma_id.in_(lemma_subq)))
    elif pos_filter == "adjective":
        conn.execute(
            adjective_metadata.delete().where(adjective_metadata.c.lemma_id.in_(lemma_subq))
        )
    # 6. sentence_lemmas (references lemmas)
    conn.execute(sentence_lemmas.delete().where(sentence_lemmas.c.lemma_id.in_(lemma_subq)))
    # 7. lemmas (direct filter, no subquery needed)
    conn.execute(lemmas.delete().where(lemmas.c.pos == pos_filter))

    return count


def _build_verb_form_row(
    lemma_id: int, form_stressed: str, tags: list[str]
) -> dict[str, Any] | None:
    """Build a verb_forms row dict from tags, or None if should filter."""
    if should_filter_form(tags):
        return None

    features = parse_verb_tags(tags)
    if features.should_filter or features.mood is None:
        return None

    return {
        "lemma_id": lemma_id,
        "form": None,  # Will be filled by Morph-it! importer
        "form_stressed": form_stressed,
        "mood": features.mood,
        "tense": features.tense,
        "person": features.person,
        "number": features.number,
        "gender": features.gender,
        "is_formal": features.is_formal,
        "is_negative": features.is_negative,
        "labels": features.labels,
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
    form_source: str = "wiktionary",
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
        form_source: Source indicator - "wiktionary" for forms from wiktextract
            forms array (default), "synthesized" for forms extracted from
            head_templates only
    """
    if should_filter_form(tags):
        return None

    features = parse_noun_tags(tags)
    if features.should_filter or features.number is None:
        return None

    # Extract gender from tags (for forms like "uova" with ["feminine", "plural"])
    gender: str | None = None
    if "masculine" in tags:
        gender = "m"
    elif "feminine" in tags:
        gender = "f"
    elif lemma_gender:
        # Fall back to lemma gender for forms without explicit gender tag
        gender = lemma_gender

    # Filter out forms without gender (incomplete data)
    if gender is None:
        return None

    # Compute definite article from orthography
    def_article, article_source = get_definite(form_stressed, gender, features.number)

    return {
        "lemma_id": lemma_id,
        "form": None,
        "form_source": form_source,  # Always include to ensure consistent batch insert keys
        "form_stressed": form_stressed,
        "gender": gender,
        "number": features.number,
        "labels": features.labels,
        "is_diminutive": features.is_diminutive,
        "is_augmentative": features.is_augmentative,
        "meaning_hint": meaning_hint,
        "def_article": def_article,
        "article_source": article_source,
    }


def _build_adjective_form_row(
    lemma_id: int,
    form_stressed: str,
    tags: list[str],
    *,
    form_origin: str = "wiktextract",
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
    """
    if should_filter_form(tags):
        return None

    features = parse_adjective_tags(tags)
    if features.should_filter or features.gender is None or features.number is None:
        return None

    # Normalize gender for article computation: "masculine" -> "m", "feminine" -> "f"
    gender_short = "m" if features.gender == "masculine" else "f"

    # Compute definite article from orthography
    def_article, article_source = get_definite(form_stressed, gender_short, features.number)

    return {
        "lemma_id": lemma_id,
        "form": None,
        "form_stressed": form_stressed,
        "gender": features.gender,
        "number": features.number,
        "degree": features.degree,
        "labels": features.labels,
        "def_article": def_article,
        "article_source": article_source,
        "form_origin": form_origin,
    }


# Mapping from POS to form row builder
POS_FORM_BUILDERS = {
    "verb": _build_verb_form_row,
    "noun": _build_noun_form_row,
    "adjective": _build_adjective_form_row,
}


def _count_lines(path: Path) -> int:
    """Count lines in a file efficiently."""
    with path.open(encoding="utf-8") as f:
        return sum(1 for _ in f)


def import_wiktextract(
    conn: Connection,
    jsonl_path: Path,
    *,
    pos_filter: str = "verb",
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
        pos_filter: Part of speech to import (default: "verb")
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
        "nouns_without_gender": 0,
        "definitions": 0,
        "skipped": 0,
        "cleared": cleared,
    }

    # Collect adjective relationship data for post-processing
    # (relationships are resolved after all lemmas are inserted)
    apocopic_links: list[tuple[int, str]] = []  # (lemma_id, parent_word)
    degree_links: list[tuple[int, str, str]] = []  # (lemma_id, base_word, relationship)

    # Get POS-specific table and row builder
    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    build_form_row = POS_FORM_BUILDERS.get(pos_filter)

    if pos_form_table is None or build_form_row is None:
        msg = f"Unsupported POS: {pos_filter}"
        raise ValueError(msg)

    form_batch: list[dict[str, Any]] = []
    lookup_batch: list[dict[str, Any]] = []
    definition_batch: list[dict[str, Any]] = []

    def flush_batches() -> None:
        nonlocal form_batch, lookup_batch, definition_batch
        if form_batch:
            result = conn.execute(
                pos_form_table.insert().returning(pos_form_table.c.id), form_batch
            )
            form_ids = [row.id for row in result]

            # Build lookup entries with the returned IDs
            for form_id, form_data in zip(form_ids, form_batch, strict=True):
                form_normalized = normalize(form_data["form_stressed"])
                lookup_batch.append(
                    {
                        "form_normalized": form_normalized,
                        "pos": pos_filter,
                        "form_id": form_id,
                    }
                )

            form_batch = []
            stats["forms"] += len(form_ids)

        if lookup_batch:
            # Use INSERT OR IGNORE for lookup (same normalized form can map to multiple form_ids)
            conn.execute(
                form_lookup.insert().prefix_with("OR IGNORE"),
                lookup_batch,
            )
            lookup_batch = []

        if definition_batch:
            conn.execute(definitions.insert(), definition_batch)
            stats["definitions"] += len(definition_batch)
            definition_batch = []

    # Map to Wiktextract's POS naming
    wiktextract_pos = WIKTEXTRACT_POS.get(pos_filter, pos_filter)

    # Build lookup of accented alternatives for nouns
    # (fixes bug where Wiktextract stores "dei" but correct spelling is "dèi")
    stressed_alternatives: dict[str, str] | None = None
    if pos_filter == "noun":
        stressed_alternatives = _build_stressed_alternatives(jsonl_path)

    # Build lookup of counterpart plurals for nouns
    # (fixes bug where "amico" gets "amici" for both genders instead of "amiche" for f)
    counterpart_plurals: dict[str, str] | None = None
    if pos_filter == "noun":
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

            # Only import lemmas, not form entries
            if not _is_pos_lemma(entry, wiktextract_pos):
                stats["skipped"] += 1
                continue

            # Extract lemma data
            word = entry["word"]
            lemma_normalized = normalize(word)
            lemma_stressed = _extract_lemma_stressed(entry)

            # Insert lemma
            try:
                result = conn.execute(
                    lemmas.insert().values(
                        lemma=lemma_normalized,
                        lemma_stressed=lemma_stressed,
                        pos=pos_filter,
                        ipa=_extract_ipa(entry),
                    )
                )
                pk = result.inserted_primary_key
                if pk is None:
                    continue
                lemma_id: int = pk[0]
                stats["lemmas"] += 1
            except Exception:
                # Duplicate lemma - skip
                stats["skipped"] += 1
                continue

            # Insert POS-specific metadata
            lemma_gender: str | None = None
            noun_class: dict[str, Any] | None = None
            if pos_filter == "noun":
                noun_class = _extract_noun_classification(entry)
                gender_class = noun_class.get("gender_class")
                number_class = noun_class.get("number_class", "standard")

                if gender_class is None:
                    stats["nouns_without_gender"] += 1
                    # Fall back to simple gender for backward compatibility
                    lemma_gender = _extract_gender(entry)
                else:
                    # Insert noun_metadata
                    conn.execute(
                        noun_metadata.insert().values(
                            lemma_id=lemma_id,
                            gender_class=gender_class,
                            number_class=number_class,
                        )
                    )
                    # Set lemma_gender for form generation (fallback for forms without explicit gender)
                    if gender_class in ("m", "f"):
                        lemma_gender = gender_class
                    elif gender_class == "common_gender_fixed":
                        # For fixed common gender (mfbysense), same form for both - no default needed
                        lemma_gender = None
                    elif gender_class == "common_gender_variable":
                        # For variable common gender (amico/amica), the lemma has a specific gender
                        # that tells us which gender untagged forms belong to
                        lemma_gender = _extract_gender(entry)

            # For nouns: extract plural qualifiers and set up meaning_hint tracking
            plural_qualifiers: dict[str, tuple[str | None, str | None]] = {}
            form_meaning_hints: dict[str, str] = {}  # form_text -> meaning_hint
            synthesize_plurals: list[tuple[str, str, str]] = []  # (form, gender, hint)

            if pos_filter == "noun":
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

            elif pos_filter == "verb":
                auxiliary = _extract_auxiliary(entry)
                transitivity = _extract_transitivity(entry)
                if auxiliary or transitivity:
                    conn.execute(
                        verb_metadata.insert().values(
                            lemma_id=lemma_id,
                            auxiliary=auxiliary,
                            transitivity=transitivity,
                        )
                    )

            elif pos_filter == "adjective":
                # Insert adjective metadata with inflection class
                inflection_class = _get_adjective_inflection_class(entry)
                conn.execute(
                    adjective_metadata.insert().values(
                        lemma_id=lemma_id,
                        inflection_class=inflection_class,
                        # apocopic_of, base_lemma_id, degree_relationship
                        # are populated in post-processing after all lemmas are inserted
                    )
                )

                # Collect apocopic relationships for post-processing
                if _is_apocopic_adjective(entry):
                    parent_word = _get_apocopic_parent(entry)
                    if parent_word:
                        apocopic_links.append((lemma_id, parent_word))

                # Collect comparative/superlative relationships for post-processing
                degree_info = _extract_degree_relationship(entry)
                if degree_info:
                    base_word, relationship = degree_info
                    degree_links.append((lemma_id, base_word, relationship))

            # Queue forms for batch insert (using POS-specific builder)
            # Track what number/gender combinations we've already added for nouns
            seen_noun_forms: set[tuple[str, str]] = set()  # (number, gender)

            # Pre-scan: collect explicit gender-tagged plurals from this entry
            # (used to avoid duplicating untagged plurals when explicit ones exist)
            explicit_fem_plurals: set[str] = set()
            explicit_masc_plurals: set[str] = set()
            if pos_filter == "noun":
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
                if pos_filter == "noun":
                    # Skip singular forms for pluralia tantum nouns
                    is_pluralia_tantum = (
                        noun_class and noun_class.get("number_class") == "pluralia_tantum"
                    )
                    if is_pluralia_tantum and "singular" in tags:
                        continue

                    # Check if this is a common gender noun without explicit gender in tags
                    has_gender_tag = "masculine" in tags or "feminine" in tags
                    is_common_gender = noun_class and noun_class.get("gender_class") in (
                        "common_gender_fixed",
                        "common_gender_variable",
                        "mfbysense",
                    )

                    if is_common_gender and not has_gender_tag:
                        # For common_gender nouns without explicit gender tags:
                        # - common_gender_fixed/mfbysense: same form works for both genders
                        # - common_gender_variable: different forms for m/f (need counterpart lookup)
                        gender_class = noun_class.get("gender_class") if noun_class else None
                        is_variable_gender = gender_class == "common_gender_variable"

                        if is_variable_gender and "plural" in tags:
                            # Smart handling for variable-gender nouns (e.g., amico/amica)
                            # Guard: need lemma_gender to determine which gender this belongs to
                            if not lemma_gender:
                                logger.warning(
                                    f"Noun '{word}' is common_gender_variable with untagged "
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
                                    form_batch.append(row)
                                    number = "plural"
                                    seen_noun_forms.add((number, own_gender))
                                else:
                                    stats["forms_filtered"] += 1
                                continue

                            # Case B: Try counterpart lookup (e.g., "amico" → "amica" → "amiche")
                            counterpart = _get_counterpart_form(entry, lemma_gender)
                            if counterpart and counterpart_plurals:
                                if counterpart in counterpart_plurals:
                                    # Generate own gender with this form
                                    row = _build_noun_form_row(
                                        lemma_id,
                                        form_stressed,
                                        tags,
                                        own_gender,
                                        meaning_hint=form_meaning_hints.get(form_stressed),
                                    )
                                    if row:
                                        form_batch.append(row)
                                        seen_noun_forms.add(("plural", own_gender))
                                    else:
                                        stats["forms_filtered"] += 1

                                    # Generate other gender with looked-up plural
                                    other_plural = counterpart_plurals[counterpart]
                                    row = _build_noun_form_row(
                                        lemma_id,
                                        other_plural,
                                        tags,
                                        other_gender,
                                        meaning_hint=form_meaning_hints.get(other_plural),
                                    )
                                    if row:
                                        form_batch.append(row)
                                        seen_noun_forms.add(("plural", other_gender))
                                    else:
                                        stats["forms_filtered"] += 1
                                    continue
                                else:
                                    # Case C: Counterpart exists but not in lookup
                                    logger.warning(
                                        f"Noun '{word}' ({own_gender}) has counterpart "
                                        f"'{counterpart}' but no plural found in lookup. "
                                        f"Skipping {other_gender} plural."
                                    )
                                    row = _build_noun_form_row(
                                        lemma_id,
                                        form_stressed,
                                        tags,
                                        own_gender,
                                        meaning_hint=form_meaning_hints.get(form_stressed),
                                    )
                                    if row:
                                        form_batch.append(row)
                                        seen_noun_forms.add(("plural", own_gender))
                                    else:
                                        stats["forms_filtered"] += 1
                                    continue

                            # Case D: Plural but no counterpart info - use own gender only
                            logger.warning(
                                f"Noun '{word}' ({own_gender}) is common_gender_variable but "
                                f"plural '{form_stressed}' has no gender tag and no "
                                f"counterpart to look up. Using {own_gender} only."
                            )
                            row = _build_noun_form_row(
                                lemma_id,
                                form_stressed,
                                tags,
                                own_gender,
                                meaning_hint=form_meaning_hints.get(form_stressed),
                            )
                            if row:
                                form_batch.append(row)
                                seen_noun_forms.add(("plural", own_gender))
                            else:
                                stats["forms_filtered"] += 1
                            continue

                        else:
                            # For fixed-gender nouns (mfbysense) or non-plural forms:
                            # duplicate for both genders with same form
                            for gender in ("m", "f"):
                                row = _build_noun_form_row(
                                    lemma_id,
                                    form_stressed,
                                    tags,
                                    gender,
                                    meaning_hint=form_meaning_hints.get(form_stressed),
                                )
                                if row is None:
                                    stats["forms_filtered"] += 1
                                    continue
                                form_batch.append(row)
                                number = "plural" if "plural" in tags else "singular"
                                seen_noun_forms.add((number, gender))
                    else:
                        row = _build_noun_form_row(
                            lemma_id,
                            form_stressed,
                            tags,
                            lemma_gender,
                            meaning_hint=form_meaning_hints.get(form_stressed),
                        )
                        if row is None:
                            stats["forms_filtered"] += 1
                            continue
                        form_batch.append(row)
                        # Track what we've added
                        number = "plural" if "plural" in tags else "singular"
                        gender = (
                            "m"
                            if "masculine" in tags
                            else ("f" if "feminine" in tags else lemma_gender)
                        )
                        if gender:
                            seen_noun_forms.add((number, gender))
                else:
                    # For adjectives, pass form_origin; for verbs, use default builder
                    if pos_filter == "adjective":
                        row = _build_adjective_form_row(
                            lemma_id, form_stressed, tags, form_origin=form_origin
                        )
                    else:
                        row = build_form_row(lemma_id, form_stressed, tags)
                    if row is None:
                        stats["forms_filtered"] += 1
                        continue
                    form_batch.append(row)

                if len(form_batch) >= batch_size:
                    flush_batches()

            # For nouns: synthesize plurals from head_templates (braccio-type cases)
            # These are forms that only exist in head_templates, not in the forms array
            if pos_filter == "noun" and synthesize_plurals:
                for form_text, gender, hint in synthesize_plurals:
                    if ("plural", gender) not in seen_noun_forms:
                        row = _build_noun_form_row(
                            lemma_id,
                            form_text,
                            ["plural"],
                            gender,
                            meaning_hint=hint if hint else None,
                            form_source="synthesized",
                        )
                        if row:
                            form_batch.append(row)
                            seen_noun_forms.add(("plural", gender))

            # For nouns: add base form from lemma word if not already present
            # The lemma word is always the base form (singular for regular, plural for pluralia tantum)
            if pos_filter == "noun" and noun_class:
                number_class = noun_class.get("number_class", "standard")
                gender_class = noun_class.get("gender_class")
                is_pluralia_tantum = number_class == "pluralia_tantum"
                base_number = "plural" if is_pluralia_tantum else "singular"

                is_common_gender = gender_class in (
                    "common_gender_fixed",
                    "common_gender_variable",
                    "mfbysense",
                )

                if is_common_gender:
                    # Add base form for both genders if not already present
                    for gender in ("m", "f"):
                        if (base_number, gender) not in seen_noun_forms:
                            row = _build_noun_form_row(
                                lemma_id, lemma_stressed, [base_number], gender
                            )
                            if row:
                                form_batch.append(row)
                elif lemma_gender and (base_number, lemma_gender) not in seen_noun_forms:
                    # Add base form for single gender if not already present
                    row = _build_noun_form_row(
                        lemma_id, lemma_stressed, [base_number], lemma_gender
                    )
                    if row:
                        form_batch.append(row)

            # Queue definitions with form_meaning_hint for soft key linkage
            if pos_filter == "noun" and word in DEFINITION_FORM_LINKAGE:
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
                                "tags": json.dumps(def_tags) if def_tags else None,
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
                                "tags": json.dumps(def_tags) if def_tags else None,
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
                            "tags": json.dumps(def_tags) if def_tags else None,
                            "form_meaning_hint": None,  # Consistent keys for batch insert
                        }
                    )

    # Final flush
    flush_batches()

    # Post-processing: Link adjective relationships
    # (must happen after all lemmas are inserted so we can resolve lemma IDs)
    if pos_filter == "adjective":
        apocopic_stats = link_apocopic_adjectives(conn, apocopic_links)
        degree_stats = link_comparative_superlative(conn, degree_links)

        # Add linking stats to main stats dict
        stats["apocopic_linked"] = apocopic_stats["linked"]
        stats["apocopic_parent_not_found"] = apocopic_stats["parent_not_found"]
        stats["degree_linked"] = degree_stats["linked"]
        stats["degree_base_not_found"] = degree_stats["base_not_found"]

    # Final progress callback
    if progress_callback:
        progress_callback(total_lines, total_lines)

    return stats


def _is_form_of_entry(entry: dict[str, Any], pos: str) -> bool:
    """Check if entry is a form-of entry (inflected form reference) for the given POS."""
    if entry.get("pos") != pos:
        return False
    # Form-of entries have form_of in at least one sense
    return any("form_of" in sense for sense in entry.get("senses", []))


def _extract_form_of_info(
    entry: dict[str, Any],
) -> Iterator[tuple[str, str, str | None]]:
    """Extract form-of info from an entry.

    Yields (form_word, lemma_word, labels) tuples.
    A form-of entry can reference multiple lemmas in different senses.
    Labels are comma-separated if multiple.
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
        labels = ",".join(sorted(canonical)) if canonical else None

        # Only proceed if there are labels to apply
        if labels is None:
            continue

        # Get lemma(s) this form belongs to
        for form_of in form_of_list:
            lemma_word = form_of.get("word", "")
            if lemma_word:
                yield form_word, lemma_word, labels


def enrich_from_form_of(
    conn: Connection,
    jsonl_path: Path,
    *,
    pos_filter: str = "verb",
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Enrich forms with labels from form-of entries.

    This second pass scans form-of entries (which we skip during main import)
    to extract labels (literary, archaic, regional, etc.) and apply
    them to existing forms in the database.

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to the Wiktextract JSONL file
        pos_filter: Part of speech to enrich (default: "verb")
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    from sqlalchemy import update

    stats = {"scanned": 0, "with_labels": 0, "updated": 0, "not_found": 0}

    # Get POS-specific table
    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        msg = f"Unsupported POS: {pos_filter}"
        raise ValueError(msg)

    # Build lemma lookup: normalized_lemma -> lemma_id
    lemma_result = conn.execute(
        select(lemmas.c.lemma_id, lemmas.c.lemma).where(lemmas.c.pos == pos_filter)
    )
    lemma_lookup: dict[str, int] = {row.lemma: row.lemma_id for row in lemma_result}

    # Build form lookup: (lemma_id, normalized_form) -> list of form_ids
    form_result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.lemma_id, pos_form_table.c.form_stressed)
    )
    form_lookup: dict[tuple[int, str], list[int]] = {}
    for row in form_result:
        normalized = normalize(row.form_stressed)
        key = (row.lemma_id, normalized)
        if key not in form_lookup:
            form_lookup[key] = []
        form_lookup[key].append(row.id)

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

            # Extract form-of info and apply labels
            for form_word, lemma_word, labels in _extract_form_of_info(entry):
                if labels is None:
                    continue

                stats["with_labels"] += 1

                # Look up lemma
                lemma_normalized = normalize(lemma_word)
                lemma_id = lemma_lookup.get(lemma_normalized)
                if lemma_id is None:
                    stats["not_found"] += 1
                    continue

                # Look up form
                form_normalized = normalize(form_word)
                key = (lemma_id, form_normalized)
                form_ids = form_lookup.get(key)
                if not form_ids:
                    stats["not_found"] += 1
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
                        stats["updated"] += 1

    # Final progress callback
    if progress_callback:
        progress_callback(total_lines, total_lines)

    return stats


def enrich_form_spelling_from_form_of(
    conn: Connection,
    jsonl_path: Path,
    *,
    pos_filter: str = "verb",
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Fill form column from form-of entries where Morph-it! didn't have it.

    This is a fallback enrichment that runs after Morph-it! to fill
    the 'form' column using the spelling from Wiktionary form-of entries.

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to the Wiktextract JSONL file
        pos_filter: Part of speech to enrich (default: "verb")
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    from sqlalchemy import update

    stats = {"scanned": 0, "updated": 0, "already_filled": 0, "not_found": 0}

    # Get POS-specific table
    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        msg = f"Unsupported POS: {pos_filter}"
        raise ValueError(msg)

    # Build lemma lookup: normalized_lemma -> lemma_id
    lemma_result = conn.execute(
        select(lemmas.c.lemma_id, lemmas.c.lemma).where(lemmas.c.pos == pos_filter)
    )
    lemma_lookup: dict[str, int] = {row.lemma: row.lemma_id for row in lemma_result}

    # Build form lookup: (lemma_id, normalized_form) -> list of form_ids
    # Only include forms where form IS NULL (not already filled by Morph-it!)
    form_result = conn.execute(
        select(
            pos_form_table.c.id, pos_form_table.c.lemma_id, pos_form_table.c.form_stressed
        ).where(pos_form_table.c.form.is_(None))
    )
    form_lookup: dict[tuple[int, str], list[int]] = {}
    for row in form_result:
        normalized = normalize(row.form_stressed)
        key = (row.lemma_id, normalized)
        if key not in form_lookup:
            form_lookup[key] = []
        form_lookup[key].append(row.id)

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

            # Process each sense's form_of references
            for sense in entry.get("senses", []):
                form_of_list = sense.get("form_of", [])
                if not form_of_list:
                    continue

                for form_of in form_of_list:
                    lemma_word = form_of.get("word", "")
                    if not lemma_word:
                        continue

                    # Look up lemma
                    lemma_normalized = normalize(lemma_word)
                    lemma_id = lemma_lookup.get(lemma_normalized)
                    if lemma_id is None:
                        stats["not_found"] += 1
                        continue

                    # Look up form (only forms with NULL 'form' are in the lookup)
                    form_normalized = normalize(form_word)
                    key = (lemma_id, form_normalized)
                    form_ids = form_lookup.get(key)
                    if not form_ids:
                        # Either already filled by Morph-it! or not found
                        stats["already_filled"] += 1
                        continue

                    # Update form and form_source for all matching forms
                    for form_id in form_ids:
                        conn.execute(
                            update(pos_form_table)
                            .where(pos_form_table.c.id == form_id)
                            .values(form=form_word, form_source="wiktionary")
                        )
                        stats["updated"] += 1

                    # Remove from lookup to avoid duplicate updates
                    del form_lookup[key]

    # Final progress callback
    if progress_callback:
        progress_callback(total_lines, total_lines)

    return stats


def link_apocopic_adjectives(
    conn: Connection,
    apocopic_links: list[tuple[int, str]],
) -> dict[str, int]:
    """Populate adjective_metadata.apocopic_of with parent lemma_ids.

    Args:
        conn: SQLAlchemy connection
        apocopic_links: List of (child_lemma_id, parent_lemma_word) tuples
            collected during import

    Returns:
        Statistics dict with 'linked' and 'parent_not_found' counts
    """
    stats = {"linked": 0, "parent_not_found": 0}

    if not apocopic_links:
        return stats

    # Build lookup: normalized lemma -> lemma_id for adjectives
    result = conn.execute(
        select(lemmas.c.lemma_id, lemmas.c.lemma).where(lemmas.c.pos == "adjective")
    )
    lemma_lookup = {normalize(row.lemma): row.lemma_id for row in result}

    for child_lemma_id, parent_word in apocopic_links:
        parent_normalized = normalize(parent_word)
        parent_lemma_id = lemma_lookup.get(parent_normalized)

        if parent_lemma_id is None:
            stats["parent_not_found"] += 1
            continue

        conn.execute(
            update(adjective_metadata)
            .where(adjective_metadata.c.lemma_id == child_lemma_id)
            .values(apocopic_of=parent_lemma_id)
        )
        stats["linked"] += 1

    return stats


def link_comparative_superlative(
    conn: Connection,
    degree_links: list[tuple[int, str, str]],
) -> dict[str, int]:
    """Populate adjective_metadata.base_lemma_id and degree_relationship.

    Args:
        conn: SQLAlchemy connection
        degree_links: List of (lemma_id, base_lemma_word, relationship) tuples
            collected during import

    Returns:
        Statistics dict with 'linked' and 'base_not_found' counts
    """
    stats = {"linked": 0, "base_not_found": 0}

    if not degree_links:
        return stats

    # Build lookup: normalized lemma -> lemma_id for adjectives
    result = conn.execute(
        select(lemmas.c.lemma_id, lemmas.c.lemma).where(lemmas.c.pos == "adjective")
    )
    lemma_lookup = {normalize(row.lemma): row.lemma_id for row in result}

    for lemma_id, base_word, relationship in degree_links:
        base_normalized = normalize(base_word)
        base_lemma_id = lemma_lookup.get(base_normalized)

        if base_lemma_id is None:
            stats["base_not_found"] += 1
            continue

        conn.execute(
            update(adjective_metadata)
            .where(adjective_metadata.c.lemma_id == lemma_id)
            .values(base_lemma_id=base_lemma_id, degree_relationship=relationship)
        )
        stats["linked"] += 1

    return stats
