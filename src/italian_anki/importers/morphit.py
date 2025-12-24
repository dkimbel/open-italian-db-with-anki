"""Enrich forms with real Italian spelling from Morph-it!."""

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Table, select, update

from italian_anki.articles import get_definite
from italian_anki.db.schema import (
    adjective_forms,
    form_lookup,
    lemmas,
    noun_forms,
    verb_forms,
)
from italian_anki.normalize import normalize

logger = logging.getLogger(__name__)

# Mapping of our POS names to Morph-it! tag prefixes
POS_TAG_PREFIXES = {
    "verb": "VER:",
    "noun": "NOUN-",
    "adjective": "ADJ:",
}

# Mapping of our POS names to their form tables
POS_FORM_TABLES: dict[str, Table] = {
    "verb": verb_forms,
    "noun": noun_forms,
    "adjective": adjective_forms,
}

# Morphit adjective tag components
_DEGREE_MAP = {"pos": "positive", "sup": "superlative", "comp": "comparative"}
_GENDER_MAP = {"m": "masculine", "f": "feminine"}
_NUMBER_MAP = {"s": "singular", "p": "plural"}


@dataclass
class MorphitEntry:
    """Parsed Morphit adjective entry with full grammatical features."""

    form: str  # Real Italian spelling (e.g., "grandi")
    lemma: str  # Lemma word (e.g., "grande")
    degree: str  # "positive", "superlative", "comparative"
    gender: str  # "masculine", "feminine"
    number: str  # "singular", "plural"


def _parse_adjective_tag(tags: str) -> tuple[str, str, str] | None:
    """Parse ADJ:{degree}+{gender}+{number} format.

    Examples:
        ADJ:pos+m+s -> ("positive", "masculine", "singular")
        ADJ:sup+f+p -> ("superlative", "feminine", "plural")

    Returns:
        Tuple of (degree, gender, number) or None if not a valid adjective tag.
    """
    if not tags.startswith("ADJ:"):
        return None

    # Remove "ADJ:" prefix and split on "+"
    parts = tags[4:].split("+")
    if len(parts) != 3:
        return None

    degree_raw, gender_raw, number_raw = parts

    degree = _DEGREE_MAP.get(degree_raw)
    gender = _GENDER_MAP.get(gender_raw)
    number = _NUMBER_MAP.get(number_raw)

    if degree is None or gender is None or number is None:
        return None

    return (degree, gender, number)


def _parse_morphit(
    morphit_path: Path,
) -> Iterator[tuple[str, str, str]]:
    """Parse Morph-it! file, yielding (form, lemma, tags) tuples.

    Format: tab-separated, one entry per line
    Example: abbacchia\tabbacchiare\tVER:impr+pres+2+s

    Note: Morph-it! file is ISO-8859-1 encoded.
    """
    with morphit_path.open(encoding="iso-8859-1") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) != 3:
                continue

            form, lemma, tags = parts
            yield form, lemma, tags


def _matches_pos(tags: str, pos_filter: str) -> bool:
    """Check if a Morph-it! tag matches the given POS filter.

    Tag formats:
    - Verb: VER:{mood}+{tense}+... (e.g., VER:ind+pres+1+s)
    - Noun: NOUN-{G}:{N} (e.g., NOUN-M:s, NOUN-F:p)
    - Adjective: ADJ:{deg}+{g}+{n} (e.g., ADJ:pos+m+s)
    """
    prefix = POS_TAG_PREFIXES.get(pos_filter)
    if prefix is None:
        return False
    return tags.startswith(prefix)


def _build_form_lookup(morphit_path: Path, pos_filter: str = "verb") -> dict[str, str]:
    """Build a lookup dict: normalized_form -> real_form for the given POS.

    When multiple entries exist for the same normalized form,
    the first occurrence is kept.
    """
    lookup: dict[str, str] = {}

    for form, _lemma, tags in _parse_morphit(morphit_path):
        if not _matches_pos(tags, pos_filter):
            continue

        normalized = normalize(form)

        # Keep first occurrence (most entries share the same spelling)
        if normalized not in lookup:
            lookup[normalized] = form

    return lookup


def _build_adjective_lookup(morphit_path: Path) -> dict[str, list[MorphitEntry]]:
    """Build lemma -> list of MorphitEntry for adjectives.

    This enables looking up ALL forms for a given lemma, including their
    grammatical features (degree, gender, number).

    Returns:
        Dict mapping normalized lemma to list of all its adjective forms.
        E.g., {"grande": [MorphitEntry(form="grande", gender="masculine", ...),
                          MorphitEntry(form="grandi", gender="masculine", number="plural", ...),
                          ...]}
    """
    lookup: dict[str, list[MorphitEntry]] = {}

    for form, lemma, tags in _parse_morphit(morphit_path):
        parsed = _parse_adjective_tag(tags)
        if parsed is None:
            continue

        degree, gender, number = parsed

        entry = MorphitEntry(
            form=form,
            lemma=lemma,
            degree=degree,
            gender=gender,
            number=number,
        )

        normalized_lemma = normalize(lemma)
        if normalized_lemma not in lookup:
            lookup[normalized_lemma] = []
        lookup[normalized_lemma].append(entry)

    return lookup


def import_morphit(
    conn: Connection,
    morphit_path: Path,
    *,
    pos_filter: str = "verb",
    batch_size: int = 1000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Update POS-specific form tables with real Italian spelling from Morph-it!.

    This enrichment phase:
    1. Parses Morph-it! to build normalized_form -> real_form lookup
    2. Updates form (currently NULL) with real spelling in verb_forms/noun_forms/adjective_forms
    3. Adds new entries to form_lookup for Morph-it! normalized forms

    Args:
        conn: SQLAlchemy connection
        morphit_path: Path to morph-it.txt file
        pos_filter: Part of speech to enrich (default: "verb")
        batch_size: Number of updates per batch
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    stats = {"updated": 0, "not_found": 0, "lookup_added": 0}

    # Get POS-specific form table
    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        msg = f"Unsupported POS: {pos_filter}"
        raise ValueError(msg)

    # Build the lookup dictionary for the specified POS
    morphit_lookup = _build_form_lookup(morphit_path, pos_filter)

    # Get all forms that don't have real spelling yet from POS-specific table
    result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.form_stressed)
        .select_from(pos_form_table.join(lemmas, pos_form_table.c.lemma_id == lemmas.c.lemma_id))
        .where(pos_form_table.c.form.is_(None))
    )
    all_forms = result.fetchall()
    total_forms = len(all_forms)

    # Batch updates
    update_batch: list[dict[str, Any]] = []
    lookup_batch: list[dict[str, Any]] = []

    def flush_batches() -> None:
        nonlocal update_batch, lookup_batch

        if update_batch:
            # Update form column in POS-specific table
            for item in update_batch:
                conn.execute(
                    update(pos_form_table)
                    .where(pos_form_table.c.id == item["id"])
                    .values(form=item["form"], form_source=item["form_source"])
                )
            stats["updated"] += len(update_batch)
            update_batch = []

        if lookup_batch:
            conn.execute(
                form_lookup.insert().prefix_with("OR IGNORE"),
                lookup_batch,
            )
            stats["lookup_added"] += len(lookup_batch)
            lookup_batch = []

    for idx, row in enumerate(all_forms, 1):
        if progress_callback and idx % 10000 == 0:
            progress_callback(idx, total_forms)

        form_id = row.id
        form_stressed = row.form_stressed

        # Normalize the stressed form to look up in Morph-it!
        normalized = normalize(form_stressed)
        real_form = morphit_lookup.get(normalized)

        if real_form:
            update_batch.append({"id": form_id, "form": real_form, "form_source": "morphit"})

            # Also add the Morph-it! normalized form to lookup
            # (in case it differs from Wiktextract normalization)
            morphit_normalized = normalize(real_form)
            if morphit_normalized != normalized:
                lookup_batch.append(
                    {
                        "form_normalized": morphit_normalized,
                        "pos": pos_filter,
                        "form_id": form_id,
                    }
                )
        else:
            stats["not_found"] += 1

        if len(update_batch) >= batch_size:
            flush_batches()

    # Final flush
    flush_batches()

    # Final progress callback
    if progress_callback:
        progress_callback(total_forms, total_forms)

    return stats


def fill_missing_adjective_forms(
    conn: Connection,
    morphit_path: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Fill missing adjective forms using Morphit as authoritative source.

    For ALL adjectives, looks up forms in Morphit and inserts any missing
    (gender, number) combinations. This includes:
    - Standard forms (m/f x sg/pl) for adjectives with incomplete data
    - Elided forms (ending with ') like grand', sant' for allomorphs

    Args:
        conn: SQLAlchemy connection
        morphit_path: Path to morph-it.txt file
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts:
        - adjectives_checked: Number of adjectives processed
        - forms_added: Number of new forms inserted from Morphit
        - adjectives_completed: Adjectives that gained forms
        - not_in_morphit: Adjectives not found in Morphit
        - elided_added: Elided forms (ending with ') added
        - combos_skipped: Forms skipped because they already exist
    """
    stats = {
        "adjectives_checked": 0,
        "forms_added": 0,
        "adjectives_completed": 0,
        "not_in_morphit": 0,
        "elided_added": 0,
        "combos_skipped": 0,
    }

    # Build Morphit lookup: normalized_lemma -> list of MorphitEntry
    morphit_lookup = _build_adjective_lookup(morphit_path)

    # Get ALL adjectives (not just incomplete ones)
    # The existing_combos logic prevents duplicate insertions
    result = conn.execute(
        select(lemmas.c.lemma_id, lemmas.c.lemma).where(lemmas.c.pos == "adjective")
    )
    all_adjectives = result.fetchall()
    stats["adjectives_checked"] = len(all_adjectives)

    if progress_callback:
        progress_callback(0, len(all_adjectives))

    for idx, (lemma_id, lemma_word) in enumerate(all_adjectives, 1):
        if progress_callback and idx % 100 == 0:
            progress_callback(idx, len(all_adjectives))

        # Look up in Morphit by normalized lemma
        normalized_lemma = normalize(lemma_word)
        morphit_forms = morphit_lookup.get(normalized_lemma, [])

        if not morphit_forms:
            stats["not_in_morphit"] += 1
            logger.debug("Adjective '%s' not found in Morphit", lemma_word)
            continue

        # Get existing forms for this lemma (positive degree only)
        existing_result = conn.execute(
            select(
                adjective_forms.c.form_stressed,
                adjective_forms.c.gender,
                adjective_forms.c.number,
            )
            .where(adjective_forms.c.lemma_id == lemma_id)
            .where(adjective_forms.c.degree == "positive")
        )
        existing_rows = existing_result.fetchall()

        # Key matches DB constraint: UNIQUE (lemma_id, form_stressed, gender, number, degree)
        # This allows multiple forms per (gender, number) as long as form_stressed differs
        existing_combos = {(row.form_stressed, row.gender, row.number) for row in existing_rows}

        forms_added_for_lemma = 0

        # Insert missing forms from Morphit (only positive degree)
        for entry in morphit_forms:
            if entry.degree != "positive":
                continue  # Only fill base forms, not superlatives/comparatives

            # Track elided forms (ending with ') - they get labels='elided'
            is_elided = entry.form.endswith("'")

            # Key includes form to allow multiple forms per (gender, number)
            combo = (entry.form, entry.gender, entry.number)

            if combo in existing_combos:
                # Exact duplicate - skip silently
                stats["combos_skipped"] += 1
                logger.debug(
                    "Skipped duplicate '%s' for '%s' (%s/%s)",
                    entry.form,
                    lemma_word,
                    entry.gender,
                    entry.number,
                )
                continue

            # Compute definite article for this form
            gender_abbr = "m" if entry.gender == "masculine" else "f"
            def_article, article_source = get_definite(entry.form, gender_abbr, entry.number)

            # Insert new form
            conn.execute(
                adjective_forms.insert().values(
                    lemma_id=lemma_id,
                    form=entry.form,  # Morphit provides real spelling directly
                    form_source="morphit",
                    form_stressed=entry.form,  # Morphit doesn't have stress; use form
                    gender=entry.gender,
                    number=entry.number,
                    degree="positive",
                    labels="elided" if is_elided else None,
                    def_article=def_article,
                    article_source=article_source,
                    form_origin="morphit",
                )
            )
            # Mark this combo as filled to prevent duplicate insertions
            existing_combos.add(combo)
            forms_added_for_lemma += 1
            stats["forms_added"] += 1
            if is_elided:
                stats["elided_added"] += 1

        # Check if now complete (4 positive-degree forms)
        # existing_combos now includes forms we just added
        if forms_added_for_lemma > 0 and len(existing_combos) >= 4:
            stats["adjectives_completed"] += 1

    if progress_callback:
        progress_callback(len(all_adjectives), len(all_adjectives))

    return stats


# Accented characters in Italian
_ACCENTED_CHARS = set("àèéìòóùÀÈÉÌÒÓÙ")


def _has_accents(text: str) -> bool:
    """Check if text contains any accented characters."""
    return any(c in _ACCENTED_CHARS for c in text)


def apply_unstressed_fallback(
    conn: Connection,
    pos_filter: str = "adjective",
) -> dict[str, int]:
    """Copy form_stressed to form where form is NULL and form_stressed has no accents.

    When Morphit lookup fails for a form, and that form has no accent marks,
    we can safely assume form_stressed IS the correct form spelling.

    This handles cases like:
    - form_stressed="belli" (no accents) -> form="belli"
    - form_stressed="bèlla" (has accent) -> form stays NULL

    Sets form_source='fallback:no_accent' to track provenance.

    Args:
        conn: SQLAlchemy connection
        pos_filter: Part of speech to process (default: "adjective")

    Returns:
        Statistics dict with 'updated' count
    """
    stats = {"updated": 0}

    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        return stats

    # Find forms with NULL form and check if form_stressed has accents
    result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.form_stressed).where(
            pos_form_table.c.form.is_(None)
        )
    )

    for row in result:
        form_stressed = row.form_stressed
        if not _has_accents(form_stressed):
            conn.execute(
                update(pos_form_table)
                .where(pos_form_table.c.id == row.id)
                .values(form=form_stressed, form_source="fallback:no_accent")
            )
            stats["updated"] += 1

    return stats
