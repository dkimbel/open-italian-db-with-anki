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


def _build_form_lookup(
    morphit_path: Path, pos_filter: str = "verb"
) -> tuple[dict[str, str], dict[str, str]]:
    """Build lookup dicts for Morphit forms.

    Returns two lookups:
    1. exact_lookup: form (with accents) -> form (exact match)
    2. normalized_lookup: normalized_form -> real_form (fallback)

    The exact lookup is used first to preserve written accents (e.g., "parlò").
    The normalized lookup is used as fallback for pronunciation-only stress marks
    (e.g., "pàrlo" -> "parlo").

    When multiple entries exist for the same normalized form in the fallback,
    the first occurrence is kept.
    """
    exact_lookup: dict[str, str] = {}
    normalized_lookup: dict[str, str] = {}

    for form, _lemma, tags in _parse_morphit(morphit_path):
        if not _matches_pos(tags, pos_filter):
            continue

        # Store exact form (with accents intact)
        exact_lookup[form] = form

        # Also store normalized for fallback
        normalized = normalize(form)
        if normalized not in normalized_lookup:
            normalized_lookup[normalized] = form

    return exact_lookup, normalized_lookup


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
    2. Updates written (currently NULL) with real spelling in verb_forms/noun_forms/adjective_forms
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
    stats = {"updated": 0, "not_found": 0, "lookup_added": 0, "exact_matched": 0}

    # Get POS-specific form table
    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        msg = f"Unsupported POS: {pos_filter}"
        raise ValueError(msg)

    # Build the lookup dictionaries for the specified POS
    # exact_lookup: preserves accents (e.g., "parlò" -> "parlò")
    # normalized_lookup: fallback for pronunciation-only marks (e.g., "parlo" -> "parlo")
    exact_lookup, normalized_lookup = _build_form_lookup(morphit_path, pos_filter)

    # Get all forms that don't have real spelling yet from POS-specific table
    result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.stressed)
        .select_from(pos_form_table.join(lemmas, pos_form_table.c.lemma_id == lemmas.c.lemma_id))
        .where(pos_form_table.c.written.is_(None))
    )
    all_forms = result.fetchall()
    total_forms = len(all_forms)

    # Batch updates
    update_batch: list[dict[str, Any]] = []
    lookup_batch: list[dict[str, Any]] = []

    def flush_batches() -> None:
        nonlocal update_batch, lookup_batch

        if update_batch:
            # Update written column in POS-specific table
            for item in update_batch:
                conn.execute(
                    update(pos_form_table)
                    .where(pos_form_table.c.id == item["id"])
                    .values(written=item["written"], written_source=item["written_source"])
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
        stressed_form = row.stressed

        # Try exact match first (preserves written accents like "parlò")
        real_form = exact_lookup.get(stressed_form)
        if real_form:
            stats["exact_matched"] += 1
        else:
            # Fall back to normalized lookup (for pronunciation-only marks like "pàrlo")
            normalized = normalize(stressed_form)
            real_form = normalized_lookup.get(normalized)

        if real_form:
            update_batch.append({"id": form_id, "written": real_form, "written_source": "morphit"})

            # Also add the Morph-it! normalized form to lookup
            # (in case it differs from Wiktextract normalization)
            morphit_normalized = normalize(real_form)
            normalized = normalize(stressed_form)
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
        select(lemmas.c.lemma_id, lemmas.c.normalized).where(lemmas.c.pos == "adjective")
    )
    all_adjectives = result.fetchall()
    stats["adjectives_checked"] = len(all_adjectives)

    if progress_callback:
        progress_callback(0, len(all_adjectives))

    for idx, (lemma_id, lemma_normalized) in enumerate(all_adjectives, 1):
        if progress_callback and idx % 100 == 0:
            progress_callback(idx, len(all_adjectives))

        # Look up in Morphit by normalized lemma
        morphit_forms = morphit_lookup.get(lemma_normalized, [])

        if not morphit_forms:
            stats["not_in_morphit"] += 1
            logger.debug("Adjective '%s' not found in Morphit", lemma_normalized)
            continue

        # Get existing forms for this lemma (positive degree only)
        existing_result = conn.execute(
            select(
                adjective_forms.c.stressed,
                adjective_forms.c.gender,
                adjective_forms.c.number,
            )
            .where(adjective_forms.c.lemma_id == lemma_id)
            .where(adjective_forms.c.degree == "positive")
        )
        existing_rows = existing_result.fetchall()

        # Key matches DB constraint: UNIQUE (lemma_id, stressed, gender, number, degree)
        # This allows multiple forms per (gender, number) as long as stressed differs
        existing_combos = {(row.stressed, row.gender, row.number) for row in existing_rows}

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
                    lemma_normalized,
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
                    written=entry.form,  # Morphit provides real spelling directly
                    written_source="morphit",
                    stressed=entry.form,  # Morphit doesn't have stress; use form
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
    """Copy stressed to written where written is NULL and stressed has no accents.

    When Morphit lookup fails for a form, and that form has no accent marks,
    we can safely assume stressed IS the correct written spelling.

    This handles cases like:
    - stressed="belli" (no accents) -> written="belli"
    - stressed="bèlla" (has accent) -> written stays NULL

    Sets written_source='fallback:no_accent' to track provenance.

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

    # Find forms with NULL written and check if stressed has accents
    result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.stressed).where(
            pos_form_table.c.written.is_(None)
        )
    )

    for row in result:
        stressed_form = row.stressed
        # Skip "-" which represents missing forms for defective verbs
        if stressed_form != "-" and not _has_accents(stressed_form):
            conn.execute(
                update(pos_form_table)
                .where(pos_form_table.c.id == row.id)
                .values(written=stressed_form, written_source="fallback:no_accent")
            )
            stats["updated"] += 1

    return stats


def apply_orthography_fallback(
    conn: Connection,
    pos_filter: str = "noun",
) -> dict[str, int]:
    """Derive written from stressed for remaining NULL values using orthography rules.

    This is the final fallback for forms that:
    - Were not found in Morph-it!
    - Could not use the unstressed fallback (have accent marks)

    Uses Italian orthography rules to derive the correct written form from the
    stressed form. Handles French loanwords with multiple accents via whitelist.

    Sets written_source to either:
    - 'derived:orthography_rule' for standard derivation
    - 'hardcoded:loanword' for French loanword whitelist matches

    Args:
        conn: SQLAlchemy connection
        pos_filter: Part of speech to process (default: "noun")

    Returns:
        Statistics dict with 'updated', 'loanwords', 'failed' counts
    """
    from italian_anki.normalize import (
        FRENCH_LOANWORD_WHITELIST,
        derive_written_from_stressed,
    )

    stats = {"updated": 0, "loanwords": 0, "failed": 0}

    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        return stats

    # Find forms with NULL written
    result = conn.execute(
        select(pos_form_table.c.id, pos_form_table.c.stressed).where(
            pos_form_table.c.written.is_(None)
        )
    )

    for row in result:
        stressed_form = row.stressed
        # Skip "-" which represents missing forms for defective verbs
        if stressed_form == "-":
            continue

        # Try to derive written form
        written = derive_written_from_stressed(stressed_form)
        if written is None:
            stats["failed"] += 1
            continue

        # Determine source: loanword whitelist or regular derivation
        if stressed_form in FRENCH_LOANWORD_WHITELIST:
            written_source = "hardcoded:loanword"
            stats["loanwords"] += 1
        else:
            written_source = "derived:orthography_rule"

        conn.execute(
            update(pos_form_table)
            .where(pos_form_table.c.id == row.id)
            .values(written=written, written_source=written_source)
        )
        stats["updated"] += 1

    return stats


def _build_lemma_lookup(morphit_path: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Build lookup dicts for Morphit lemmas.

    Returns two lookups:
    1. exact_lookup: lemma (with accents) -> lemma (exact match)
    2. normalized_lookup: normalized_lemma -> real_lemma (fallback)

    Similar to _build_form_lookup but for lemmas.
    """
    exact_lookup: dict[str, str] = {}
    normalized_lookup: dict[str, str] = {}

    for _form, lemma, _tags in _parse_morphit(morphit_path):
        # Store exact lemma (with accents intact)
        exact_lookup[lemma] = lemma

        # Also store normalized for fallback
        normalized = normalize(lemma)
        if normalized not in normalized_lookup:
            normalized_lookup[normalized] = lemma

    return exact_lookup, normalized_lookup


def enrich_lemma_written(
    conn: Connection,
    morphit_path: Path,
    *,
    pos_filter: str = "verb",
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Update lemmas.written with real Italian spelling from Morph-it!.

    Uses exact matching first (to preserve written accents like "città"),
    then falls back to normalized matching (for pronunciation-only marks).

    Args:
        conn: SQLAlchemy connection
        morphit_path: Path to morph-it.txt file
        pos_filter: Part of speech to enrich (default: "verb")
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    stats = {"updated": 0, "not_found": 0, "exact_matched": 0}

    # Build the lookup dictionaries for lemmas
    exact_lookup, normalized_lookup = _build_lemma_lookup(morphit_path)

    # Get all lemmas that don't have written form yet
    result = conn.execute(
        select(lemmas.c.lemma_id, lemmas.c.stressed)
        .where(lemmas.c.pos == pos_filter)
        .where(lemmas.c.written.is_(None))
    )
    all_lemmas = result.fetchall()
    total_lemmas = len(all_lemmas)

    for idx, row in enumerate(all_lemmas, 1):
        if progress_callback and idx % 5000 == 0:
            progress_callback(idx, total_lemmas)

        lemma_id = row.lemma_id
        stressed_lemma = row.stressed

        # Try exact match first (preserves written accents like "città")
        real_lemma = exact_lookup.get(stressed_lemma)
        if real_lemma:
            stats["exact_matched"] += 1
        else:
            # Fall back to normalized lookup
            normalized = normalize(stressed_lemma)
            real_lemma = normalized_lookup.get(normalized)

        if real_lemma:
            conn.execute(
                update(lemmas).where(lemmas.c.lemma_id == lemma_id).values(written=real_lemma)
            )
            stats["updated"] += 1
        else:
            stats["not_found"] += 1

    if progress_callback:
        progress_callback(total_lemmas, total_lemmas)

    return stats
