"""Enrich forms with real Italian spelling from Morph-it!."""

import logging
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Table, select, update

from italian_anki.db.schema import (
    adjective_forms,
    lemmas,
    noun_forms,
    verb_forms,
)
from italian_anki.enums import POS
from italian_anki.normalize import (
    FRENCH_LOANWORD_WHITELIST,
    derive_written_from_stressed,
)

logger = logging.getLogger(__name__)

# Mapping of our POS names to Morph-it! tag prefixes
POS_TAG_PREFIXES: dict[POS, str] = {
    POS.VERB: "VER:",
    POS.NOUN: "NOUN-",
    POS.ADJECTIVE: "ADJ:",
}

# Mapping of our POS names to their form tables
POS_FORM_TABLES: dict[POS, Table] = {
    POS.VERB: verb_forms,
    POS.NOUN: noun_forms,
    POS.ADJECTIVE: adjective_forms,
}

# Corrections for known Morphit errors in noun forms
# Applied when enriching noun_forms.written from Morphit data
NOUN_WRITTEN_CORRECTIONS: dict[str, str] = {
    "toto": "totò",  # Morphit error: pluralia tantum game name needs final accent
}


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


def _matches_pos(tags: str, pos_filter: POS) -> bool:
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
    morphit_path: Path, pos_filter: POS = POS.VERB
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

        # Also store written form for fallback (preserves meaningful final accents)
        # Use warn=False since Morphit contains French loanwords with multi-accents
        written = derive_written_from_stressed(form, warn=False) or form
        if written not in normalized_lookup:
            normalized_lookup[written] = form

    return exact_lookup, normalized_lookup


def import_morphit(
    conn: Connection,
    morphit_path: Path,
    *,
    pos_filter: POS = POS.VERB,
    batch_size: int = 1000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Update POS-specific form tables with real Italian spelling from Morph-it!.

    This enrichment phase:
    1. Parses Morph-it! to build normalized_form -> real_form lookup
    2. Updates written (currently NULL) with real spelling in verb_forms/noun_forms/adjective_forms
    3. Adds new entries to form_lookup for Morph-it! normalized forms

    Note: For verbs, Morph-it! has no accented forms, so written values are derived
    directly from stressed forms during enrich_lemma_written(). This function is
    a no-op for verbs.

    Args:
        conn: SQLAlchemy connection
        morphit_path: Path to morph-it.txt file
        pos_filter: Part of speech to enrich (default: "verb")
        batch_size: Number of updates per batch
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    stats = {"updated": 0, "not_found": 0, "exact_matched": 0}

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
        .select_from(pos_form_table.join(lemmas, pos_form_table.c.lemma_id == lemmas.c.id))
        .where(pos_form_table.c.written.is_(None))
    )
    all_forms = result.fetchall()
    total_forms = len(all_forms)

    # Batch updates
    update_batch: list[dict[str, Any]] = []

    def flush_batches() -> None:
        nonlocal update_batch

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
            # Only use written-form fallback if the form has accent marks to strip.
            # Unaccented forms (e.g., "eta") should not acquire accents via fallback,
            # as this conflates homographs (Greek letter eta vs Italian età).
            # Use warn=False since French loanwords may have multiple accents.
            if _has_accents(stressed_form):
                written = derive_written_from_stressed(stressed_form, warn=False) or stressed_form
                real_form = normalized_lookup.get(written)

        if real_form:
            # Check if this is a French loanword that should preserve its accent
            # Morph-it! may have stripped the accent (e.g., "defaillance" not "défaillance")
            if stressed_form in FRENCH_LOANWORD_WHITELIST:
                real_form = FRENCH_LOANWORD_WHITELIST[stressed_form]
                written_source = "hardcoded:loanword"
            # Check if this is a known Morphit error for nouns
            elif pos_filter == POS.NOUN and real_form in NOUN_WRITTEN_CORRECTIONS:
                real_form = NOUN_WRITTEN_CORRECTIONS[real_form]
                written_source = "hardcoded:correction"
            else:
                written_source = "morphit"
            update_batch.append(
                {"id": form_id, "written": real_form, "written_source": written_source}
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


# Accented characters in Italian
_ACCENTED_CHARS = set("àèéìòóùÀÈÉÌÒÓÙ")


def _has_accents(text: str) -> bool:
    """Check if text contains any accented characters."""
    return any(c in _ACCENTED_CHARS for c in text)


def apply_unstressed_fallback(
    conn: Connection,
    pos_filter: POS = POS.ADJECTIVE,
) -> dict[str, int]:
    """Copy stressed to written where written is NULL and stressed has no accents.

    When Morphit lookup fails for a form, and that form has no accent marks,
    we can safely assume stressed IS the correct written spelling.

    This handles cases like:
    - stressed="belli" (no accents) -> written="belli"
    - stressed="bèlla" (has accent) -> written stays NULL

    Sets written_source='fallback:no_accent' to track provenance.

    Note: For verbs, all written values are derived during enrich_lemma_written(),
    so there are no NULL values to fill. This function is a no-op for verbs.

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
    pos_filter: POS = POS.NOUN,
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

    Note: For verbs, all written values are derived during enrich_lemma_written(),
    so there are no NULL values to fill. This function is a no-op for verbs.

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
        # Use warn=False since French loanwords may have multiple accents
        written = derive_written_from_stressed(stressed_form, warn=False)
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


def enrich_lemma_written(
    conn: Connection,
    *,
    pos_filter: POS = POS.VERB,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Update lemmas.written by copying from the citation form.

    Citation forms are identified by the is_citation_form=True column:
    - verb: infinitive form
    - adjective: masculine singular form
    - noun: singular (or plural for pluralia tantum) form matching lemma.stressed

    If the citation form's written is NULL, falls back to orthography rules.

    Args:
        conn: SQLAlchemy connection
        pos_filter: Part of speech to enrich (default: "verb")
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    from italian_anki.normalize import (
        FRENCH_LOANWORD_WHITELIST,
        derive_written_from_stressed,
    )

    stats = {
        "updated": 0,
        "from_form": 0,
        "derived": 0,
        "loanwords": 0,
        "no_citation_form": 0,
    }

    pos_form_table = POS_FORM_TABLES.get(pos_filter)
    if pos_form_table is None:
        return stats

    # Get all lemmas that don't have written form yet
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.stressed)
        .where(lemmas.c.pos == pos_filter)
        .where(lemmas.c.written.is_(None))
    )
    all_lemmas = result.fetchall()
    total_lemmas = len(all_lemmas)

    for idx, row in enumerate(all_lemmas, 1):
        if progress_callback and idx % 5000 == 0:
            progress_callback(idx, total_lemmas)

        lemma_id = row.id
        stressed_lemma = row.stressed

        # Query the citation form using is_citation_form flag (unified across all POS)
        form_result = conn.execute(
            select(pos_form_table.c.written, pos_form_table.c.written_source)
            .where(pos_form_table.c.lemma_id == lemma_id)
            .where(pos_form_table.c.is_citation_form == True)  # noqa: E712
            .limit(1)
        ).fetchone()

        written: str | None = None
        written_source: str | None = None

        if form_result and form_result.written:
            # Copy from citation form
            written = form_result.written
            written_source = f"from:{pos_filter}_forms"
            stats["from_form"] += 1
        elif stressed_lemma != "-":
            # Fallback: apply orthography rules
            # Use warn=False since French loanwords may have multiple accents
            written = derive_written_from_stressed(stressed_lemma, warn=False)
            if written is not None:
                if stressed_lemma in FRENCH_LOANWORD_WHITELIST:
                    written_source = "hardcoded:loanword"
                    stats["loanwords"] += 1
                else:
                    written_source = "derived:orthography_rule"
                    stats["derived"] += 1

        if written is not None:
            conn.execute(
                update(lemmas)
                .where(lemmas.c.id == lemma_id)
                .values(written=written, written_source=written_source)
            )
            stats["updated"] += 1
        else:
            stats["no_citation_form"] += 1

    if progress_callback:
        progress_callback(total_lemmas, total_lemmas)

    return stats
