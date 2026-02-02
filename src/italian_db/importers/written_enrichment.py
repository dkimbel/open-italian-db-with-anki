"""Fallback functions for enriching forms with written Italian spelling.

Note: The import_morphit() function has been removed. Wiktextract form-of entries
now provide all written forms that Morphit was providing, and the only disagreements
found were Morphit errors. See DATA_SOURCES.md for details.
"""

from collections.abc import Callable

from sqlalchemy import Connection, Table, select, text

from italian_db.db.schema import (
    adjective_forms,
    lemmas,
    noun_forms,
    verb_forms,
)
from italian_db.enums import POS

# Mapping of our POS names to their form tables
POS_FORM_TABLES: dict[POS, Table] = {
    POS.VERB: verb_forms,
    POS.NOUN: noun_forms,
    POS.ADJECTIVE: adjective_forms,
}


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

    table_name = pos_form_table.name
    result = conn.execute(
        text(
            f"UPDATE {table_name} "  # noqa: S608 - table_name from schema, not user input
            "SET written = stressed, written_source = 'fallback:no_accent' "
            "WHERE written IS NULL "
            "AND stressed != '-' "
            "AND stressed NOT GLOB '*[àèéìòóùÀÈÉÌÒÓÙ]*'"
        )
    )
    stats["updated"] = result.rowcount

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
    from italian_db.normalize import (
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

    update_batch: list[dict[str, str | int]] = []

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

        update_batch.append({"_id": row.id, "written": written, "written_source": written_source})
        stats["updated"] += 1

    if update_batch:
        table_name = pos_form_table.name
        conn.execute(
            text(
                f"UPDATE {table_name} "  # noqa: S608 - table_name from schema, not user input
                "SET written = :written, written_source = :written_source "
                "WHERE id = :_id"
            ),
            update_batch,
        )

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
    from italian_db.normalize import (
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

    # Preload all citation forms in a single query to avoid N+1
    citation_result = conn.execute(
        select(
            pos_form_table.c.lemma_id, pos_form_table.c.written, pos_form_table.c.written_source
        ).where(pos_form_table.c.is_citation_form == True)  # noqa: E712
    )
    citation_lookup: dict[int, tuple[str | None, str | None]] = {
        row.lemma_id: (row.written, row.written_source) for row in citation_result
    }

    # Get all lemmas that don't have written form yet
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.stressed)
        .where(lemmas.c.pos == pos_filter)
        .where(lemmas.c.written.is_(None))
    )
    all_lemmas = result.fetchall()
    total_lemmas = len(all_lemmas)

    update_batch: list[dict[str, str | int | None]] = []

    for idx, row in enumerate(all_lemmas, 1):
        if progress_callback and idx % 5000 == 0:
            progress_callback(idx, total_lemmas)

        lemma_id = row.id
        stressed_lemma = row.stressed

        # Look up citation form from preloaded dict
        citation_data = citation_lookup.get(lemma_id)

        written: str | None = None
        written_source: str | None = None

        if citation_data and citation_data[0]:
            # Copy from citation form
            written = citation_data[0]
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
            update_batch.append(
                {"_id": lemma_id, "written": written, "written_source": written_source}
            )
            stats["updated"] += 1
        else:
            stats["no_citation_form"] += 1

    if update_batch:
        conn.execute(
            text("""
                UPDATE lemmas
                SET written = :written, written_source = :written_source
                WHERE id = :_id
            """),
            update_batch,
        )

    if progress_callback:
        progress_callback(total_lemmas, total_lemmas)

    return stats
