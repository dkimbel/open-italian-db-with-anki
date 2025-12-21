"""Enrich forms with real Italian spelling from Morph-it!."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Table, select, update

from italian_anki.db.schema import (
    adjective_forms,
    form_lookup_new,
    lemmas,
    noun_forms,
    verb_forms,
)
from italian_anki.normalize import normalize

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


def import_morphit(
    conn: Connection,
    morphit_path: Path,
    *,
    pos_filter: str = "verb",
    batch_size: int = 1000,
) -> dict[str, int]:
    """Update POS-specific form tables with real Italian spelling from Morph-it!.

    This enrichment phase:
    1. Parses Morph-it! to build normalized_form -> real_form lookup
    2. Updates form (currently NULL) with real spelling in verb_forms/noun_forms/adjective_forms
    3. Adds new entries to form_lookup_new for Morph-it! normalized forms

    Args:
        conn: SQLAlchemy connection
        morphit_path: Path to morph-it.txt file
        pos_filter: Part of speech to enrich (default: "verb")
        batch_size: Number of updates per batch

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
                    .values(form=item["form"])
                )
            stats["updated"] += len(update_batch)
            update_batch = []

        if lookup_batch:
            conn.execute(
                form_lookup_new.insert().prefix_with("OR IGNORE"),
                lookup_batch,
            )
            stats["lookup_added"] += len(lookup_batch)
            lookup_batch = []

    for row in all_forms:
        form_id = row.id
        form_stressed = row.form_stressed

        # Normalize the stressed form to look up in Morph-it!
        normalized = normalize(form_stressed)
        real_form = morphit_lookup.get(normalized)

        if real_form:
            update_batch.append({"id": form_id, "form": real_form})

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

    return stats
