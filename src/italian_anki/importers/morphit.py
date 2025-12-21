"""Enrich forms with real Italian spelling from Morph-it!."""

from collections.abc import Iterator
from pathlib import Path

from sqlalchemy import Connection, select, update

from italian_anki.db.schema import form_lookup, forms, lemmas
from italian_anki.normalize import normalize

# Mapping of our POS names to Morph-it! tag prefixes
POS_TAG_PREFIXES = {
    "verb": "VER:",
    "noun": "NOUN-",
    "adjective": "ADJ:",
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
    """Update forms.form with real Italian spelling from Morph-it!.

    This enrichment phase:
    1. Parses Morph-it! to build normalized_form -> real_form lookup
    2. Updates forms.form (currently NULL) with real spelling
    3. Adds new entries to form_lookup for Morph-it! normalized forms

    Args:
        conn: SQLAlchemy connection
        morphit_path: Path to morph-it.txt file
        pos_filter: Part of speech to enrich (default: "verb")
        batch_size: Number of updates per batch

    Returns:
        Statistics dict with counts
    """
    stats = {"updated": 0, "not_found": 0, "lookup_added": 0}

    # Build the lookup dictionary for the specified POS
    form_lookup_dict = _build_form_lookup(morphit_path, pos_filter)

    # Get all forms that don't have real spelling yet, filtered by POS
    result = conn.execute(
        select(forms.c.id, forms.c.form_stressed)
        .select_from(forms.join(lemmas, forms.c.lemma_id == lemmas.c.lemma_id))
        .where(forms.c.form.is_(None))
        .where(lemmas.c.pos == pos_filter)
    )
    all_forms = result.fetchall()

    # Batch updates
    update_batch: list[dict[str, int | str]] = []
    lookup_batch: list[dict[str, int | str]] = []

    def flush_batches() -> None:
        nonlocal update_batch, lookup_batch

        if update_batch:
            # SQLite bulk update via CASE WHEN
            for item in update_batch:
                conn.execute(
                    update(forms).where(forms.c.id == item["id"]).values(form=item["form"])
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

    for row in all_forms:
        form_id = row.id
        form_stressed = row.form_stressed

        # Normalize the stressed form to look up in Morph-it!
        normalized = normalize(form_stressed)
        real_form = form_lookup_dict.get(normalized)

        if real_form:
            update_batch.append({"id": form_id, "form": real_form})

            # Also add the Morph-it! normalized form to lookup
            # (in case it differs from Wiktextract normalization)
            morphit_normalized = normalize(real_form)
            if morphit_normalized != normalized:
                lookup_batch.append({"form_normalized": morphit_normalized, "form_id": form_id})
        else:
            stats["not_found"] += 1

        if len(update_batch) >= batch_size:
            flush_batches()

    # Final flush
    flush_batches()

    return stats
