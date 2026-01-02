"""Import verb irregularity pattern classifications into the database.

This importer reads the manual classifications from verb_irregularity_data.py
and populates the verb_irregularity table.
"""

from collections.abc import Callable
from dataclasses import dataclass, field

from sqlalchemy import Connection, delete, select

from italian_anki.data.verb_irregularity_data import VERB_IRREGULARITY_CLASSIFICATIONS
from italian_anki.db.schema import lemmas, verb_irregularity
from italian_anki.enums import POS

# Classification source identifier
CLASSIFICATION_SOURCE = "manual"


@dataclass
class VerbIrregularityStats:
    """Statistics from verb irregularity import."""

    total: int = 0
    matched: int = 0
    not_found: int = 0
    not_found_list: list[str] = field(default_factory=lambda: [])


def import_verb_irregularity(
    conn: Connection,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> VerbIrregularityStats:
    """Import verb irregularity patterns into the database.

    This function:
    1. Clears existing verb_irregularity entries (idempotent re-import)
    2. Looks up each verb's lemma_id by stressed form
    3. Inserts pattern classifications

    Args:
        conn: SQLAlchemy connection
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        VerbIrregularityStats with:
        - total: Number of classifications in data file
        - matched: Successfully matched and inserted
        - not_found: Verbs not found in lemmas table
        - not_found_list: List of verbs not found (for debugging)
    """
    stats = VerbIrregularityStats(total=len(VERB_IRREGULARITY_CLASSIFICATIONS))

    # Clear existing entries for clean re-import
    conn.execute(delete(verb_irregularity))

    # Build lookup dict: written form -> lemma_id (for verbs only)
    # Using written form (not stressed) avoids accent variations (è vs é) causing mismatches
    result = conn.execute(select(lemmas.c.id, lemmas.c.written).where(lemmas.c.pos == POS.VERB))
    written_to_id: dict[str, int] = {row.written: row.id for row in result}

    total = len(VERB_IRREGULARITY_CLASSIFICATIONS)
    insert_batch: list[dict[str, str | int | None]] = []

    for idx, (written_form, patterns) in enumerate(VERB_IRREGULARITY_CLASSIFICATIONS.items(), 1):
        if progress_callback and idx % 50 == 0:
            progress_callback(idx, total)

        lemma_id = written_to_id.get(written_form)

        if lemma_id is None:
            stats.not_found += 1
            stats.not_found_list.append(written_form)
            continue

        present, remote, future, participle, subjunctive = patterns

        insert_batch.append(
            {
                "lemma_id": lemma_id,
                "present_pattern": present.value if present else None,
                "remote_pattern": remote.value if remote else None,
                "future_pattern": future.value if future else None,
                "participle_pattern": participle.value if participle else None,
                "subjunctive_pattern": subjunctive.value if subjunctive else None,
                "classification_source": CLASSIFICATION_SOURCE,
                "notes": None,
            }
        )
        stats.matched += 1

    # Batch insert all classifications
    if insert_batch:
        conn.execute(verb_irregularity.insert(), insert_batch)

    # Final progress callback
    if progress_callback:
        progress_callback(total, total)

    return stats


def get_pattern_statistics(conn: Connection) -> dict[str, dict[str, int]]:
    """Get counts of verbs per irregularity pattern.

    Useful for verifying coverage and finding representative verbs.

    Returns:
        Dict mapping pattern column -> (pattern_value -> count)
    """
    from sqlalchemy import func

    pattern_cols = [
        "present_pattern",
        "remote_pattern",
        "future_pattern",
        "participle_pattern",
        "subjunctive_pattern",
    ]

    result: dict[str, dict[str, int]] = {}

    for col_name in pattern_cols:
        col = getattr(verb_irregularity.c, col_name)
        query = (
            select(col, func.count().label("count"))
            .where(col.isnot(None))
            .group_by(col)
            .order_by(func.count().desc())
        )
        rows = conn.execute(query).fetchall()
        result[col_name] = {row[0]: row[1] for row in rows}

    return result
