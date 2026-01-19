"""Database queries for Italian language data.

This module provides query helpers for lemma relationships and derived words.
"""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Connection, select

from italian_db.db import definitions, lemma_relationships, lemmas


@dataclass(frozen=True)
class RelatedLemma:
    """A lemma related to another lemma."""

    lemma_id: int
    written: str
    pos: str
    direction: str  # "derives_from" or "base_of"
    relationship_type: str
    level: str  # "lemma" or "definition"
    bidirectional: bool = False
    gloss: str | None = None  # For definition-level relationships


def get_all_related_lemmas(conn: Connection, lemma_id: int) -> list[RelatedLemma]:
    """Find all lemmas related to this one.

    IMPORTANT: Must check TWO places:
    1. lemma_relationships - where ALL definitions of source relate to target
    2. definitions.derived_from_lemma_id - where INDIVIDUAL definitions relate

    Args:
        conn: Database connection
        lemma_id: The lemma ID to find relationships for

    Returns:
        Combined, deduplicated results with relationship info.
    """
    results: list[dict[str, Any]] = []

    # 1a. This lemma derives FROM others (lemma-level)
    # e.g., bici → bicicletta
    stmt = (
        select(
            lemma_relationships.c.relationship_type,
            lemma_relationships.c.bidirectional,
            lemmas.c.id.label("related_id"),
            lemmas.c.written.label("related_written"),
            lemmas.c.pos,
        )
        .join(lemmas, lemma_relationships.c.target_lemma_id == lemmas.c.id)
        .where(lemma_relationships.c.source_lemma_id == lemma_id)
    )
    results.extend(
        {
            "direction": "derives_from",
            "lemma_id": row.related_id,
            "written": row.related_written,
            "pos": row.pos,
            "relationship_type": row.relationship_type,
            "level": "lemma",
            "bidirectional": row.bidirectional,
        }
        for row in conn.execute(stmt)
    )

    # 1b. Other lemmas derive FROM this one (lemma-level)
    # e.g., bicicletta ← bici
    stmt = (
        select(
            lemma_relationships.c.relationship_type,
            lemma_relationships.c.bidirectional,
            lemmas.c.id.label("related_id"),
            lemmas.c.written.label("related_written"),
            lemmas.c.pos,
        )
        .join(lemmas, lemma_relationships.c.source_lemma_id == lemmas.c.id)
        .where(lemma_relationships.c.target_lemma_id == lemma_id)
    )
    results.extend(
        {
            "direction": "base_of",
            "lemma_id": row.related_id,
            "written": row.related_written,
            "pos": row.pos,
            "relationship_type": row.relationship_type,
            "level": "lemma",
            "bidirectional": row.bidirectional,
        }
        for row in conn.execute(stmt)
    )

    # 2a. Definitions of this lemma derive FROM others (definition-level)
    # e.g., cagnolino "little dog" → cane
    stmt = (
        select(
            definitions.c.gloss,
            definitions.c.derivation_type,
            lemmas.c.id.label("related_id"),
            lemmas.c.written.label("related_written"),
            lemmas.c.pos,
        )
        .join(lemmas, definitions.c.derived_from_lemma_id == lemmas.c.id)
        .where(definitions.c.lemma_id == lemma_id)
        .where(definitions.c.derived_from_lemma_id.isnot(None))
    )
    results.extend(
        {
            "direction": "derives_from",
            "lemma_id": row.related_id,
            "written": row.related_written,
            "pos": row.pos,
            "relationship_type": row.derivation_type,
            "level": "definition",
            "gloss": row.gloss,
        }
        for row in conn.execute(stmt)
    )

    # 2b. Definitions of OTHER lemmas derive FROM this one (definition-level)
    # e.g., cane ← cagnolino "little dog"
    stmt = (
        select(
            definitions.c.gloss,
            definitions.c.derivation_type,
            lemmas.c.id.label("related_id"),
            lemmas.c.written.label("related_written"),
            lemmas.c.pos,
        )
        .join(lemmas, definitions.c.lemma_id == lemmas.c.id)
        .where(definitions.c.derived_from_lemma_id == lemma_id)
    )
    results.extend(
        {
            "direction": "base_of",
            "lemma_id": row.related_id,
            "written": row.related_written,
            "pos": row.pos,
            "relationship_type": row.derivation_type,
            "level": "definition",
            "gloss": row.gloss,
        }
        for row in conn.execute(stmt)
    )

    # Deduplicate by (lemma_id, direction), keeping the most informative entry
    seen: dict[tuple[int, str], dict[str, Any]] = {}
    for r in results:
        key = (r["lemma_id"], r["direction"])
        if key not in seen:
            seen[key] = r
        elif r["level"] == "definition" and seen[key]["level"] == "lemma":
            # Prefer definition-level as it has more detail
            seen[key] = r

    # Convert to dataclass
    return [
        RelatedLemma(
            lemma_id=r["lemma_id"],
            written=r["written"],
            pos=r["pos"],
            direction=r["direction"],
            relationship_type=r["relationship_type"],
            level=r["level"],
            bidirectional=r.get("bidirectional", False),
            gloss=r.get("gloss"),
        )
        for r in seen.values()
    ]


def get_derived_words(conn: Connection, lemma_id: int) -> list[RelatedLemma]:
    """Find all words that derive FROM this lemma.

    For displaying on Anki cards: "cane → cagnolino (diminutive)"

    Args:
        conn: Database connection
        lemma_id: The base lemma ID

    Returns:
        List of derived word info
    """
    return [r for r in get_all_related_lemmas(conn, lemma_id) if r.direction == "base_of"]


def get_base_words(conn: Connection, lemma_id: int) -> list[RelatedLemma]:
    """Find all words this lemma derives FROM.

    For displaying on Anki cards: "cagnolino ← cane (diminutive)"

    Args:
        conn: Database connection
        lemma_id: The derived lemma ID

    Returns:
        List of base word info
    """
    return [r for r in get_all_related_lemmas(conn, lemma_id) if r.direction == "derives_from"]
