"""Import sentence token annotations from Stanza POS-tagged JSONL.

This importer reads the JSONL output from scripts/stanza_pos_tagging.py and
populates the sentence_tokens table with token-level annotations.

Token annotations enable:
- Filtering example sentences by grammatical features (noun vs adjective)
- Finding sentences with specific verb tense/mood/person for conjugation examples
- Lemma-based sentence search (find sentences containing a specific lemma)
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, delete, select, text

from italian_db.db.schema import sentence_tokens, sentences

# Features to extract into individual columns
KEY_FEATURES = {"VerbForm", "Mood", "Tense", "Person", "Number", "Gender"}


@dataclass
class SentenceTokensStats:
    """Statistics from sentence tokens import."""

    sentences_processed: int = 0
    tokens_inserted: int = 0
    sentences_not_found: int = 0


def _parse_jsonl(path: Path) -> list[dict[str, Any]]:
    """Parse JSONL file into list of records."""
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _extract_token_row(
    surrogate_id: int,
    token_index: int,
    token: dict[str, Any],
) -> dict[str, Any]:
    """Extract a database row from a Stanza token dict.

    Args:
        surrogate_id: Database sentence ID (surrogate key)
        token_index: 0-indexed position of token in sentence
        token: Token dict from Stanza JSONL

    Returns:
        Dict ready for insertion into sentence_tokens table
    """
    raw_feats: dict[str, str] = token.get("feats") or {}
    feats: dict[str, str] = dict(raw_feats)

    # Extract key features (all are strings or None)
    verbform: str | None = feats.get("VerbForm")
    mood: str | None = feats.get("Mood")
    tense: str | None = feats.get("Tense")
    person_str: str | None = feats.get("Person")
    person: int | None = int(person_str) if person_str else None
    number: str | None = feats.get("Number")
    gender: str | None = feats.get("Gender")

    # Remaining features go to feats_extra
    extra_feats: dict[str, str] = {k: v for k, v in feats.items() if k not in KEY_FEATURES}
    feats_extra: str | None = json.dumps(extra_feats) if extra_feats else None

    return {
        "sentence_id": surrogate_id,
        "token_index": token_index,
        "text": token["text"],
        "lemma": token["lemma"],
        "upos": token["upos"],
        "verbform": verbform,
        "mood": mood,
        "tense": tense,
        "person": person,
        "number": number,
        "gender": gender,
        "feats_extra": feats_extra,
        "head": token.get("head"),
        "deprel": token.get("deprel"),
    }


def import_sentence_tokens(
    conn: Connection,
    jsonl_path: Path,
    *,
    batch_size: int = 1000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> SentenceTokensStats:
    """Import sentence tokens from Stanza JSONL into the database.

    This function:
    1. Clears existing sentence_tokens entries (idempotent re-import)
    2. Builds mapping from native sentence_id to surrogate id
    3. Parses JSONL and inserts token rows in batches

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to JSONL file from stanza_pos_tagging.py
        batch_size: Number of token rows to insert per batch
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        SentenceTokensStats with counts of processed sentences and tokens
    """
    stats = SentenceTokensStats()

    # Clear existing entries for clean re-import
    conn.execute(delete(sentence_tokens))

    # Build mapping from native sentence_id to surrogate id
    # Only for Italian sentences (tokens are for Italian)
    result = conn.execute(
        select(sentences.c.id, sentences.c.sentence_id).where(
            sentences.c.lang == "ita", sentences.c.source == "tatoeba"
        )
    )
    native_to_surrogate: dict[int, int] = {row.sentence_id: row.id for row in result}

    # Parse JSONL
    records = _parse_jsonl(jsonl_path)
    total = len(records)

    # Process and insert tokens in batches
    token_batch: list[dict[str, Any]] = []

    for idx, record in enumerate(records, 1):
        native_id = record["sentence_id"]
        surrogate_id = native_to_surrogate.get(native_id)

        if surrogate_id is None:
            stats.sentences_not_found += 1
            continue

        stats.sentences_processed += 1

        # Extract tokens (0-indexed)
        for token_index, token in enumerate(record["tokens"]):
            row = _extract_token_row(surrogate_id, token_index, token)
            token_batch.append(row)

            if len(token_batch) >= batch_size:
                conn.execute(sentence_tokens.insert(), token_batch)
                stats.tokens_inserted += len(token_batch)
                token_batch = []

        # Progress reporting
        if progress_callback and idx % 1000 == 0:
            progress_callback(idx, total)

    # Insert remaining batch
    if token_batch:
        conn.execute(sentence_tokens.insert(), token_batch)
        stats.tokens_inserted += len(token_batch)

    # Final progress callback
    if progress_callback:
        progress_callback(total, total)

    return stats


def get_token_statistics(conn: Connection) -> dict[str, Any]:
    """Get statistics about sentence tokens.

    Returns:
        Dict with:
        - total_sentences: Sentences with tokens
        - total_tokens: Total token count
        - tokens_per_sentence_avg: Average tokens per sentence
        - upos_distribution: Count by POS tag
    """
    result: dict[str, Any] = {}

    # Total sentences with tokens
    query = text("SELECT COUNT(DISTINCT sentence_id) FROM sentence_tokens")
    result["total_sentences"] = conn.execute(query).scalar() or 0

    # Total tokens
    query = text("SELECT COUNT(*) FROM sentence_tokens")
    result["total_tokens"] = conn.execute(query).scalar() or 0

    # Average tokens per sentence
    if result["total_sentences"] > 0:
        result["tokens_per_sentence_avg"] = round(
            result["total_tokens"] / result["total_sentences"], 1
        )
    else:
        result["tokens_per_sentence_avg"] = 0

    # UPOS distribution
    query = text("""
        SELECT upos, COUNT(*) as count
        FROM sentence_tokens
        GROUP BY upos
        ORDER BY count DESC
    """)
    result["upos_distribution"] = {row[0]: row[1] for row in conn.execute(query)}

    return result
