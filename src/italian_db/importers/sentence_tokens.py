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

from sqlalchemy import Connection, select, text

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
        "compound_mood": None,
        "compound_tense": None,
        "feats_extra": feats_extra,
        "head": token.get("head"),
        "deprel": token.get("deprel"),
        # Stanza's 1-indexed token id within each sub-sentence (resets at sentence
        # boundaries in multi-sentence records). Used by _resolve_compound_tenses()
        # to correctly resolve head references, then stripped before DB insertion.
        "_stanza_id": token.get("id"),
    }


def _resolve_compound_tenses(token_rows: list[dict[str, Any]]) -> None:
    """Resolve compound tense features from AUX dependents onto VERB past participles.

    In compound tenses (passato prossimo, trapassato, etc.), Stanza tags:
    - AUX (avere/essere): VerbForm=Fin, Mood=X, Tense=Y, deprel=aux
    - VERB (past participle): VerbForm=Part, Tense=Past, but no Mood

    This function copies the AUX's mood/tense onto the VERB row as compound_mood
    and compound_tense, enabling direct compound tense matching in queries.

    Handles multi-sentence records where Stanza's token id resets at each
    sub-sentence boundary (e.g., "Come stai? Hai fatto un buon viaggio?"
    has two sub-sentences with independent id sequences). Head references
    are resolved within each sub-sentence using _stanza_id.

    Mutates token_rows in place.
    """
    if not token_rows:
        return

    # Segment into sub-sentences by detecting when _stanza_id resets
    sub_sentences: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = [token_rows[0]]
    prev_id: int = token_rows[0].get("_stanza_id") or 0

    for row in token_rows[1:]:
        cur_id: int = row.get("_stanza_id") or 0
        if cur_id <= prev_id:
            # id reset — new sub-sentence
            sub_sentences.append(current)
            current = []
        current.append(row)
        prev_id = cur_id

    sub_sentences.append(current)

    # Resolve within each sub-sentence
    for sub in sub_sentences:
        # Build index from Stanza id -> row within this sub-sentence
        by_stanza_id: dict[int, dict[str, Any]] = {}
        for row in sub:
            stanza_id = row.get("_stanza_id")
            if stanza_id is not None:
                by_stanza_id[stanza_id] = row

        for row in sub:
            if (
                row.get("deprel") == "aux"
                and row.get("upos") == "AUX"
                and row.get("verbform") == "Fin"
                and row.get("mood") is not None
                and row.get("tense") is not None
            ):
                head_row = by_stanza_id.get(row["head"])
                if (
                    head_row is not None
                    and head_row.get("upos") == "VERB"
                    and head_row.get("verbform") == "Part"
                    and head_row.get("tense") == "Past"
                ):
                    head_row["compound_mood"] = row["mood"]
                    head_row["compound_tense"] = row["tense"]


def import_sentence_tokens(
    conn: Connection,
    jsonl_path: Path,
    *,
    source: str,
    batch_size: int = 1000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> SentenceTokensStats:
    """Import sentence tokens from Stanza JSONL into the database.

    This function:
    1. Clears existing sentence_tokens entries for the given source
    2. Builds mapping from native sentence_id to surrogate id
    3. Parses JSONL and inserts token rows in batches

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to JSONL file from stanza_pos_tagging.py
        source: Sentence source to scope the import ('tatoeba' or 'opensubtitles').
            Only clears and imports tokens for sentences from this source.
        batch_size: Number of token rows to insert per batch
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        SentenceTokensStats with counts of processed sentences and tokens
    """
    stats = SentenceTokensStats()

    # Clear existing entries for this source
    conn.execute(
        text("""
            DELETE FROM sentence_tokens
            WHERE sentence_id IN (
                SELECT id FROM sentences WHERE source = :source AND lang = 'ita'
            )
        """),
        {"source": source},
    )

    # Build mapping from native sentence_id to surrogate id
    result = conn.execute(
        select(sentences.c.id, sentences.c.sentence_id).where(
            sentences.c.lang == "ita", sentences.c.source == source
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

        # Extract all tokens for this sentence
        sentence_rows: list[dict[str, Any]] = []
        for token_index, token in enumerate(record["tokens"]):
            row = _extract_token_row(surrogate_id, token_index, token)
            sentence_rows.append(row)

        # Resolve compound tense features (AUX mood/tense → VERB past participle)
        _resolve_compound_tenses(sentence_rows)

        # Strip temporary _stanza_id before DB insertion
        for row in sentence_rows:
            del row["_stanza_id"]

        # Add to batch
        token_batch.extend(sentence_rows)

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
