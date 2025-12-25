"""Import Tatoeba sentences with FTS5 search index."""

from pathlib import Path
from typing import Any

from sqlalchemy import Connection, select, text

from italian_anki.db.schema import sentences, translations


def _clear_existing_data(conn: Connection) -> int:
    """Clear all existing Tatoeba data.

    Deletes in FK-safe order: sentences_fts → translations → sentences.
    Returns the number of sentences cleared.
    """
    # Count existing sentences
    result = conn.execute(select(sentences.c.sentence_id))
    existing_count = len(result.fetchall())

    if existing_count == 0:
        return 0

    # Delete in FK-safe order
    conn.execute(text("DELETE FROM sentences_fts"))
    conn.execute(translations.delete())
    conn.execute(sentences.delete())

    return existing_count


def _parse_sentences_tsv(path: Path) -> dict[int, str]:
    """Parse a Tatoeba sentences TSV file.

    Format: sentence_id<TAB>lang<TAB>text (no header)
    Returns dict mapping sentence_id -> text.
    """
    result: dict[int, str] = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 3:
                try:
                    sentence_id = int(parts[0])
                    text = parts[2]
                    result[sentence_id] = text
                except ValueError:
                    continue
    return result


def _stream_links(path: Path, italian_ids: set[int]) -> tuple[set[int], list[tuple[int, int]]]:
    """Stream links TSV and filter to Italian→English pairs.

    Returns:
        - Set of English sentence IDs that are translations of Italian sentences
        - List of (ita_id, eng_id) translation pairs
    """
    english_ids: set[int] = set()
    pairs: list[tuple[int, int]] = []

    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                try:
                    id1 = int(parts[0])
                    id2 = int(parts[1])

                    # Check if this is Italian → something
                    if id1 in italian_ids:
                        english_ids.add(id2)
                        pairs.append((id1, id2))
                except ValueError:
                    continue

    return english_ids, pairs


def import_tatoeba(
    conn: Connection,
    ita_sentences_path: Path,
    eng_sentences_path: Path,
    links_path: Path,
    *,
    batch_size: int = 1000,
) -> dict[str, int]:
    """Import Tatoeba sentences and build FTS5 search index.

    This function is idempotent: it clears existing Tatoeba data before importing.

    Args:
        conn: SQLAlchemy connection
        ita_sentences_path: Path to Italian sentences TSV
        eng_sentences_path: Path to English sentences TSV
        links_path: Path to Italian-English links TSV
        batch_size: Number of rows to insert per batch

    Returns:
        Statistics dict with counts
    """
    # Clear existing data first (idempotency)
    cleared = _clear_existing_data(conn)

    stats: dict[str, int] = {
        "cleared": cleared,
        "ita_sentences": 0,
        "eng_sentences": 0,
        "translations": 0,
    }

    # Step 1: Parse Italian sentences
    ita_sentences = _parse_sentences_tsv(ita_sentences_path)
    italian_ids = set(ita_sentences.keys())

    # Step 2: Stream links and find needed English IDs
    needed_eng_ids, translation_pairs = _stream_links(links_path, italian_ids)

    # Step 3: Parse English sentences (only those we need)
    all_eng_sentences = _parse_sentences_tsv(eng_sentences_path)
    eng_sentences = {
        sid: sent_text for sid, sent_text in all_eng_sentences.items() if sid in needed_eng_ids
    }

    # Filter translation pairs to only include English sentences we have
    eng_ids_we_have = set(eng_sentences.keys())
    translation_pairs = [(ita, eng) for ita, eng in translation_pairs if eng in eng_ids_we_have]

    # Step 4: Insert Italian sentences
    ita_batch: list[dict[str, Any]] = []
    for sentence_id, sent_text in ita_sentences.items():
        ita_batch.append({"sentence_id": sentence_id, "lang": "ita", "text": sent_text})
        if len(ita_batch) >= batch_size:
            conn.execute(sentences.insert(), ita_batch)
            stats["ita_sentences"] += len(ita_batch)
            ita_batch = []
    if ita_batch:
        conn.execute(sentences.insert(), ita_batch)
        stats["ita_sentences"] += len(ita_batch)

    # Step 5: Insert English sentences
    eng_batch: list[dict[str, Any]] = []
    for sentence_id, sent_text in eng_sentences.items():
        eng_batch.append({"sentence_id": sentence_id, "lang": "eng", "text": sent_text})
        if len(eng_batch) >= batch_size:
            conn.execute(sentences.insert(), eng_batch)
            stats["eng_sentences"] += len(eng_batch)
            eng_batch = []
    if eng_batch:
        conn.execute(sentences.insert(), eng_batch)
        stats["eng_sentences"] += len(eng_batch)

    # Step 6: Insert translation pairs
    trans_batch: list[dict[str, int]] = []
    for ita_id, eng_id in translation_pairs:
        trans_batch.append({"ita_sentence_id": ita_id, "eng_sentence_id": eng_id})
        if len(trans_batch) >= batch_size:
            conn.execute(translations.insert().prefix_with("OR IGNORE"), trans_batch)
            stats["translations"] += len(trans_batch)
            trans_batch = []
    if trans_batch:
        conn.execute(translations.insert().prefix_with("OR IGNORE"), trans_batch)
        stats["translations"] += len(trans_batch)

    # Step 7: Populate FTS5 index for Italian sentences
    conn.execute(
        text("""
            INSERT INTO sentences_fts(sentence_id, text)
            SELECT sentence_id, text FROM sentences WHERE lang='ita'
        """)
    )

    return stats
