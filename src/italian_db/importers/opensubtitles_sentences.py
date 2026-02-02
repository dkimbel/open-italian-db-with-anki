"""Import OpenSubtitles parallel sentences.

OpenSubtitles v2024 parallel sentences from OPUS, preprocessed by download.py
into Tatoeba-compatible TSV format.

License: Attribution in the form of citing the original source.
Source: https://opus.nlpl.eu/OpenSubtitles/v2024/en-it

The TSV format matches Tatoeba's format for maximum code reuse:
- it_sentences.tsv: line_number<TAB>ita<TAB>text
- en_sentences.tsv: line_number<TAB>eng<TAB>text
- links.tsv: ita_line_number<TAB>eng_line_number
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, text

from italian_db.db.schema import sentences, translations


def _clear_existing_data(conn: Connection) -> int:
    """Clear all existing OpenSubtitles data.

    Deletes in FK-safe order: sentences_fts → sentence_tokens → translations → sentences.
    Returns the number of sentences cleared.
    """
    result = conn.execute(text("SELECT COUNT(*) FROM sentences WHERE source = 'opensubtitles'"))
    existing_count = result.scalar() or 0

    if existing_count == 0:
        return 0

    # Delete FTS entries for OpenSubtitles sentences
    conn.execute(
        text("""
            DELETE FROM sentences_fts
            WHERE id IN (SELECT id FROM sentences WHERE source = 'opensubtitles')
        """)
    )

    # Delete sentence tokens for OpenSubtitles sentences
    conn.execute(
        text("""
            DELETE FROM sentence_tokens
            WHERE sentence_id IN (SELECT id FROM sentences WHERE source = 'opensubtitles')
        """)
    )

    # Delete translations involving OpenSubtitles sentences
    conn.execute(
        text("""
            DELETE FROM translations
            WHERE ita_sentence_id IN (SELECT id FROM sentences WHERE source = 'opensubtitles')
               OR eng_sentence_id IN (SELECT id FROM sentences WHERE source = 'opensubtitles')
        """)
    )

    # Delete OpenSubtitles sentences
    conn.execute(text("DELETE FROM sentences WHERE source = 'opensubtitles'"))

    return existing_count


def _parse_sentences_tsv(path: Path) -> dict[int, str]:
    """Parse a sentences TSV file (Tatoeba-compatible format).

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
                    sentence_text = parts[2]
                    result[sentence_id] = sentence_text
                except ValueError:
                    continue
    return result


def _parse_links_tsv(path: Path) -> list[tuple[int, int]]:
    """Parse a links TSV file.

    Format: ita_id<TAB>eng_id (no header)
    Returns list of (ita_id, eng_id) pairs.
    """
    pairs: list[tuple[int, int]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                try:
                    ita_id = int(parts[0])
                    eng_id = int(parts[1])
                    pairs.append((ita_id, eng_id))
                except ValueError:
                    continue
    return pairs


def import_opensubtitles_sentences(
    conn: Connection,
    ita_sentences_path: Path,
    eng_sentences_path: Path,
    links_path: Path,
    *,
    batch_size: int = 5000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import OpenSubtitles sentences and build FTS5 search index.

    This function is idempotent: it clears existing OpenSubtitles data before importing.
    No CK whitelist filtering (unlike Tatoeba) - sentences are already preprocessed
    and sampled during download.

    Args:
        conn: SQLAlchemy connection
        ita_sentences_path: Path to Italian sentences TSV
        eng_sentences_path: Path to English sentences TSV
        links_path: Path to links TSV
        batch_size: Number of rows to insert per batch
        progress_callback: Optional callback for progress reporting (current, total)

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

    # Step 2: Parse English sentences
    eng_sentences = _parse_sentences_tsv(eng_sentences_path)

    # Step 3: Parse links
    translation_pairs = _parse_links_tsv(links_path)

    # Calculate total items for progress reporting
    total_items = len(ita_sentences) + len(eng_sentences) + len(translation_pairs)
    processed_items = 0

    # Step 4: Insert Italian sentences (with source='opensubtitles')
    ita_batch: list[dict[str, Any]] = []
    for sentence_id, sent_text in ita_sentences.items():
        ita_batch.append(
            {
                "sentence_id": sentence_id,
                "lang": "ita",
                "text": sent_text,
                "source": "opensubtitles",
            }
        )
        if len(ita_batch) >= batch_size:
            conn.execute(sentences.insert(), ita_batch)
            stats["ita_sentences"] += len(ita_batch)
            processed_items += len(ita_batch)
            if progress_callback:
                progress_callback(processed_items, total_items)
            ita_batch = []
    if ita_batch:
        conn.execute(sentences.insert(), ita_batch)
        stats["ita_sentences"] += len(ita_batch)
        processed_items += len(ita_batch)

    # Step 5: Insert English sentences (with source='opensubtitles')
    eng_batch: list[dict[str, Any]] = []
    for sentence_id, sent_text in eng_sentences.items():
        eng_batch.append(
            {
                "sentence_id": sentence_id,
                "lang": "eng",
                "text": sent_text,
                "source": "opensubtitles",
            }
        )
        if len(eng_batch) >= batch_size:
            conn.execute(sentences.insert(), eng_batch)
            stats["eng_sentences"] += len(eng_batch)
            processed_items += len(eng_batch)
            if progress_callback:
                progress_callback(processed_items, total_items)
            eng_batch = []
    if eng_batch:
        conn.execute(sentences.insert(), eng_batch)
        stats["eng_sentences"] += len(eng_batch)
        processed_items += len(eng_batch)

    # Step 6: Build per-language mappings from native sentence_id to surrogate id
    # Italian and English can share native IDs (line-aligned Moses format),
    # so we need separate lookups per language.
    ita_id_result = conn.execute(
        text(
            "SELECT id, sentence_id FROM sentences WHERE source = 'opensubtitles' AND lang = 'ita'"
        )
    )
    ita_id_to_surrogate: dict[int, int] = {row[1]: row[0] for row in ita_id_result}

    eng_id_result = conn.execute(
        text(
            "SELECT id, sentence_id FROM sentences WHERE source = 'opensubtitles' AND lang = 'eng'"
        )
    )
    eng_id_to_surrogate: dict[int, int] = {row[1]: row[0] for row in eng_id_result}

    # Step 7: Insert translation pairs using surrogate IDs
    trans_batch: list[dict[str, int]] = []
    for ita_native_id, eng_native_id in translation_pairs:
        ita_surrogate = ita_id_to_surrogate.get(ita_native_id)
        eng_surrogate = eng_id_to_surrogate.get(eng_native_id)
        if ita_surrogate is not None and eng_surrogate is not None:
            trans_batch.append({"ita_sentence_id": ita_surrogate, "eng_sentence_id": eng_surrogate})
        if len(trans_batch) >= batch_size:
            conn.execute(translations.insert().prefix_with("OR IGNORE"), trans_batch)
            stats["translations"] += len(trans_batch)
            processed_items += len(trans_batch)
            if progress_callback:
                progress_callback(processed_items, total_items)
            trans_batch = []
    if trans_batch:
        conn.execute(translations.insert().prefix_with("OR IGNORE"), trans_batch)
        stats["translations"] += len(trans_batch)
        processed_items += len(trans_batch)

    # Final progress callback
    if progress_callback:
        progress_callback(total_items, total_items)

    # Step 8: Populate FTS5 index for Italian sentences
    conn.execute(
        text("""
            INSERT INTO sentences_fts(id, text)
            SELECT id, text FROM sentences WHERE lang='ita' AND source='opensubtitles'
        """)
    )

    return stats
