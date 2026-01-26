"""Import Tatoeba sentences with FTS5 search index.

Tatoeba File Structure
----------------------
Downloaded from https://downloads.tatoeba.org/exports/per_language/ita/

Files used:
- ita_sentences.tsv: sentence_id <TAB> lang <TAB> text (no header)
- eng_sentences.tsv: sentence_id <TAB> lang <TAB> text (no header)
- ita_eng_links.tsv: ita_sentence_id <TAB> eng_sentence_id (no header)
- tags.csv: sentence_id <TAB> tag_name (no header)
- sentences_in_lists.csv: list_id <TAB> sentence_id (no header)

Quality Filtering:
- CK whitelist (List 907): High-quality English sentences curated for language learning
- Only import Italian sentences that have translations to CK-whitelisted English sentences
- Import tags for tense matching (presente, imperfetto, etc.) and proverb preference
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, text

from italian_db.db.schema import sentence_tags, sentences, translations

# Default CK whitelist list ID
# List 907 is Charles Kelly's curated list of high-quality English sentences
CK_LIST_ID = 907


def _clear_existing_data(conn: Connection) -> int:
    """Clear all existing Tatoeba data.

    Deletes in FK-safe order: sentences_fts → sentence_tags → translations → sentences.
    Returns the number of sentences cleared.
    """
    # Count existing Tatoeba sentences
    result = conn.execute(text("SELECT COUNT(*) FROM sentences WHERE source = 'tatoeba'"))
    existing_count = result.scalar() or 0

    if existing_count == 0:
        return 0

    # Delete FTS entries for Tatoeba sentences
    conn.execute(
        text("""
            DELETE FROM sentences_fts
            WHERE id IN (SELECT id FROM sentences WHERE source = 'tatoeba')
        """)
    )

    # Delete sentence tags for Tatoeba sentences
    conn.execute(
        text("""
            DELETE FROM sentence_tags
            WHERE sentence_id IN (SELECT id FROM sentences WHERE source = 'tatoeba')
        """)
    )

    # Delete translations involving Tatoeba sentences
    conn.execute(
        text("""
            DELETE FROM translations
            WHERE ita_sentence_id IN (SELECT id FROM sentences WHERE source = 'tatoeba')
               OR eng_sentence_id IN (SELECT id FROM sentences WHERE source = 'tatoeba')
        """)
    )

    # Delete Tatoeba sentences
    conn.execute(text("DELETE FROM sentences WHERE source = 'tatoeba'"))

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


def _stream_links(
    path: Path, italian_ids: set[int], *, english_whitelist: set[int] | None = None
) -> tuple[set[int], list[tuple[int, int]]]:
    """Stream links TSV and filter to Italian→English pairs.

    Args:
        path: Path to links TSV file
        italian_ids: Set of Italian sentence IDs
        english_whitelist: Optional set of English sentence IDs to filter to.
            If provided, only links where the English ID is in this set are included.

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
                        # If whitelist provided, only include whitelisted English IDs
                        if english_whitelist is not None and id2 not in english_whitelist:
                            continue
                        english_ids.add(id2)
                        pairs.append((id1, id2))
                except ValueError:
                    continue

    return english_ids, pairs


def _load_ck_whitelist(path: Path, list_id: int = CK_LIST_ID) -> set[int]:
    """Load English sentence IDs from CK whitelist.

    Args:
        path: Path to sentences_in_lists.csv file
        list_id: The list ID to filter by (default: 907 for CK whitelist)

    Returns:
        Set of sentence IDs in the specified list
    """
    sentence_ids: set[int] = set()

    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                try:
                    file_list_id = int(parts[0])
                    sentence_id = int(parts[1])
                    if file_list_id == list_id:
                        sentence_ids.add(sentence_id)
                except ValueError:
                    continue

    return sentence_ids


def _parse_tags(path: Path, sentence_ids: set[int]) -> dict[int, list[str]]:
    """Parse tags for given sentence IDs.

    Args:
        path: Path to tags.csv file
        sentence_ids: Set of sentence IDs to load tags for

    Returns:
        Dict mapping sentence_id -> list of tags
    """
    tags_by_id: dict[int, list[str]] = {}

    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                try:
                    sentence_id = int(parts[0])
                    tag = parts[1].strip()
                    if sentence_id in sentence_ids and tag:
                        if sentence_id not in tags_by_id:
                            tags_by_id[sentence_id] = []
                        tags_by_id[sentence_id].append(tag)
                except ValueError:
                    continue

    return tags_by_id


def _insert_tags(
    conn: Connection,
    tags_by_native_id: dict[int, list[str]],
    native_to_surrogate: dict[int, int],
    batch_size: int = 1000,
) -> int:
    """Batch insert tags into sentence_tags table.

    Args:
        conn: Database connection
        tags_by_native_id: Dict mapping native sentence_id -> list of tags
        native_to_surrogate: Dict mapping native sentence_id -> surrogate id
        batch_size: Number of rows to insert per batch

    Returns:
        Number of tag rows inserted
    """
    tag_rows: list[dict[str, Any]] = []
    total_inserted = 0

    for native_id, tags in tags_by_native_id.items():
        surrogate_id = native_to_surrogate.get(native_id)
        if surrogate_id is None:
            continue
        for tag in tags:
            tag_rows.append({"sentence_id": surrogate_id, "tag": tag})
            if len(tag_rows) >= batch_size:
                conn.execute(sentence_tags.insert(), tag_rows)
                total_inserted += len(tag_rows)
                tag_rows = []

    if tag_rows:
        conn.execute(sentence_tags.insert(), tag_rows)
        total_inserted += len(tag_rows)

    return total_inserted


def import_tatoeba(
    conn: Connection,
    ita_sentences_path: Path,
    eng_sentences_path: Path,
    links_path: Path,
    *,
    tags_path: Path | None = None,
    sentences_in_lists_path: Path | None = None,
    ck_list_id: int = CK_LIST_ID,
    batch_size: int = 1000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import Tatoeba sentences and build FTS5 search index.

    This function is idempotent: it clears existing Tatoeba data before importing.

    When sentences_in_lists_path is provided, filters to only import Italian sentences
    that have translations to English sentences in the CK whitelist (List 907).
    This reduces the corpus from ~952K to ~377K high-quality Italian sentences.

    When tags_path is provided, imports sentence tags for:
    - Tense matching (presente, imperfetto, passato remoto, etc.)
    - Proverb preference in example sentence ranking
    - Quality filtering (exclude @change, @needs native check, etc.)

    Args:
        conn: SQLAlchemy connection
        ita_sentences_path: Path to Italian sentences TSV
        eng_sentences_path: Path to English sentences TSV
        links_path: Path to Italian-English links TSV
        tags_path: Optional path to tags.csv file
        sentences_in_lists_path: Optional path to sentences_in_lists.csv file
        ck_list_id: List ID for CK whitelist filtering (default: 907)
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
        "tags": 0,
        "ck_whitelist_size": 0,
    }

    # Step 1: Load CK whitelist if available
    ck_whitelist: set[int] | None = None
    if sentences_in_lists_path is not None and sentences_in_lists_path.exists():
        ck_whitelist = _load_ck_whitelist(sentences_in_lists_path, ck_list_id)
        stats["ck_whitelist_size"] = len(ck_whitelist)

    # Step 2: Parse Italian sentences
    ita_sentences = _parse_sentences_tsv(ita_sentences_path)
    italian_ids = set(ita_sentences.keys())

    # Step 3: Stream links and find needed English IDs
    # If CK whitelist available, only include links to whitelisted English sentences
    needed_eng_ids, translation_pairs = _stream_links(
        links_path, italian_ids, english_whitelist=ck_whitelist
    )

    # Step 4: Parse English sentences (only those we need)
    all_eng_sentences = _parse_sentences_tsv(eng_sentences_path)
    eng_sentences = {
        sid: sent_text for sid, sent_text in all_eng_sentences.items() if sid in needed_eng_ids
    }

    # Filter translation pairs to only include English sentences we have
    eng_ids_we_have = set(eng_sentences.keys())
    translation_pairs = [(ita, eng) for ita, eng in translation_pairs if eng in eng_ids_we_have]

    # Step 5: Determine which Italian sentences to actually import
    # If CK whitelist filtering is active, only import Italian sentences that have
    # at least one translation to a whitelisted English sentence
    if ck_whitelist is not None:
        ita_ids_with_ck_translation = {ita_id for ita_id, _ in translation_pairs}
        ita_sentences = {
            sid: sent_text
            for sid, sent_text in ita_sentences.items()
            if sid in ita_ids_with_ck_translation
        }

    # Calculate total items for progress reporting
    total_items = len(ita_sentences) + len(eng_sentences) + len(translation_pairs)
    processed_items = 0

    # Step 6: Insert Italian sentences (with source='tatoeba')
    ita_batch: list[dict[str, Any]] = []
    for sentence_id, sent_text in ita_sentences.items():
        ita_batch.append(
            {"sentence_id": sentence_id, "lang": "ita", "text": sent_text, "source": "tatoeba"}
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

    # Step 7: Insert English sentences (with source='tatoeba')
    eng_batch: list[dict[str, Any]] = []
    for sentence_id, sent_text in eng_sentences.items():
        eng_batch.append(
            {"sentence_id": sentence_id, "lang": "eng", "text": sent_text, "source": "tatoeba"}
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

    # Step 8: Build mapping from native sentence_id to surrogate id
    # This is needed because translations and tags reference the surrogate id
    result = conn.execute(text("SELECT id, sentence_id FROM sentences WHERE source = 'tatoeba'"))
    sentence_id_to_surrogate: dict[int, int] = {row[1]: row[0] for row in result}

    # Step 9: Insert translation pairs using surrogate IDs
    trans_batch: list[dict[str, int]] = []
    for ita_native_id, eng_native_id in translation_pairs:
        ita_surrogate = sentence_id_to_surrogate.get(ita_native_id)
        eng_surrogate = sentence_id_to_surrogate.get(eng_native_id)
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

    # Step 10: Import tags if tags file provided
    if tags_path is not None and tags_path.exists():
        # Get native IDs for Italian sentences we imported
        imported_ita_ids = set(ita_sentences.keys())
        tags_by_id = _parse_tags(tags_path, imported_ita_ids)
        stats["tags"] = _insert_tags(conn, tags_by_id, sentence_id_to_surrogate, batch_size)

    # Step 11: Populate FTS5 index for Italian sentences (using surrogate id)
    conn.execute(
        text("""
            INSERT INTO sentences_fts(id, text)
            SELECT id, text FROM sentences WHERE lang='ita' AND source='tatoeba'
        """)
    )

    return stats
