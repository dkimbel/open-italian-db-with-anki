"""Compute frequency rankings from imported frequency data.

This module provides functions to compute per-POS frequency rankings
after frequency data has been imported from any corpus (PAISA, OpenSubtitles, etc.).
"""

from sqlalchemy import Connection, text


def compute_pos_frequency_ranks(
    conn: Connection,
    corpus: str,
) -> dict[str, int]:
    """Compute per-POS frequency rankings and update the frequencies table.

    Uses DENSE_RANK() to assign ranks within each POS, so lemmas with the same
    freq_zipf get the same rank. Rankings are based on freq_zipf in descending
    order (highest = rank 1).

    Args:
        conn: SQLAlchemy connection
        corpus: Corpus name to filter by (e.g., 'paisa', 'opensubtitles')

    Returns:
        Stats dict mapping POS name to count of ranked lemmas:
        {'verb': N, 'noun': M, 'adjective': P}
    """
    # Use a CTE to compute DENSE_RANK() for each POS, then update in one pass
    # DENSE_RANK ensures ties get the same rank and next rank is consecutive
    update_sql = text("""
        WITH ranked AS (
            SELECT
                f.lemma_id,
                f.corpus,
                DENSE_RANK() OVER (
                    PARTITION BY l.pos
                    ORDER BY f.freq_zipf DESC
                ) as rank_in_pos
            FROM frequencies f
            JOIN lemmas l ON f.lemma_id = l.id
            WHERE f.corpus = :corpus
        )
        UPDATE frequencies
        SET freq_rank_in_pos = (
            SELECT rank_in_pos FROM ranked
            WHERE ranked.lemma_id = frequencies.lemma_id
              AND ranked.corpus = frequencies.corpus
        )
        WHERE corpus = :corpus
    """)

    conn.execute(update_sql, {"corpus": corpus})

    # Count ranked lemmas per POS
    count_sql = text("""
        SELECT l.pos, COUNT(*) as cnt
        FROM frequencies f
        JOIN lemmas l ON f.lemma_id = l.id
        WHERE f.corpus = :corpus AND f.freq_rank_in_pos IS NOT NULL
        GROUP BY l.pos
    """)
    result = conn.execute(count_sql, {"corpus": corpus})
    stats = {row[0]: row[1] for row in result}

    return stats
