"""Compute frequency data from Stanza-tagged sentence tokens.

Instead of importing pre-computed frequency lists (PAISA/OpenSubtitles),
this module derives frequency data directly from the sentence_tokens table.
Stanza provides accurate lemmatization for all POS, solving the surface form
collision problem for verbs and giving consistent frequency data across all POS.
"""

import math
from collections import defaultdict
from collections.abc import Callable

from sqlalchemy import Connection, text

from italian_db.db.schema import frequencies

CORPUS_NAME = "stanza"
CORPUS_VERSION = "tatoeba+opensubtitles_v2024"

# Map Stanza UPOS to our POS for frequency ranking
# Only rank VERB, NOUN, ADJ (skip DET, PRON, ADP, etc.)
UPOS_TO_POS: dict[str, str] = {
    "VERB": "verb",
    "AUX": "verb",
    "NOUN": "noun",
    "ADJ": "adjective",
}

# UPOS tags to exclude from total token count (not content words)
EXCLUDED_UPOS = frozenset({"PUNCT", "SYM", "X"})


def _compute_zipf(freq_raw: int, total_tokens: int) -> float:
    """Compute Zipf score from raw frequency.

    Zipf = log10(freq_per_million) + 3
    """
    if freq_raw <= 0 or total_tokens <= 0:
        return 0.0
    fpmw = freq_raw * 1e6 / total_tokens
    return math.log10(fpmw) + 3


def compute_frequencies_from_tokens(
    conn: Connection,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Compute lemma frequencies from sentence_tokens and insert into frequencies table.

    Algorithm:
    1. Count tokens by (lemma, upos), excluding PUNCT/SYM/X
    2. Map UPOS to our POS (VERB, NOUN, ADJ only)
    3. For each (lemma, mapped_pos), look up in lemmas table
    4. Aggregate counts when multiple UPOS map to same lemma (e.g., VERB+AUX)
    5. Compute Zipf scores and insert into frequencies table
    6. Returns stats dict

    Args:
        conn: SQLAlchemy connection (must have sentence_tokens populated)
        progress_callback: Optional callback for progress reporting

    Returns:
        Stats dict with: total_tokens, matched, not_found
    """
    stats: dict[str, int] = {
        "total_tokens": 0,
        "matched": 0,
        "not_found": 0,
    }

    # Step 1: Clear existing frequency data
    conn.execute(text("DELETE FROM frequencies"))

    # Step 2: Count tokens by (lemma, upos)
    count_query = text("""
        SELECT lemma, upos, COUNT(*) as cnt
        FROM sentence_tokens
        WHERE upos NOT IN ('PUNCT', 'SYM', 'X')
        GROUP BY lemma, upos
    """)
    token_counts = conn.execute(count_query).fetchall()

    # Step 3: Get total countable tokens
    total_query = text("""
        SELECT COUNT(*) FROM sentence_tokens
        WHERE upos NOT IN ('PUNCT', 'SYM', 'X')
    """)
    total_tokens = conn.execute(total_query).scalar() or 0
    stats["total_tokens"] = total_tokens

    # Step 4: Aggregate by (stanza_lemma, mapped_pos)
    # Key: (stanza_lemma, mapped_pos) -> total count
    aggregated: dict[tuple[str, str], int] = defaultdict(int)
    for row in token_counts:
        stanza_lemma: str = row[0]
        upos: str = row[1]
        count: int = row[2]

        mapped_pos = UPOS_TO_POS.get(upos)
        if mapped_pos is None:
            continue

        aggregated[(stanza_lemma, mapped_pos)] += count

    # Step 5: Build lookup map from (written, pos) -> lemma_id
    # Load all lemmas with their written forms
    lemma_query = text("SELECT id, written, stressed, pos FROM lemmas")
    all_lemmas = conn.execute(lemma_query).fetchall()

    # Primary index: (written, pos) -> lemma_id (first match wins for homonyms)
    written_pos_to_id: dict[tuple[str, str], int] = {}
    for lemma_row in all_lemmas:
        written = lemma_row[1]  # lemmas.written
        stressed = lemma_row[2]  # lemmas.stressed
        pos = lemma_row[3]  # lemmas.pos (POS enum value)

        # Use written if available, else stressed as key
        key_form = written if written else stressed
        if key_form:
            key = (key_form, pos)
            if key not in written_pos_to_id:
                written_pos_to_id[key] = lemma_row[0]

    # Step 6: Match and insert
    total_to_match = len(aggregated)
    insert_batch: list[dict[str, str | int | float]] = []

    for idx, ((stanza_lemma, mapped_pos), total_count) in enumerate(aggregated.items(), 1):
        if progress_callback and idx % 10000 == 0:
            progress_callback(idx, total_to_match)

        lemma_id = written_pos_to_id.get((stanza_lemma, mapped_pos))
        if lemma_id is None:
            stats["not_found"] += 1
            continue

        stats["matched"] += 1
        zipf = _compute_zipf(total_count, total_tokens)

        insert_batch.append(
            {
                "lemma_id": lemma_id,
                "corpus": CORPUS_NAME,
                "freq_raw": total_count,
                "freq_zipf": zipf,
                "corpus_version": CORPUS_VERSION,
            }
        )

    # Batch insert
    if insert_batch:
        # Use OR REPLACE to handle any duplicate lemma_id entries
        # (shouldn't happen but defensive)
        conn.execute(
            frequencies.insert().prefix_with("OR REPLACE"),
            insert_batch,
        )

    if progress_callback:
        progress_callback(total_to_match, total_to_match)

    return stats
