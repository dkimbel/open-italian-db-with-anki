"""Import frequency data from PAISA corpus.

PAISA (Paisà - Piattaforma per l'Apprendimento dell'Italiano Su corpora Annotati)
is a large Italian web corpus (~250M words from .it domains, 2010).

License: CC-BY-NC-SA 4.0 (NonCommercial)
Source: https://clarin.eurac.edu/repository/xmlui/handle/20.500.12124/3

PAISA provides pre-lemmatized frequency data, making it ideal for verb frequencies
where surface form collisions (e.g., "parte" = verb/noun) are problematic.
"""

import math
from collections.abc import Callable
from pathlib import Path

from sqlalchemy import Connection, select

from italian_db.db.schema import frequencies, lemmas
from italian_db.enums import POS
from italian_db.normalize import derive_written_from_stressed

# PAISA corpus is approximately 250 million words
PAISA_CORPUS_SIZE = 250_000_000

CORPUS_NAME = "paisa"
CORPUS_VERSION = "1.0"

# Punctuation tokens to skip
PUNCTUATION_TOKENS = frozenset({",", ".", '"', "(", ")", ":", ";", "?", "!", "-", "'", "/"})


def _compute_zipf(freq: int, corpus_size: float = PAISA_CORPUS_SIZE) -> float:
    """Compute Zipf score from raw frequency.

    Zipf = log10(freq * 10^9 / corpus_size)

    For PAISA, corpus size is ~250 million words.
    """
    if freq <= 0:
        return 0.0
    fpmw = freq * 1e6 / corpus_size  # frequency per million words
    return math.log10(fpmw) + 3  # Zipf = log10(fpmw) + 3


def _parse_paisa_csv(csv_path: Path) -> dict[str, tuple[int, float]]:
    """Parse PAISA lemma frequency file.

    Format: 2 comment header lines starting with #, then lemma,frequency pairs.
    Note: We don't use csv.reader because the file contains literal quote
    characters as lemmas (e.g., `",5186375`) which break CSV parsing.

    Skips punctuation tokens.

    Returns:
        Dict mapping lemma -> (raw_freq, zipf_score)
    """
    result: dict[str, tuple[int, float]] = {}

    with csv_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            # Skip comment lines
            if line.startswith("#"):
                continue

            # Split on last comma (lemma may contain commas in theory)
            # Actually, split on first comma since lemmas shouldn't have commas
            parts = line.split(",", 1)
            if len(parts) != 2:
                continue

            lemma = parts[0]
            if not lemma or lemma in PUNCTUATION_TOKENS:
                continue

            try:
                freq = int(parts[1])
            except ValueError:
                continue

            zipf = _compute_zipf(freq)
            result[lemma] = (freq, zipf)

    return result


def import_paisa(
    conn: Connection,
    csv_path: Path,
    *,
    pos_filter: POS = POS.VERB,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import PAISA frequency data into the database.

    PAISA is already lemmatized, so we match directly against database lemmas
    by their written form. This makes it ideal for verbs where surface form
    collisions are problematic.

    Args:
        conn: SQLAlchemy connection
        csv_path: Path to PAISA frequency CSV file
        pos_filter: Part of speech to import (typically VERB)
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts:
        - matched: Number of lemmas matched in database
        - not_found: Number of PAISA lemmas not in database
        - matched_freq: Sum of frequencies for matched lemmas
        - total_corpus_freq: Sum of all frequencies in corpus
    """
    stats: dict[str, int] = {
        "matched": 0,
        "not_found": 0,
        "matched_freq": 0,
        "total_corpus_freq": 0,
    }

    # Parse PAISA data
    freq_data = _parse_paisa_csv(csv_path)

    # Calculate total corpus frequency
    total_corpus_freq = sum(freq for freq, _ in freq_data.values())
    stats["total_corpus_freq"] = total_corpus_freq

    # Get lemmas from database for the specified POS
    # Build a map from written form to lemma ID
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.written, lemmas.c.stressed).where(lemmas.c.pos == pos_filter)
    )
    all_lemmas = result.fetchall()
    total_lemmas = len(all_lemmas)

    # Map written form to lemma ID (there may be homonyms, take first match)
    # Use stored written if available, otherwise derive from stressed
    written_to_id: dict[str, int] = {}
    for row in all_lemmas:
        written = row.written
        if not written:
            # Derive written form from stressed (for lemmas that haven't been enriched yet)
            written = derive_written_from_stressed(row.stressed) or row.stressed
        if written and written not in written_to_id:
            written_to_id[written] = row.id

    insert_batch: list[dict[str, str | int | float]] = []
    matched_freq = 0

    for idx, (written, lemma_id) in enumerate(written_to_id.items(), 1):
        if progress_callback and idx % 5000 == 0:
            progress_callback(idx, len(written_to_id))

        if written in freq_data:
            lemma_freq, zipf = freq_data[written]
            insert_batch.append(
                {
                    "lemma_id": lemma_id,
                    "corpus": CORPUS_NAME,
                    "freq_raw": lemma_freq,
                    "freq_zipf": zipf,
                    "corpus_version": CORPUS_VERSION,
                }
            )
            stats["matched"] += 1
            matched_freq += lemma_freq
        else:
            stats["not_found"] += 1

    stats["matched_freq"] = matched_freq

    # Insert all frequency data
    if insert_batch:
        conn.execute(
            frequencies.insert().prefix_with("OR REPLACE"),
            insert_batch,
        )

    # Final progress callback
    if progress_callback:
        progress_callback(total_lemmas, total_lemmas)

    return stats
