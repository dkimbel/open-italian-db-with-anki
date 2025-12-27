"""Import frequency data from ItWaC corpus."""

import csv
import math
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

from sqlalchemy import Connection, select

from italian_anki.db.schema import frequencies, lemmas
from italian_anki.normalize import normalize

# Default CSV filenames by POS (relative to data/itwac/)
ITWAC_CSV_FILES = {
    "verb": "itwac_verbs_lemmas_notail_2_1_0.csv",
    "noun": "itwac_nouns_lemmas_notail_2_0_0.csv",
    "adjective": "itwac_adj_lemmas_notail_2_1_0.csv",
}

# ItWaC versions by POS (extracted from filenames)
ITWAC_VERSIONS = {
    "verb": "2.1.0",
    "noun": "2.0.0",
    "adjective": "2.1.0",
}

CORPUS_NAME = "itwac"


def _compute_zipf(freq: int, corpus_size: float = 1.9e9) -> float:
    """Compute Zipf score from raw frequency.

    Zipf = log10(freq * 10^9 / corpus_size)

    ItWaC is ~1.9 billion words.
    """
    if freq <= 0:
        return 0.0
    fpmw = freq * 1e6 / corpus_size  # frequency per million words
    return math.log10(fpmw) + 3  # Zipf = log10(fpmw) + 3


def _parse_itwac_csv(csv_path: Path) -> dict[str, tuple[int, float]]:
    """Parse ItWaC CSV and aggregate frequencies by lemma.

    Works for verbs, nouns, and adjectives (same CSV format).
    Returns dict mapping normalized_lemma -> (total_freq, zipf_score)
    """
    lemma_freqs: dict[str, int] = defaultdict(int)

    with csv_path.open(encoding="iso-8859-1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lemma = row.get("lemma", "")
            if not lemma:
                continue

            try:
                freq = int(row.get("Freq", 0))
            except ValueError:
                continue

            # Normalize the lemma for matching
            normalized = normalize(lemma)

            # Aggregate frequency by lemma (sum all form frequencies)
            lemma_freqs[normalized] += freq

    # Compute Zipf scores for aggregated frequencies
    result: dict[str, tuple[int, float]] = {}
    for normalized, total_freq in lemma_freqs.items():
        zipf = _compute_zipf(total_freq)
        result[normalized] = (total_freq, zipf)

    return result


def import_itwac(
    conn: Connection,
    csv_path: Path,
    *,
    pos_filter: str = "verb",
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import ItWaC frequency data into the database.

    Args:
        conn: SQLAlchemy connection
        csv_path: Path to ItWaC CSV file (verb, noun, or adjective)
        pos_filter: Part of speech to import (default: "verb")
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    stats = {"matched": 0, "not_found": 0}

    # Parse and aggregate ItWaC data
    freq_data = _parse_itwac_csv(csv_path)

    # Get version for this POS
    corpus_version = ITWAC_VERSIONS.get(pos_filter, "unknown")

    # Get lemmas from database for the specified POS
    result = conn.execute(
        select(lemmas.c.id, lemmas.c.normalized).where(lemmas.c.pos == pos_filter)
    )
    all_lemmas = result.fetchall()
    total_lemmas = len(all_lemmas)

    insert_batch: list[dict[str, str | int | float]] = []

    for idx, row in enumerate(all_lemmas, 1):
        if progress_callback and idx % 5000 == 0:
            progress_callback(idx, total_lemmas)
        lemma_id = row.id
        normalized = row.normalized  # Already normalized in DB

        if normalized in freq_data:
            total_freq, zipf = freq_data[normalized]
            insert_batch.append(
                {
                    "lemma_id": lemma_id,
                    "corpus": CORPUS_NAME,
                    "freq_raw": total_freq,
                    "freq_zipf": zipf,
                    "corpus_version": corpus_version,
                }
            )
            stats["matched"] += 1
        else:
            stats["not_found"] += 1

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
