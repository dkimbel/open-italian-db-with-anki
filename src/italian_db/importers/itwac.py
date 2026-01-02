"""Import frequency data from ItWaC corpus."""

import csv
import math
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

from sqlalchemy import Connection, select

from italian_db.db.schema import frequencies, lemmas
from italian_db.enums import POS
from italian_db.normalize import derive_written_from_stressed

# Default CSV filenames by POS (relative to data/itwac/)
ITWAC_CSV_FILES: dict[POS, str] = {
    POS.VERB: "itwac_verbs_lemmas_notail_2_1_0.csv",
    POS.NOUN: "itwac_nouns_lemmas_notail_2_0_0.csv",
    POS.ADJECTIVE: "itwac_adj_lemmas_notail_2_1_0.csv",
}

# ItWaC versions by POS (extracted from filenames)
ITWAC_VERSIONS: dict[POS, str] = {
    POS.VERB: "2.1.0",
    POS.NOUN: "2.0.0",
    POS.ADJECTIVE: "2.1.0",
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


def _parse_itwac_csv(csv_path: Path) -> tuple[dict[str, tuple[int, float]], int]:
    """Parse ItWaC CSV and aggregate frequencies by lemma.

    Works for verbs, nouns, and adjectives (same CSV format).

    Returns:
        Tuple of:
        - Dict mapping written_lemma -> (total_freq, zipf_score)
        - Count of multi-accent entries (data quality issues in ItWaC)
    """
    lemma_freqs: dict[str, int] = defaultdict(int)
    multi_accent_count = 0

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

            # Derive written form for matching (preserves meaningful final accents)
            # Use warn=False since ItWaC has known data quality issues
            written = derive_written_from_stressed(lemma, warn=False)
            if written is None:
                # Derivation failed (likely multi-accent garbage from web corpus)
                multi_accent_count += 1
                written = lemma  # Use original as fallback

            # Aggregate frequency by lemma (sum all form frequencies)
            lemma_freqs[written] += freq

    # Compute Zipf scores for aggregated frequencies
    result: dict[str, tuple[int, float]] = {}
    for written, total_freq in lemma_freqs.items():
        zipf = _compute_zipf(total_freq)
        result[written] = (total_freq, zipf)

    return result, multi_accent_count


def import_itwac(
    conn: Connection,
    csv_path: Path,
    *,
    pos_filter: POS = POS.VERB,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import ItWaC frequency data into the database.

    Args:
        conn: SQLAlchemy connection
        csv_path: Path to ItWaC CSV file (verb, noun, or adjective)
        pos_filter: Part of speech to import
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    stats: dict[str, int] = {"matched": 0, "not_found": 0, "multi_accent": 0}

    # Parse and aggregate ItWaC data
    freq_data, multi_accent_count = _parse_itwac_csv(csv_path)
    stats["multi_accent"] = multi_accent_count

    # Calculate total corpus frequency for percentage calculation
    total_corpus_freq = sum(freq for freq, _ in freq_data.values())
    matched_freq = 0

    # Get version for this POS
    corpus_version = ITWAC_VERSIONS.get(pos_filter, "unknown")

    # Get lemmas from database for the specified POS
    result = conn.execute(select(lemmas.c.id, lemmas.c.stressed).where(lemmas.c.pos == pos_filter))
    all_lemmas = result.fetchall()
    total_lemmas = len(all_lemmas)

    insert_batch: list[dict[str, str | int | float]] = []

    for idx, row in enumerate(all_lemmas, 1):
        if progress_callback and idx % 5000 == 0:
            progress_callback(idx, total_lemmas)
        lemma_id = row.id
        # Derive written form for matching (preserves meaningful final accents)
        written = derive_written_from_stressed(row.stressed) or row.stressed

        if written in freq_data:
            lemma_freq, zipf = freq_data[written]
            insert_batch.append(
                {
                    "lemma_id": lemma_id,
                    "corpus": CORPUS_NAME,
                    "freq_raw": lemma_freq,
                    "freq_zipf": zipf,
                    "corpus_version": corpus_version,
                }
            )
            stats["matched"] += 1
            matched_freq += lemma_freq
        else:
            stats["not_found"] += 1

    # Store frequency-weighted stats
    stats["matched_freq"] = matched_freq
    stats["total_corpus_freq"] = total_corpus_freq

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
