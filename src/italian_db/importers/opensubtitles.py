"""Import frequency data from OpenSubtitles corpus.

OpenSubtitles frequency data is derived from the OpenSubtitles2018 corpus
of movie/TV subtitles, providing conversational vocabulary frequencies.

License: CC-BY-SA 4.0
Source: https://github.com/hermitdave/FrequencyWords

OpenSubtitles provides surface form frequencies (not lemmatized), so we need
to aggregate frequencies from surface forms to lemmas using the form tables.
This makes it suitable for nouns and adjectives where surface form collision
is less problematic than for verbs.
"""

import math
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

from sqlalchemy import Connection, select

from italian_db.db.schema import adjective_forms, frequencies, noun_forms
from italian_db.enums import POS
from italian_db.normalize import derive_written_from_stressed

# OpenSubtitles corpus is approximately 500 million tokens
OPENSUBTITLES_CORPUS_SIZE = 500_000_000

CORPUS_NAME = "opensubtitles"
CORPUS_VERSION = "2018"


def _compute_zipf(freq: int, corpus_size: float = OPENSUBTITLES_CORPUS_SIZE) -> float:
    """Compute Zipf score from raw frequency.

    Zipf = log10(freq * 10^9 / corpus_size)
    """
    if freq <= 0:
        return 0.0
    fpmw = freq * 1e6 / corpus_size  # frequency per million words
    return math.log10(fpmw) + 3  # Zipf = log10(fpmw) + 3


def _parse_opensubtitles(file_path: Path) -> dict[str, int]:
    """Parse OpenSubtitles frequency file.

    Format: Space-separated 'word frequency' pairs, no header.

    Returns:
        Dict mapping word -> raw_frequency
    """
    result: dict[str, int] = {}

    with file_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Split on last space (in case word contains spaces)
            parts = line.rsplit(" ", 1)
            if len(parts) != 2:
                continue

            word, freq_str = parts
            try:
                freq = int(freq_str)
            except ValueError:
                continue

            result[word] = freq

    return result


def _build_form_to_lemmas_map(conn: Connection, pos: POS) -> dict[str, list[int]]:
    """Build a map from written surface forms to lemma IDs.

    For nouns/adjectives, uses the appropriate forms table to find all lemmas
    that have a given surface form. A single surface form may map to multiple
    lemmas (e.g., "parte" can be both a noun and derived from different lemmas).

    If a form doesn't have a written value, we derive it from the stressed form
    using Italian orthography rules.

    Args:
        conn: Database connection
        pos: Part of speech (NOUN or ADJECTIVE)

    Returns:
        Dict mapping written_form -> list of lemma IDs
    """
    if pos == POS.NOUN:
        forms_table = noun_forms
    elif pos == POS.ADJECTIVE:
        forms_table = adjective_forms
    else:
        raise ValueError(f"Unsupported POS for OpenSubtitles import: {pos}. Use PAISA for verbs.")

    # Query all forms with their lemma IDs (include stressed for derivation)
    stmt = select(forms_table.c.written, forms_table.c.stressed, forms_table.c.lemma_id)
    rows = conn.execute(stmt).fetchall()

    form_to_lemmas: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        written = row.written
        if not written:
            # Derive written form from stressed (for forms that haven't been enriched yet)
            written = derive_written_from_stressed(row.stressed) or row.stressed
        if written:
            lemma_id = row.lemma_id
            if lemma_id not in form_to_lemmas[written]:
                form_to_lemmas[written].append(lemma_id)

    return dict(form_to_lemmas)


def import_opensubtitles(
    conn: Connection,
    file_path: Path,
    *,
    pos_filter: POS = POS.NOUN,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import OpenSubtitles frequency data into the database.

    OpenSubtitles provides surface form frequencies. We aggregate these to
    lemma frequencies by:
    1. Building a map from surface forms to lemma IDs
    2. For each surface form in OpenSubtitles, adding its frequency to all
       matching lemmas

    When a surface form maps to multiple lemmas (collision), we attribute the
    frequency to all of them. This causes slight overcount but:
    - Collisions are tracked in stats for auditing
    - Core vocabulary ranks correctly
    - Affects ~15% of forms, mostly rare words

    Args:
        conn: SQLAlchemy connection
        file_path: Path to OpenSubtitles frequency file (it_full.txt)
        pos_filter: Part of speech to import (NOUN or ADJECTIVE only)
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts:
        - matched: Number of surface forms matched
        - not_found: Number of surface forms not in database
        - lemmas_updated: Number of unique lemmas with frequency data
        - collisions: Number of surface forms mapping to multiple lemmas
        - matched_freq: Sum of frequencies for matched forms
        - total_corpus_freq: Sum of all frequencies in corpus

    Raises:
        ValueError: If pos_filter is VERB (must use PAISA for verbs)
    """
    if pos_filter == POS.VERB:
        raise ValueError(
            "Cannot use OpenSubtitles for verbs due to surface form collisions. "
            "Use import_paisa for verbs instead."
        )

    stats: dict[str, int] = {
        "matched": 0,
        "not_found": 0,
        "lemmas_updated": 0,
        "collisions": 0,
        "matched_freq": 0,
        "total_corpus_freq": 0,
    }

    # Parse OpenSubtitles data
    freq_data = _parse_opensubtitles(file_path)
    stats["total_corpus_freq"] = sum(freq_data.values())

    # Build form-to-lemmas map
    form_to_lemmas = _build_form_to_lemmas_map(conn, pos_filter)

    # Aggregate frequencies by lemma
    lemma_freqs: dict[int, int] = defaultdict(int)
    total_forms = len(freq_data)

    for idx, (word, freq) in enumerate(freq_data.items(), 1):
        if progress_callback and idx % 50000 == 0:
            progress_callback(idx, total_forms)

        if word in form_to_lemmas:
            lemma_ids = form_to_lemmas[word]
            stats["matched"] += 1
            stats["matched_freq"] += freq

            if len(lemma_ids) > 1:
                stats["collisions"] += 1

            for lemma_id in lemma_ids:
                lemma_freqs[lemma_id] += freq
        else:
            stats["not_found"] += 1

    stats["lemmas_updated"] = len(lemma_freqs)

    # Insert aggregated frequencies
    if lemma_freqs:
        insert_batch: list[dict[str, str | int | float]] = []
        for lemma_id, total_freq in lemma_freqs.items():
            zipf = _compute_zipf(total_freq)
            insert_batch.append(
                {
                    "lemma_id": lemma_id,
                    "corpus": CORPUS_NAME,
                    "freq_raw": total_freq,
                    "freq_zipf": zipf,
                    "corpus_version": CORPUS_VERSION,
                }
            )

        conn.execute(
            frequencies.insert().prefix_with("OR REPLACE"),
            insert_batch,
        )

    # Final progress callback
    if progress_callback:
        progress_callback(total_forms, total_forms)

    return stats
