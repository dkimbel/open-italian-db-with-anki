"""Tests for Stanza-derived frequency computation and ranking."""

import json
import math
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, select, text

from italian_db.db import (
    frequencies,
    get_connection,
    get_engine,
    init_db,
    lemmas,
    sentence_tokens,
    sentences,
)
from italian_db.importers.frequency_from_tokens import (
    CORPUS_NAME,
    TATOEBA_WEIGHT,
    compute_frequencies_from_tokens,
)
from italian_db.importers.frequency_ranking import compute_pos_frequency_ranks
from italian_db.importers.wiktextract import import_wiktextract
from italian_db.normalize import normalize

# Sample verb entries from Wiktextract
SAMPLE_VERB = {
    "pos": "verb",
    "word": "parlare",
    "forms": [
        {"form": "parlàre", "tags": ["canonical"]},
        {"form": "pàrlo", "tags": ["first-person", "indicative", "present", "singular"]},
    ],
    "senses": [{"glosses": ["to speak"]}],
}

SAMPLE_VERB_2 = {
    "pos": "verb",
    "word": "essere",
    "forms": [
        {"form": "èssere", "tags": ["canonical"]},
        {"form": "sono", "tags": ["first-person", "indicative", "present", "singular"]},
    ],
    "senses": [{"glosses": ["to be"]}],
}

SAMPLE_NOUN = {
    "pos": "noun",
    "word": "casa",
    "head_templates": [{"args": {"1": "f"}}],
    "forms": [
        {"form": "casa", "tags": ["canonical", "feminine", "singular"]},
        {"form": "case", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["house"]}],
}

SAMPLE_NOUN_2 = {
    "pos": "noun",
    "word": "libro",
    "head_templates": [{"args": {"1": "m"}}],
    "forms": [
        {"form": "libro", "tags": ["canonical", "masculine", "singular"]},
        {"form": "libri", "tags": ["masculine", "plural"]},
    ],
    "senses": [{"glosses": ["book"]}],
}

SAMPLE_ADJ = {
    "pos": "adj",
    "word": "bello",
    "forms": [
        {"form": "bello", "tags": ["canonical", "masculine", "singular"]},
        {"form": "bella", "tags": ["feminine", "singular"]},
        {"form": "belli", "tags": ["masculine", "plural"]},
        {"form": "belle", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["beautiful"]}],
}


def _create_test_jsonl(entries: list[dict[str, Any]]) -> Path:
    """Create a temporary JSONL file with test entries."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
        return Path(f.name)


def _populate_written_from_stressed(conn: Connection) -> None:
    """Set written = normalize(stressed) for all lemmas where written is NULL.

    In the real pipeline, the enrichment step populates `written`. Tests skip
    enrichment, so this helper fills it in so frequency lookup works.
    """
    rows = conn.execute(
        text("SELECT id, stressed FROM lemmas WHERE written IS NULL AND stressed IS NOT NULL")
    ).fetchall()
    for row in rows:
        conn.execute(
            text("UPDATE lemmas SET written = :written WHERE id = :id"),
            {"written": normalize(row[1]), "id": row[0]},
        )


def _insert_test_sentences_and_tokens(
    conn: Any,
    token_data: list[tuple[str, str, str]],
) -> None:
    """Insert test sentences and tokens into the database.

    Args:
        conn: SQLAlchemy connection
        token_data: List of (text, lemma, upos) tuples for tokens.
            All tokens are placed in a single test sentence.
    """
    # Insert a test Italian sentence
    conn.execute(
        sentences.insert(),
        {"sentence_id": 1, "lang": "ita", "text": "Test sentence.", "source": "tatoeba"},
    )
    # Get the surrogate ID
    row = conn.execute(
        text("SELECT id FROM sentences WHERE sentence_id = 1 AND source = 'tatoeba'")
    ).fetchone()
    sentence_surrogate_id = row[0]

    # Insert tokens
    for idx, (tok_text, lemma, upos) in enumerate(token_data):
        conn.execute(
            sentence_tokens.insert(),
            {
                "sentence_id": sentence_surrogate_id,
                "token_index": idx,
                "text": tok_text,
                "lemma": lemma,
                "upos": upos,
            },
        )


def _compute_zipf_for_test(freq_raw: int, total_tokens: int) -> float:
    """Compute Zipf score for test assertions. Mirrors the module-private function."""
    if freq_raw <= 0 or total_tokens <= 0:
        return 0.0
    fpmw = freq_raw * 1e6 / total_tokens
    return math.log10(fpmw) + 3


class TestComputeZipf:
    """Tests for the Zipf score computation."""

    def test_basic_zipf(self) -> None:
        # 1000 occurrences in 1M tokens = freq_per_million = 1000
        # zipf = log10(1000) + 3 = 3 + 3 = 6
        result = _compute_zipf_for_test(1000, 1_000_000)
        assert abs(result - 6.0) < 0.01

    def test_zero_frequency(self) -> None:
        assert _compute_zipf_for_test(0, 1_000_000) == 0.0

    def test_zero_total(self) -> None:
        assert _compute_zipf_for_test(100, 0) == 0.0


class TestFrequencyFromTokens:
    """Tests for computing frequency from sentence tokens."""

    def test_computes_verb_frequency(self) -> None:
        """Verbs in sentence_tokens should be matched to lemmas table."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_VERB_2])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # Import lemmas and populate written forms
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                _populate_written_from_stressed(conn)

            # Insert sentence tokens
            with get_connection(db_path) as conn:
                _insert_test_sentences_and_tokens(
                    conn,
                    [
                        ("Parlo", "parlare", "VERB"),
                        ("italiano", "italiano", "NOUN"),
                        (".", ".", "PUNCT"),
                    ],
                )

            # Compute frequencies
            with get_connection(db_path) as conn:
                stats = compute_frequencies_from_tokens(conn)

            assert stats["matched"] >= 1  # At least parlare should match
            assert stats["total_tokens"] > 0

            # Check frequency data was inserted
            with get_connection(db_path) as conn:
                parlare_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "parlàre")
                ).fetchone()
                assert parlare_row is not None
                assert parlare_row.freq_raw == 1 * TATOEBA_WEIGHT
                assert parlare_row.corpus == CORPUS_NAME

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_aggregates_verb_and_aux(self) -> None:
        """VERB and AUX tokens for same lemma should be aggregated."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB_2])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                _populate_written_from_stressed(conn)

            # Insert tokens where "essere" appears as both VERB and AUX
            with get_connection(db_path) as conn:
                _insert_test_sentences_and_tokens(
                    conn,
                    [
                        ("è", "essere", "AUX"),
                        ("è", "essere", "VERB"),
                        ("stato", "essere", "AUX"),
                    ],
                )

            with get_connection(db_path) as conn:
                compute_frequencies_from_tokens(conn)

            # Check that essere's frequency is aggregated (AUX + VERB)
            with get_connection(db_path) as conn:
                essere_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "èssere")
                ).fetchone()
                assert essere_row is not None
                assert essere_row.freq_raw == 3 * TATOEBA_WEIGHT  # 2 AUX + 1 VERB

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_skips_punct_sym_x(self) -> None:
        """PUNCT, SYM, and X tokens should be excluded from frequency."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                _populate_written_from_stressed(conn)

            with get_connection(db_path) as conn:
                _insert_test_sentences_and_tokens(
                    conn,
                    [
                        ("Parlo", "parlare", "VERB"),
                        (".", ".", "PUNCT"),
                        ("$", "$", "SYM"),
                        ("asdf", "asdf", "X"),
                    ],
                )

            with get_connection(db_path) as conn:
                stats = compute_frequencies_from_tokens(conn)

            # Only the VERB token should be counted (weighted)
            assert stats["total_tokens"] == 1 * TATOEBA_WEIGHT

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_unmatched_lemmas_counted(self) -> None:
        """Tokens with lemmas not in our DB should be counted as not_found."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                _populate_written_from_stressed(conn)

            # Insert a token with a lemma not in our DB
            with get_connection(db_path) as conn:
                _insert_test_sentences_and_tokens(
                    conn,
                    [
                        ("Parlo", "parlare", "VERB"),
                        ("mangio", "mangiare", "VERB"),  # Not in DB
                    ],
                )

            with get_connection(db_path) as conn:
                stats = compute_frequencies_from_tokens(conn)

            assert stats["matched"] == 1  # parlare
            assert stats["not_found"] == 1  # mangiare

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_noun_frequency(self) -> None:
        """Noun tokens should produce frequency entries."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN, SAMPLE_NOUN_2])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                from italian_db.enums import POS

                import_wiktextract(conn, jsonl_path, pos_filter=POS.NOUN)
                _populate_written_from_stressed(conn)

            with get_connection(db_path) as conn:
                _insert_test_sentences_and_tokens(
                    conn,
                    [
                        ("la", "il", "DET"),
                        ("casa", "casa", "NOUN"),
                        ("e", "e", "CCONJ"),
                        ("il", "il", "DET"),
                        ("libro", "libro", "NOUN"),
                    ],
                )

            with get_connection(db_path) as conn:
                stats = compute_frequencies_from_tokens(conn)

            assert stats["matched"] == 2  # casa and libro

            with get_connection(db_path) as conn:
                freq_rows = conn.execute(select(frequencies)).fetchall()
                assert len(freq_rows) == 2

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_clears_existing_frequencies(self) -> None:
        """Running compute_frequencies_from_tokens should clear old data first."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                _populate_written_from_stressed(conn)

            with get_connection(db_path) as conn:
                _insert_test_sentences_and_tokens(
                    conn,
                    [
                        ("Parlo", "parlare", "VERB"),
                    ],
                )

            # Run twice
            with get_connection(db_path) as conn:
                compute_frequencies_from_tokens(conn)
            with get_connection(db_path) as conn:
                compute_frequencies_from_tokens(conn)

            # Should not have duplicates
            with get_connection(db_path) as conn:
                # Should have at most 1 entry for parlare
                parlare_rows = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "parlàre")
                ).fetchall()
                assert len(parlare_rows) == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_tatoeba_weighted_higher_than_opensubtitles(self) -> None:
        """Tatoeba tokens should be weighted TATOEBA_WEIGHT times higher than OpenSubtitles."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_VERB_2])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                _populate_written_from_stressed(conn)

            # Insert two sentences from different sources, each with one verb occurrence
            with get_connection(db_path) as conn:
                conn.execute(
                    sentences.insert(),
                    {"sentence_id": 1, "lang": "ita", "text": "Parlo bene.", "source": "tatoeba"},
                )
                conn.execute(
                    sentences.insert(),
                    {
                        "sentence_id": 2,
                        "lang": "ita",
                        "text": "Sono qui.",
                        "source": "opensubtitles",
                    },
                )
                tat_id = conn.execute(
                    text("SELECT id FROM sentences WHERE sentence_id = 1 AND source = 'tatoeba'")
                ).scalar()
                osub_id = conn.execute(
                    text(
                        "SELECT id FROM sentences WHERE sentence_id = 2 AND source = 'opensubtitles'"
                    )
                ).scalar()

                # One "parlare" token in tatoeba sentence
                conn.execute(
                    sentence_tokens.insert(),
                    {
                        "sentence_id": tat_id,
                        "token_index": 0,
                        "text": "Parlo",
                        "lemma": "parlare",
                        "upos": "VERB",
                    },
                )
                # One "essere" token in opensubtitles sentence
                conn.execute(
                    sentence_tokens.insert(),
                    {
                        "sentence_id": osub_id,
                        "token_index": 0,
                        "text": "Sono",
                        "lemma": "essere",
                        "upos": "VERB",
                    },
                )

            with get_connection(db_path) as conn:
                stats = compute_frequencies_from_tokens(conn)

            # Total weighted tokens: 1*TATOEBA_WEIGHT (tatoeba) + 1*1 (opensubtitles)
            assert stats["total_tokens"] == TATOEBA_WEIGHT + 1

            with get_connection(db_path) as conn:
                parlare_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "parlàre")
                ).fetchone()
                essere_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "èssere")
                ).fetchone()

                assert parlare_row is not None
                assert essere_row is not None

                # Tatoeba verb gets weighted count
                assert parlare_row.freq_raw == TATOEBA_WEIGHT
                # OpenSubtitles verb gets unweighted count
                assert essere_row.freq_raw == 1

                # Tatoeba verb should have higher Zipf score (ranks higher)
                assert parlare_row.freq_zipf > essere_row.freq_zipf

        finally:
            db_path.unlink()
            jsonl_path.unlink()


class TestFrequencyRanking:
    """Tests for the compute_pos_frequency_ranks function."""

    def test_computes_rankings(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_VERB_2])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                _populate_written_from_stressed(conn)

            # Insert tokens with different frequencies
            with get_connection(db_path) as conn:
                _insert_test_sentences_and_tokens(
                    conn,
                    [
                        ("parlo", "parlare", "VERB"),
                        ("sono", "essere", "VERB"),
                        ("sono", "essere", "VERB"),
                        ("sono", "essere", "VERB"),
                    ],
                )

            # Compute frequencies
            with get_connection(db_path) as conn:
                compute_frequencies_from_tokens(conn)

            # Compute rankings
            with get_connection(db_path) as conn:
                stats = compute_pos_frequency_ranks(conn, CORPUS_NAME)

            # Should have ranked 2 verbs
            assert stats.get("verb", 0) == 2

            # Check rankings
            with get_connection(db_path) as conn:
                # essere should be rank 1 (higher frequency)
                essere_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "èssere")
                ).fetchone()
                assert essere_row is not None
                assert essere_row.freq_rank_in_pos == 1

                # parlare should be rank 2
                parlare_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "parlàre")
                ).fetchone()
                assert parlare_row is not None
                assert parlare_row.freq_rank_in_pos == 2

        finally:
            db_path.unlink()
            jsonl_path.unlink()
