"""Tests for frequency importers (PAISA and OpenSubtitles)."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_db.db import (
    frequencies,
    get_connection,
    get_engine,
    init_db,
    lemmas,
    noun_forms,
)
from italian_db.enums import POS
from italian_db.importers.frequency_ranking import compute_pos_frequency_ranks
from italian_db.importers.opensubtitles import import_opensubtitles
from italian_db.importers.paisa import import_paisa
from italian_db.importers.wiktextract import import_wiktextract

# Sample verb entry from Wiktextract
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


def _create_test_jsonl(entries: list[dict[str, Any]]) -> Path:
    """Create a temporary JSONL file with test entries."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
        return Path(f.name)


def _create_test_paisa(lines: list[str]) -> Path:
    """Create a temporary PAISA CSV file with test entries."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        # Two comment header lines
        f.write("# lemma frequencies for paisa corpus\n")
        f.write("# test data\n")
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


def _create_test_opensubtitles(lines: list[str]) -> Path:
    """Create a temporary OpenSubtitles frequency file with test entries."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


class TestPaisaImporter:
    """Tests for the PAISA importer."""

    def test_imports_frequency_data(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_VERB_2])
        paisa_path = _create_test_paisa(
            [
                "parlare,1000",
                "essere,5000",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import Wiktextract data
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Then import PAISA frequencies
            with get_connection(db_path) as conn:
                stats = import_paisa(conn, paisa_path)

            # Check stats
            assert stats["matched"] == 2  # parlare and essere
            assert stats["not_found"] == 0

            # Check frequency data was inserted
            with get_connection(db_path) as conn:
                freq_rows = conn.execute(select(frequencies)).fetchall()
                assert len(freq_rows) == 2

                # Check parlare frequency
                parlare_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "parlàre")
                ).fetchone()
                assert parlare_row is not None
                assert parlare_row.freq_raw == 1000
                assert parlare_row.corpus == "paisa"

                # Check essere frequency
                essere_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "èssere")
                ).fetchone()
                assert essere_row is not None
                assert essere_row.freq_raw == 5000

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            paisa_path.unlink()

    def test_handles_unmatched_lemmas(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])  # Only parlare
        paisa_path = _create_test_paisa(
            [
                "parlare,1000",
                "mangiare,500",  # Not in DB
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_paisa(conn, paisa_path)

            # Only parlare should match
            assert stats["matched"] == 1
            # mangiare not in DB, so not found for matching
            assert stats["not_found"] == 0  # not_found counts DB lemmas not in PAISA

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            paisa_path.unlink()


class TestOpenSubtitlesImporter:
    """Tests for the OpenSubtitles importer."""

    def test_imports_frequency_data(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN, SAMPLE_NOUN_2])
        opensub_path = _create_test_opensubtitles(
            [
                "casa 10000",
                "case 5000",  # Plural form of casa
                "libro 8000",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import Wiktextract data
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.NOUN)

            # Verify forms have written spellings
            with get_connection(db_path) as conn:
                forms = conn.execute(select(noun_forms)).fetchall()
                assert len(forms) > 0

            # Then import OpenSubtitles frequencies
            with get_connection(db_path) as conn:
                stats = import_opensubtitles(conn, opensub_path, pos_filter=POS.NOUN)

            # Check stats
            assert stats["matched"] >= 2  # casa, case, libro
            assert stats["lemmas_updated"] == 2  # casa and libro lemmas

            # Check frequency data was inserted
            with get_connection(db_path) as conn:
                freq_rows = conn.execute(select(frequencies)).fetchall()
                assert len(freq_rows) == 2

                # Check casa frequency (aggregated: 10000 + 5000 = 15000)
                # Note: Query by stressed since written may not be set yet
                casa_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "casa")
                ).fetchone()
                assert casa_row is not None
                assert casa_row.freq_raw == 15000  # casa + case forms
                assert casa_row.corpus == "opensubtitles"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            opensub_path.unlink()

    def test_rejects_verb_import(self) -> None:
        """OpenSubtitles should raise error for verbs (use PAISA instead)."""
        import pytest

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        opensub_path = _create_test_opensubtitles(["test 100"])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with (
                get_connection(db_path) as conn,
                pytest.raises(ValueError, match="Cannot use OpenSubtitles for verbs"),
            ):
                import_opensubtitles(conn, opensub_path, pos_filter=POS.VERB)

        finally:
            db_path.unlink()
            opensub_path.unlink()


class TestFrequencyRanking:
    """Tests for the compute_pos_frequency_ranks function."""

    def test_computes_rankings(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_VERB_2])
        paisa_path = _create_test_paisa(
            [
                "parlare,1000",
                "essere,5000",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                import_paisa(conn, paisa_path)

            with get_connection(db_path) as conn:
                stats = compute_pos_frequency_ranks(conn, "paisa")

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
            paisa_path.unlink()
