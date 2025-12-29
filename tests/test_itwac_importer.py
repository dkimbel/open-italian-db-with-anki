"""Tests for ItWaC frequency importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_anki.db import (
    frequencies,
    get_connection,
    get_engine,
    init_db,
    lemmas,
)
from italian_anki.importers.itwac import import_itwac
from italian_anki.importers.wiktextract import import_wiktextract

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


def _create_test_jsonl(entries: list[dict[str, Any]]) -> Path:
    """Create a temporary JSONL file with test entries."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
        return Path(f.name)


def _create_test_itwac(lines: list[str]) -> Path:
    """Create a temporary ItWaC CSV file with test entries."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="iso-8859-1"
    ) as f:
        # Header
        f.write('"Form","Freq","lemma","POS","mode","POS2","fpmw","Zipf"\n')
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


class TestItwacImporter:
    """Tests for the ItWaC importer."""

    def test_imports_frequency_data(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_VERB_2])
        itwac_path = _create_test_itwac(
            [
                '"parlo",1000,"parlare","VER","fin","VER",0.5,3.7',
                '"parli",500,"parlare","VER","fin","VER",0.25,3.4',
                '"sono",5000,"essere","VER","fin","VER",2.5,4.4',
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import Wiktextract data
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Then import ItWaC frequencies
            with get_connection(db_path) as conn:
                stats = import_itwac(conn, itwac_path)

            # Check stats
            assert stats["matched"] == 2  # parlare and essere
            assert stats["not_found"] == 0

            # Check frequency data was inserted
            with get_connection(db_path) as conn:
                freq_rows = conn.execute(select(frequencies)).fetchall()
                assert len(freq_rows) == 2

                # Check parlare frequency (aggregated: 1000 + 500 = 1500)
                parlare_row = conn.execute(
                    select(frequencies)
                    .join(lemmas, frequencies.c.lemma_id == lemmas.c.id)
                    .where(lemmas.c.stressed == "parlàre")
                ).fetchone()
                assert parlare_row is not None
                assert parlare_row.freq_raw == 1500
                assert parlare_row.corpus == "itwac"
                assert parlare_row.corpus_version == "2.1.0"

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
            itwac_path.unlink()

    def test_handles_unmatched_lemmas(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])  # Only parlare
        itwac_path = _create_test_itwac(
            [
                '"parlo",1000,"parlare","VER","fin","VER",0.5,3.7',
                '"mangio",500,"mangiare","VER","fin","VER",0.25,3.4',  # Not in DB
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_itwac(conn, itwac_path)

            # Only parlare should match
            assert stats["matched"] == 1
            # essere in DB but not in ItWaC data for this test

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            itwac_path.unlink()

    def test_handles_empty_csv(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        itwac_path = _create_test_itwac([])  # Empty (just header)

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_itwac(conn, itwac_path)

            # No matches
            assert stats["matched"] == 0
            assert stats["not_found"] == 1  # parlare not found in ItWaC

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            itwac_path.unlink()

    def test_computes_zipf_score(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        itwac_path = _create_test_itwac(
            [
                '"parlo",1900000,"parlare","VER","fin","VER",1.0,4.0',
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                import_itwac(conn, itwac_path)

            with get_connection(db_path) as conn:
                freq_row = conn.execute(select(frequencies)).fetchone()
                assert freq_row is not None
                # Zipf = log10(fpmw) + 3 where fpmw = freq * 1e6 / corpus_size
                # fpmw = 1.9M * 1e6 / 1.9e9 = 1000
                # Zipf = log10(1000) + 3 = 3 + 3 = 6
                assert freq_row.freq_zipf is not None
                assert 5.9 < freq_row.freq_zipf < 6.1

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            itwac_path.unlink()

    def test_idempotent_when_run_twice(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        itwac_path = _create_test_itwac(
            [
                '"parlo",1000,"parlare","VER","fin","VER",0.5,3.7',
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # First import
            with get_connection(db_path) as conn:
                stats1 = import_itwac(conn, itwac_path)

            # Second import (should replace)
            with get_connection(db_path) as conn:
                stats2 = import_itwac(conn, itwac_path)

            assert stats1["matched"] == 1
            assert stats2["matched"] == 1

            # Should still have only one frequency entry
            with get_connection(db_path) as conn:
                freq_rows = conn.execute(select(frequencies)).fetchall()
                assert len(freq_rows) == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            itwac_path.unlink()
