"""Tests for Tatoeba importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select, text

from italian_anki.db import (
    get_connection,
    get_engine,
    init_db,
    sentences,
    translations,
)
from italian_anki.importers.tatoeba import import_tatoeba
from italian_anki.importers.wiktextract import import_wiktextract

# Sample verb entry from Wiktextract
SAMPLE_VERB = {
    "pos": "verb",
    "word": "parlare",
    "forms": [
        {"form": "parlàre", "tags": ["canonical"]},
        {"form": "pàrlo", "tags": ["first-person", "indicative", "present", "singular"]},
        {"form": "pàrla", "tags": ["third-person", "indicative", "present", "singular"]},
    ],
    "senses": [{"glosses": ["to speak"]}],
}


def _create_test_jsonl(entries: list[dict[str, Any]]) -> Path:
    """Create a temporary JSONL file with test entries."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
        return Path(f.name)


def _create_test_sentences_tsv(lines: list[str]) -> Path:
    """Create a temporary sentences TSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False, encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


def _create_test_links_csv(lines: list[str]) -> Path:
    """Create a temporary links CSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


class TestTatoebaImporter:
    """Tests for the Tatoeba importer."""

    def test_imports_sentences(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv(
            [
                "100\tita\tIo parlo italiano.",
                "101\tita\tLui parla bene.",
            ]
        )
        eng_path = _create_test_sentences_tsv(
            [
                "200\teng\tI speak Italian.",
                "201\teng\tHe speaks well.",
            ]
        )
        links_path = _create_test_links_csv(
            [
                "100\t200",
                "101\t201",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import verbs
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Then import Tatoeba
            with get_connection(db_path) as conn:
                stats = import_tatoeba(conn, ita_path, eng_path, links_path)

            assert stats["ita_sentences"] == 2
            assert stats["eng_sentences"] == 2
            assert stats["translations"] == 2

            with get_connection(db_path) as conn:
                ita_rows = conn.execute(
                    select(sentences).where(sentences.c.lang == "ita")
                ).fetchall()
                eng_rows = conn.execute(
                    select(sentences).where(sentences.c.lang == "eng")
                ).fetchall()
                assert len(ita_rows) == 2
                assert len(eng_rows) == 2

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_imports_only_needed_english(self) -> None:
        """English sentences without Italian links should not be imported."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv(
            [
                "100\tita\tIo parlo italiano.",
            ]
        )
        eng_path = _create_test_sentences_tsv(
            [
                "200\teng\tI speak Italian.",  # Has link
                "201\teng\tHello world.",  # No link
                "202\teng\tGoodbye.",  # No link
            ]
        )
        links_path = _create_test_links_csv(
            [
                "100\t200",  # Only this link exists
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_tatoeba(conn, ita_path, eng_path, links_path)

            # Only 1 English sentence should be imported
            assert stats["eng_sentences"] == 1
            assert stats["translations"] == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_fts5_search_works(self) -> None:
        """FTS5 index should be populated and searchable."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv(
            [
                "100\tita\tIo parlo italiano.",
                "101\tita\tLui parla bene.",
                "102\tita\tBuongiorno!",
            ]
        )
        eng_path = _create_test_sentences_tsv([])
        links_path = _create_test_links_csv([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                import_tatoeba(conn, ita_path, eng_path, links_path)

            # Test FTS5 search
            with get_connection(db_path) as conn:
                # Search for "parlo"
                results = conn.execute(
                    text("SELECT text FROM sentences_fts WHERE text MATCH 'parlo'")
                ).fetchall()
                assert len(results) == 1
                assert "parlo" in results[0][0].lower()

                # Search for "parla"
                results = conn.execute(
                    text("SELECT text FROM sentences_fts WHERE text MATCH 'parla'")
                ).fetchall()
                assert len(results) == 1
                assert "parla" in results[0][0].lower()

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_idempotent_when_run_twice(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv(
            [
                "100\tita\tIo parlo italiano.",
            ]
        )
        eng_path = _create_test_sentences_tsv(
            [
                "200\teng\tI speak Italian.",
            ]
        )
        links_path = _create_test_links_csv(
            [
                "100\t200",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # First import
            with get_connection(db_path) as conn:
                stats1 = import_tatoeba(conn, ita_path, eng_path, links_path)

            assert stats1["cleared"] == 0

            # Second import
            with get_connection(db_path) as conn:
                stats2 = import_tatoeba(conn, ita_path, eng_path, links_path)

            assert stats2["cleared"] > 0  # Should have cleared previous data

            # Counts should be the same
            assert stats2["ita_sentences"] == stats1["ita_sentences"]
            assert stats2["eng_sentences"] == stats1["eng_sentences"]

            # Verify no duplicates
            with get_connection(db_path) as conn:
                all_sentences = conn.execute(select(sentences)).fetchall()
                all_trans = conn.execute(select(translations)).fetchall()

            assert len(all_sentences) == 2  # 1 Italian + 1 English
            assert len(all_trans) == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_handles_empty_files(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv([])
        eng_path = _create_test_sentences_tsv([])
        links_path = _create_test_links_csv([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_tatoeba(conn, ita_path, eng_path, links_path)

            assert stats["ita_sentences"] == 0
            assert stats["eng_sentences"] == 0
            assert stats["translations"] == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()
