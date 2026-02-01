"""Tests for OpenSubtitles sentence importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select, text

from italian_db.db import (
    get_connection,
    get_engine,
    init_db,
    sentences,
    translations,
)
from italian_db.importers.opensubtitles_sentences import import_opensubtitles_sentences
from italian_db.importers.wiktextract import import_wiktextract

# Minimal verb entry so import_wiktextract can run (needed to init DB properly)
SAMPLE_VERB = {
    "pos": "verb",
    "word": "parlare",
    "forms": [
        {"form": "parlàre", "tags": ["canonical"]},
        {"form": "pàrlo", "tags": ["first-person", "indicative", "present", "singular"]},
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


def _create_test_tsv(lines: list[str]) -> Path:
    """Create a temporary TSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False, encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


class TestOpenSubtitlesSentences:
    """Tests for the OpenSubtitles sentence importer."""

    def test_imports_sentences_and_translations(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        # Same sentence_id for both languages (Moses line-aligned format).
        # Unique constraint is (source, lang, sentence_id) so this is fine.
        ita_path = _create_test_tsv(
            [
                "1\tita\tCiao, come stai?",
                "2\tita\tBuongiorno!",
            ]
        )
        eng_path = _create_test_tsv(
            [
                "1\teng\tHi, how are you?",
                "2\teng\tGood morning!",
            ]
        )
        links_path = _create_test_tsv(
            [
                "1\t1",
                "2\t2",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_opensubtitles_sentences(conn, ita_path, eng_path, links_path)

            assert stats["ita_sentences"] == 2
            assert stats["eng_sentences"] == 2
            assert stats["translations"] == 2

            # Verify sentences are stored with source='opensubtitles'
            with get_connection(db_path) as conn:
                ita_rows = conn.execute(
                    select(sentences).where(
                        sentences.c.lang == "ita",
                        sentences.c.source == "opensubtitles",
                    )
                ).fetchall()
                assert len(ita_rows) == 2

                eng_rows = conn.execute(
                    select(sentences).where(
                        sentences.c.lang == "eng",
                        sentences.c.source == "opensubtitles",
                    )
                ).fetchall()
                assert len(eng_rows) == 2

                trans_rows = conn.execute(select(translations)).fetchall()
                assert len(trans_rows) == 2

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_idempotent_reimport(self) -> None:
        """Running import twice should not create duplicates."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_tsv(
            [
                "1\tita\tCiao!",
            ]
        )
        eng_path = _create_test_tsv(
            [
                "1\teng\tHi!",
            ]
        )
        links_path = _create_test_tsv(
            [
                "1\t1",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # First import
            with get_connection(db_path) as conn:
                stats1 = import_opensubtitles_sentences(conn, ita_path, eng_path, links_path)
            assert stats1["cleared"] == 0

            # Second import
            with get_connection(db_path) as conn:
                stats2 = import_opensubtitles_sentences(conn, ita_path, eng_path, links_path)
            assert stats2["cleared"] > 0  # Should have cleared previous data

            # Same counts
            assert stats2["ita_sentences"] == stats1["ita_sentences"]
            assert stats2["eng_sentences"] == stats1["eng_sentences"]

            # Verify no duplicates
            with get_connection(db_path) as conn:
                all_sentences = conn.execute(
                    select(sentences).where(sentences.c.source == "opensubtitles")
                ).fetchall()
                assert len(all_sentences) == 2  # 1 Italian + 1 English

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_fts5_search_works(self) -> None:
        """FTS5 index should be populated for OpenSubtitles Italian sentences."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_tsv(
            [
                "1\tita\tIo parlo italiano.",
                "2\tita\tBuongiorno!",
            ]
        )
        eng_path = _create_test_tsv([])
        links_path = _create_test_tsv([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                import_opensubtitles_sentences(conn, ita_path, eng_path, links_path)

            # Test FTS5 search
            with get_connection(db_path) as conn:
                results = conn.execute(
                    text("SELECT text FROM sentences_fts WHERE text MATCH 'parlo'")
                ).fetchall()
                assert len(results) == 1
                assert "parlo" in results[0][0].lower()

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_does_not_affect_tatoeba_data(self) -> None:
        """Importing OpenSubtitles should not clear Tatoeba sentences."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_tsv(
            [
                "1\tita\tCiao dal OpenSubtitles!",
            ]
        )
        eng_path = _create_test_tsv([])
        links_path = _create_test_tsv([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Insert a Tatoeba sentence first
            with get_connection(db_path) as conn:
                conn.execute(
                    sentences.insert(),
                    {
                        "sentence_id": 999,
                        "lang": "ita",
                        "text": "Tatoeba sentence.",
                        "source": "tatoeba",
                    },
                )

            # Import OpenSubtitles
            with get_connection(db_path) as conn:
                import_opensubtitles_sentences(conn, ita_path, eng_path, links_path)

            # Tatoeba sentence should still exist
            with get_connection(db_path) as conn:
                tatoeba_rows = conn.execute(
                    select(sentences).where(sentences.c.source == "tatoeba")
                ).fetchall()
                assert len(tatoeba_rows) == 1
                assert tatoeba_rows[0].text == "Tatoeba sentence."

                opensub_rows = conn.execute(
                    select(sentences).where(sentences.c.source == "opensubtitles")
                ).fetchall()
                assert len(opensub_rows) == 1

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
        ita_path = _create_test_tsv([])
        eng_path = _create_test_tsv([])
        links_path = _create_test_tsv([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_opensubtitles_sentences(conn, ita_path, eng_path, links_path)

            assert stats["ita_sentences"] == 0
            assert stats["eng_sentences"] == 0
            assert stats["translations"] == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()
