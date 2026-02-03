"""Tests for Tatoeba importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select, text

from italian_db.db import (
    get_connection,
    get_engine,
    init_db,
    sentence_tags,
    sentences,
    translations,
)
from italian_db.importers.tatoeba import import_tatoeba
from italian_db.importers.wiktextract import import_wiktextract

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


def _create_test_tags_csv(lines: list[str]) -> Path:
    """Create a temporary tags CSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


def _create_test_sentences_in_lists_csv(lines: list[str]) -> Path:
    """Create a temporary sentences_in_lists CSV file."""
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

    def test_filters_to_ck_whitelist(self) -> None:
        """Only sentences with translations to CK whitelist should be imported."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv(
            [
                "100\tita\tIo parlo italiano.",  # Has CK-whitelisted translation
                "101\tita\tLui parla bene.",  # Has non-CK translation
                "102\tita\tBuongiorno!",  # Has CK-whitelisted translation
            ]
        )
        eng_path = _create_test_sentences_tsv(
            [
                "200\teng\tI speak Italian.",  # CK-whitelisted
                "201\teng\tHe speaks well.",  # NOT in CK whitelist
                "202\teng\tGood morning!",  # CK-whitelisted
            ]
        )
        links_path = _create_test_links_csv(
            [
                "100\t200",  # Italian 100 -> English 200 (CK)
                "101\t201",  # Italian 101 -> English 201 (not CK)
                "102\t202",  # Italian 102 -> English 202 (CK)
            ]
        )
        # CK whitelist (List 907) contains only sentence IDs 200 and 202
        sentences_in_lists_path = _create_test_sentences_in_lists_csv(
            [
                "907\t200",  # English 200 is in CK whitelist
                "907\t202",  # English 202 is in CK whitelist
                "123\t201",  # English 201 is in a DIFFERENT list, not CK
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_tatoeba(
                    conn,
                    ita_path,
                    eng_path,
                    links_path,
                    sentences_in_lists_path=sentences_in_lists_path,
                )

            # Should only import 2 Italian sentences (100 and 102)
            # because only they have CK-whitelisted translations
            assert stats["ita_sentences"] == 2
            assert stats["eng_sentences"] == 2
            assert stats["translations"] == 2
            assert stats["ck_whitelist_size"] == 2

            # Verify the right sentences were imported
            with get_connection(db_path) as conn:
                ita_texts = conn.execute(
                    select(sentences.c.text).where(sentences.c.lang == "ita")
                ).fetchall()
                ita_texts = [row[0] for row in ita_texts]

            assert "Io parlo italiano." in ita_texts
            assert "Buongiorno!" in ita_texts
            assert "Lui parla bene." not in ita_texts

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()
            sentences_in_lists_path.unlink()

    def test_imports_tags(self) -> None:
        """Tags should be imported for Italian sentences."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv(
            [
                "100\tita\tChi dorme non piglia pesci.",
                "101\tita\tIo parlo italiano.",
            ]
        )
        eng_path = _create_test_sentences_tsv(
            [
                "200\teng\tYou snooze, you lose.",
                "201\teng\tI speak Italian.",
            ]
        )
        links_path = _create_test_links_csv(
            [
                "100\t200",
                "101\t201",
            ]
        )
        tags_path = _create_test_tags_csv(
            [
                "100\tproverb",
                "100\t@change",
                "101\tpresente",  # Tense tag — should be skipped
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_tatoeba(conn, ita_path, eng_path, links_path, tags_path=tags_path)

            assert stats["tags"] == 2  # proverb + @change for sentence 100; presente skipped

            # Verify tags are correctly associated
            with get_connection(db_path) as conn:
                # Get surrogate ID for sentence 100
                result = conn.execute(
                    select(sentences.c.id).where(
                        sentences.c.sentence_id == 100, sentences.c.source == "tatoeba"
                    )
                ).fetchone()
                assert result is not None
                sent_100_id: int = result[0]

                tags_for_100_rows = conn.execute(
                    select(sentence_tags.c.tag).where(sentence_tags.c.sentence_id == sent_100_id)
                ).fetchall()
                tags_for_100 = {row[0] for row in tags_for_100_rows}

            assert "proverb" in tags_for_100
            assert "@change" in tags_for_100
            assert "presente" not in tags_for_100

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()
            tags_path.unlink()

    def test_ck_filter_with_tags(self) -> None:
        """CK filtering and tag import should work together."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_sentences_tsv(
            [
                "100\tita\tChi dorme non piglia pesci.",  # CK-linked
                "101\tita\tLui parla bene.",  # Not CK-linked
            ]
        )
        eng_path = _create_test_sentences_tsv(
            [
                "200\teng\tYou snooze, you lose.",  # CK
                "201\teng\tHe speaks well.",  # Not CK
            ]
        )
        links_path = _create_test_links_csv(
            [
                "100\t200",
                "101\t201",
            ]
        )
        tags_path = _create_test_tags_csv(
            [
                "100\tproverb",
                "101\tproverb",  # Tag for non-CK sentence (should not be imported)
            ]
        )
        sentences_in_lists_path = _create_test_sentences_in_lists_csv(
            [
                "907\t200",  # Only 200 in CK whitelist
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_tatoeba(
                    conn,
                    ita_path,
                    eng_path,
                    links_path,
                    tags_path=tags_path,
                    sentences_in_lists_path=sentences_in_lists_path,
                )

            # Only 1 Italian sentence should be imported (100)
            assert stats["ita_sentences"] == 1
            # Only 1 tag should be imported (proverb for 100)
            assert stats["tags"] == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()
            tags_path.unlink()
            sentences_in_lists_path.unlink()
