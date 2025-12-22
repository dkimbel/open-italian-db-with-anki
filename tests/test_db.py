"""Tests for database schema and connection using SQLAlchemy Core."""

import tempfile
from pathlib import Path

from sqlalchemy import Connection, inspect, select, text

from italian_anki.db import (
    get_connection,
    get_engine,
    init_db,
    lemmas,
    verb_forms,
)


class TestConnection:
    """Tests for database connection management."""

    def test_get_engine_creates_engine(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)

        try:
            engine = get_engine(db_path)
            assert engine is not None
            # Same path should return cached engine
            engine2 = get_engine(db_path)
            assert engine is engine2
        finally:
            db_path.unlink()

    def test_connection_context_manager(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)

        try:
            with get_connection(db_path) as conn:
                assert isinstance(conn, Connection)
        finally:
            db_path.unlink()

    def test_foreign_keys_enabled(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)

        try:
            with get_connection(db_path) as conn:
                result = conn.execute(text("PRAGMA foreign_keys")).fetchone()
                assert result is not None
                assert result[0] == 1
        finally:
            db_path.unlink()


class TestSchema:
    """Tests for database schema initialization."""

    def test_init_db_creates_tables(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # Check that core tables exist
            inspector = inspect(engine)
            table_names = set(inspector.get_table_names())

            expected_tables = {
                "lemmas",
                "frequencies",
                "verb_forms",
                "noun_forms",
                "adjective_forms",
                "form_lookup",
                "definitions",
                "sentences",
                "translations",
                "sentence_lemmas",
                "verb_metadata",
            }
            assert expected_tables.issubset(table_names)
        finally:
            db_path.unlink()

    def test_init_db_is_idempotent(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)

        try:
            engine = get_engine(db_path)
            # Call init_db twice - should not raise
            init_db(engine)
            init_db(engine)

            # Verify tables still exist
            inspector = inspect(engine)
            assert len(inspector.get_table_names()) > 0
        finally:
            db_path.unlink()

    def test_lemmas_table_structure(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                # Insert a lemma with IPA (uses Unicode stress/length markers)
                ipa = "par\u02c8la\u02d0re"
                conn.execute(
                    lemmas.insert().values(
                        lemma="parlare",
                        lemma_stressed="parlare",
                        pos="verb",
                        ipa=ipa,
                    )
                )

                row = conn.execute(select(lemmas).where(lemmas.c.lemma == "parlare")).fetchone()
                assert row is not None
                assert row.lemma == "parlare"
                assert row.lemma_stressed == "parlare"
                assert row.pos == "verb"
                assert row.ipa == ipa
        finally:
            db_path.unlink()

    def test_verb_forms_foreign_key(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                # Insert a lemma first
                result = conn.execute(
                    lemmas.insert().values(lemma="parlare", lemma_stressed="parlare", pos="verb")
                )
                pk = result.inserted_primary_key
                assert pk is not None
                lemma_id: int = pk[0]

                # Insert a verb form
                conn.execute(
                    verb_forms.insert().values(
                        lemma_id=lemma_id,
                        form="parlo",
                        form_stressed="parlo",
                        mood="indicative",
                        tense="present",
                        person=1,
                        number="singular",
                    )
                )

                row = conn.execute(
                    select(verb_forms).where(verb_forms.c.lemma_id == lemma_id)
                ).fetchone()
                assert row is not None
                assert row.form == "parlo"
                assert row.form_stressed == "parlo"
                assert row.mood == "indicative"
        finally:
            db_path.unlink()
