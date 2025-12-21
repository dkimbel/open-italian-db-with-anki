"""Tests for Morph-it! importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_anki.db import (
    form_lookup_new,
    get_connection,
    get_engine,
    init_db,
    verb_forms,
)
from italian_anki.importers.morphit import import_morphit
from italian_anki.importers.wiktextract import import_wiktextract

# Sample verb entry from Wiktextract (with stressed forms)
SAMPLE_VERB = {
    "pos": "verb",
    "word": "parlare",
    "forms": [
        {"form": "parlàre", "tags": ["canonical"]},
        {"form": "parlàre", "tags": ["infinitive"]},
        {"form": "avére", "tags": ["auxiliary"]},
        {"form": "pàrlo", "tags": ["first-person", "indicative", "present", "singular"]},
        {"form": "pàrli", "tags": ["second-person", "indicative", "present", "singular"]},
        {"form": "pàrla", "tags": ["third-person", "indicative", "present", "singular"]},
        {
            "form": "parliàmo",
            "tags": ["first-person", "indicative", "present", "plural"],
        },
    ],
    "senses": [{"glosses": ["to speak"], "tags": ["intransitive"]}],
}


def _create_test_jsonl(entries: list[dict[str, Any]]) -> Path:
    """Create a temporary JSONL file with test entries."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
        return Path(f.name)


def _create_test_morphit(lines: list[str]) -> Path:
    """Create a temporary Morph-it! file with test entries."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        return Path(f.name)


class TestMorphitImporter:
    """Tests for the Morph-it! importer."""

    def test_updates_forms_with_real_spelling(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        morphit_path = _create_test_morphit(
            [
                "parlo\tparlare\tVER:ind+pres+1+s",
                "parli\tparlare\tVER:ind+pres+2+s",
                "parla\tparlare\tVER:ind+pres+3+s",
                "parliamo\tparlare\tVER:ind+pres+1+p",
                "parlare\tparlare\tVER:inf+pres",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import Wiktextract data
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Then enrich with Morph-it!
            with get_connection(db_path) as conn:
                stats = import_morphit(conn, morphit_path)

            # Check stats
            assert stats["updated"] > 0, "Should have updated some forms"
            assert stats["not_found"] >= 0

            # Check that forms now have real spelling
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(verb_forms).where(verb_forms.c.form.isnot(None))
                ).fetchall()

                assert len(form_rows) > 0, "Should have forms with real spelling"

                # Check specific forms
                for row in form_rows:
                    # Real form should not have stress marks
                    assert "à" not in row.form
                    assert "ò" not in row.form
                    # Stressed form should have marks
                    assert row.form_stressed is not None

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_forms_not_in_morphit_remain_null(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        # Morph-it! with only some forms
        morphit_path = _create_test_morphit(
            [
                "parlo\tparlare\tVER:ind+pres+1+s",
                # Missing: parli, parla, parliamo
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_morphit(conn, morphit_path)

            # Should have some not found
            assert stats["not_found"] > 0

            # Check that some forms still have NULL form
            with get_connection(db_path) as conn:
                null_forms = conn.execute(
                    select(verb_forms).where(verb_forms.c.form.is_(None))
                ).fetchall()
                assert len(null_forms) > 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_skips_non_verbs_in_morphit(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        # Morph-it! with nouns (should be ignored)
        morphit_path = _create_test_morphit(
            [
                "casa\tcasa\tNOUN-F:s",
                "case\tcasa\tNOUN-F:p",
                "parlo\tparlare\tVER:ind+pres+1+s",  # Only verb entry
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_morphit(conn, morphit_path)

            # Should have updated at least one form
            assert stats["updated"] >= 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_adds_lookup_entries(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        morphit_path = _create_test_morphit(
            [
                "parlo\tparlare\tVER:ind+pres+1+s",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)
                # Count lookup entries before
                before_count = conn.execute(select(form_lookup_new)).fetchall()

            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path)

            with get_connection(db_path) as conn:
                # Count lookup entries after
                after_count = conn.execute(select(form_lookup_new)).fetchall()

                # Should have at least as many entries as before
                assert len(after_count) >= len(before_count)

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_handles_empty_morphit_file(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_morphit(conn, morphit_path)

            # All forms should be not found
            assert stats["updated"] == 0
            assert stats["not_found"] > 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_idempotent_when_run_twice(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        morphit_path = _create_test_morphit(
            [
                "parlo\tparlare\tVER:ind+pres+1+s",
                "parli\tparlare\tVER:ind+pres+2+s",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # First enrichment
            with get_connection(db_path) as conn:
                stats1 = import_morphit(conn, morphit_path)

            # Second enrichment should update 0 (forms already have values)
            with get_connection(db_path) as conn:
                stats2 = import_morphit(conn, morphit_path)

            assert stats1["updated"] > 0
            assert stats2["updated"] == 0  # No more NULL forms to update

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()
