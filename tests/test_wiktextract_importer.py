"""Tests for Wiktextract importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_anki.db import (
    definitions,
    form_lookup,
    forms,
    get_connection,
    get_engine,
    init_db,
    lemmas,
)
from italian_anki.importers.wiktextract import import_wiktextract

# Sample verb entry from Wiktextract
SAMPLE_VERB = {
    "pos": "verb",
    "word": "parlare",
    "forms": [
        {"form": "parlàre", "tags": ["canonical"]},
        {"form": "parlàre", "tags": ["infinitive"], "source": "conjugation"},
        {"form": "avére", "tags": ["auxiliary"], "source": "conjugation"},
        {
            "form": "pàrlo",
            "tags": ["first-person", "indicative", "present", "singular"],
            "source": "conjugation",
        },
        {
            "form": "pàrli",
            "tags": ["second-person", "indicative", "present", "singular"],
            "source": "conjugation",
        },
        {
            "form": "pàrla",
            "tags": ["third-person", "indicative", "present", "singular"],
            "source": "conjugation",
        },
    ],
    "senses": [
        {"glosses": ["to speak", "to talk"], "tags": ["intransitive"]},
        {"glosses": ["to discuss"], "tags": ["transitive"]},
    ],
    "sounds": [{"ipa": "/par\u02c8la\u02d0re/"}],
}

# Sample form entry (should be skipped)
SAMPLE_FORM_ENTRY = {
    "pos": "verb",
    "word": "parlo",
    "senses": [
        {
            "glosses": ["first-person singular present indicative of parlare"],
            "tags": ["form-of", "first-person", "indicative", "present", "singular"],
            "form_of": [{"word": "parlare"}],
        }
    ],
}


def _create_test_jsonl(entries: list[dict[str, Any]]) -> Path:
    """Create a temporary JSONL file with test entries."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
        return Path(f.name)


class TestWiktextractImporter:
    """Tests for the Wiktextract importer."""

    def test_imports_verb_lemma(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path)

            assert stats["lemmas"] == 1
            assert stats["forms"] > 0
            assert stats["definitions"] == 2

            with get_connection(db_path) as conn:
                # Check lemma was inserted
                row = conn.execute(select(lemmas).where(lemmas.c.lemma == "parlare")).fetchone()
                assert row is not None
                assert row.lemma_stressed == "parlàre"
                assert row.auxiliary == "avere"
                assert row.transitivity == "both"
                assert row.ipa == "/par\u02c8la\u02d0re/"

                # Check forms were inserted
                lemma_id = row.lemma_id
                form_rows = conn.execute(
                    select(forms).where(forms.c.lemma_id == lemma_id)
                ).fetchall()
                assert len(form_rows) >= 3  # At least infinitive + some conjugations

                # Check definitions were inserted
                def_rows = conn.execute(
                    select(definitions).where(definitions.c.lemma_id == lemma_id)
                ).fetchall()
                assert len(def_rows) == 2

                # Check form_lookup was populated
                lookup_rows = conn.execute(select(form_lookup)).fetchall()
                assert len(lookup_rows) > 0
        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_skips_form_entries(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_FORM_ENTRY])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path)

            assert stats["lemmas"] == 0
            assert stats["skipped"] == 1
        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_skips_non_verbs(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        noun_entry = {"pos": "noun", "word": "casa", "senses": [{"glosses": ["house"]}]}
        jsonl_path = _create_test_jsonl([noun_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path)

            assert stats["lemmas"] == 0
        finally:
            db_path.unlink()
            jsonl_path.unlink()
