"""Tests for Wiktextract importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_anki.db import (
    adjective_forms,
    definitions,
    form_lookup_new,
    get_connection,
    get_engine,
    init_db,
    lemmas,
    noun_metadata,
    verb_forms,
    verb_metadata,
)
from italian_anki.importers.wiktextract import enrich_from_form_of, import_wiktextract

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

# Sample form entry (should be skipped during main import)
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

# Sample form-of entry with label tag (for enrichment test)
SAMPLE_FORM_OF_WITH_LABEL = {
    "pos": "verb",
    "word": "pàrlo",  # Same form as in SAMPLE_VERB but as a form-of entry
    "senses": [
        {
            "glosses": ["first-person singular present indicative of parlare"],
            "tags": [
                "form-of",
                "first-person",
                "indicative",
                "present",
                "singular",
                "literary",  # Label tag
            ],
            "form_of": [{"word": "parlare"}],
        }
    ],
}

# Sample masculine noun entry
SAMPLE_NOUN_MASCULINE = {
    "pos": "noun",
    "word": "libro",
    "categories": ["Italian masculine nouns", "Italian lemmas"],
    "forms": [
        {"form": "libro", "tags": ["canonical", "singular"]},
        {"form": "libri", "tags": ["plural"]},
    ],
    "senses": [
        {"glosses": ["book"], "tags": ["masculine"]},
    ],
    "sounds": [{"ipa": "/ˈli.bro/"}],  # noqa: RUF001 (IPA stress marker)
}

# Sample feminine noun entry
SAMPLE_NOUN_FEMININE = {
    "pos": "noun",
    "word": "casa",
    "categories": ["Italian feminine nouns", "Italian lemmas"],
    "forms": [
        {"form": "casa", "tags": ["canonical", "singular"]},
        {"form": "case", "tags": ["plural"]},
    ],
    "senses": [
        {"glosses": ["house", "home"], "tags": ["feminine"]},
    ],
    "sounds": [{"ipa": "/ˈka.sa/"}],  # noqa: RUF001 (IPA stress marker)
}

# Sample adjective entry (Wiktextract uses "adj" for adjectives)
SAMPLE_ADJECTIVE = {
    "pos": "adj",
    "word": "bello",
    "forms": [
        {"form": "bello", "tags": ["canonical", "masculine", "singular"]},
        {"form": "bella", "tags": ["feminine", "singular"]},
        {"form": "belli", "tags": ["masculine", "plural"]},
        {"form": "belle", "tags": ["feminine", "plural"]},
    ],
    "senses": [
        {"glosses": ["beautiful", "handsome"]},
    ],
    "sounds": [{"ipa": "/ˈbɛl.lo/"}],  # noqa: RUF001 (IPA stress marker)
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
                assert row.ipa == "/par\u02c8la\u02d0re/"

                # Check verb_metadata was inserted
                meta = conn.execute(
                    select(verb_metadata).where(verb_metadata.c.lemma_id == row.lemma_id)
                ).fetchone()
                assert meta is not None
                assert meta.auxiliary == "avere"
                assert meta.transitivity == "both"

                # Check forms were inserted in verb_forms table
                lemma_id = row.lemma_id
                form_rows = conn.execute(
                    select(verb_forms).where(verb_forms.c.lemma_id == lemma_id)
                ).fetchall()
                assert len(form_rows) >= 3  # At least infinitive + some conjugations

                # Check definitions were inserted
                def_rows = conn.execute(
                    select(definitions).where(definitions.c.lemma_id == lemma_id)
                ).fetchall()
                assert len(def_rows) == 2

                # Check form_lookup_new was populated
                lookup_rows = conn.execute(select(form_lookup_new)).fetchall()
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

    def test_idempotent_when_run_twice(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import
            with get_connection(db_path) as conn:
                stats1 = import_wiktextract(conn, jsonl_path)

            assert stats1["lemmas"] == 1
            assert stats1["cleared"] == 0  # Nothing to clear on first run

            # Second import (should clear and reimport)
            with get_connection(db_path) as conn:
                stats2 = import_wiktextract(conn, jsonl_path)

            assert stats2["lemmas"] == 1
            assert stats2["cleared"] == 1  # Cleared the previous import

            # Verify we still have exactly one lemma (not duplicates)
            with get_connection(db_path) as conn:
                lemma_count = len(conn.execute(select(lemmas)).fetchall())
                form_count = len(conn.execute(select(verb_forms)).fetchall())
                def_count = len(conn.execute(select(definitions)).fetchall())

            assert lemma_count == 1
            assert form_count == stats2["forms"]
            assert def_count == stats2["definitions"]

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_clears_related_data(self) -> None:
        """Verify that forms, definitions, and lookup are cleared on reimport."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Get counts after first import
            with get_connection(db_path) as conn:
                forms_before = len(conn.execute(select(verb_forms)).fetchall())
                lookup_before = len(conn.execute(select(form_lookup_new)).fetchall())

            # Second import
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Counts should be the same (not doubled)
            with get_connection(db_path) as conn:
                forms_after = len(conn.execute(select(verb_forms)).fetchall())
                lookup_after = len(conn.execute(select(form_lookup_new)).fetchall())

            assert forms_after == forms_before
            assert lookup_after == lookup_before

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_imports_noun_with_gender(self) -> None:
        """Test importing nouns with gender metadata."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_MASCULINE, SAMPLE_NOUN_FEMININE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 2
            assert stats["nouns_with_gender"] == 2
            assert stats["nouns_no_gender"] == 0

            with get_connection(db_path) as conn:
                # Check masculine noun
                libro = conn.execute(select(lemmas).where(lemmas.c.lemma == "libro")).fetchone()
                assert libro is not None
                assert libro.pos == "noun"

                libro_gender = conn.execute(
                    select(noun_metadata).where(noun_metadata.c.lemma_id == libro.lemma_id)
                ).fetchone()
                assert libro_gender is not None
                assert libro_gender.gender == "m"

                # Check feminine noun
                casa = conn.execute(select(lemmas).where(lemmas.c.lemma == "casa")).fetchone()
                assert casa is not None

                casa_gender = conn.execute(
                    select(noun_metadata).where(noun_metadata.c.lemma_id == casa.lemma_id)
                ).fetchone()
                assert casa_gender is not None
                assert casa_gender.gender == "f"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_imports_adjective(self) -> None:
        """Test importing adjectives with all gender/number forms."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            assert stats["lemmas"] == 1
            assert stats["forms"] >= 4  # 4 forms (canonical kept for adjectives + gender/number)

            with get_connection(db_path) as conn:
                # Check adjective lemma
                bello = conn.execute(select(lemmas).where(lemmas.c.lemma == "bello")).fetchone()
                assert bello is not None
                assert bello.pos == "adjective"
                assert bello.ipa == "/ˈbɛl.lo/"  # noqa: RUF001 (IPA stress marker)

                # Check forms were inserted in adjective_forms table
                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == bello.lemma_id)
                ).fetchall()
                form_texts = [row.form_stressed for row in form_rows]
                assert "bello" in form_texts  # canonical kept for adjectives
                assert "bella" in form_texts
                assert "belli" in form_texts
                assert "belle" in form_texts

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_pos_filter_isolates_data(self) -> None:
        """Verify that different POS imports don't affect each other."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Create JSONL with verb, noun, and adjective
        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_NOUN_MASCULINE, SAMPLE_ADJECTIVE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # Import verb
            with get_connection(db_path) as conn:
                verb_stats = import_wiktextract(conn, jsonl_path, pos_filter="verb")
            assert verb_stats["lemmas"] == 1

            # Import noun
            with get_connection(db_path) as conn:
                noun_stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")
            assert noun_stats["lemmas"] == 1
            assert noun_stats["cleared"] == 0  # No nouns to clear from first import

            # Import adjective
            with get_connection(db_path) as conn:
                adj_stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")
            assert adj_stats["lemmas"] == 1
            assert adj_stats["cleared"] == 0

            # Verify all three exist
            with get_connection(db_path) as conn:
                total_lemmas = len(conn.execute(select(lemmas)).fetchall())
                assert total_lemmas == 3

                verb_count = len(
                    conn.execute(select(lemmas).where(lemmas.c.pos == "verb")).fetchall()
                )
                noun_count = len(
                    conn.execute(select(lemmas).where(lemmas.c.pos == "noun")).fetchall()
                )
                adj_count = len(
                    conn.execute(select(lemmas).where(lemmas.c.pos == "adjective")).fetchall()
                )

                assert verb_count == 1
                assert noun_count == 1
                assert adj_count == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_enrich_from_form_of_applies_labels(self) -> None:
        """Test that form-of entries with label tags update existing forms."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # JSONL with lemma and form-of entry that has a label tag
        jsonl_path = _create_test_jsonl([SAMPLE_VERB, SAMPLE_FORM_OF_WITH_LABEL])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First, import the lemma
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Verify form exists without labels
            with get_connection(db_path) as conn:
                parlare = conn.execute(select(lemmas).where(lemmas.c.lemma == "parlare")).fetchone()
                assert parlare is not None

                # Find the first-person singular form
                form_row = conn.execute(
                    select(verb_forms).where(
                        verb_forms.c.lemma_id == parlare.lemma_id,
                        verb_forms.c.person == 1,
                        verb_forms.c.number == "singular",
                        verb_forms.c.mood == "indicative",
                        verb_forms.c.tense == "present",
                    )
                ).fetchone()
                assert form_row is not None
                assert form_row.labels is None  # No labels yet

            # Now enrich from form-of entries
            with get_connection(db_path) as conn:
                stats = enrich_from_form_of(conn, jsonl_path)

            assert stats["scanned"] >= 1
            assert stats["updated"] >= 1

            # Verify labels was applied
            with get_connection(db_path) as conn:
                parlare = conn.execute(select(lemmas).where(lemmas.c.lemma == "parlare")).fetchone()
                assert parlare is not None

                form_row = conn.execute(
                    select(verb_forms).where(
                        verb_forms.c.lemma_id == parlare.lemma_id,
                        verb_forms.c.person == 1,
                        verb_forms.c.number == "singular",
                        verb_forms.c.mood == "indicative",
                        verb_forms.c.tense == "present",
                    )
                ).fetchone()
                assert form_row is not None
                assert form_row.labels == "literary"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
