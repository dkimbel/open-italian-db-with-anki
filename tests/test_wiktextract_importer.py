"""Tests for Wiktextract importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_anki.db import (
    adjective_forms,
    definitions,
    form_lookup,
    get_connection,
    get_engine,
    init_db,
    lemmas,
    noun_forms,
    noun_metadata,
    sentence_lemmas,
    verb_forms,
    verb_metadata,
)
from italian_anki.importers.tatoeba import import_tatoeba
from italian_anki.importers.wiktextract import (
    enrich_form_spelling_from_form_of,
    enrich_from_form_of,
    import_wiktextract,
)

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

# Sample noun entry without gender (should be filtered out)
SAMPLE_NOUN_NO_GENDER = {
    "pos": "noun",
    "word": "acronimo",
    # No categories containing gender info
    "categories": ["Italian lemmas"],
    "forms": [
        {"form": "acronimo", "tags": ["canonical", "singular"]},
        {"form": "acronimi", "tags": ["plural"]},
    ],
    "senses": [
        # No gender tags
        {"glosses": ["acronym"]},
    ],
    "sounds": [{"ipa": "/aˈkrɔ.ni.mo/"}],  # noqa: RUF001 (IPA stress marker)
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


def _create_test_csv(lines: list[str]) -> Path:
    """Create a temporary CSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
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
                lookup_before = len(conn.execute(select(form_lookup)).fetchall())

            # Second import
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Counts should be the same (not doubled)
            with get_connection(db_path) as conn:
                forms_after = len(conn.execute(select(verb_forms)).fetchall())
                lookup_after = len(conn.execute(select(form_lookup)).fetchall())

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

            with get_connection(db_path) as conn:
                # Check masculine noun
                libro = conn.execute(select(lemmas).where(lemmas.c.lemma == "libro")).fetchone()
                assert libro is not None
                assert libro.pos == "noun"

                # Gender is now stored per-form in noun_forms
                libro_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == libro.lemma_id)
                ).fetchall()
                assert len(libro_forms) >= 1
                # Check that forms have gender
                assert all(f.gender == "m" for f in libro_forms)
                # Check that articles are computed
                libro_sing = [f for f in libro_forms if f.number == "singular"]
                assert len(libro_sing) >= 1
                assert libro_sing[0].def_article == "il"  # il libro
                assert libro_sing[0].article_source == "inferred"

                # Check feminine noun
                casa = conn.execute(select(lemmas).where(lemmas.c.lemma == "casa")).fetchone()
                assert casa is not None

                casa_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == casa.lemma_id)
                ).fetchall()
                assert len(casa_forms) >= 1
                assert all(f.gender == "f" for f in casa_forms)
                # Check feminine articles
                casa_sing = [f for f in casa_forms if f.number == "singular"]
                assert len(casa_sing) >= 1
                assert casa_sing[0].def_article == "la"  # la casa
                assert casa_sing[0].article_source == "inferred"

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

                # Check articles are computed for adjectives
                bello_form = next(f for f in form_rows if f.form_stressed == "bello")
                assert bello_form.def_article == "il"  # il bello
                assert bello_form.article_source == "inferred"

                bella_form = next(f for f in form_rows if f.form_stressed == "bella")
                assert bella_form.def_article == "la"  # la bella
                assert bella_form.article_source == "inferred"

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

    def test_idempotent_after_tatoeba(self) -> None:
        """Verify reimport works after tatoeba populates sentence_lemmas (FK constraint)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        ita_path = _create_test_tsv(["100\tita\tIo parlo italiano."])
        eng_path = _create_test_tsv(["200\teng\tI speak Italian."])
        links_path = _create_test_csv(["100\t200"])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First: import wiktextract
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Then: import tatoeba (creates sentence_lemmas links)
            with get_connection(db_path) as conn:
                import_tatoeba(conn, ita_path, eng_path, links_path)

            # Verify sentence_lemmas was populated
            with get_connection(db_path) as conn:
                sl_count = len(conn.execute(select(sentence_lemmas)).fetchall())
                assert sl_count > 0, "Tatoeba should have created sentence_lemmas"

            # Re-import wiktextract (must clear sentence_lemmas first - tests FK)
            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path)

            assert stats["cleared"] == 1
            assert stats["lemmas"] == 1  # Still have our verb

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            ita_path.unlink()
            eng_path.unlink()
            links_path.unlink()

    def test_filters_noun_without_gender(self) -> None:
        """Test that nouns without gender are filtered out and counted."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Include both nouns with gender and one without
        jsonl_path = _create_test_jsonl(
            [SAMPLE_NOUN_MASCULINE, SAMPLE_NOUN_FEMININE, SAMPLE_NOUN_NO_GENDER]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            # Two nouns with gender should be imported
            assert stats["lemmas"] == 3  # All 3 lemmas are created
            assert stats["nouns_without_gender"] == 1  # One noun lacks gender

            with get_connection(db_path) as conn:
                # The noun without gender should have no forms
                acronimo = conn.execute(
                    select(lemmas).where(lemmas.c.lemma == "acronimo")
                ).fetchone()
                assert acronimo is not None  # Lemma exists

                acronimo_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == acronimo.lemma_id)
                ).fetchall()
                assert len(acronimo_forms) == 0  # But no forms (filtered out)

                # Nouns with gender should have forms
                libro_forms = conn.execute(
                    select(noun_forms).join(lemmas).where(lemmas.c.lemma == "libro")
                ).fetchall()
                assert len(libro_forms) > 0
                assert all(f.gender is not None for f in libro_forms)

        finally:
            db_path.unlink()
            jsonl_path.unlink()


class TestEnrichFormSpellingFromFormOf:
    """Tests for the form-of spelling fallback enrichment."""

    def test_fills_form_from_formof_entry(self) -> None:
        """Test that form column is filled from form-of entry when form is NULL."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Lemma entry with stressed forms
        lemma_entry = {
            "pos": "verb",
            "word": "parlare",
            "forms": [
                {"form": "parlàre", "tags": ["canonical"]},
                {"form": "parlàre", "tags": ["infinitive"]},
                {"form": "pàrlo", "tags": ["first-person", "indicative", "present", "singular"]},
            ],
            "senses": [{"glosses": ["to speak"]}],
        }

        # Form-of entry with unaccented spelling
        formof_entry = {
            "pos": "verb",
            "word": "parlo",  # Unaccented form
            "senses": [
                {
                    "glosses": ["first-person singular present indicative of parlare"],
                    "tags": ["form-of", "first-person", "indicative", "present", "singular"],
                    "form_of": [{"word": "parlare"}],
                }
            ],
        }

        jsonl_path = _create_test_jsonl([lemma_entry, formof_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # Import Wiktextract (form column will be NULL)
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Verify form is NULL before enrichment
            with get_connection(db_path) as conn:
                form_row = conn.execute(
                    select(verb_forms).where(verb_forms.c.form_stressed == "pàrlo")
                ).fetchone()
                assert form_row is not None
                assert form_row.form is None

            # Run form-of spelling enrichment
            with get_connection(db_path) as conn:
                stats = enrich_form_spelling_from_form_of(conn, jsonl_path)

            assert stats["updated"] > 0

            # Verify form is now filled
            with get_connection(db_path) as conn:
                form_row = conn.execute(
                    select(verb_forms).where(verb_forms.c.form_stressed == "pàrlo")
                ).fetchone()
                assert form_row is not None
                assert form_row.form == "parlo"
                assert form_row.form_source == "wiktionary"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_does_not_overwrite_morphit_spelling(self) -> None:
        """Test that form-of doesn't overwrite forms already filled by Morph-it!."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        lemma_entry = {
            "pos": "verb",
            "word": "parlare",
            "forms": [
                {"form": "parlàre", "tags": ["canonical"]},
                {"form": "pàrlo", "tags": ["first-person", "indicative", "present", "singular"]},
            ],
            "senses": [{"glosses": ["to speak"]}],
        }

        formof_entry = {
            "pos": "verb",
            "word": "parlo",
            "senses": [
                {
                    "form_of": [{"word": "parlare"}],
                    "tags": ["form-of"],
                }
            ],
        }

        jsonl_path = _create_test_jsonl([lemma_entry, formof_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Simulate Morph-it! having already filled the form
            with get_connection(db_path) as conn:
                from sqlalchemy import update

                conn.execute(
                    update(verb_forms)
                    .where(verb_forms.c.form_stressed == "pàrlo")
                    .values(form="parlo", form_source="morphit")
                )
                conn.commit()

            # Run form-of enrichment
            with get_connection(db_path) as conn:
                stats = enrich_form_spelling_from_form_of(conn, jsonl_path)

            # Should not have updated anything (already filled)
            assert stats["updated"] == 0
            assert stats["already_filled"] > 0

            # Verify form_source is still "morphit"
            with get_connection(db_path) as conn:
                form_row = conn.execute(
                    select(verb_forms).where(verb_forms.c.form_stressed == "pàrlo")
                ).fetchone()
                assert form_row is not None
                assert form_row.form_source == "morphit"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_handles_missing_lemma(self) -> None:
        """Test that form-of entries referencing missing lemmas are counted as not_found."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Only a form-of entry, no lemma
        formof_entry = {
            "pos": "verb",
            "word": "parlo",
            "senses": [
                {
                    "form_of": [{"word": "parlare"}],  # This lemma doesn't exist
                    "tags": ["form-of"],
                }
            ],
        }

        jsonl_path = _create_test_jsonl([formof_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = enrich_form_spelling_from_form_of(conn, jsonl_path)

            # Should count as not found since lemma doesn't exist
            assert stats["not_found"] > 0
            assert stats["updated"] == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()


# Sample common gender variable noun (collega) - different plural forms by gender
# Real Wiktextract pattern: gender marker in args["1"], only plurals in forms array
SAMPLE_NOUN_COMMON_GENDER_VARIABLE = {
    "pos": "noun",
    "word": "collega",
    "head_templates": [{"args": {"1": "mf"}}],  # Gender in position 1
    "categories": ["Italian lemmas"],
    "forms": [
        # Real data: only plural forms, singular comes from word field
        {"form": "colleghi", "tags": ["masculine", "plural"]},
        {"form": "colleghe", "tags": ["feminine", "plural"]},
    ],
    "senses": [
        {"glosses": ["colleague"], "tags": []},
    ],
}

# Sample common gender fixed noun (cantante) - same form for both genders
# Real Wiktextract pattern: mfbysense in args, only plural in forms array
SAMPLE_NOUN_COMMON_GENDER_FIXED = {
    "pos": "noun",
    "word": "cantante",
    "head_templates": [{"args": {"1": "mfbysense"}}],  # Gender in position 1
    "categories": ["Italian lemmas"],
    "forms": [
        # Real data: only plural form, singular comes from word field
        {"form": "cantanti", "tags": ["plural"]},
    ],
    "senses": [
        {"glosses": ["singer"], "tags": []},
    ],
}

# Sample pluralia tantum noun (forbici)
# Real Wiktextract pattern: f-p in args, EMPTY forms array!
SAMPLE_NOUN_PLURALIA_TANTUM = {
    "pos": "noun",
    "word": "forbici",
    "head_templates": [{"args": {"1": "f-p"}}],  # f-p = feminine pluralia tantum
    "categories": ["Italian pluralia tantum", "Italian lemmas"],
    "forms": [],  # Real data: empty forms array! Plural form is the word field itself.
    "senses": [
        {"glosses": ["scissors"], "tags": ["feminine"]},
    ],
}

# Sample invariable noun (città)
# Real Wiktextract pattern: gender in args, # marker for invariable
SAMPLE_NOUN_INVARIABLE = {
    "pos": "noun",
    "word": "città",
    "head_templates": [{"args": {"1": "f", "2": "#"}}],  # f = feminine, # = invariable
    "categories": ["Italian indeclinable nouns", "Italian lemmas", "Italian feminine nouns"],
    "forms": [
        # Real data may have archaic alternatives but same form for both numbers
        {"form": "città", "tags": ["singular"]},
        {"form": "città", "tags": ["plural"]},  # Same form
    ],
    "senses": [
        {"glosses": ["city", "town"], "tags": ["feminine"]},
    ],
}


class TestNounClassification:
    """Tests for noun classification and noun_metadata."""

    def test_common_gender_variable_generates_both_genders(self) -> None:
        """Test that common gender variable nouns generate M/F singular forms."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_COMMON_GENDER_VARIABLE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 1

            with get_connection(db_path) as conn:
                # Check lemma
                collega = conn.execute(select(lemmas).where(lemmas.c.lemma == "collega")).fetchone()
                assert collega is not None

                # Check noun_metadata
                meta = conn.execute(
                    select(noun_metadata).where(noun_metadata.c.lemma_id == collega.lemma_id)
                ).fetchone()
                assert meta is not None
                assert meta.gender_class == "common_gender_variable"
                assert meta.number_class == "standard"

                # Check forms - should have 4 forms: M/F singular, M/F plural
                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == collega.lemma_id)
                ).fetchall()
                assert len(forms) >= 4

                # Check we have both genders for singular
                sing_forms = [f for f in forms if f.number == "singular"]
                sing_genders = {f.gender for f in sing_forms}
                assert "m" in sing_genders
                assert "f" in sing_genders

                # Check plurals have explicit gender
                plural_forms = [f for f in forms if f.number == "plural"]
                plural_genders = {f.gender for f in plural_forms}
                assert "m" in plural_genders
                assert "f" in plural_genders

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_common_gender_fixed_generates_both_genders(self) -> None:
        """Test that mfbysense nouns generate M/F forms with same text."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_COMMON_GENDER_FIXED])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 1

            with get_connection(db_path) as conn:
                # Check lemma
                cantante = conn.execute(
                    select(lemmas).where(lemmas.c.lemma == "cantante")
                ).fetchone()
                assert cantante is not None

                # Check noun_metadata - mfbysense is detected from args
                meta = conn.execute(
                    select(noun_metadata).where(noun_metadata.c.lemma_id == cantante.lemma_id)
                ).fetchone()
                assert meta is not None
                assert meta.gender_class == "mfbysense"

                # Check forms - should have M/F singular and M/F plural
                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == cantante.lemma_id)
                ).fetchall()
                assert len(forms) >= 4

                # Check both genders exist for singular
                sing_forms = [f for f in forms if f.number == "singular"]
                sing_genders = {f.gender for f in sing_forms}
                assert "m" in sing_genders
                assert "f" in sing_genders

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_pluralia_tantum_classified_correctly(self) -> None:
        """Test that pluralia tantum nouns are correctly classified."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_PLURALIA_TANTUM])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 1

            with get_connection(db_path) as conn:
                # Check lemma
                forbici = conn.execute(select(lemmas).where(lemmas.c.lemma == "forbici")).fetchone()
                assert forbici is not None

                # Check noun_metadata
                meta = conn.execute(
                    select(noun_metadata).where(noun_metadata.c.lemma_id == forbici.lemma_id)
                ).fetchone()
                assert meta is not None
                assert meta.gender_class == "f"
                assert meta.number_class == "pluralia_tantum"

                # Check forms - should only have plural form
                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == forbici.lemma_id)
                ).fetchall()
                assert len(forms) >= 1
                assert all(f.number == "plural" for f in forms)

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_invariable_noun_classified_correctly(self) -> None:
        """Test that invariable nouns are correctly classified."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_INVARIABLE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 1

            with get_connection(db_path) as conn:
                # Check lemma
                citta = conn.execute(select(lemmas).where(lemmas.c.lemma == "citta")).fetchone()
                assert citta is not None

                # Check noun_metadata
                meta = conn.execute(
                    select(noun_metadata).where(noun_metadata.c.lemma_id == citta.lemma_id)
                ).fetchone()
                assert meta is not None
                assert meta.gender_class == "f"
                assert meta.number_class == "invariable"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_noun_metadata_cleared_on_reimport(self) -> None:
        """Test that noun_metadata is cleared on reimport."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_MASCULINE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # First import
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="noun")

            # Check metadata exists
            with get_connection(db_path) as conn:
                meta_count = len(conn.execute(select(noun_metadata)).fetchall())
                assert meta_count == 1

            # Second import (should clear and reimport)
            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["cleared"] == 1

            # Check we still have exactly one metadata entry
            with get_connection(db_path) as conn:
                meta_count = len(conn.execute(select(noun_metadata)).fetchall())
                assert meta_count == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_counterpart_marker_detects_feminine(self) -> None:
        """Test that 'f': '+' in head_templates marks noun as having feminine forms."""
        # This matches real Wiktextract data for "amico" which has "f": "+"
        sample_amico = {
            "pos": "noun",
            "word": "amico",
            "head_templates": [{"args": {"1": "m", "f": "+"}}],
            "categories": ["Italian lemmas"],
            "forms": [
                {"form": "amici", "tags": ["plural"]},
                {"form": "amica", "tags": ["feminine"]},  # No number tag!
                {"form": "amiche", "tags": ["feminine", "plural"]},
            ],
            "senses": [{"glosses": ["friend"], "tags": []}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([sample_amico])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 1

            with get_connection(db_path) as conn:
                # Check lemma
                amico = conn.execute(select(lemmas).where(lemmas.c.lemma == "amico")).fetchone()
                assert amico is not None

                # Check noun_metadata - should detect both genders from "f": "+"
                meta = conn.execute(
                    select(noun_metadata).where(noun_metadata.c.lemma_id == amico.lemma_id)
                ).fetchone()
                assert meta is not None
                assert meta.gender_class == "common_gender_variable"

                # Check forms - should have masculine and feminine forms
                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == amico.lemma_id)
                ).fetchall()

                # Check we have feminine singular form (amica)
                fem_sing = [f for f in forms if f.gender == "f" and f.number == "singular"]
                assert len(fem_sing) == 1, f"Expected 1 feminine singular, got {len(fem_sing)}"
                assert fem_sing[0].form_stressed == "amica"

                # Check we have feminine plural form (amiche)
                fem_plur = [f for f in forms if f.gender == "f" and f.number == "plural"]
                assert len(fem_plur) >= 1, f"Expected feminine plural, got {len(fem_plur)}"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_counterpart_lookup_provides_other_gender_plural(self) -> None:
        """Test that counterpart lookup correctly finds the other gender's plural.

        This tests the real-world case where "amico" doesn't have "amiche" in its
        forms array - it only has "amici" (untagged plural). The "amiche" form
        lives in the separate "amica" entry, and we look it up from there.
        """
        # amico entry: NO amiche in forms (matches real Wiktextract data structure)
        sample_amico = {
            "pos": "noun",
            "word": "amico",
            "head_templates": [{"args": {"1": "m", "f": "+"}}],
            "categories": ["Italian lemmas"],
            "forms": [
                {"form": "amici", "tags": ["plural"]},  # Untagged - belongs to masculine
                {"form": "amica", "tags": ["feminine"]},  # Feminine counterpart
            ],
            "senses": [{"glosses": ["friend"], "tags": []}],
        }

        # amica entry: has "amiche" as its plural (this is what we look up)
        sample_amica = {
            "pos": "noun",
            "word": "amica",
            "head_templates": [{"args": {"1": "f", "m": "+"}}],
            "categories": ["Italian lemmas"],
            "forms": [
                {"form": "amiche", "tags": ["plural"]},
                {"form": "amico", "tags": ["masculine"]},
            ],
            "senses": [{"glosses": ["female friend"], "tags": []}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Both entries in the JSONL - order matters for counterpart lookup
        jsonl_path = _create_test_jsonl([sample_amico, sample_amica])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            # Should import only 1 lemma (amico) - amica is the counterpart
            # (both would be imported as separate lemmas, but we check amico's forms)
            assert stats["lemmas"] == 2

            with get_connection(db_path) as conn:
                # Check amico's forms
                amico = conn.execute(select(lemmas).where(lemmas.c.lemma == "amico")).fetchone()
                assert amico is not None

                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == amico.lemma_id)
                ).fetchall()

                # Check masculine plural (amici)
                masc_plur = [f for f in forms if f.gender == "m" and f.number == "plural"]
                assert len(masc_plur) == 1, f"Expected 1 masculine plural, got {len(masc_plur)}"
                assert masc_plur[0].form_stressed == "amici"

                # Check feminine plural (amiche) - from counterpart lookup!
                fem_plur = [f for f in forms if f.gender == "f" and f.number == "plural"]
                assert len(fem_plur) == 1, f"Expected 1 feminine plural, got {len(fem_plur)}"
                assert fem_plur[0].form_stressed == "amiche"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_explicit_gender_plural_prevents_duplication(self) -> None:
        """Test that entries with explicit gender plurals don't duplicate untagged ones.

        For nouns like "dio" that have explicit feminine plural "dee", the untagged
        plurals "dei/dii" should only be used for masculine, not duplicated.
        """
        sample_dio = {
            "pos": "noun",
            "word": "dio",
            "head_templates": [{"args": {"1": "m", "f": "+"}}],
            "categories": ["Italian lemmas"],
            "forms": [
                {"form": "dei", "tags": ["plural"]},  # Untagged - should be masc only
                {"form": "dii", "tags": ["archaic", "dialectal", "plural"]},  # Also masc only
                {"form": "dea", "tags": ["feminine"]},  # Feminine singular
                {"form": "dee", "tags": ["feminine", "plural"]},  # Explicit feminine plural
            ],
            "senses": [{"glosses": ["god"], "tags": []}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([sample_dio])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 1

            with get_connection(db_path) as conn:
                dio = conn.execute(select(lemmas).where(lemmas.c.lemma == "dio")).fetchone()
                assert dio is not None

                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == dio.lemma_id)
                ).fetchall()

                # Check masculine plurals - should have both dei and dii
                masc_plur = [f for f in forms if f.gender == "m" and f.number == "plural"]
                masc_forms = {f.form_stressed for f in masc_plur}
                assert "dei" in masc_forms, f"Expected 'dei' in masculine plurals, got {masc_forms}"
                assert "dii" in masc_forms, f"Expected 'dii' in masculine plurals, got {masc_forms}"

                # Check feminine plural - should ONLY have dee, NOT dei/dii
                fem_plur = [f for f in forms if f.gender == "f" and f.number == "plural"]
                fem_forms = {f.form_stressed for f in fem_plur}
                assert fem_forms == {"dee"}, f"Expected only 'dee' for feminine, got {fem_forms}"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_stressed_alternatives_enriches_forms(self) -> None:
        """Test that unaccented forms get enriched with accented alternatives."""
        # Main lemma entry: dio with unaccented "dei" plural
        sample_dio = {
            "pos": "noun",
            "word": "dio",
            "head_templates": [{"args": {"1": "m"}}],
            "categories": ["Italian lemmas"],
            "forms": [
                {"form": "dei", "tags": ["plural"]},  # Unaccented
            ],
            "senses": [{"glosses": ["god"], "tags": []}],
        }

        # Form-of entry: "dei" with accented alternative "dèi"
        sample_dei_formof = {
            "pos": "noun",
            "word": "dei",
            "head_templates": [{"args": {"1": "it", "2": "noun form"}}],
            "categories": [],
            "forms": [
                {"form": "dèi", "tags": ["alternative"]},  # Accented alternative
            ],
            "senses": [{"form_of": [{"word": "dio"}], "glosses": ["plural of dio"]}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([sample_dio, sample_dei_formof])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="noun")

            assert stats["lemmas"] == 1  # Only dio is a lemma

            with get_connection(db_path) as conn:
                # Check the plural form is accented
                dio = conn.execute(select(lemmas).where(lemmas.c.lemma == "dio")).fetchone()
                assert dio is not None

                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == dio.lemma_id)
                ).fetchall()

                plural_forms = [f for f in forms if f.number == "plural"]
                assert len(plural_forms) >= 1

                # The plural should be the accented "dèi", not unaccented "dei"
                plural_stressed = [f.form_stressed for f in plural_forms]
                assert "dèi" in plural_stressed, f"Expected 'dèi' in {plural_stressed}"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
