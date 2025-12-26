"""Tests for Wiktextract importer."""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_anki.db import (
    adjective_forms,
    adjective_metadata,
    definitions,
    form_lookup,
    get_connection,
    get_engine,
    init_db,
    lemmas,
    noun_forms,
    noun_metadata,
    verb_forms,
    verb_metadata,
)
from italian_anki.importers.tatoeba import import_tatoeba
from italian_anki.importers.wiktextract import (
    _is_alt_form_entry,  # pyright: ignore[reportPrivateUsage]
    enrich_form_spelling_from_form_of,
    enrich_from_form_of,
    import_adjective_allomorphs,
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

# Adjective with incomplete tags (missing "singular" on feminine form)
# This matches real Wiktextract data where forms like {"form": "alta", "tags": ["feminine"]}
# are common - the importer should infer "singular"
SAMPLE_ADJECTIVE_INCOMPLETE_TAGS = {
    "pos": "adj",
    "word": "alto",
    "forms": [
        {"form": "alta", "tags": ["feminine"]},  # Missing "singular" - should be inferred
        {"form": "alti", "tags": ["masculine", "plural"]},
        {"form": "alte", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["tall", "high"]}],
}

# Two-form adjective where forms have number but no gender
# (e.g., {"form": "facili", "tags": ["plural"]} needs both masculine and feminine rows)
SAMPLE_ADJECTIVE_TWO_FORM = {
    "pos": "adj",
    "word": "facile",
    "forms": [
        {"form": "facili", "tags": ["plural"]},  # No gender - should generate m.pl AND f.pl
    ],
    "senses": [{"glosses": ["easy"]}],
}

# Invariable adjective - same form for all gender/number combinations (blu, rosa)
# The inv:1 flag in head_templates signals this
SAMPLE_ADJECTIVE_INVARIABLE = {
    "pos": "adj",
    "word": "blu",
    "head_templates": [
        {"name": "it-adj", "args": {"inv": "1"}},  # inv:1 marks invariable
    ],
    "forms": [],  # No explicit forms - all 4 should be generated from lemma
    "senses": [{"glosses": ["blue"]}],
}

# Two-form adjective detected via "m or f by sense" in head_templates expansion
# (e.g., ottimista, belga, pessimista - forms have gender tags but the adjective
# is still 2-form because singular is shared for both genders)
SAMPLE_ADJECTIVE_TWO_FORM_BY_SENSE: dict[str, Any] = {
    "pos": "adj",
    "word": "ottimista",
    "head_templates": [
        {
            "name": "it-adj",
            "expansion": "ottimista (m or f by sense, plural ottimisti or ottimiste)",
            "args": {},
        }
    ],
    "forms": [
        # Note: gender-tagged plurals - the old logic would miss the 2-form detection
        {"form": "ottimisti", "tags": ["masculine", "plural"]},
        {"form": "ottimiste", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["optimistic"]}],
}

# Sample misspelling entry (should be filtered out)
SAMPLE_MISSPELLING_ADJ: dict[str, Any] = {
    "pos": "adj",
    "word": "metereologico",  # Common misspelling of "meteorologico"
    "senses": [{"tags": ["misspelling"], "glosses": ["Misspelling of meteorologico."]}],
}

# Sample superlative adjective with hardcoded mapping (pessimo -> cattivo)
SAMPLE_ADJECTIVE_SUPERLATIVE: dict[str, Any] = {
    "pos": "adj",
    "word": "pessimo",
    "forms": [
        {"form": "pessimo", "tags": ["masculine", "singular"]},
        {"form": "pessima", "tags": ["feminine", "singular"]},
        {"form": "pessimi", "tags": ["masculine", "plural"]},
        {"form": "pessime", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["worst"]}],
}

# Sample base adjective (cattivo) for superlative linking
SAMPLE_ADJECTIVE_CATTIVO: dict[str, Any] = {
    "pos": "adj",
    "word": "cattivo",
    "forms": [
        {"form": "cattivo", "tags": ["masculine", "singular"]},
        {"form": "cattiva", "tags": ["feminine", "singular"]},
        {"form": "cattivi", "tags": ["masculine", "plural"]},
        {"form": "cattive", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["bad"]}],
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
                row = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "parlare")
                ).fetchone()
                assert row is not None
                assert row.stressed == "parlàre"
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
                libro = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "libro")
                ).fetchone()
                assert libro is not None
                assert libro.pos == "noun"

                # Gender is now stored per-form in noun_forms
                libro_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == libro.lemma_id)
                ).fetchall()
                assert len(libro_forms) >= 1
                # Check that forms have gender
                assert all(f.gender == "masculine" for f in libro_forms)
                # Check that articles are computed
                libro_sing = [f for f in libro_forms if f.number == "singular"]
                assert len(libro_sing) >= 1
                assert libro_sing[0].def_article == "il"  # il libro
                assert libro_sing[0].article_source == "inferred"

                # Check feminine noun
                casa = conn.execute(select(lemmas).where(lemmas.c.normalized == "casa")).fetchone()
                assert casa is not None

                casa_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == casa.lemma_id)
                ).fetchall()
                assert len(casa_forms) >= 1
                assert all(f.gender == "feminine" for f in casa_forms)
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
                bello = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "bello")
                ).fetchone()
                assert bello is not None
                assert bello.pos == "adjective"
                assert bello.ipa == "/ˈbɛl.lo/"  # noqa: RUF001 (IPA stress marker)

                # Check forms were inserted in adjective_forms table
                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == bello.lemma_id)
                ).fetchall()
                form_texts = [row.stressed for row in form_rows]
                assert "bello" in form_texts  # canonical kept for adjectives
                assert "bella" in form_texts
                assert "belli" in form_texts
                assert "belle" in form_texts

                # Check articles are computed for adjectives
                bello_form = next(f for f in form_rows if f.stressed == "bello")
                assert bello_form.def_article == "il"  # il bello
                assert bello_form.article_source == "inferred"

                bella_form = next(f for f in form_rows if f.stressed == "bella")
                assert bella_form.def_article == "la"  # la bella
                assert bella_form.article_source == "inferred"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_imports_adjective_with_inferred_singular(self) -> None:
        """Test that feminine forms without 'singular' tag get it inferred."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INCOMPLETE_TAGS])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            assert stats["lemmas"] == 1
            # Should have 4 forms: alto (base), alta (inferred singular), alti, alte
            assert stats["forms"] >= 4

            with get_connection(db_path) as conn:
                alto = conn.execute(select(lemmas).where(lemmas.c.normalized == "alto")).fetchone()
                assert alto is not None

                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == alto.lemma_id)
                ).fetchall()

                # Check alta was imported with inferred singular
                alta_form = next((f for f in form_rows if f.stressed == "alta"), None)
                assert alta_form is not None, "alta should be imported"
                assert alta_form.gender == "feminine"
                assert alta_form.number == "singular"
                assert alta_form.def_article == "l'"  # l'alta

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_imports_adjective_two_form_plural(self) -> None:
        """Test that plural-only forms generate both masculine and feminine entries."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_TWO_FORM])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            assert stats["lemmas"] == 1
            # Should have 4 forms for 2-form adjective:
            # - facile m.sg (base form auto-added)
            # - facile f.sg (base form auto-added for -e adjectives)
            # - facili m.pl (from plural tag + inferred masculine)
            # - facili f.pl (from plural tag + inferred feminine)
            assert stats["forms"] == 4

            with get_connection(db_path) as conn:
                facile = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "facile")
                ).fetchone()
                assert facile is not None

                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == facile.lemma_id)
                ).fetchall()

                # Check facili appears as both masculine and feminine plural
                facili_forms = [f for f in form_rows if f.stressed == "facili"]
                assert len(facili_forms) == 2, "facili should appear twice (m.pl and f.pl)"

                genders = {f.gender for f in facili_forms}
                assert genders == {"masculine", "feminine"}

                for f in facili_forms:
                    assert f.number == "plural"

                # Check facile appears as both masculine and feminine singular
                facile_forms = [f for f in form_rows if f.stressed == "facile"]
                assert len(facile_forms) == 2, "facile should appear twice (m.sg and f.sg)"

                genders = {f.gender for f in facile_forms}
                assert genders == {"masculine", "feminine"}

                for f in facile_forms:
                    assert f.number == "singular"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_imports_adjective_invariable_generates_four_forms(self) -> None:
        """Test that invariable adjectives (inv:1) generate all 4 gender/number forms."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INVARIABLE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            assert stats["lemmas"] == 1
            # Should have exactly 4 forms for invariable adjective:
            # blu m.sg, blu f.sg, blu m.pl, blu f.pl
            assert stats["forms"] == 4

            with get_connection(db_path) as conn:
                blu = conn.execute(select(lemmas).where(lemmas.c.normalized == "blu")).fetchone()
                assert blu is not None

                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == blu.lemma_id)
                ).fetchall()

                assert len(form_rows) == 4

                # All forms should be "blu"
                for f in form_rows:
                    assert f.stressed == "blu"

                # Check all 4 gender/number combinations exist
                combos = {(f.gender, f.number) for f in form_rows}
                assert combos == {
                    ("masculine", "singular"),
                    ("masculine", "plural"),
                    ("feminine", "singular"),
                    ("feminine", "plural"),
                }

                # All forms should have form_origin = "inferred:invariable"
                for f in form_rows:
                    assert f.form_origin == "inferred:invariable"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_adjective_form_origin_tracking(self) -> None:
        """Test that form_origin correctly tracks how each form was determined."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Test with both invariable and two-form adjectives
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INVARIABLE, SAMPLE_ADJECTIVE_TWO_FORM])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                # Check invariable adjective form_origin
                blu = conn.execute(select(lemmas).where(lemmas.c.normalized == "blu")).fetchone()
                assert blu is not None
                blu_forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == blu.lemma_id)
                ).fetchall()
                for f in blu_forms:
                    assert f.form_origin == "inferred:invariable"

                # Check two-form adjective form_origin
                facile = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "facile")
                ).fetchone()
                assert facile is not None
                facile_forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == facile.lemma_id)
                ).fetchall()

                # Plural forms from wiktextract should have "inferred:two_form"
                plural_forms = [f for f in facile_forms if f.number == "plural"]
                for f in plural_forms:
                    assert f.form_origin == "inferred:two_form"

                # Singular forms (base form) should have "inferred:base_form"
                singular_forms = [f for f in facile_forms if f.number == "singular"]
                for f in singular_forms:
                    assert f.form_origin == "inferred:base_form"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_adjective_metadata_population(self) -> None:
        """Test that adjective_metadata is populated with correct inflection_class."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Test with all three adjective types
        jsonl_path = _create_test_jsonl(
            [SAMPLE_ADJECTIVE, SAMPLE_ADJECTIVE_TWO_FORM, SAMPLE_ADJECTIVE_INVARIABLE]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                # Check 4-form adjective (bello)
                bello = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "bello")
                ).fetchone()
                assert bello is not None
                bello_meta = conn.execute(
                    select(adjective_metadata).where(
                        adjective_metadata.c.lemma_id == bello.lemma_id
                    )
                ).fetchone()
                assert bello_meta is not None
                assert bello_meta.inflection_class == "4-form"

                # Check 2-form adjective (facile)
                facile = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "facile")
                ).fetchone()
                assert facile is not None
                facile_meta = conn.execute(
                    select(adjective_metadata).where(
                        adjective_metadata.c.lemma_id == facile.lemma_id
                    )
                ).fetchone()
                assert facile_meta is not None
                assert facile_meta.inflection_class == "2-form"

                # Check invariable adjective (blu)
                blu = conn.execute(select(lemmas).where(lemmas.c.normalized == "blu")).fetchone()
                assert blu is not None
                blu_meta = conn.execute(
                    select(adjective_metadata).where(adjective_metadata.c.lemma_id == blu.lemma_id)
                ).fetchone()
                assert blu_meta is not None
                assert blu_meta.inflection_class == "invariable"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_two_form_detection_m_or_f_by_sense(self) -> None:
        """Test that 'ottimista' is detected as 2-form via head_templates expansion.

        Adjectives like ottimista, belga, pessimista have gender-tagged plurals
        in the forms array, but are still 2-form because the singular is shared
        for both genders. The "m or f by sense" in head_templates.expansion signals this.
        """
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_TWO_FORM_BY_SENSE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                # Check ottimista is detected as 2-form
                ottimista = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "ottimista")
                ).fetchone()
                assert ottimista is not None

                meta = conn.execute(
                    select(adjective_metadata).where(
                        adjective_metadata.c.lemma_id == ottimista.lemma_id
                    )
                ).fetchone()
                assert meta is not None
                assert meta.inflection_class == "2-form"

                # Check that feminine singular was generated from the shared singular
                forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == ottimista.lemma_id)
                ).fetchall()

                # Should have 4 forms: m.sg, f.sg (shared text), m.pl, f.pl
                assert len(forms) == 4, f"Expected 4 forms, got {len(forms)}"

                # Verify both singular genders have 'ottimista'
                sing_forms = [f for f in forms if f.number == "singular"]
                assert len(sing_forms) == 2
                sing_genders = {f.gender for f in sing_forms}
                assert sing_genders == {"masculine", "feminine"}
                # Both singulars should have the same text
                assert all(f.stressed == "ottimista" for f in sing_forms)

                # Verify plurals have different forms
                plur_forms = [f for f in forms if f.number == "plural"]
                assert len(plur_forms) == 2
                plur_texts = {f.stressed for f in plur_forms}
                assert plur_texts == {"ottimisti", "ottimiste"}

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_misspelling_filtered(self) -> None:
        """Test that entries marked as misspellings are filtered out during import."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Include both a valid adjective and a misspelling
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE, SAMPLE_MISSPELLING_ADJ])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Only the valid adjective should be imported
            assert stats["lemmas"] == 1
            assert stats["misspellings_skipped"] == 1

            with get_connection(db_path) as conn:
                # Check that bello is imported
                bello = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "bello")
                ).fetchone()
                assert bello is not None

                # Check that metereologico is NOT imported
                misspelling = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "metereologico")
                ).fetchone()
                assert misspelling is None

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_comparative_superlative_hardcoded_fallback(self) -> None:
        """Test that hardcoded degree relationships are linked with source tracking.

        When Wiktextract data doesn't contain explicit relationship tags,
        we fall back to hardcoded mappings (e.g., pessimo -> cattivo).
        """
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Both the superlative and base adjective
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_SUPERLATIVE, SAMPLE_ADJECTIVE_CATTIVO])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Both should be imported
            assert stats["lemmas"] == 2

            with get_connection(db_path) as conn:
                # Check pessimo has degree relationship to cattivo
                pessimo = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "pessimo")
                ).fetchone()
                assert pessimo is not None

                cattivo = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "cattivo")
                ).fetchone()
                assert cattivo is not None

                pessimo_meta = conn.execute(
                    select(adjective_metadata).where(
                        adjective_metadata.c.lemma_id == pessimo.lemma_id
                    )
                ).fetchone()
                assert pessimo_meta is not None
                assert pessimo_meta.base_lemma_id == cattivo.lemma_id
                assert pessimo_meta.degree_relationship == "superlative_of"
                assert pessimo_meta.degree_relationship_source == "hardcoded"

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
                parlare = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "parlare")
                ).fetchone()
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
                parlare = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "parlare")
                ).fetchone()
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
        """Verify reimport works after tatoeba has populated sentences."""
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

            # Then: import tatoeba (creates sentences and FTS5 index)
            with get_connection(db_path) as conn:
                tatoeba_stats = import_tatoeba(conn, ita_path, eng_path, links_path)
                assert tatoeba_stats["ita_sentences"] == 1

            # Re-import wiktextract (should work fine)
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
                    select(lemmas).where(lemmas.c.normalized == "acronimo")
                ).fetchone()
                assert acronimo is not None  # Lemma exists

                acronimo_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == acronimo.lemma_id)
                ).fetchall()
                assert len(acronimo_forms) == 0  # But no forms (filtered out)

                # Nouns with gender should have forms
                libro_forms = conn.execute(
                    select(noun_forms).join(lemmas).where(lemmas.c.normalized == "libro")
                ).fetchall()
                assert len(libro_forms) > 0
                assert all(f.gender is not None for f in libro_forms)

        finally:
            db_path.unlink()
            jsonl_path.unlink()


class TestEnrichFormSpellingFromFormOf:
    """Tests for the form-of spelling fallback enrichment.

    Note: Verb forms now get their `written` values from the orthography rule
    during wiktextract import. The form-of enrichment is now only used as a
    fallback for cases where the orthography rule couldn't derive a written
    form (e.g., forms with multiple accents). For verbs with simple accent
    patterns, the orthography rule handles the written derivation.
    """

    def test_verb_written_already_filled_by_orthography_rule(self) -> None:
        """Verb forms get written from orthography rule, form-of enrichment skips them."""
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

            # Import Wiktextract - verb forms now get written from orthography rule
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Verify form is already filled by orthography rule
            with get_connection(db_path) as conn:
                form_row = conn.execute(
                    select(verb_forms).where(verb_forms.c.stressed == "pàrlo")
                ).fetchone()
                assert form_row is not None
                assert form_row.written == "parlo"
                assert form_row.written_source == "derived:orthography_rule"

            # Run form-of spelling enrichment - should skip since already filled
            with get_connection(db_path) as conn:
                stats = enrich_form_spelling_from_form_of(conn, jsonl_path)

            # Should not update anything since orthography rule already filled it
            assert stats["updated"] == 0
            assert stats["already_filled"] > 0

            # Verify written_source is still from orthography rule
            with get_connection(db_path) as conn:
                form_row = conn.execute(
                    select(verb_forms).where(verb_forms.c.stressed == "pàrlo")
                ).fetchone()
                assert form_row is not None
                assert form_row.written_source == "derived:orthography_rule"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_does_not_overwrite_existing_written_source(self) -> None:
        """Form-of enrichment doesn't overwrite forms already filled (orthography rule or morphit)."""
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

            # Verify it was filled by orthography rule
            with get_connection(db_path) as conn:
                form_row = conn.execute(
                    select(verb_forms).where(verb_forms.c.stressed == "pàrlo")
                ).fetchone()
                assert form_row is not None
                assert form_row.written == "parlo"
                assert form_row.written_source == "derived:orthography_rule"

            # Run form-of enrichment
            with get_connection(db_path) as conn:
                stats = enrich_form_spelling_from_form_of(conn, jsonl_path)

            # Should not have updated anything (already filled by orthography rule)
            assert stats["updated"] == 0
            assert stats["already_filled"] > 0

            # Verify written_source is still from orthography rule
            with get_connection(db_path) as conn:
                form_row = conn.execute(
                    select(verb_forms).where(verb_forms.c.stressed == "pàrlo")
                ).fetchone()
                assert form_row is not None
                assert form_row.written_source == "derived:orthography_rule"

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
                collega = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "collega")
                ).fetchone()
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
                assert "masculine" in sing_genders
                assert "feminine" in sing_genders

                # Check plurals have explicit gender
                plural_forms = [f for f in forms if f.number == "plural"]
                plural_genders = {f.gender for f in plural_forms}
                assert "masculine" in plural_genders
                assert "feminine" in plural_genders

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
                    select(lemmas).where(lemmas.c.normalized == "cantante")
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
                assert "masculine" in sing_genders
                assert "feminine" in sing_genders

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
                forbici = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "forbici")
                ).fetchone()
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
                citta = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "citta")
                ).fetchone()
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
                amico = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "amico")
                ).fetchone()
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
                fem_sing = [f for f in forms if f.gender == "feminine" and f.number == "singular"]
                assert len(fem_sing) == 1, f"Expected 1 feminine singular, got {len(fem_sing)}"
                assert fem_sing[0].stressed == "amica"

                # Check we have feminine plural form (amiche)
                fem_plur = [f for f in forms if f.gender == "feminine" and f.number == "plural"]
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
                amico = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "amico")
                ).fetchone()
                assert amico is not None

                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == amico.lemma_id)
                ).fetchall()

                # Check masculine plural (amici)
                masc_plur = [f for f in forms if f.gender == "masculine" and f.number == "plural"]
                assert len(masc_plur) == 1, f"Expected 1 masculine plural, got {len(masc_plur)}"
                assert masc_plur[0].stressed == "amici"

                # Check feminine plural (amiche) - from counterpart lookup!
                fem_plur = [f for f in forms if f.gender == "feminine" and f.number == "plural"]
                assert len(fem_plur) == 1, f"Expected 1 feminine plural, got {len(fem_plur)}"
                assert fem_plur[0].stressed == "amiche"

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
                dio = conn.execute(select(lemmas).where(lemmas.c.normalized == "dio")).fetchone()
                assert dio is not None

                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == dio.lemma_id)
                ).fetchall()

                # Check masculine plurals - should have both dei and dii
                masc_plur = [f for f in forms if f.gender == "masculine" and f.number == "plural"]
                masc_forms = {f.stressed for f in masc_plur}
                assert "dei" in masc_forms, f"Expected 'dei' in masculine plurals, got {masc_forms}"
                assert "dii" in masc_forms, f"Expected 'dii' in masculine plurals, got {masc_forms}"

                # Check feminine plural - should ONLY have dee, NOT dei/dii
                fem_plur = [f for f in forms if f.gender == "feminine" and f.number == "plural"]
                fem_forms = {f.stressed for f in fem_plur}
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
                dio = conn.execute(select(lemmas).where(lemmas.c.normalized == "dio")).fetchone()
                assert dio is not None

                forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.lemma_id == dio.lemma_id)
                ).fetchall()

                plural_forms = [f for f in forms if f.number == "plural"]
                assert len(plural_forms) >= 1

                # The plural should be the accented "dèi", not unaccented "dei"
                plural_stressed = [f.stressed for f in plural_forms]
                assert "dèi" in plural_stressed, f"Expected 'dèi' in {plural_stressed}"

        finally:
            db_path.unlink()
            jsonl_path.unlink()


class TestAltFormFiltering:
    """Tests for alt-of entry filtering (apocopic/elided forms)."""

    def test_is_alt_form_entry_with_alt_of(self) -> None:
        """Entry with alt_of should return True."""
        entry = {
            "pos": "adj",
            "word": "gran",
            "senses": [
                {
                    "tags": ["apocopic"],
                    "alt_of": [{"word": "grande"}],
                    "glosses": ["apocopic form of grande"],
                }
            ],
        }
        assert _is_alt_form_entry(entry) is True

    def test_is_alt_form_entry_without_alt_of(self) -> None:
        """Regular entry without alt_of should return False."""
        entry = {
            "pos": "adj",
            "word": "grande",
            "senses": [{"glosses": ["big", "large"]}],
        }
        assert _is_alt_form_entry(entry) is False

    def test_is_alt_form_entry_empty_senses(self) -> None:
        """Entry with no senses should return False."""
        entry: dict[str, Any] = {"pos": "adj", "word": "test", "senses": []}
        assert _is_alt_form_entry(entry) is False

    def test_alt_form_entries_skipped_during_import(self) -> None:
        """Alt-form entries should be skipped during adjective import."""
        # Parent adjective entry
        grande_entry = {
            "pos": "adj",
            "word": "grande",
            "forms": [{"form": "grànde", "tags": ["canonical"]}],
            "senses": [{"glosses": ["big", "large"]}],
        }

        # Alt-form entry (should be skipped)
        gran_entry = {
            "pos": "adj",
            "word": "gran",
            "senses": [
                {
                    "tags": ["apocopic"],
                    "alt_of": [{"word": "grande"}],
                    "glosses": ["apocopic form of grande"],
                }
            ],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([grande_entry, gran_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                stats = import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Only grande should be imported, gran should be skipped
            assert stats["lemmas"] == 1
            assert stats["alt_forms_skipped"] == 1

            with get_connection(db_path) as conn:
                all_lemmas = conn.execute(
                    select(lemmas).where(lemmas.c.pos == "adjective")
                ).fetchall()
                lemma_words = [lem.normalized for lem in all_lemmas]

                assert "grande" in lemma_words
                assert "gran" not in lemma_words

        finally:
            db_path.unlink()
            jsonl_path.unlink()


class TestImportAdjAllomorphs:
    """Tests for import_adjective_allomorphs function."""

    def test_allomorph_import_adds_forms_to_parent(self) -> None:
        """Allomorph import should add forms under parent lemma."""
        # Parent adjective entry
        grande_entry = {
            "pos": "adj",
            "word": "grande",
            "forms": [{"form": "grànde", "tags": ["canonical"]}],
            "senses": [{"glosses": ["big", "large"]}],
        }

        # Alt-form entry (should be imported as allomorph)
        gran_entry = {
            "pos": "adj",
            "word": "gran",
            "senses": [
                {
                    "tags": ["apocopic"],
                    "alt_of": [{"word": "grande"}],
                    "glosses": ["apocopic form of grande"],
                }
            ],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([grande_entry, gran_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                # First import adjectives (grande only, gran skipped)
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

                # Then import allomorphs
                stats = import_adjective_allomorphs(conn, jsonl_path)

            assert stats["allomorphs_added"] == 1
            assert stats["forms_added"] == 4  # All 4 gender/number combinations

            with get_connection(db_path) as conn:
                grande = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "grande")
                ).fetchone()
                assert grande is not None

                forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == grande.lemma_id)
                ).fetchall()

                # Find allomorph forms (labeled apocopic)
                allomorph_forms = [f for f in forms if f.labels == "apocopic"]
                assert len(allomorph_forms) == 4

                # All should have form="gran"
                for f in allomorph_forms:
                    assert f.written == "gran"
                    assert f.form_origin == "alt_of"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_elided_form_gets_elided_label(self) -> None:
        """Elided forms (ending with ') should get labels='elided'."""
        grande_entry = {
            "pos": "adj",
            "word": "grande",
            "forms": [{"form": "grànde", "tags": ["canonical"]}],
            "senses": [{"glosses": ["big", "large"]}],
        }

        # Elided form
        grand_prime_entry = {
            "pos": "adj",
            "word": "grand'",
            "senses": [
                {
                    "alt_of": [{"word": "grande"}],
                    "glosses": ["elided form of grande"],
                }
            ],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([grande_entry, grand_prime_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")
                import_adjective_allomorphs(conn, jsonl_path)

            with get_connection(db_path) as conn:
                grande = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "grande")
                ).fetchone()
                assert grande is not None

                forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.lemma_id == grande.lemma_id)
                ).fetchall()

                elided_forms = [f for f in forms if f.labels == "elided"]
                assert len(elided_forms) == 4

                for f in elided_forms:
                    assert f.written == "grand'"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_parent_not_found_tracked(self) -> None:
        """If parent doesn't exist, should track as parent_not_found."""
        # Only alt-form entry, no parent
        gran_entry = {
            "pos": "adj",
            "word": "gran",
            "senses": [
                {
                    "tags": ["apocopic"],
                    "alt_of": [{"word": "grande"}],
                    "glosses": ["apocopic form of grande"],
                }
            ],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([gran_entry])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                # Import without parent
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")
                stats = import_adjective_allomorphs(conn, jsonl_path)

            # Should track as parent_not_found
            assert stats["parent_not_found"] == 1
            assert stats["allomorphs_added"] == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_hardcoded_allomorph_forms_added(self) -> None:
        """Hardcoded allomorph forms (san) should be added to santo.

        Note: sant' is NOT hardcoded - it comes from Morphit via fill_missing_adjective_forms().
        """
        # Parent adjective with standard forms
        santo = {
            "pos": "adj",
            "word": "santo",
            "forms": [
                {"form": "sànto", "tags": ["masculine", "singular"]},
                {"form": "sànta", "tags": ["feminine", "singular"]},
                {"form": "sànti", "tags": ["masculine", "plural"]},
                {"form": "sànte", "tags": ["feminine", "plural"]},
            ],
            "senses": [{"glosses": ["holy"]}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([santo])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")
                stats = import_adjective_allomorphs(conn, jsonl_path)

            # Should have added 1 hardcoded form: san (sant' comes from Morphit)
            assert stats["hardcoded_added"] == 1

            with get_connection(db_path) as conn:
                santo_lemma = conn.execute(
                    select(lemmas).where(lemmas.c.normalized == "santo")
                ).fetchone()
                assert santo_lemma is not None

                forms = conn.execute(
                    select(adjective_forms).where(
                        adjective_forms.c.lemma_id == santo_lemma.lemma_id
                    )
                ).fetchall()

                # Check that 'san' was added with correct attributes
                san_forms = [f for f in forms if f.written == "san"]
                assert len(san_forms) == 1
                san_form = san_forms[0]
                assert san_form.gender == "masculine"
                assert san_form.number == "singular"
                assert san_form.labels == "apocopic"
                assert san_form.form_origin == "hardcoded"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
