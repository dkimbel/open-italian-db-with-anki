"""Tests for Morph-it! importer."""

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select

from italian_anki.db import (
    adjective_forms,
    form_lookup,
    get_connection,
    get_engine,
    init_db,
    verb_forms,
)
from italian_anki.importers.morphit import (
    apply_unstressed_fallback,
    fill_missing_adjective_forms,
    import_morphit,
)
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
                before_count = conn.execute(select(form_lookup)).fetchall()

            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path)

            with get_connection(db_path) as conn:
                # Count lookup entries after
                after_count = conn.execute(select(form_lookup)).fetchall()

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

    def test_sets_form_source_to_morphit(self) -> None:
        """Verify that form_source is set to 'morphit' when updating forms."""
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

            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path)

            # Check that form_source is set to "morphit"
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(verb_forms).where(verb_forms.c.form.isnot(None))
                ).fetchall()

                assert len(form_rows) > 0, "Should have forms with real spelling"

                for row in form_rows:
                    assert (
                        row.form_source == "morphit"
                    ), f"Expected form_source='morphit', got '{row.form_source}'"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()


# Sample adjective entries for testing
# NOTE: Wiktextract uses "adj" for adjective POS, not "adjective"
SAMPLE_ADJECTIVE_INCOMPLETE = {
    "pos": "adj",
    "word": "grande",
    "forms": [
        # Only masculine singular - incomplete forms array
        {"form": "grande", "tags": ["masculine", "singular"]},
    ],
    "senses": [{"glosses": ["big", "large"]}],
}

SAMPLE_ADJECTIVE_COMPLETE = {
    "pos": "adj",
    "word": "bello",
    "forms": [
        {"form": "bello", "tags": ["masculine", "singular"]},
        {"form": "bella", "tags": ["feminine", "singular"]},
        {"form": "belli", "tags": ["masculine", "plural"]},
        {"form": "belle", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["beautiful"]}],
}

SAMPLE_ADJECTIVE_ACCENTED = {
    "pos": "adj",
    "word": "blu",
    "head_templates": [{"args": {"inv": "1"}}],  # invariable
    "forms": [],
    "senses": [{"glosses": ["blue"]}],
}


class TestMorphitAdjectiveFallback:
    """Tests for fill_missing_adjective_forms function."""

    def test_fills_missing_adjective_forms(self) -> None:
        """Adjective with only m.sg gets all 4 forms from Morphit."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INCOMPLETE])
        morphit_path = _create_test_morphit(
            [
                "grande\tgrande\tADJ:pos+m+s",
                "grande\tgrande\tADJ:pos+f+s",  # Same form for feminine
                "grandi\tgrande\tADJ:pos+m+p",
                "grandi\tgrande\tADJ:pos+f+p",  # Same form for plural
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Check initial state - should have some forms
            with get_connection(db_path) as conn:
                initial_count = conn.execute(select(adjective_forms)).fetchall()
                # May have inferred forms, but incomplete set
                initial_form_count = len(initial_count)

            # Run Morphit fallback
            with get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            assert stats["adjectives_checked"] >= 1
            # Should add forms if incomplete
            if initial_form_count < 4:
                assert stats["forms_added"] > 0

            # Verify we now have 4 positive-degree forms
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.degree == "positive")
                ).fetchall()

                assert len(form_rows) == 4, f"Expected 4 forms, got {len(form_rows)}"

                # Check all gender/number combinations exist
                combos = {(r.gender, r.number) for r in form_rows}
                expected = {
                    ("masculine", "singular"),
                    ("feminine", "singular"),
                    ("masculine", "plural"),
                    ("feminine", "plural"),
                }
                assert combos == expected

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_does_not_duplicate_existing_forms(self) -> None:
        """Existing forms are not overwritten."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Bello already has complete forms
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])
        morphit_path = _create_test_morphit(
            [
                "bello\tbello\tADJ:pos+m+s",
                "bella\tbello\tADJ:pos+f+s",
                "belli\tbello\tADJ:pos+m+p",
                "belle\tbello\tADJ:pos+f+p",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Get initial form count
            with get_connection(db_path) as conn:
                initial_forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.degree == "positive")
                ).fetchall()
                initial_count = len(initial_forms)

            # Run Morphit fallback
            with get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            # Should not add any forms (already complete)
            assert stats["forms_added"] == 0

            # Form count should be unchanged
            with get_connection(db_path) as conn:
                final_forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.degree == "positive")
                ).fetchall()
                assert len(final_forms) == initial_count

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_logs_discrepancies(self, caplog: Any) -> None:
        """Conflicts between existing forms and Morphit forms are logged."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Adjective with a form
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INCOMPLETE])
        # Morphit has DIFFERENT form for same position (hypothetical conflict)
        morphit_path = _create_test_morphit(
            [
                "grandissimo\tgrande\tADJ:pos+m+s",  # Wrong - this would be a conflict
                "grande\tgrande\tADJ:pos+f+s",
                "grandi\tgrande\tADJ:pos+m+p",
                "grandi\tgrande\tADJ:pos+f+p",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Run Morphit fallback with logging enabled
            with caplog.at_level(logging.WARNING), get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            # Should have logged a discrepancy (skipped form with different value)
            assert stats["discrepancies_logged"] >= 1
            assert "Skipped" in caplog.text
            assert "already has" in caplog.text

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_handles_adjective_not_in_morphit(self) -> None:
        """Graceful handling when adjective not found in Morphit."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INCOMPLETE])
        # Empty Morphit - adjective won't be found
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            # Should track not found
            assert stats["not_in_morphit"] >= 1
            assert stats["forms_added"] == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_only_fills_positive_degree(self) -> None:
        """Superlative/comparative forms are not auto-filled."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INCOMPLETE])
        morphit_path = _create_test_morphit(
            [
                "grande\tgrande\tADJ:pos+m+s",
                "grande\tgrande\tADJ:pos+f+s",
                "grandi\tgrande\tADJ:pos+m+p",
                "grandi\tgrande\tADJ:pos+f+p",
                # Superlative forms - should NOT be filled
                "grandissimo\tgrande\tADJ:sup+m+s",
                "grandissima\tgrande\tADJ:sup+f+s",
                "grandissimi\tgrande\tADJ:sup+m+p",
                "grandissime\tgrande\tADJ:sup+f+p",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                fill_missing_adjective_forms(conn, morphit_path)

            # Check no superlative forms were added
            with get_connection(db_path) as conn:
                superlative_forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.degree == "superlative")
                ).fetchall()

                # Should have no superlative forms (function only fills positive)
                assert len(superlative_forms) == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_sets_form_origin_to_morphit(self) -> None:
        """Verify form_origin is set to 'morphit' for added forms."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_INCOMPLETE])
        morphit_path = _create_test_morphit(
            [
                "grande\tgrande\tADJ:pos+m+s",
                "grande\tgrande\tADJ:pos+f+s",
                "grandi\tgrande\tADJ:pos+m+p",
                "grandi\tgrande\tADJ:pos+f+p",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            # Check that morphit-added forms have correct origin
            with get_connection(db_path) as conn:
                morphit_forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.form_origin == "morphit")
                ).fetchall()

                # Should have some morphit-origin forms if any were added
                if stats["forms_added"] > 0:
                    assert len(morphit_forms) > 0
                    for form in morphit_forms:
                        assert form.form_source == "morphit"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()


class TestUnstressedFallback:
    """Tests for apply_unstressed_fallback function."""

    def test_copies_unaccented_form(self) -> None:
        """form_stressed without accents is copied to form."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Adjective with simple forms (no accents needed)
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])
        # Empty morphit so forms stay NULL
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Run morphit import (will find nothing, leaving forms NULL)
            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path, pos_filter="adjective")

            # Count NULL forms before fallback
            with get_connection(db_path) as conn:
                null_before = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.form.is_(None))
                ).fetchall()

            # Apply unstressed fallback
            with get_connection(db_path) as conn:
                stats = apply_unstressed_fallback(conn, pos_filter="adjective")

            # Check forms were updated
            with get_connection(db_path) as conn:
                # Forms without accents (bello, bella, belli, belle)
                # should now have form = form_stressed
                form_rows = conn.execute(
                    select(adjective_forms).where(
                        adjective_forms.c.form_source == "fallback:no_accent"
                    )
                ).fetchall()

                # Should have updated some forms
                if len(null_before) > 0:
                    assert stats["updated"] > 0
                    assert len(form_rows) > 0

                    for row in form_rows:
                        # form should equal form_stressed
                        assert row.form == row.form_stressed

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_skips_accented_form(self) -> None:
        """form_stressed with accents stays NULL in form column."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Adjective with accented forms
        accented_adj = {
            "pos": "adj",  # Wiktextract uses "adj" not "adjective"
            "word": "perché",  # hypothetical adj with accent
            "forms": [
                {"form": "perchè", "tags": ["masculine", "singular"]},
            ],
            "senses": [{"glosses": ["test"]}],
        }
        jsonl_path = _create_test_jsonl([accented_adj])
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            # Run morphit import (will find nothing)
            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path, pos_filter="adjective")

            # Apply unstressed fallback
            with get_connection(db_path) as conn:
                apply_unstressed_fallback(conn, pos_filter="adjective")

            # Check that accented forms still have NULL form
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.form_stressed.contains("è"))
                ).fetchall()

                for row in form_rows:
                    # Accented forms should NOT have been updated
                    # (fallback should skip forms with accents in form_stressed)
                    assert (
                        row.form_source != "fallback:no_accent"
                    ), "Accented form should not get fallback"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_sets_form_source_correctly(self) -> None:
        """Verify form_source is set to 'fallback:no_accent'."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                stats = apply_unstressed_fallback(conn, pos_filter="adjective")

            if stats["updated"] > 0:
                with get_connection(db_path) as conn:
                    fallback_forms = conn.execute(
                        select(adjective_forms).where(
                            adjective_forms.c.form_source == "fallback:no_accent"
                        )
                    ).fetchall()

                    assert len(fallback_forms) == stats["updated"]

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()


class TestMorphitElidedFormHandling:
    """Tests for elided form handling in fill_missing_adjective_forms."""

    def test_elided_forms_added_with_label(self) -> None:
        """Elided forms (ending with ') should be added with labels='elided'."""
        # Adjective with only 2 forms (missing plural)
        incomplete_adj = {
            "pos": "adj",
            "word": "grande",
            "forms": [
                {"form": "grànde", "tags": ["canonical"]},
                {"form": "grànde", "tags": ["masculine", "singular"]},
                {"form": "grànde", "tags": ["feminine", "singular"]},
                # Missing plurals
            ],
            "senses": [{"glosses": ["big", "large"]}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([incomplete_adj])

        # Morphit file with elided forms FIRST (so they get added for missing slots)
        morphit_path = _create_test_morphit(
            [
                # Elided forms first (should be added with labels='elided' for missing plurals)
                "grand'\tgrande\tADJ:pos+m+s",
                "grand'\tgrande\tADJ:pos+f+s",
                "grand'\tgrande\tADJ:pos+m+p",
                "grand'\tgrande\tADJ:pos+f+p",
                # Regular forms after (singulars will be discrepancies, plurals will be skipped)
                "grande\tgrande\tADJ:pos+m+s",
                "grande\tgrande\tADJ:pos+f+s",
                "grandi\tgrande\tADJ:pos+m+p",
                "grandi\tgrande\tADJ:pos+f+p",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            # Elided forms should be added
            assert stats["elided_added"] > 0

            with get_connection(db_path) as conn:
                forms = conn.execute(select(adjective_forms)).fetchall()

                # Elided forms should be added for plural slots (processed first)
                elided_forms = [f for f in forms if f.form and f.form.endswith("'")]
                assert len(elided_forms) == 2  # grand' m.pl and f.pl

                for form in elided_forms:
                    assert form.labels == "elided"
                    assert form.form_origin == "morphit"
                    assert form.number == "plural"  # Only plurals were missing

                # Regular grandi forms should NOT be added (grand' took plural slots)
                grandi_forms = [f for f in forms if f.form == "grandi"]
                assert len(grandi_forms) == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_elided_added_stat_tracked(self) -> None:
        """Verify elided_added stat is correctly tracked."""
        incomplete_adj = {
            "pos": "adj",
            "word": "bello",
            "forms": [
                {"form": "bèllo", "tags": ["canonical"]},
                {"form": "bèllo", "tags": ["masculine", "singular"]},
                # Missing other forms
            ],
            "senses": [{"glosses": ["beautiful"]}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([incomplete_adj])

        # Morphit with elided forms FIRST for slots we're missing
        morphit_path = _create_test_morphit(
            [
                # Elided forms first (f.s will be added since it's missing)
                "bell'\tbello\tADJ:pos+m+s",
                "bell'\tbello\tADJ:pos+f+s",
                # Regular forms after (f.s will be skipped since bell' took it)
                "bello\tbello\tADJ:pos+m+s",
                "bella\tbello\tADJ:pos+f+s",
                "belli\tbello\tADJ:pos+m+p",
                "belle\tbello\tADJ:pos+f+p",
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="adjective")

            with get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            # Should track elided forms that were added
            assert "elided_added" in stats
            # Only f.s is missing, so only 1 elided form added (bell' f.s)
            # m.s already exists from Wiktextract
            assert stats["elided_added"] == 1

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()
