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
    noun_forms,
    verb_forms,
)
from italian_anki.importers.morphit import (
    apply_orthography_fallback,
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
            "written": "parliàmo",
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
    """Tests for the Morph-it! importer.

    Note: Verb forms now get their `written` values from the orthography rule
    during wiktextract import, not from Morphit. Morphit enrichment for verbs
    will show updated=0 since verb forms already have written values.
    These tests verify that behavior.
    """

    def test_verb_written_populated_during_wiktextract(self) -> None:
        """Verb forms get written values from orthography rule during wiktextract import."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            # Import Wiktextract data - verbs should already have written values
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Check that verb forms already have real spelling from orthography rule
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(verb_forms).where(verb_forms.c.written.isnot(None))
                ).fetchall()

                assert len(form_rows) > 0, "Should have forms with real spelling"

                # Check specific forms
                for row in form_rows:
                    # Real form should not have non-final stress marks
                    # (final accents like parlò are kept)
                    assert row.written is not None
                    # Stressed form should have marks
                    assert row.stressed is not None
                    # written_source should be from orthography rule
                    assert row.written_source == "derived:orthography_rule"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_morphit_does_not_update_verbs(self) -> None:
        """Morphit import for verbs shows updated=0 since they already have written values."""
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

            # First import Wiktextract data (verbs get written from orthography rule)
            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Then enrich with Morph-it! - should update 0 verb forms
            with get_connection(db_path) as conn:
                stats = import_morphit(conn, morphit_path)

            # Verbs already have written values, so morphit updates 0
            assert stats["updated"] == 0, "Verbs already have written from orthography rule"

            # written_source should still be from orthography rule, not morphit
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(verb_forms).where(verb_forms.c.written.isnot(None))
                ).fetchall()

                for row in form_rows:
                    assert row.written_source == "derived:orthography_rule"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_all_verb_forms_have_written(self) -> None:
        """All verb forms should have written values after wiktextract import."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            # Check that NO verb forms have NULL written
            with get_connection(db_path) as conn:
                null_forms = conn.execute(
                    select(verb_forms).where(verb_forms.c.written.is_(None))
                ).fetchall()
                assert len(null_forms) == 0, "All verb forms should have written values"

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_skips_non_verbs_in_morphit(self) -> None:
        """Morphit skips non-verb entries when importing verbs."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_VERB])
        # Morph-it! with nouns (should be ignored for verb import)
        morphit_path = _create_test_morphit(
            [
                "casa\tcasa\tNOUN-F:s",
                "case\tcasa\tNOUN-F:p",
                "parlo\tparlare\tVER:ind+pres+1+s",  # Verb entry
            ]
        )

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path)

            with get_connection(db_path) as conn:
                stats = import_morphit(conn, morphit_path)

            # Verbs already have written, so updated=0
            # The point is it shouldn't crash on non-verb entries
            assert stats["updated"] == 0

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
        """Empty morphit file doesn't cause errors - verbs already have written."""
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

            # Verbs already have written from orthography rule
            assert stats["updated"] == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_morphit_idempotent_for_verbs(self) -> None:
        """Morphit is idempotent for verbs - both runs show updated=0."""
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

            # First enrichment - verbs already have written
            with get_connection(db_path) as conn:
                stats1 = import_morphit(conn, morphit_path)

            # Second enrichment - still updated=0
            with get_connection(db_path) as conn:
                stats2 = import_morphit(conn, morphit_path)

            # Both runs should update 0 since verbs get written from orthography rule
            assert stats1["updated"] == 0
            assert stats2["updated"] == 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_verb_written_source_is_orthography_rule(self) -> None:
        """Verify that verb written_source is 'derived:orthography_rule'."""
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

            # Check that written_source is set to orthography rule (not morphit)
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(verb_forms).where(verb_forms.c.written.isnot(None))
                ).fetchall()

                assert len(form_rows) > 0, "Should have forms with real spelling"

                for row in form_rows:
                    assert row.written_source == "derived:orthography_rule", (
                        f"Expected written_source='derived:orthography_rule', "
                        f"got '{row.written_source}'"
                    )

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

    def test_skips_exact_duplicates(self, caplog: Any) -> None:
        """Exact duplicates (same stressed, gender, number) are skipped."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Adjective with complete forms from Wiktextract
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])
        # Morphit has the same forms (exact duplicates)
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

            # Run Morphit fallback with logging enabled
            with caplog.at_level(logging.DEBUG), get_connection(db_path) as conn:
                stats = fill_missing_adjective_forms(conn, morphit_path)

            # Should skip exact duplicates
            assert stats["combos_skipped"] >= 4
            assert "Skipped duplicate" in caplog.text

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
                        assert form.written_source == "morphit"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()


class TestUnstressedFallback:
    """Tests for apply_unstressed_fallback function."""

    def test_copies_unaccented_form(self) -> None:
        """stressed without accents is copied to form."""
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
                    select(adjective_forms).where(adjective_forms.c.written.is_(None))
                ).fetchall()

            # Apply unstressed fallback
            with get_connection(db_path) as conn:
                stats = apply_unstressed_fallback(conn, pos_filter="adjective")

            # Check forms were updated
            with get_connection(db_path) as conn:
                # Forms without accents (bello, bella, belli, belle)
                # should now have form = stressed
                form_rows = conn.execute(
                    select(adjective_forms).where(
                        adjective_forms.c.written_source == "fallback:no_accent"
                    )
                ).fetchall()

                # Should have updated some forms
                if len(null_before) > 0:
                    assert stats["updated"] > 0
                    assert len(form_rows) > 0

                    for row in form_rows:
                        # form should equal stressed
                        assert row.written == row.stressed

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_skips_accented_form(self) -> None:
        """stressed with accents stays NULL in form column."""
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
                    select(adjective_forms).where(adjective_forms.c.stressed.contains("è"))
                ).fetchall()

                for row in form_rows:
                    # Accented forms should NOT have been updated
                    # (fallback should skip forms with accents in stressed)
                    assert (
                        row.written_source != "fallback:no_accent"
                    ), "Accented form should not get fallback"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_sets_written_source_correctly(self) -> None:
        """Verify written_source is set to 'fallback:no_accent'."""
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
                            adjective_forms.c.written_source == "fallback:no_accent"
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

        # Morphit file with both elided and regular forms
        # With the new key (stressed, gender, number), BOTH get added
        morphit_path = _create_test_morphit(
            [
                # Elided forms (all 4 get added with labels='elided')
                "grand'\tgrande\tADJ:pos+m+s",
                "grand'\tgrande\tADJ:pos+f+s",
                "grand'\tgrande\tADJ:pos+m+p",
                "grand'\tgrande\tADJ:pos+f+p",
                # Regular forms (grande singulars skipped as exact dups, grandi plurals added)
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

            # Elided forms should be added (all 4)
            assert stats["elided_added"] == 4

            with get_connection(db_path) as conn:
                forms = conn.execute(select(adjective_forms)).fetchall()

                # All 4 grand' forms should be added (different stressed than grande)
                elided_forms = [f for f in forms if f.written and f.written.endswith("'")]
                assert len(elided_forms) == 4

                for form in elided_forms:
                    assert form.labels == "elided"
                    assert form.form_origin == "morphit"

                # Regular grandi forms SHOULD be added (different stressed than grand')
                grandi_forms = [f for f in forms if f.written == "grandi"]
                assert len(grandi_forms) == 2  # m.pl and f.pl

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

        # Morphit with elided forms and regular forms
        # With new key (stressed, gender, number), both elided AND regular get added
        morphit_path = _create_test_morphit(
            [
                # Elided forms (both m.s and f.s get added - different stressed than bello)
                "bell'\tbello\tADJ:pos+m+s",
                "bell'\tbello\tADJ:pos+f+s",
                # Regular forms (bello m.s skipped as dup, bella/belli/belle added)
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
            # Both bell' m.s and f.s get added (different stressed than existing bello)
            assert stats["elided_added"] == 2

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()


# Sample noun entries for testing orthography fallback
SAMPLE_NOUN_WITH_ACCENT = {
    "pos": "noun",
    "word": "canina",
    "head_templates": [
        {"name": "it-noun", "args": {"1": "f"}, "expansion": "canina f (plural canine)"}
    ],
    "forms": [
        {"form": "canìna", "tags": ["feminine", "singular"]},
        {"form": "canìne", "tags": ["feminine", "plural"]},
    ],
    "senses": [{"glosses": ["kennel"]}],
}

SAMPLE_NOUN_FRENCH_LOANWORD = {
    "pos": "noun",
    "word": "décolleté",
    "head_templates": [
        {"name": "it-noun", "args": {"1": "m", "2": "#"}, "expansion": "décolleté m (invariable)"}
    ],
    "forms": [
        {"form": "décolleté", "tags": ["masculine", "singular"]},
        {"form": "décolleté", "tags": ["masculine", "plural"]},
    ],
    "senses": [{"glosses": ["neckline"]}],
}


class TestOrthographyFallback:
    """Tests for apply_orthography_fallback function."""

    def test_derives_written_from_stressed(self) -> None:
        """Derives written form by stripping non-final accents."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_WITH_ACCENT])
        # Empty morphit so forms stay NULL
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="noun")

            # Run morphit (finds nothing, forms stay NULL)
            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path, pos_filter="noun")

            # Apply orthography fallback
            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter="noun")

            assert stats["updated"] >= 1

            # Check that forms now have derived written values
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(noun_forms).where(
                        noun_forms.c.written_source == "derived:orthography_rule"
                    )
                ).fetchall()

                assert len(form_rows) >= 1
                for row in form_rows:
                    # Non-final accents should be stripped
                    assert "ì" not in row.written, f"Accent not stripped: {row.written}"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_handles_french_loanword_whitelist(self) -> None:
        """French loanwords with multiple accents are handled via whitelist."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_FRENCH_LOANWORD])
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="noun")

            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path, pos_filter="noun")

            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter="noun")

            # Should have loanwords tracked
            assert stats["loanwords"] >= 1

            # Check written_source is hardcoded:loanword
            with get_connection(db_path) as conn:
                loanword_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.written_source == "hardcoded:loanword")
                ).fetchall()

                assert len(loanword_forms) >= 1
                for row in loanword_forms:
                    # Written should preserve accents
                    assert row.written == "décolleté"

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_does_not_overwrite_existing_written(self) -> None:
        """Forms that already have written values are not modified."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])
        # Morphit with proper spellings
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

            # Run morphit (fills written from morphit)
            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path, pos_filter="adjective")

            # Get count of morphit-sourced forms
            with get_connection(db_path) as conn:
                morphit_forms = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.written_source == "morphit")
                ).fetchall()
                morphit_count = len(morphit_forms)

            # Apply orthography fallback (should not modify morphit-sourced forms)
            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter="adjective")

            # Should update 0 (all forms already have written)
            assert stats["updated"] == 0

            # Verify morphit-sourced forms unchanged
            with get_connection(db_path) as conn:
                morphit_forms_after = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.written_source == "morphit")
                ).fetchall()
                assert len(morphit_forms_after) == morphit_count

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()

    def test_sets_written_source_correctly(self) -> None:
        """Verify written_source is set correctly for different cases."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Mix of regular and loanword nouns
        nouns = [SAMPLE_NOUN_WITH_ACCENT, SAMPLE_NOUN_FRENCH_LOANWORD]
        jsonl_path = _create_test_jsonl(nouns)
        morphit_path = _create_test_morphit([])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter="noun")

            with get_connection(db_path) as conn:
                import_morphit(conn, morphit_path, pos_filter="noun")

            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter="noun")

            # Should have both regular derivations and loanwords
            assert stats["updated"] > 0
            assert stats["loanwords"] >= 1

            # Check written sources
            with get_connection(db_path) as conn:
                derived_forms = conn.execute(
                    select(noun_forms).where(
                        noun_forms.c.written_source == "derived:orthography_rule"
                    )
                ).fetchall()
                loanword_forms = conn.execute(
                    select(noun_forms).where(noun_forms.c.written_source == "hardcoded:loanword")
                ).fetchall()

                assert len(derived_forms) > 0
                assert len(loanword_forms) > 0

        finally:
            db_path.unlink()
            jsonl_path.unlink()
            morphit_path.unlink()
