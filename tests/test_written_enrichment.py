"""Tests for fallback functions (apply_unstressed_fallback, apply_orthography_fallback).

Note: The import_morphit() function has been removed. Wiktextract form-of entries
now provide all written forms. These tests verify the fallback functions still work.
"""

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select, update

from italian_db.db import (
    POS,
    adjective_forms,
    get_connection,
    get_engine,
    init_db,
    noun_forms,
    verb_forms,
)
from italian_db.importers.wiktextract import import_wiktextract
from italian_db.importers.written_enrichment import (
    apply_orthography_fallback,
    apply_unstressed_fallback,
)

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


class TestVerbWrittenFromOrthographyRule:
    """Tests verifying that verb forms get written values from orthography rule."""

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


class TestUnstressedFallback:
    """Tests for apply_unstressed_fallback function."""

    def test_copies_unaccented_form(self) -> None:
        """stressed without accents is copied to written."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Adjective with simple forms (no accents needed)
        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.ADJECTIVE)

            # Clear written values to simulate forms needing fallback
            with get_connection(db_path) as conn:
                conn.execute(update(adjective_forms).values(written=None, written_source=None))

            # Count NULL forms before fallback
            with get_connection(db_path) as conn:
                null_before = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.written.is_(None))
                ).fetchall()

            # Apply unstressed fallback
            with get_connection(db_path) as conn:
                stats = apply_unstressed_fallback(conn, pos_filter=POS.ADJECTIVE)

            # Check forms were updated
            with get_connection(db_path) as conn:
                # Forms without accents (bello, bella, belli, belle)
                # should now have written = stressed
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
                        # written should equal stressed
                        assert row.written == row.stressed

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_skips_accented_form(self) -> None:
        """stressed with accents stays NULL in written column."""
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

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.ADJECTIVE)

            # Clear written values to simulate forms needing fallback
            with get_connection(db_path) as conn:
                conn.execute(update(adjective_forms).values(written=None, written_source=None))

            # Apply unstressed fallback
            with get_connection(db_path) as conn:
                apply_unstressed_fallback(conn, pos_filter=POS.ADJECTIVE)

            # Check that accented forms still have NULL written
            with get_connection(db_path) as conn:
                form_rows = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.stressed.contains("è"))
                ).fetchall()

                for row in form_rows:
                    # Accented forms should NOT have been updated
                    # (fallback should skip forms with accents in stressed)
                    assert row.written_source != "fallback:no_accent", (
                        "Accented form should not get fallback"
                    )

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_sets_written_source_correctly(self) -> None:
        """Verify written_source is set to 'fallback:no_accent'."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.ADJECTIVE)

            # Clear written values to simulate forms needing fallback
            with get_connection(db_path) as conn:
                conn.execute(update(adjective_forms).values(written=None, written_source=None))

            with get_connection(db_path) as conn:
                stats = apply_unstressed_fallback(conn, pos_filter=POS.ADJECTIVE)

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

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.NOUN)

            # Clear written values to simulate forms needing fallback
            with get_connection(db_path) as conn:
                conn.execute(update(noun_forms).values(written=None, written_source=None))

            # Apply orthography fallback
            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter=POS.NOUN)

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

    def test_handles_french_loanword_whitelist(self) -> None:
        """French loanwords with multiple accents are handled via whitelist."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_NOUN_FRENCH_LOANWORD])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.NOUN)

            # Clear written values to simulate forms needing fallback
            with get_connection(db_path) as conn:
                conn.execute(update(noun_forms).values(written=None, written_source=None))

            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter=POS.NOUN)

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

    def test_does_not_overwrite_existing_written(self) -> None:
        """Forms that already have written values are not modified."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        jsonl_path = _create_test_jsonl([SAMPLE_ADJECTIVE_COMPLETE])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.ADJECTIVE)

            # Set written values manually (simulating form-of enrichment)
            with get_connection(db_path) as conn:
                conn.execute(
                    update(adjective_forms).values(written="existing", written_source="test")
                )

            # Get count of forms with written values
            with get_connection(db_path) as conn:
                forms_with_written = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.written.isnot(None))
                ).fetchall()
                written_count = len(forms_with_written)
                assert written_count > 0, "Should have forms with written values"

            # Apply orthography fallback (should not modify existing forms)
            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter=POS.ADJECTIVE)

            # Should update 0 (all forms already have written)
            assert stats["updated"] == 0

            # Verify count unchanged and values still "existing"
            with get_connection(db_path) as conn:
                forms_with_written_after = conn.execute(
                    select(adjective_forms).where(adjective_forms.c.written == "existing")
                ).fetchall()
                assert len(forms_with_written_after) == written_count

        finally:
            db_path.unlink()
            jsonl_path.unlink()

    def test_sets_written_source_correctly(self) -> None:
        """Verify written_source is set correctly for different cases."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)

        # Mix of regular and loanword nouns
        nouns = [SAMPLE_NOUN_WITH_ACCENT, SAMPLE_NOUN_FRENCH_LOANWORD]
        jsonl_path = _create_test_jsonl(nouns)

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.NOUN)

            # Clear written values to simulate forms needing fallback
            with get_connection(db_path) as conn:
                conn.execute(update(noun_forms).values(written=None, written_source=None))

            with get_connection(db_path) as conn:
                stats = apply_orthography_fallback(conn, pos_filter=POS.NOUN)

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


class TestOrthographyRuleForVerbs:
    """Tests verifying orthography rule correctly handles verb forms."""

    def test_accented_form_gets_correct_written_form(self):
        """Accented forms with non-final stress should get correct written form.

        When form has stressed="pàrlo" (pedagogical accent on non-final syllable),
        the orthography rule correctly strips it to "parlo".

        Note: The orthography rule runs during wiktextract import, so this test
        verifies the overall pipeline produces the correct result.
        """
        sample_verb = {
            "pos": "verb",
            "word": "parlare",
            "forms": [
                {"form": "parlàre", "tags": ["canonical"]},
                {"form": "parlàre", "tags": ["infinitive"]},
                {"form": "avére", "tags": ["auxiliary"]},
                {"form": "pàrlo", "tags": ["first-person", "indicative", "present", "singular"]},
            ],
            "senses": [{"glosses": ["to speak"]}],
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)
        jsonl_path = _create_test_jsonl([sample_verb])

        try:
            engine = get_engine(db_path)
            init_db(engine)

            with get_connection(db_path) as conn:
                import_wiktextract(conn, jsonl_path, pos_filter=POS.VERB)

            # Verify that form with stressed="pàrlo" got written="parlo"
            with get_connection(db_path) as conn:
                forms = conn.execute(
                    select(verb_forms).where(verb_forms.c.stressed == "pàrlo")
                ).fetchall()

                assert len(forms) == 1
                # Should have written form with accent stripped
                assert forms[0].written == "parlo", (
                    "Form with stressed='pàrlo' should get written='parlo' "
                    "(non-final pedagogical accent stripped)"
                )
                # The orthography rule derives this during wiktextract import
                assert forms[0].written_source == "derived:orthography_rule"

        finally:
            db_path.unlink(missing_ok=True)
            jsonl_path.unlink(missing_ok=True)
