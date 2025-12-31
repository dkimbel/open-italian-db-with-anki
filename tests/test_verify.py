"""Tests for database verification system."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import text

from italian_anki.db import get_connection, get_engine, init_db
from italian_anki.verify import (
    CheckResult,
    VerificationReport,
    check_adjective_class_consistency,
    check_citation_form_existence,
    check_metadata_row_existence,
    check_noun_form_uniqueness,
    check_number_class_consistency,
    check_orphaned_frequencies,
    check_verb_form_uniqueness,
    verify_database,
)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    try:
        engine = get_engine(db_path)
        init_db(engine)
        yield db_path
    finally:
        db_path.unlink(missing_ok=True)


class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_passed_check(self) -> None:
        result = CheckResult(
            name="test",
            passed=True,
            message="Test passed",
        )
        assert result.passed
        assert result.message == "Test passed"
        assert result.details is None

    def test_failed_check_with_details(self) -> None:
        result = CheckResult(
            name="test",
            passed=False,
            message="Test failed",
            details=["issue 1", "issue 2"],
        )
        assert not result.passed
        assert result.details is not None
        assert len(result.details) == 2


class TestVerificationReport:
    """Tests for VerificationReport dataclass."""

    def test_empty_report_passes(self) -> None:
        report = VerificationReport()
        assert report.all_passed
        assert report.failed_count == 0
        assert report.total_count == 0

    def test_all_passed(self) -> None:
        report = VerificationReport(
            integrity_checks=[
                CheckResult(name="a", passed=True, message="A"),
                CheckResult(name="b", passed=True, message="B"),
            ],
            consistency_checks=[
                CheckResult(name="c", passed=True, message="C"),
            ],
        )
        assert report.all_passed
        assert report.failed_count == 0
        assert report.total_count == 3

    def test_one_failed(self) -> None:
        report = VerificationReport(
            integrity_checks=[
                CheckResult(name="a", passed=True, message="A"),
                CheckResult(name="b", passed=False, message="B failed"),
            ],
        )
        assert not report.all_passed
        assert report.failed_count == 1
        assert report.total_count == 2

    def test_summary_output(self) -> None:
        report = VerificationReport(
            integrity_checks=[
                CheckResult(name="a", passed=True, message="Test A"),
            ],
        )
        summary = report.summary()
        assert "\033[32m[PASS]\033[0m Test A" in summary
        assert "All 1 checks passed" in summary


class TestIntegrityChecks:
    """Tests for integrity check functions."""

    def test_verb_form_uniqueness_clean(self, temp_db: Path) -> None:
        """Test verb form uniqueness check with no duplicates."""
        with get_connection(temp_db) as conn:
            # Insert a lemma and unique verb forms
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('parlare', 'verb')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()
            conn.execute(
                text("""
                    INSERT INTO verb_forms
                    (lemma_id, written, stressed, mood, tense, person, number)
                    VALUES (:id, 'parlo', 'parlo', 'indicative', 'present', 1, 'singular')
                """),
                {"id": lemma_id},
            )

            result = check_verb_form_uniqueness(conn)
            assert result.passed

    def test_verb_form_uniqueness_duplicate(self, temp_db: Path) -> None:
        """Test verb form uniqueness check with duplicates."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('parlare', 'verb')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            # Insert duplicate forms
            for _ in range(2):
                conn.execute(
                    text("""
                        INSERT INTO verb_forms
                        (lemma_id, written, stressed, mood, tense, person, number)
                        VALUES (:id, 'parlo', 'parlo', 'indicative', 'present', 1, 'singular')
                    """),
                    {"id": lemma_id},
                )

            result = check_verb_form_uniqueness(conn)
            assert not result.passed
            assert "duplicates" in result.message

    def test_noun_form_uniqueness_clean(self, temp_db: Path) -> None:
        """Test noun form uniqueness with no duplicates."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('casa', 'noun')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()
            conn.execute(
                text("""
                    INSERT INTO noun_forms (lemma_id, written, stressed, gender, number)
                    VALUES (:id, 'casa', 'casa', 'f', 'singular')
                """),
                {"id": lemma_id},
            )

            result = check_noun_form_uniqueness(conn)
            assert result.passed

    def test_orphaned_frequencies(self, temp_db: Path) -> None:
        """Test orphaned frequencies check."""
        with get_connection(temp_db) as conn:
            # Insert valid lemma and frequency
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('test', 'verb')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()
            conn.execute(
                text("""
                    INSERT INTO frequencies (lemma_id, corpus, freq_raw)
                    VALUES (:id, 'test', 100)
                """),
                {"id": lemma_id},
            )

            result = check_orphaned_frequencies(conn)
            assert result.passed


class TestConsistencyChecks:
    """Tests for consistency check functions."""

    def test_number_class_consistency_clean(self, temp_db: Path) -> None:
        """Test number class consistency with valid data."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('casa', 'noun')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            # Add metadata with variable number class
            conn.execute(
                text("""
                    INSERT INTO noun_metadata (lemma_id, gender_class, number_class)
                    VALUES (:id, 'f', 'variable')
                """),
                {"id": lemma_id},
            )

            # Add singular and plural forms
            conn.execute(
                text("""
                    INSERT INTO noun_forms (lemma_id, written, stressed, gender, number)
                    VALUES (:id, 'casa', 'casa', 'f', 'singular')
                """),
                {"id": lemma_id},
            )
            conn.execute(
                text("""
                    INSERT INTO noun_forms (lemma_id, written, stressed, gender, number)
                    VALUES (:id, 'case', 'case', 'f', 'plural')
                """),
                {"id": lemma_id},
            )

            result = check_number_class_consistency(conn)
            assert result.passed

    def test_number_class_consistency_pluralia_tantum_violation(self, temp_db: Path) -> None:
        """Test number class consistency with pluralia_tantum having singular forms."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('forbici', 'noun')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            # Mark as pluralia_tantum
            conn.execute(
                text("""
                    INSERT INTO noun_metadata (lemma_id, gender_class, number_class)
                    VALUES (:id, 'f', 'pluralia_tantum')
                """),
                {"id": lemma_id},
            )

            # Incorrectly add a singular form
            conn.execute(
                text("""
                    INSERT INTO noun_forms (lemma_id, written, stressed, gender, number)
                    VALUES (:id, 'forbice', 'forbice', 'f', 'singular')
                """),
                {"id": lemma_id},
            )

            result = check_number_class_consistency(conn)
            assert not result.passed
            assert result.details is not None
            assert "pluralia_tantum" in result.details[0]

    def test_citation_form_existence(self, temp_db: Path) -> None:
        """Test citation form existence check."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('parlare', 'verb')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            # Add verb metadata
            conn.execute(
                text("""
                    INSERT INTO verb_metadata (lemma_id, auxiliary, transitivity)
                    VALUES (:id, 'avere', 'transitive')
                """),
                {"id": lemma_id},
            )

            # Add verb form with citation flag
            conn.execute(
                text("""
                    INSERT INTO verb_forms
                    (lemma_id, written, stressed, mood, tense, person, number, is_citation_form)
                    VALUES (:id, 'parlare', 'parlare', 'infinitive', 'present', NULL, NULL, 1)
                """),
                {"id": lemma_id},
            )

            result = check_citation_form_existence(conn)
            assert result.passed

    def test_citation_form_missing(self, temp_db: Path) -> None:
        """Test citation form existence check with missing citation form."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('parlare', 'verb')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            # Add verb metadata
            conn.execute(
                text("""
                    INSERT INTO verb_metadata (lemma_id, auxiliary, transitivity)
                    VALUES (:id, 'avere', 'transitive')
                """),
                {"id": lemma_id},
            )

            # Add verb form WITHOUT citation flag
            conn.execute(
                text("""
                    INSERT INTO verb_forms
                    (lemma_id, written, stressed, mood, tense, person, number, is_citation_form)
                    VALUES (:id, 'parlo', 'parlo', 'indicative', 'present', 1, 'singular', 0)
                """),
                {"id": lemma_id},
            )

            result = check_citation_form_existence(conn)
            assert not result.passed
            assert "parlare" in str(result.details)

    def test_metadata_row_existence(self, temp_db: Path) -> None:
        """Test metadata row existence check."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('parlare', 'verb')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            # Add verb metadata
            conn.execute(
                text("""
                    INSERT INTO verb_metadata (lemma_id, auxiliary, transitivity)
                    VALUES (:id, 'avere', 'transitive')
                """),
                {"id": lemma_id},
            )

            result = check_metadata_row_existence(conn)
            assert result.passed

    def test_metadata_row_missing(self, temp_db: Path) -> None:
        """Test metadata row existence check with missing metadata."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('parlare', 'verb')"))
            # No metadata row added

            result = check_metadata_row_existence(conn)
            assert not result.passed
            assert result.details is not None
            assert "verb without metadata" in result.details[0]


class TestAdjectives:
    """Tests for adjective-specific checks."""

    def test_adjective_class_consistency_4form(self, temp_db: Path) -> None:
        """Test adjective class consistency with correct 4-form adjective."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('bello', 'adjective')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            conn.execute(
                text("""
                    INSERT INTO adjective_metadata (lemma_id, inflection_class)
                    VALUES (:id, '4-form')
                """),
                {"id": lemma_id},
            )

            # Add all 4 forms
            for gender, number, form in [
                ("m", "singular", "bello"),
                ("f", "singular", "bella"),
                ("m", "plural", "belli"),
                ("f", "plural", "belle"),
            ]:
                conn.execute(
                    text("""
                        INSERT INTO adjective_forms
                        (lemma_id, written, stressed, gender, number, degree)
                        VALUES (:id, :form, :form, :gender, :number, 'positive')
                    """),
                    {"id": lemma_id, "form": form, "gender": gender, "number": number},
                )

            result = check_adjective_class_consistency(conn)
            assert result.passed

    def test_adjective_class_consistency_violation(self, temp_db: Path) -> None:
        """Test adjective class consistency with missing forms."""
        with get_connection(temp_db) as conn:
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('test', 'adjective')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            conn.execute(
                text("""
                    INSERT INTO adjective_metadata (lemma_id, inflection_class)
                    VALUES (:id, '4-form')
                """),
                {"id": lemma_id},
            )

            # Only add 2 forms (should have 4)
            for gender, number, form in [
                ("m", "singular", "test"),
                ("f", "singular", "testa"),
            ]:
                conn.execute(
                    text("""
                        INSERT INTO adjective_forms
                        (lemma_id, written, stressed, gender, number, degree)
                        VALUES (:id, :form, :form, :gender, :number, 'positive')
                    """),
                    {"id": lemma_id, "form": form, "gender": gender, "number": number},
                )

            result = check_adjective_class_consistency(conn)
            assert not result.passed
            assert result.details is not None
            assert "2 combos (expected 4)" in result.details[0]


class TestVerifyDatabase:
    """Tests for the main verify_database function."""

    def test_empty_database(self, temp_db: Path) -> None:
        """Test verification on empty database.

        Note: Empty database will fail spot checks (known lemmas don't exist)
        and coverage checks, but integrity/consistency checks should pass.
        """
        with get_connection(temp_db) as conn:
            report = verify_database(conn)

        # Integrity and consistency checks should pass on empty DB
        assert all(c.passed for c in report.integrity_checks)
        assert all(c.passed for c in report.consistency_checks)

        # Spot checks and coverage will fail (no data)
        assert not all(c.passed for c in report.spot_checks)
        assert not all(c.passed for c in report.coverage_checks)

    def test_with_verbose(self, temp_db: Path) -> None:
        """Test verification with verbose flag."""
        with get_connection(temp_db) as conn:
            report = verify_database(conn, verbose=True)

        # Should have metrics when verbose
        assert "avg_verb_forms" in report.metrics

    def test_full_verification(self, temp_db: Path) -> None:
        """Test full verification with valid data."""
        with get_connection(temp_db) as conn:
            # Insert a complete verb
            conn.execute(text("INSERT INTO lemmas (stressed, pos) VALUES ('parlare', 'verb')"))
            lemma_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

            conn.execute(
                text("""
                    INSERT INTO verb_metadata (lemma_id, auxiliary, transitivity)
                    VALUES (:id, 'avere', 'transitive')
                """),
                {"id": lemma_id},
            )

            conn.execute(
                text("""
                    INSERT INTO verb_forms
                    (lemma_id, written, stressed, mood, tense, person, number, is_citation_form)
                    VALUES (:id, 'parlare', 'parlare', 'infinitive', 'present', NULL, NULL, 1)
                """),
                {"id": lemma_id},
            )

            report = verify_database(conn)

        # Integrity and consistency should pass
        assert all(c.passed for c in report.integrity_checks)
        assert all(c.passed for c in report.consistency_checks)
