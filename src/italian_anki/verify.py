"""Database verification system for post-ETL validation.

This module provides comprehensive checks to validate database integrity,
consistency, and coverage after the import pipeline runs.

Usage:
    from italian_anki.verify import verify_database
    from italian_anki.db import get_connection

    with get_connection(db_path) as conn:
        report = verify_database(conn, verbose=True)
        if not report.all_passed:
            sys.exit(1)
"""

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Connection, text


@dataclass
class CheckResult:
    """Result of a single verification check."""

    name: str
    passed: bool
    message: str
    details: list[str] | None = None


@dataclass
class VerificationReport:
    """Complete verification report with all check results and metrics."""

    integrity_checks: list[CheckResult] = field(default_factory=lambda: list[CheckResult]())
    consistency_checks: list[CheckResult] = field(default_factory=lambda: list[CheckResult]())
    coverage_checks: list[CheckResult] = field(default_factory=lambda: list[CheckResult]())
    spot_checks: list[CheckResult] = field(default_factory=lambda: list[CheckResult]())
    metrics: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())

    @property
    def all_passed(self) -> bool:
        """Return True if all checks passed."""
        all_checks = (
            self.integrity_checks
            + self.consistency_checks
            + self.coverage_checks
            + self.spot_checks
        )
        return all(check.passed for check in all_checks)

    @property
    def failed_count(self) -> int:
        """Return the number of failed checks."""
        all_checks = (
            self.integrity_checks
            + self.consistency_checks
            + self.coverage_checks
            + self.spot_checks
        )
        return sum(1 for check in all_checks if not check.passed)

    @property
    def total_count(self) -> int:
        """Return the total number of checks."""
        return (
            len(self.integrity_checks)
            + len(self.consistency_checks)
            + len(self.coverage_checks)
            + len(self.spot_checks)
        )

    def summary(self, *, verbose: bool = False) -> str:
        """Generate a human-readable summary of the verification results."""
        lines: list[str] = []

        def format_checks(title: str, checks: list[CheckResult]) -> None:
            if not checks:
                return
            lines.append(f"\n{title}:")
            for check in checks:
                status = "\033[32m[PASS]\033[0m" if check.passed else "\033[31m[FAIL]\033[0m"
                lines.append(f"  {status} {check.message}")
                if verbose and check.details:
                    lines.extend(f"    - {d}" for d in check.details[:10])
                    if len(check.details) > 10:
                        lines.append(f"    ... and {len(check.details) - 10} more")

        format_checks("Integrity Checks", self.integrity_checks)
        format_checks("Consistency Checks", self.consistency_checks)
        format_checks("Coverage", self.coverage_checks)
        format_checks("Spot Checks", self.spot_checks)

        if self.metrics and verbose:
            lines.append("\nMetrics:")
            for key, value in self.metrics.items():
                if isinstance(value, float):
                    lines.append(f"  {key}: {value:.1f}%")
                else:
                    lines.append(f"  {key}: {value:,}")

        lines.append("")
        if self.all_passed:
            lines.append(f"Result: All {self.total_count} checks passed")
        else:
            lines.append(f"Result: FAILED ({self.failed_count} check(s) failed)")

        return "\n".join(lines)


# =============================================================================
# Integrity Checks
# =============================================================================


def check_orphaned_frequencies(conn: Connection) -> CheckResult:
    """Check that all frequency records reference existing lemmas."""
    query = text("""
        SELECT f.lemma_id
        FROM frequencies f
        LEFT JOIN lemmas l ON f.lemma_id = l.id
        WHERE l.id IS NULL
    """)
    result = conn.execute(query).fetchall()
    count = len(result)

    if count == 0:
        return CheckResult(
            name="orphaned_frequencies",
            passed=True,
            message="No orphaned frequencies",
        )
    else:
        details = [f"lemma_id={row[0]}" for row in result[:10]]
        return CheckResult(
            name="orphaned_frequencies",
            passed=False,
            message=f"Orphaned frequencies: {count} records without lemmas",
            details=details,
        )


def check_orphaned_translations(conn: Connection) -> CheckResult:
    """Check that all translations reference existing sentences."""
    issues: list[str] = []

    # Check Italian sentence references
    query = text("""
        SELECT t.ita_sentence_id
        FROM translations t
        LEFT JOIN sentences s ON t.ita_sentence_id = s.sentence_id
        WHERE s.sentence_id IS NULL
    """)
    result = conn.execute(query).fetchall()
    issues.extend(f"ita_sentence_id={row[0]}" for row in result)

    # Check English sentence references
    query = text("""
        SELECT t.eng_sentence_id
        FROM translations t
        LEFT JOIN sentences s ON t.eng_sentence_id = s.sentence_id
        WHERE s.sentence_id IS NULL
    """)
    result = conn.execute(query).fetchall()
    issues.extend(f"eng_sentence_id={row[0]}" for row in result)

    if not issues:
        return CheckResult(
            name="orphaned_translations",
            passed=True,
            message="No orphaned translations",
        )
    else:
        return CheckResult(
            name="orphaned_translations",
            passed=False,
            message=f"Orphaned translations: {len(issues)} records without sentences",
            details=issues[:10],
        )


# =============================================================================
# Consistency Checks
# =============================================================================


def check_number_class_consistency(conn: Connection) -> CheckResult:
    """Check that number_class aligns with actual form data.

    - pluralia_tantum: should have no singular forms
    - singularia_tantum: should have no plural forms
    """
    issues: list[str] = []

    # Pluralia tantum with singular forms
    query = text("""
        SELECT l.stressed, COUNT(*) as sg_count
        FROM noun_metadata nm
        JOIN lemmas l ON nm.lemma_id = l.id
        JOIN noun_forms nf ON nm.lemma_id = nf.lemma_id
        WHERE nm.number_class = 'pluralia_tantum' AND nf.number = 'singular'
        GROUP BY nm.lemma_id
    """)
    result = conn.execute(query).fetchall()
    issues.extend(f"pluralia_tantum with {row[1]} singular forms: {row[0]}" for row in result)

    # Singularia tantum with plural forms
    query = text("""
        SELECT l.stressed, COUNT(*) as pl_count
        FROM noun_metadata nm
        JOIN lemmas l ON nm.lemma_id = l.id
        JOIN noun_forms nf ON nm.lemma_id = nf.lemma_id
        WHERE nm.number_class = 'singularia_tantum' AND nf.number = 'plural'
        GROUP BY nm.lemma_id
    """)
    result = conn.execute(query).fetchall()
    issues.extend(f"singularia_tantum with {row[1]} plural forms: {row[0]}" for row in result)

    if not issues:
        return CheckResult(
            name="number_class_consistency",
            passed=True,
            message="Number class consistency",
        )
    else:
        return CheckResult(
            name="number_class_consistency",
            passed=False,
            message=f"Number class consistency: {len(issues)} violation(s)",
            details=issues,
        )


def check_adjective_class_consistency(conn: Connection) -> CheckResult:
    """Check that adjective inflection_class aligns with actual form data.

    4-form adjectives should have exactly 4 distinct (gender, number) combos
    for positive degree.
    """
    query = text("""
        SELECT l.stressed, am.inflection_class,
               COUNT(DISTINCT af.gender || af.number) as combos
        FROM adjective_metadata am
        JOIN lemmas l ON am.lemma_id = l.id
        JOIN adjective_forms af ON am.lemma_id = af.lemma_id
        WHERE am.inflection_class = '4-form' AND af.degree = 'positive'
        GROUP BY am.lemma_id
        HAVING combos <> 4
    """)
    result = conn.execute(query).fetchall()

    if not result:
        return CheckResult(
            name="adjective_class_consistency",
            passed=True,
            message="Adjective class consistency",
        )
    else:
        details = [f"{row[0]}: {row[2]} combos (expected 4)" for row in result]
        return CheckResult(
            name="adjective_class_consistency",
            passed=False,
            message=f"Adjective class consistency: {len(result)} violation(s)",
            details=details,
        )


def check_citation_form_existence(conn: Connection) -> CheckResult:
    """Check that every verb lemma has at least one is_citation_form=True."""
    query = text("""
        SELECT l.id, l.stressed
        FROM lemmas l
        LEFT JOIN verb_forms vf ON l.id = vf.lemma_id AND vf.is_citation_form = 1
        WHERE l.pos = 'verb' AND vf.id IS NULL
    """)
    result = conn.execute(query).fetchall()

    if not result:
        return CheckResult(
            name="citation_form_existence",
            passed=True,
            message="Citation form markers",
        )
    else:
        details = [f"lemma_id={row[0]} ({row[1]})" for row in result]
        return CheckResult(
            name="citation_form_existence",
            passed=False,
            message=f"Citation form markers: {len(result)} lemma(s) missing",
            details=details,
        )


def check_metadata_row_existence(conn: Connection) -> CheckResult:
    """Check that every verb/noun/adjective lemma has a metadata row."""
    issues: list[str] = []

    # Verbs without metadata
    query = text("""
        SELECT l.id, l.stressed
        FROM lemmas l
        LEFT JOIN verb_metadata vm ON l.id = vm.lemma_id
        WHERE l.pos = 'verb' AND vm.lemma_id IS NULL
    """)
    result = conn.execute(query).fetchall()
    issues.extend(f"verb without metadata: {row[1]} (id={row[0]})" for row in result)

    # Nouns without metadata
    query = text("""
        SELECT l.id, l.stressed
        FROM lemmas l
        LEFT JOIN noun_metadata nm ON l.id = nm.lemma_id
        WHERE l.pos = 'noun' AND nm.lemma_id IS NULL
    """)
    result = conn.execute(query).fetchall()
    issues.extend(f"noun without metadata: {row[1]} (id={row[0]})" for row in result)

    # Adjectives without metadata
    query = text("""
        SELECT l.id, l.stressed
        FROM lemmas l
        LEFT JOIN adjective_metadata am ON l.id = am.lemma_id
        WHERE l.pos = 'adjective' AND am.lemma_id IS NULL
    """)
    result = conn.execute(query).fetchall()
    issues.extend(f"adjective without metadata: {row[1]} (id={row[0]})" for row in result)

    if not issues:
        return CheckResult(
            name="metadata_row_existence",
            passed=True,
            message="Metadata row existence",
        )
    else:
        return CheckResult(
            name="metadata_row_existence",
            passed=False,
            message=f"Metadata row existence: {len(issues)} missing",
            details=issues,
        )


# =============================================================================
# Coverage Checks
# =============================================================================


# Minimum thresholds based on current database stats
COVERAGE_THRESHOLDS = {
    "total_lemmas": 100_000,
    "verb_lemmas": 10_000,
    "noun_lemmas": 50_000,
    "adjective_lemmas": 20_000,
    "total_forms": 900_000,
    "written_spelling_pct": 100.0,
    "written_source_pct": 100.0,
    "frequency_coverage_pct": 60.0,
    "italian_sentences": 900_000,
}


def check_coverage_thresholds(conn: Connection) -> list[CheckResult]:
    """Check that database meets minimum coverage thresholds."""
    results: list[CheckResult] = []

    # Total lemmas
    query = text("SELECT COUNT(*) FROM lemmas")
    count = conn.execute(query).scalar() or 0
    threshold = COVERAGE_THRESHOLDS["total_lemmas"]
    results.append(
        CheckResult(
            name="total_lemmas",
            passed=count >= threshold,
            message=f"Lemmas: {count:,} (min: {threshold:,})",
        )
    )

    # Verb lemmas
    query = text("SELECT COUNT(*) FROM lemmas WHERE pos = 'verb'")
    count = conn.execute(query).scalar() or 0
    threshold = COVERAGE_THRESHOLDS["verb_lemmas"]
    results.append(
        CheckResult(
            name="verb_lemmas",
            passed=count >= threshold,
            message=f"Verb lemmas: {count:,} (min: {threshold:,})",
        )
    )

    # Noun lemmas
    query = text("SELECT COUNT(*) FROM lemmas WHERE pos = 'noun'")
    count = conn.execute(query).scalar() or 0
    threshold = COVERAGE_THRESHOLDS["noun_lemmas"]
    results.append(
        CheckResult(
            name="noun_lemmas",
            passed=count >= threshold,
            message=f"Noun lemmas: {count:,} (min: {threshold:,})",
        )
    )

    # Adjective lemmas
    query = text("SELECT COUNT(*) FROM lemmas WHERE pos = 'adjective'")
    count = conn.execute(query).scalar() or 0
    threshold = COVERAGE_THRESHOLDS["adjective_lemmas"]
    results.append(
        CheckResult(
            name="adjective_lemmas",
            passed=count >= threshold,
            message=f"Adjective lemmas: {count:,} (min: {threshold:,})",
        )
    )

    # Total forms (verb + noun + adjective)
    query = text("""
        SELECT
            (SELECT COUNT(*) FROM verb_forms) +
            (SELECT COUNT(*) FROM noun_forms) +
            (SELECT COUNT(*) FROM adjective_forms)
    """)
    count = conn.execute(query).scalar() or 0
    threshold = COVERAGE_THRESHOLDS["total_forms"]
    results.append(
        CheckResult(
            name="total_forms",
            passed=count >= threshold,
            message=f"Total forms: {count:,} (min: {threshold:,})",
        )
    )

    # Written spelling coverage
    query = text("""
        SELECT
            CAST(SUM(CASE WHEN written IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) * 100 /
            COUNT(*)
        FROM (
            SELECT written FROM verb_forms
            UNION ALL
            SELECT written FROM noun_forms
            UNION ALL
            SELECT written FROM adjective_forms
        )
    """)
    pct = conn.execute(query).scalar() or 0.0
    threshold = COVERAGE_THRESHOLDS["written_spelling_pct"]
    results.append(
        CheckResult(
            name="written_spelling",
            passed=pct >= threshold,
            message=f"Forms with spelling: {pct:.1f}% (min: {threshold:.1f}%)",
        )
    )

    # Written source coverage (forms + lemmas)
    query = text("""
        SELECT
            CAST(SUM(CASE WHEN written_source IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) * 100 /
            COUNT(*)
        FROM (
            SELECT written_source FROM verb_forms
            UNION ALL
            SELECT written_source FROM noun_forms
            UNION ALL
            SELECT written_source FROM adjective_forms
            UNION ALL
            SELECT written_source FROM lemmas
        )
    """)
    pct = conn.execute(query).scalar() or 0.0
    threshold = COVERAGE_THRESHOLDS["written_source_pct"]
    results.append(
        CheckResult(
            name="written_source",
            passed=pct >= threshold,
            message=f"Written source coverage: {pct:.1f}% (min: {threshold:.1f}%)",
        )
    )

    # Frequency coverage
    query = text("""
        SELECT
            CAST(COUNT(DISTINCT f.lemma_id) AS FLOAT) * 100 /
            (SELECT COUNT(*) FROM lemmas)
        FROM frequencies f
    """)
    pct = conn.execute(query).scalar() or 0.0
    threshold = COVERAGE_THRESHOLDS["frequency_coverage_pct"]
    results.append(
        CheckResult(
            name="frequency_coverage",
            passed=pct >= threshold,
            message=f"Frequency coverage: {pct:.1f}% (min: {threshold:.1f}%)",
        )
    )

    # Italian sentences
    query = text("SELECT COUNT(*) FROM sentences WHERE lang = 'ita'")
    count = conn.execute(query).scalar() or 0
    threshold = COVERAGE_THRESHOLDS["italian_sentences"]
    results.append(
        CheckResult(
            name="italian_sentences",
            passed=count >= threshold,
            message=f"Italian sentences: {count:,} (min: {threshold:,})",
        )
    )

    return results


# =============================================================================
# Spot Checks
# =============================================================================


# Known facts that should always hold true
# Format: (stressed_lemma, pos, checks_dict)
SPOT_CHECKS: list[tuple[str, str, dict[str, Any]]] = [
    # Verbs (stressed forms)
    ("parlàre", "verb", {"min_forms": 50, "auxiliary": "avere"}),
    ("èssere", "verb", {"min_forms": 50, "auxiliary": "essere"}),
    ("avére", "verb", {"min_forms": 50, "auxiliary": "avere"}),
    ("andàre", "verb", {"min_forms": 50, "auxiliary": "essere"}),
    # Nouns
    ("casa", "noun", {"gender_class": "f", "has_plural": True}),
    ("uomo", "noun", {"gender_class": "m", "has_plural": True}),
    # Adjectives
    ("bello", "adjective", {"inflection_class": "4-form"}),
    ("blu", "adjective", {"inflection_class": "invariable"}),
    ("facile", "adjective", {"inflection_class": "2-form"}),
]


def run_spot_checks(conn: Connection) -> list[CheckResult]:
    """Run spot checks against known facts."""
    results: list[CheckResult] = []

    for stressed, pos, checks in SPOT_CHECKS:
        # Find the lemma
        query = text("SELECT id FROM lemmas WHERE stressed = :stressed AND pos = :pos")
        row = conn.execute(query, {"stressed": stressed, "pos": pos}).fetchone()

        if not row:
            results.append(
                CheckResult(
                    name=f"spot_{stressed}",
                    passed=False,
                    message=f"{stressed} ({pos}): lemma not found",
                )
            )
            continue

        lemma_id = row[0]
        issues: list[str] = []

        # Check min_forms for verbs
        if "min_forms" in checks:
            query = text("SELECT COUNT(*) FROM verb_forms WHERE lemma_id = :id")
            count = conn.execute(query, {"id": lemma_id}).scalar() or 0
            if count < checks["min_forms"]:
                issues.append(f"forms: {count} < {checks['min_forms']}")

        # Check auxiliary for verbs
        if "auxiliary" in checks:
            query = text("SELECT auxiliary FROM verb_metadata WHERE lemma_id = :id")
            aux = conn.execute(query, {"id": lemma_id}).scalar()
            if aux != checks["auxiliary"]:
                issues.append(f"auxiliary: {aux} != {checks['auxiliary']}")

        # Check gender_class for nouns
        if "gender_class" in checks:
            query = text("SELECT gender_class FROM noun_metadata WHERE lemma_id = :id")
            gc = conn.execute(query, {"id": lemma_id}).scalar()
            if gc != checks["gender_class"]:
                issues.append(f"gender_class: {gc} != {checks['gender_class']}")

        # Check has_plural for nouns
        if "has_plural" in checks:
            query = text("""
                SELECT COUNT(*) FROM noun_forms
                WHERE lemma_id = :id AND number = 'plural'
            """)
            count = conn.execute(query, {"id": lemma_id}).scalar() or 0
            has_plural = count > 0
            if has_plural != checks["has_plural"]:
                issues.append(f"has_plural: {has_plural} != {checks['has_plural']}")

        # Check inflection_class for adjectives
        if "inflection_class" in checks:
            query = text("SELECT inflection_class FROM adjective_metadata WHERE lemma_id = :id")
            ic = conn.execute(query, {"id": lemma_id}).scalar()
            if ic != checks["inflection_class"]:
                issues.append(f"inflection_class: {ic} != {checks['inflection_class']}")

        if issues:
            results.append(
                CheckResult(
                    name=f"spot_{stressed}",
                    passed=False,
                    message=f"{stressed} ({pos}): {len(issues)} issue(s)",
                    details=issues,
                )
            )
        else:
            results.append(
                CheckResult(
                    name=f"spot_{stressed}",
                    passed=True,
                    message=f"{stressed} ({pos}): verified",
                )
            )

    return results


# =============================================================================
# Metrics Collection
# =============================================================================


def collect_metrics(conn: Connection) -> dict[str, Any]:
    """Collect informational metrics about the database."""
    metrics: dict[str, Any] = {}

    # Average forms per lemma by POS - use separate static queries
    avg_verb_query = text("""
        SELECT AVG(cnt) FROM (
            SELECT COUNT(*) as cnt FROM verb_forms GROUP BY lemma_id
        )
    """)
    metrics["avg_verb_forms"] = round(conn.execute(avg_verb_query).scalar() or 0, 1)

    avg_noun_query = text("""
        SELECT AVG(cnt) FROM (
            SELECT COUNT(*) as cnt FROM noun_forms GROUP BY lemma_id
        )
    """)
    metrics["avg_noun_forms"] = round(conn.execute(avg_noun_query).scalar() or 0, 1)

    avg_adj_query = text("""
        SELECT AVG(cnt) FROM (
            SELECT COUNT(*) as cnt FROM adjective_forms GROUP BY lemma_id
        )
    """)
    metrics["avg_adjective_forms"] = round(conn.execute(avg_adj_query).scalar() or 0, 1)

    # % of lemmas with IPA
    query = text("""
        SELECT CAST(SUM(CASE WHEN ipa IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) * 100 /
               COUNT(*)
        FROM lemmas
    """)
    metrics["lemmas_with_ipa_pct"] = round(conn.execute(query).scalar() or 0, 1)

    # % of lemmas with definitions
    query = text("""
        SELECT CAST(COUNT(DISTINCT d.lemma_id) AS FLOAT) * 100 /
               (SELECT COUNT(*) FROM lemmas)
        FROM definitions d
    """)
    metrics["lemmas_with_definitions_pct"] = round(conn.execute(query).scalar() or 0, 1)

    # % of nouns with both singular and plural
    query = text("""
        SELECT CAST(COUNT(*) AS FLOAT) * 100 / (SELECT COUNT(*) FROM noun_metadata)
        FROM (
            SELECT nm.lemma_id
            FROM noun_metadata nm
            JOIN noun_forms nf ON nm.lemma_id = nf.lemma_id
            WHERE nm.number_class = 'variable'
            GROUP BY nm.lemma_id
            HAVING SUM(CASE WHEN nf.number = 'singular' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN nf.number = 'plural' THEN 1 ELSE 0 END) > 0
        )
    """)
    metrics["nouns_with_sg_and_pl_pct"] = round(conn.execute(query).scalar() or 0, 1)

    return metrics


# =============================================================================
# Main Entry Point
# =============================================================================


def verify_database(conn: Connection, *, verbose: bool = False) -> VerificationReport:
    """Run all verification checks and return a complete report.

    Args:
        conn: SQLAlchemy database connection
        verbose: If True, collect additional metrics

    Returns:
        VerificationReport with all check results and optional metrics
    """
    report = VerificationReport()

    # Integrity checks
    report.integrity_checks = [
        check_orphaned_frequencies(conn),
        check_orphaned_translations(conn),
    ]

    # Consistency checks
    report.consistency_checks = [
        check_number_class_consistency(conn),
        check_adjective_class_consistency(conn),
        check_citation_form_existence(conn),
        check_metadata_row_existence(conn),
    ]

    # Coverage checks
    report.coverage_checks = check_coverage_thresholds(conn)

    # Spot checks
    report.spot_checks = run_spot_checks(conn)

    # Metrics (only if verbose)
    if verbose:
        report.metrics = collect_metrics(conn)

    return report
