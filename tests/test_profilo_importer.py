"""Tests for Profilo CEFR level importer."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import text

from italian_db.db import get_connection, get_engine, init_db
from italian_db.importers.profilo import (
    MATCHABLE_POS,
    POS_MAP,
    ProfiloEntry,
    _clean_word,  # pyright: ignore[reportPrivateUsage]
    _map_pos,  # pyright: ignore[reportPrivateUsage]
    _match_entry,  # pyright: ignore[reportPrivateUsage]
    _parse_all_levels,  # pyright: ignore[reportPrivateUsage]
    _parse_entries,  # pyright: ignore[reportPrivateUsage]
    import_profilo,
)

# ruff: noqa: SIM300


# ---------------------------------------------------------------------------
# Parsing unit tests
# ---------------------------------------------------------------------------


class TestParseEntries:
    """Tests for HTML entry parsing."""

    def test_basic_entry(self) -> None:
        html = '1.\t<a href="#" onClick="mostraFrase(1)">gatto</a> (s.m.)<br>'
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("gatto", "s.m.")

    def test_multiple_entries(self) -> None:
        html = (
            '1.\t<a href="#">casa</a> (s.f.)<br>\n'
            '2.\t<a href="#">parlare</a> (v.t.)<br>\n'
            '3.\t<a href="#">bello</a> (agg.)<br>\n'
        )
        entries = _parse_entries(html)
        assert len(entries) == 3
        assert entries[0][0] == "casa"
        assert entries[1][0] == "parlare"
        assert entries[2][0] == "bello"

    def test_compound_pos(self) -> None:
        html = '1.\t<a href="#">amico</a> (s.m. \u2013 s.f.)<br>'
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("amico", "s.m. \u2013 s.f.")

    def test_reflexive_word(self) -> None:
        html = '1.\t<a href="#">chiamare/si</a> (v.t.)<br>'
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0][0] == "chiamare/si"

    def test_gender_variant(self) -> None:
        html = '1.\t<a href="#">amico/a</a> (s.m.)<br>'
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0][0] == "amico/a"

    def test_no_space_before_pos(self) -> None:
        html = '1.\t<a href="#">casa</a>(s.f.)<br>'
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("casa", "s.f.")


class TestCleanWord:
    """Tests for word cleaning."""

    def test_simple_word(self) -> None:
        word, is_multi, has_refl = _clean_word("gatto")
        assert word == "gatto"
        assert not is_multi
        assert not has_refl

    def test_gender_variant(self) -> None:
        word, is_multi, has_refl = _clean_word("amico/a")
        assert word == "amico"
        assert not is_multi
        assert not has_refl

    def test_reflexive_si(self) -> None:
        word, is_multi, has_refl = _clean_word("chiamare/si")
        assert word == "chiamare"
        assert not is_multi
        assert has_refl

    def test_reflexive_rsi(self) -> None:
        word, is_multi, has_refl = _clean_word("divertire/rsi")
        assert word == "divertire"
        assert not is_multi
        assert has_refl

    def test_parenthetical(self) -> None:
        word, is_multi, has_refl = _clean_word("aereo(aeroplano)")
        assert word == "aereo"
        assert not is_multi
        assert not has_refl

    def test_abbreviation_parenthetical(self) -> None:
        word, is_multi, has_refl = _clean_word("auto(mobile)")
        assert word == "auto"
        assert not is_multi
        assert not has_refl

    def test_multiword(self) -> None:
        word, is_multi, has_refl = _clean_word("per favore")
        assert word == "per favore"
        assert is_multi
        assert not has_refl

    def test_whitespace_stripped(self) -> None:
        word, _is_multi, _has_refl = _clean_word("  gatto  ")
        assert word == "gatto"


class TestMapPos:
    """Tests for POS mapping."""

    def test_basic_verb(self) -> None:
        assert _map_pos("v.t.") == "verb"

    def test_basic_noun_m(self) -> None:
        assert _map_pos("s.m.") == "noun"

    def test_basic_noun_f(self) -> None:
        assert _map_pos("s.f.") == "noun"

    def test_adjective(self) -> None:
        assert _map_pos("agg.") == "adjective"

    def test_compound_pos_takes_first(self) -> None:
        # s.m. - s.f. -> noun (from first component)
        assert _map_pos("s.m. \u2013 s.f.") == "noun"

    def test_non_matchable_pos(self) -> None:
        assert _map_pos("avv.") == "adverb"
        assert _map_pos("prep.") == "preposition"

    def test_typo_variant(self) -> None:
        # Missing trailing period
        assert _map_pos("v.t") == "verb"
        assert _map_pos("s.m") == "noun"

    def test_space_variant(self) -> None:
        assert _map_pos("v. int.") == "verb"

    def test_unknown_pos(self) -> None:
        assert _map_pos("xyz") is None

    def test_reflexive_verb(self) -> None:
        assert _map_pos("v.rifl.") == "verb"

    def test_pronominal_verb(self) -> None:
        assert _map_pos("v.t. pron.") == "verb"


class TestPosMapCompleteness:
    """Tests that POS_MAP covers all expected types."""

    def test_matchable_pos_values(self) -> None:
        assert MATCHABLE_POS == {"verb", "noun", "adjective"}

    def test_all_matchable_in_pos_map(self) -> None:
        # All matchable POS values should appear as values in POS_MAP
        mapped_values = {v for v in POS_MAP.values() if v is not None}
        assert MATCHABLE_POS.issubset(mapped_values)


# ---------------------------------------------------------------------------
# Parse all levels (integration with HTML files)
# ---------------------------------------------------------------------------


class TestParseAllLevels:
    """Tests for parsing and deduplication across levels."""

    def test_cumulative_dedup(self, tmp_path: Path) -> None:
        """Words appearing in multiple levels get the lowest level."""
        # Create A1 with "casa"
        (tmp_path / "liste_lessicali_a1.html").write_text(
            '1.\t<a href="#">casa</a> (s.f.)<br>\n2.\t<a href="#">gatto</a> (s.m.)<br>\n',
            encoding="utf-8",
        )
        # A2 includes A1 words plus new ones
        (tmp_path / "liste_lessicali_a2.html").write_text(
            '1.\t<a href="#">casa</a> (s.f.)<br>\n'
            '2.\t<a href="#">gatto</a> (s.m.)<br>\n'
            '3.\t<a href="#">tavolo</a> (s.m.)<br>\n',
            encoding="utf-8",
        )
        (tmp_path / "liste_lessicali_b1.html").write_text("", encoding="utf-8")
        (tmp_path / "liste_lessicali_b2.html").write_text("", encoding="utf-8")

        entries = _parse_all_levels(tmp_path)

        # Should have 3 unique entries
        assert len(entries) == 3

        # "casa" and "gatto" should be A1 (lowest level)
        by_word = {e.clean_word: e for e in entries}
        assert by_word["casa"].cefr_level == "A1"
        assert by_word["gatto"].cefr_level == "A1"
        assert by_word["tavolo"].cefr_level == "A2"

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        """Missing HTML file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            _parse_all_levels(tmp_path)


# ---------------------------------------------------------------------------
# Database matching and import (integration tests)
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db():
    """Create a temporary database with schema for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    try:
        engine = get_engine(db_path)
        init_db(engine)
        yield db_path
    finally:
        db_path.unlink(missing_ok=True)


@pytest.fixture
def profilo_dir(tmp_path: Path) -> Path:
    """Create test HTML files in a temp directory."""
    (tmp_path / "liste_lessicali_a1.html").write_text(
        '1.\t<a href="#">casa</a> (s.f.)<br>\n'
        '2.\t<a href="#">gatto</a> (s.m.)<br>\n'
        '3.\t<a href="#">parlare</a> (v.t.)<br>\n'
        '4.\t<a href="#">per favore</a> (loc.)<br>\n'
        '5.\t<a href="#">molto</a> (avv.)<br>\n',
        encoding="utf-8",
    )
    (tmp_path / "liste_lessicali_a2.html").write_text(
        '1.\t<a href="#">casa</a> (s.f.)<br>\n'
        '2.\t<a href="#">gatto</a> (s.m.)<br>\n'
        '3.\t<a href="#">parlare</a> (v.t.)<br>\n'
        '4.\t<a href="#">tavolo</a> (s.m.)<br>\n'
        '5.\t<a href="#">bello</a> (agg.)<br>\n',
        encoding="utf-8",
    )
    (tmp_path / "liste_lessicali_b1.html").write_text(
        '1.\t<a href="#">economia</a> (s.f.)<br>\n',
        encoding="utf-8",
    )
    (tmp_path / "liste_lessicali_b2.html").write_text("", encoding="utf-8")
    return tmp_path


class TestMatchEntry:
    """Tests for database matching logic."""

    def test_exact_match(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )

            entry = ProfiloEntry(
                word="casa",
                clean_word="casa",
                pos_raw="s.f.",
                pos_mapped="noun",
                cefr_level="A1",
            )
            result = _match_entry(conn, entry)
            assert result is not None

    def test_case_insensitive_match(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('cd', 'cd', 'noun')")
            )

            entry = ProfiloEntry(
                word="CD",
                clean_word="CD",
                pos_raw="s.m.",
                pos_mapped="noun",
                cefr_level="A1",
            )
            result = _match_entry(conn, entry)
            assert result is not None

    def test_reflexive_fallback(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            conn.execute(
                text(
                    "INSERT INTO lemmas (written, stressed, pos) "
                    "VALUES ('chiamarsi', 'chiamàrsi', 'verb')"
                )
            )

            entry = ProfiloEntry(
                word="chiamare/si",
                clean_word="chiamare",
                pos_raw="v.t.",
                pos_mapped="verb",
                cefr_level="A1",
                has_reflexive=True,
            )
            result = _match_entry(conn, entry)
            assert result is not None

    def test_no_match(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            entry = ProfiloEntry(
                word="nonexistent",
                clean_word="nonexistent",
                pos_raw="s.m.",
                pos_mapped="noun",
                cefr_level="A1",
            )
            result = _match_entry(conn, entry)
            assert result is None


class TestImportProfilo:
    """Integration tests for the full import function."""

    def test_basic_import(self, temp_db: Path, profilo_dir: Path) -> None:
        """Test basic import with matching lemmas."""
        with get_connection(temp_db) as conn:
            # Insert some lemmas that will match
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )
            conn.execute(
                text(
                    "INSERT INTO lemmas (written, stressed, pos) VALUES ('gatto', 'gatto', 'noun')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO lemmas (written, stressed, pos) "
                    "VALUES ('parlare', 'parlàre', 'verb')"
                )
            )
            conn.commit()

            stats = import_profilo(conn, profilo_dir)

            assert stats["matched"] == 3  # casa, gatto, parlare
            assert stats["level_A1"] == 3  # all matched are A1

            # Verify data in table
            rows = conn.execute(text("SELECT * FROM cefr_levels ORDER BY lemma_id")).fetchall()
            assert len(rows) == 3

    def test_skips_multiword(self, temp_db: Path, profilo_dir: Path) -> None:
        """Test that multiword expressions are skipped."""
        with get_connection(temp_db) as conn:
            stats = import_profilo(conn, profilo_dir)
            assert stats["skipped_multiword"] >= 1  # "per favore"

    def test_skips_non_matchable_pos(self, temp_db: Path, profilo_dir: Path) -> None:
        """Test that non-matchable POS (adverb, etc.) are skipped."""
        with get_connection(temp_db) as conn:
            stats = import_profilo(conn, profilo_dir)
            assert stats["skipped_pos"] >= 1  # "molto" (avv.)

    def test_idempotent(self, temp_db: Path, profilo_dir: Path) -> None:
        """Test that import is idempotent (clears existing data)."""
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )
            conn.commit()

            # First import
            stats1 = import_profilo(conn, profilo_dir)
            assert stats1["matched"] == 1
            assert stats1["cleared"] == 0

            # Second import should clear and re-insert
            stats2 = import_profilo(conn, profilo_dir)
            assert stats2["matched"] == 1
            assert stats2["cleared"] == 1

            # Should still have exactly 1 row
            count = conn.execute(text("SELECT COUNT(*) FROM cefr_levels")).scalar()
            assert count == 1

    def test_per_level_counts(self, temp_db: Path, profilo_dir: Path) -> None:
        """Test that per-level counts are correct."""
        with get_connection(temp_db) as conn:
            # Insert lemmas for different levels
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )
            conn.execute(
                text(
                    "INSERT INTO lemmas (written, stressed, pos) "
                    "VALUES ('tavolo', 'tàvolo', 'noun')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO lemmas (written, stressed, pos) "
                    "VALUES ('economia', 'economia', 'noun')"
                )
            )
            conn.commit()

            stats = import_profilo(conn, profilo_dir)

            assert stats["level_A1"] == 1  # casa (gatto not inserted)
            assert stats["level_A2"] == 1  # tavolo
            assert stats["level_B1"] == 1  # economia

    def test_provenance_stored(self, temp_db: Path, profilo_dir: Path) -> None:
        """Test that source_word and source_pos provenance are stored."""
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )
            conn.commit()

            import_profilo(conn, profilo_dir)

            row = conn.execute(
                text("SELECT source, source_word, source_pos FROM cefr_levels")
            ).fetchone()
            assert row is not None
            assert row[0] == "profilo"
            assert row[1] == "casa"
            assert row[2] == "s.f."
