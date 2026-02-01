"""Tests for NVdB usage tier importer."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import text

from italian_db.db import get_connection, get_engine, init_db
from italian_db.importers.nvdb import (
    MATCHABLE_POS,
    POS_MAP,
    NvdbEntry,
    _clean_word,  # pyright: ignore[reportPrivateUsage]
    _map_all_matchable_pos,  # pyright: ignore[reportPrivateUsage]
    _map_pos,  # pyright: ignore[reportPrivateUsage]
    _match_entry,  # pyright: ignore[reportPrivateUsage]
    _parse_all_entries,  # pyright: ignore[reportPrivateUsage]
    _parse_entries,  # pyright: ignore[reportPrivateUsage]
    import_nvdb,
)

# ruff: noqa: SIM300


# ---------------------------------------------------------------------------
# Parsing unit tests
# ---------------------------------------------------------------------------


class TestParseEntries:
    """Tests for HTML entry parsing."""

    def test_bold_is_fo(self) -> None:
        html = "<p><b>casa </b> s.f.,</p>"
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("casa", "s.f.", "FO")

    def test_italic_is_ad(self) -> None:
        html = "<p><i>abbaiare </i> v.intr. e tr.,</p>"
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("abbaiare", "v.intr. e tr.", "AD")

    def test_plain_is_au(self) -> None:
        html = "<p>abbandono s.m.,</p>"
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("abbandono", "s.m.", "AU")

    def test_multiple_entries(self) -> None:
        html = (
            "<p><b>abbandonare </b> v.tr.,</p>\n"
            "<p>abbandonato p.pass., agg., s.m.,</p>\n"
            "<p><i>abbasso </i> avv., inter.,</p>\n"
        )
        entries = _parse_entries(html)
        assert len(entries) == 3
        assert entries[0][2] == "FO"
        assert entries[1][2] == "AU"
        assert entries[2][2] == "AD"

    def test_compound_pos(self) -> None:
        html = "<p><i>abbagliante </i> p.pres., agg., s.m.,</p>"
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("abbagliante", "p.pres., agg., s.m.", "AD")

    def test_invariable_noun(self) -> None:
        html = "<p>abilità s.f.inv.,</p>"
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("abilità", "s.f.inv.", "AU")

    def test_pos_with_e_separator(self) -> None:
        html = "<p><i>abruzzese </i> agg., s.m. e f.,</p>"
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0] == ("abruzzese", "agg., s.m. e f.", "AD")

    def test_no_space_between_word_and_pos(self) -> None:
        """Handle data quality issue: word and POS run together."""
        html = "<p>accederev.intr.,</p>"
        entries = _parse_entries(html)
        assert len(entries) == 1
        assert entries[0][0] == "accedere"
        assert entries[0][1] == "v.intr."
        assert entries[0][2] == "AU"

    def test_skips_comments(self) -> None:
        html = "<!--- Comment line --->\n<p><b>casa </b> s.f.,</p>\n"
        entries = _parse_entries(html)
        assert len(entries) == 1

    def test_skips_empty_lines(self) -> None:
        html = "\n\n<p><b>casa </b> s.f.,</p>\n\n"
        entries = _parse_entries(html)
        assert len(entries) == 1


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

    def test_transitive_verb(self) -> None:
        assert _map_pos("v.tr.") == "verb"

    def test_intransitive_verb(self) -> None:
        assert _map_pos("v.intr.") == "verb"

    def test_pronominal_verb(self) -> None:
        assert _map_pos("v.pronom.intr.") == "verb"

    def test_masculine_noun(self) -> None:
        assert _map_pos("s.m.") == "noun"

    def test_feminine_noun(self) -> None:
        assert _map_pos("s.f.") == "noun"

    def test_invariable_noun(self) -> None:
        assert _map_pos("s.m.inv.") == "noun"
        assert _map_pos("s.f.inv.") == "noun"

    def test_adjective(self) -> None:
        assert _map_pos("agg.") == "adjective"

    def test_compound_pos_takes_first_matchable(self) -> None:
        # "p.pres., agg., s.m." -> adjective (first matchable component)
        assert _map_pos("p.pres., agg., s.m.") == "adjective"

    def test_compound_pos_noun_first(self) -> None:
        # "agg., s.m." -> adjective (first matchable)
        assert _map_pos("agg., s.m.") == "adjective"

    def test_pos_with_e_separator(self) -> None:
        # "s.m. e f." -> noun (take left side of "e")
        assert _map_pos("s.m. e f.") == "noun"
        # "v.intr. e tr." -> verb (take left side)
        assert _map_pos("v.intr. e tr.") == "verb"

    def test_non_matchable_pos(self) -> None:
        assert _map_pos("avv.") == "adverb"
        assert _map_pos("prep.") == "preposition"

    def test_unknown_pos(self) -> None:
        assert _map_pos("xyz") is None


class TestMapAllMatchablePos:
    """Tests for compound POS splitting into all matchable values."""

    def test_compound_adjective_and_noun(self) -> None:
        # "p.pres., agg., s.m." → adjective + noun (p.pres. is not matchable)
        result = _map_all_matchable_pos("p.pres., agg., s.m.")
        assert result == ["adjective", "noun"]

    def test_adjective_and_noun(self) -> None:
        # "agg., s.m. e f." → adjective + noun
        result = _map_all_matchable_pos("agg., s.m. e f.")
        assert result == ["adjective", "noun"]

    def test_verb_with_e_separator(self) -> None:
        # "v.intr. e tr." → ["verb"] (single component with "e" modifier)
        result = _map_all_matchable_pos("v.intr. e tr.")
        assert result == ["verb"]

    def test_dedup_same_pos(self) -> None:
        # "s.m., s.f." → ["noun"] (both map to noun, deduped)
        result = _map_all_matchable_pos("s.m., s.f.")
        assert result == ["noun"]

    def test_non_matchable_returns_empty(self) -> None:
        # "avv., inter." → []
        result = _map_all_matchable_pos("avv., inter.")
        assert result == []

    def test_single_matchable(self) -> None:
        result = _map_all_matchable_pos("v.tr.")
        assert result == ["verb"]

    def test_single_non_matchable(self) -> None:
        result = _map_all_matchable_pos("avv.")
        assert result == []

    def test_participle_with_adjective(self) -> None:
        # "p.pass., agg." → ["adjective"]
        result = _map_all_matchable_pos("p.pass., agg.")
        assert result == ["adjective"]


class TestPosMapCompleteness:
    """Tests that POS_MAP covers all expected types."""

    def test_matchable_pos_values(self) -> None:
        assert MATCHABLE_POS == {"verb", "noun", "adjective"}

    def test_all_matchable_in_pos_map(self) -> None:
        mapped_values = {v for v in POS_MAP.values() if v is not None}
        assert MATCHABLE_POS.issubset(mapped_values)


# ---------------------------------------------------------------------------
# Parse all entries (integration)
# ---------------------------------------------------------------------------


class TestParseAllEntries:
    """Tests for parsing and deduplication."""

    def test_deduplication(self, tmp_path: Path) -> None:
        """Duplicate words with same POS are deduplicated (first wins)."""
        html = (
            "<p><b>a </b> prep.,</p>\n"
            "<p><i>a </i> s.f. e m.inv.,</p>\n"  # different POS -> not a duplicate
        )
        (tmp_path / "nvdb.html").write_text(html, encoding="utf-8")

        entries = _parse_all_entries(tmp_path / "nvdb.html")
        # "a" as preposition and "a" as noun are different POS mappings
        # Both should survive deduplication
        assert len(entries) >= 1

    def test_tier_assignment(self, tmp_path: Path) -> None:
        """Each entry gets the correct tier."""
        html = "<p><b>casa </b> s.f.,</p>\n<p>tavolo s.m.,</p>\n<p><i>abete </i> s.m.,</p>\n"
        (tmp_path / "nvdb.html").write_text(html, encoding="utf-8")

        entries = _parse_all_entries(tmp_path / "nvdb.html")
        by_word = {e.clean_word: e for e in entries}
        assert by_word["casa"].tier == "FO"
        assert by_word["tavolo"].tier == "AU"
        assert by_word["abete"].tier == "AD"

    def test_compound_pos_creates_multiple_entries(self, tmp_path: Path) -> None:
        """Compound POS like 'p.pres., agg., s.m.' creates entries for both adjective and noun."""
        html = "<p><i>abbagliante </i> p.pres., agg., s.m.,</p>\n"
        (tmp_path / "nvdb.html").write_text(html, encoding="utf-8")

        entries = _parse_all_entries(tmp_path / "nvdb.html")

        # Should have 2 entries: abbagliante as adjective and as noun
        assert len(entries) == 2
        by_pos = {e.pos_mapped: e for e in entries}
        assert "adjective" in by_pos
        assert "noun" in by_pos
        assert by_pos["adjective"].clean_word == "abbagliante"
        assert by_pos["noun"].clean_word == "abbagliante"
        assert by_pos["adjective"].tier == "AD"
        assert by_pos["noun"].tier == "AD"

    def test_compound_pos_non_matchable_creates_single_entry(self, tmp_path: Path) -> None:
        """Non-matchable compound POS still creates a single entry for stats."""
        html = "<p><i>abbasso </i> avv., inter.,</p>\n"
        (tmp_path / "nvdb.html").write_text(html, encoding="utf-8")

        entries = _parse_all_entries(tmp_path / "nvdb.html")
        assert len(entries) == 1
        assert entries[0].pos_mapped == "adverb"

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        """Missing HTML file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            _parse_all_entries(tmp_path / "nonexistent.html")


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
def nvdb_html(tmp_path: Path) -> Path:
    """Create a test NVdB HTML file."""
    html = (
        "<p><b>casa </b> s.f.,</p>\n"
        "<p><b>gatto </b> s.m.,</p>\n"
        "<p><b>parlare </b> v.tr.,</p>\n"
        "<p>tavolo s.m.,</p>\n"
        "<p>bello agg.,</p>\n"
        "<p><i>abete </i> s.m.,</p>\n"
        "<p><b>molto </b> avv.,</p>\n"  # non-matchable POS
    )
    nvdb_path = tmp_path / "nvdb.html"
    nvdb_path.write_text(html, encoding="utf-8")
    return nvdb_path


class TestMatchEntry:
    """Tests for database matching logic."""

    def test_exact_match(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )

            entry = NvdbEntry(
                word="casa",
                clean_word="casa",
                pos_raw="s.f.",
                pos_mapped="noun",
                tier="FO",
            )
            result = _match_entry(conn, entry)
            assert result is not None

    def test_case_insensitive_match(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('cd', 'cd', 'noun')")
            )

            entry = NvdbEntry(
                word="CD",
                clean_word="CD",
                pos_raw="s.m.",
                pos_mapped="noun",
                tier="AU",
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

            entry = NvdbEntry(
                word="chiamare",
                clean_word="chiamare",
                pos_raw="v.tr.",
                pos_mapped="verb",
                tier="FO",
                has_reflexive=True,
            )
            result = _match_entry(conn, entry)
            assert result is not None

    def test_no_match(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            entry = NvdbEntry(
                word="nonexistent",
                clean_word="nonexistent",
                pos_raw="s.m.",
                pos_mapped="noun",
                tier="AU",
            )
            result = _match_entry(conn, entry)
            assert result is None


class TestImportNvdb:
    """Integration tests for the full import function."""

    def test_basic_import(self, temp_db: Path, nvdb_html: Path) -> None:
        """Test basic import with matching lemmas."""
        with get_connection(temp_db) as conn:
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

            stats = import_nvdb(conn, nvdb_html)

            assert stats["matched"] == 3  # casa, gatto, parlare
            assert stats["tier_FO"] == 3  # all matched are FO

            # Verify data in table
            rows = conn.execute(text("SELECT * FROM nvdb_tiers ORDER BY lemma_id")).fetchall()
            assert len(rows) == 3

    def test_skips_multiword(self, temp_db: Path, tmp_path: Path) -> None:
        """Test that multiword expressions are skipped."""
        html = "<p><b>per favore </b> loc.,</p>\n<p><b>casa </b> s.f.,</p>\n"
        nvdb_path = tmp_path / "nvdb.html"
        nvdb_path.write_text(html, encoding="utf-8")

        with get_connection(temp_db) as conn:
            stats = import_nvdb(conn, nvdb_path)
            assert stats["skipped_multiword"] >= 1

    def test_skips_non_matchable_pos(self, temp_db: Path, nvdb_html: Path) -> None:
        """Test that non-matchable POS (adverb, etc.) are skipped."""
        with get_connection(temp_db) as conn:
            stats = import_nvdb(conn, nvdb_html)
            assert stats["skipped_pos"] >= 1  # "molto" (avv.)

    def test_idempotent(self, temp_db: Path, nvdb_html: Path) -> None:
        """Test that import is idempotent (clears existing data)."""
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )
            conn.commit()

            # First import
            stats1 = import_nvdb(conn, nvdb_html)
            assert stats1["matched"] == 1
            assert stats1["cleared"] == 0

            # Second import should clear and re-insert
            stats2 = import_nvdb(conn, nvdb_html)
            assert stats2["matched"] == 1
            assert stats2["cleared"] == 1

            # Should still have exactly 1 row
            count = conn.execute(text("SELECT COUNT(*) FROM nvdb_tiers")).scalar()
            assert count == 1

    def test_per_tier_counts(self, temp_db: Path, nvdb_html: Path) -> None:
        """Test that per-tier counts are correct."""
        with get_connection(temp_db) as conn:
            # Insert lemmas for different tiers
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
                    "INSERT INTO lemmas (written, stressed, pos) VALUES ('abete', 'abete', 'noun')"
                )
            )
            conn.commit()

            stats = import_nvdb(conn, nvdb_html)

            assert stats["tier_FO"] >= 1  # casa
            assert stats["tier_AU"] >= 1  # tavolo
            assert stats["tier_AD"] >= 1  # abete

    def test_compound_pos_matches_both_lemmas(self, temp_db: Path, tmp_path: Path) -> None:
        """Compound POS entry matches both adjective and noun lemmas."""
        html = "<p><i>abbagliante </i> p.pres., agg., s.m.,</p>\n"
        nvdb_path = tmp_path / "nvdb.html"
        nvdb_path.write_text(html, encoding="utf-8")

        with get_connection(temp_db) as conn:
            conn.execute(
                text(
                    "INSERT INTO lemmas (written, stressed, pos) "
                    "VALUES ('abbagliante', 'abbagliante', 'adjective')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO lemmas (written, stressed, pos) "
                    "VALUES ('abbagliante', 'abbagliante', 'noun')"
                )
            )
            conn.commit()

            stats = import_nvdb(conn, nvdb_path)

            assert stats["matched"] == 2  # both adjective and noun
            rows = conn.execute(
                text("SELECT lemma_id FROM nvdb_tiers ORDER BY lemma_id")
            ).fetchall()
            assert len(rows) == 2

    def test_provenance_stored(self, temp_db: Path, nvdb_html: Path) -> None:
        """Test that source_word and source_pos provenance are stored."""
        with get_connection(temp_db) as conn:
            conn.execute(
                text("INSERT INTO lemmas (written, stressed, pos) VALUES ('casa', 'casa', 'noun')")
            )
            conn.commit()

            import_nvdb(conn, nvdb_html)

            row = conn.execute(
                text("SELECT tier, source_word, source_pos FROM nvdb_tiers")
            ).fetchone()
            assert row is not None
            assert row[0] == "FO"
            assert row[1] == "casa"
            assert row[2] == "s.f."
