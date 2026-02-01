"""Tests for CEFR and NVdB tag features in Anki card generation."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from sqlalchemy import Connection

from anki_gen.generator import build_verb_tags
from anki_gen.queries import Verb, get_cefr_level, get_nvdb_tier
from italian_db.db import (
    cefr_levels,
    frequencies,
    get_connection,
    get_engine,
    init_db,
    lemmas,
    nvdb_tiers,
)


@pytest.fixture
def temp_db() -> Generator[Path]:
    """Create a temporary database with schema for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    try:
        engine = get_engine(db_path)
        init_db(engine)
        yield db_path
    finally:
        db_path.unlink(missing_ok=True)


def _insert_lemma(conn: Connection, written: str = "parlare") -> int:
    """Insert a minimal verb lemma and return its id."""
    result = conn.execute(lemmas.insert().values(written=written, stressed=written, pos="verb"))
    pk = result.inserted_primary_key
    assert pk is not None
    return pk[0]


def _insert_frequency(conn: Connection, lemma_id: int) -> None:
    """Insert minimal frequency data (required by build_verb_tags)."""
    conn.execute(
        frequencies.insert().values(
            lemma_id=lemma_id,
            corpus="stanza",
            freq_raw=100,
            freq_zipf=4.0,
            freq_rank_in_pos=50,
        )
    )


class TestGetCefrLevel:
    """Tests for get_cefr_level query."""

    def test_found(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            lemma_id = _insert_lemma(conn)
            conn.execute(
                cefr_levels.insert().values(lemma_id=lemma_id, level="A1", source="profilo")
            )
            assert get_cefr_level(conn, lemma_id) == "A1"

    def test_not_found(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            lemma_id = _insert_lemma(conn)
            assert get_cefr_level(conn, lemma_id) is None


class TestGetNvdbTier:
    """Tests for get_nvdb_tier query."""

    def test_found(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            lemma_id = _insert_lemma(conn)
            conn.execute(nvdb_tiers.insert().values(lemma_id=lemma_id, tier="FO"))
            assert get_nvdb_tier(conn, lemma_id) == "FO"

    def test_not_found(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            lemma_id = _insert_lemma(conn)
            assert get_nvdb_tier(conn, lemma_id) is None


class TestBuildVerbTagsCefrNvdb:
    """Tests for CEFR/NVdB tags in build_verb_tags."""

    def test_includes_cefr_and_nvdb(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            lemma_id = _insert_lemma(conn)
            _insert_frequency(conn, lemma_id)
            conn.execute(
                cefr_levels.insert().values(lemma_id=lemma_id, level="B1", source="profilo")
            )
            conn.execute(nvdb_tiers.insert().values(lemma_id=lemma_id, tier="AU"))

            verb = Verb(lemma_id=lemma_id, written="parlare", stressed="parlare", ipa=None)
            tags = build_verb_tags(conn, verb, "presente_indicativo")

            assert "cefr::B1" in tags
            assert "nvdb::AU" in tags

    def test_omits_when_no_data(self, temp_db: Path) -> None:
        with get_connection(temp_db) as conn:
            lemma_id = _insert_lemma(conn)
            _insert_frequency(conn, lemma_id)

            verb = Verb(lemma_id=lemma_id, written="parlare", stressed="parlare", ipa=None)
            tags = build_verb_tags(conn, verb, "presente_indicativo")

            assert not any(t.startswith("cefr::") for t in tags)
            assert not any(t.startswith("nvdb::") for t in tags)

    def test_tag_ordering(self, temp_db: Path) -> None:
        """CEFR/NVdB tags appear after freq bands and before infinitive tag."""
        with get_connection(temp_db) as conn:
            lemma_id = _insert_lemma(conn)
            _insert_frequency(conn, lemma_id)
            conn.execute(
                cefr_levels.insert().values(lemma_id=lemma_id, level="A2", source="profilo")
            )
            conn.execute(nvdb_tiers.insert().values(lemma_id=lemma_id, tier="FO"))

            verb = Verb(lemma_id=lemma_id, written="parlare", stressed="parlare", ipa=None)
            tags = build_verb_tags(conn, verb, "presente_indicativo")

            cefr_idx = tags.index("cefr::A2")
            nvdb_idx = tags.index("nvdb::FO")
            inf_idx = tags.index("infinitive::parlare")

            # cefr and nvdb should come before infinitive
            assert cefr_idx < inf_idx
            assert nvdb_idx < inf_idx
