"""Tests for Anki tag features: CEFR, NVdB, and thematic tags."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from sqlalchemy import Connection

from anki_gen.generator import build_verb_tags
from anki_gen.queries import Verb, get_cefr_level, get_nvdb_tier, get_thematic_tags
from anki_gen.topic_tags import (
    THEMATIC_TAG_RENAMES,
    THEMATIC_TAG_WHITELIST,
    normalize_thematic_tag,
)
from italian_db.db import (
    cefr_levels,
    definition_tags,
    definitions,
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


# ── Helpers for thematic tag tests ────────────────────────────────────────


def _insert_definition(conn: Connection, lemma_id: int, gloss: str = "to cook") -> int:
    """Insert a minimal definition and return its id."""
    result = conn.execute(definitions.insert().values(lemma_id=lemma_id, gloss=gloss))
    pk = result.inserted_primary_key
    assert pk is not None
    return pk[0]


def _insert_definition_tag(
    conn: Connection,
    definition_id: int,
    tag: str,
    source: str = "wiktextract:topic",
    kind: str | None = None,
) -> None:
    """Insert a definition tag."""
    conn.execute(
        definition_tags.insert().values(
            definition_id=definition_id,
            tag=tag,
            source=source,
            kind=kind,
        )
    )


# ── normalize_thematic_tag unit tests ─────────────────────────────────────


class TestNormalizeThematicTag:
    """Unit tests for the normalize_thematic_tag function."""

    def test_topic_lowercase_passthrough(self) -> None:
        """Topic 'cooking' is already canonical."""
        assert normalize_thematic_tag("cooking") == "cooking"

    def test_category_title_case_auto_normalize(self) -> None:
        """Category 'Cooking' auto-normalizes to 'cooking'."""
        assert normalize_thematic_tag("Cooking") == "cooking"

    def test_multi_word_category_auto_normalize(self) -> None:
        """Category 'Organic chemistry' auto-normalizes to 'organic-chemistry'."""
        assert normalize_thematic_tag("Organic chemistry") == "organic-chemistry"

    def test_rename_parenthetical(self) -> None:
        """'Football (soccer)' uses rename to 'football'."""
        assert normalize_thematic_tag("Football (soccer)") == "football"

    def test_rename_plural(self) -> None:
        """'Foods' uses rename to 'food'."""
        assert normalize_thematic_tag("Foods") == "food"

    def test_rename_compound(self) -> None:
        """'Cakes and pastries' uses rename to 'pastries'."""
        assert normalize_thematic_tag("Cakes and pastries") == "pastries"

    def test_rename_topic_form(self) -> None:
        """Topic 'underwater-diving' uses rename to 'diving'."""
        assert normalize_thematic_tag("underwater-diving") == "diving"

    def test_excluded_ancestor(self) -> None:
        """Auto-generalized ancestor 'sciences' is excluded."""
        assert normalize_thematic_tag("sciences") is None

    def test_excluded_ancestor_lifestyle(self) -> None:
        """Auto-generalized ancestor 'lifestyle' is excluded."""
        assert normalize_thematic_tag("lifestyle") is None

    def test_excluded_noise_category(self) -> None:
        """Structural category 'Italian onomatopoeias' is excluded."""
        assert normalize_thematic_tag("Italian onomatopoeias") is None

    def test_excluded_hyper_specific(self) -> None:
        """Hyper-specific taxonomy tag is excluded."""
        assert normalize_thematic_tag("Borage family plants") is None

    def test_excluded_demonym(self) -> None:
        """'Demonyms' is excluded."""
        assert normalize_thematic_tag("Demonyms") is None

    @pytest.mark.parametrize("target", sorted(set(THEMATIC_TAG_RENAMES.values())))
    def test_rename_targets_in_whitelist(self, target: str) -> None:
        """Every rename target must exist in the whitelist."""
        assert target in THEMATIC_TAG_WHITELIST


# ── get_thematic_tags integration tests ───────────────────────────────────


class TestGetThematicTags:
    """Integration tests for get_thematic_tags with normalization."""

    def test_category_cooking(self, temp_db: Path) -> None:
        """Category 'Cooking' → topic::cooking."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn, "cucinare")
            did = _insert_definition(conn, lid)
            _insert_definition_tag(
                conn, did, "Cooking", source="wiktextract:category", kind="other"
            )
            assert get_thematic_tags(conn, lid) == ["topic::cooking"]

    def test_topic_cooking(self, temp_db: Path) -> None:
        """Topic 'cooking' → topic::cooking."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn, "cucinare")
            did = _insert_definition(conn, lid)
            _insert_definition_tag(conn, did, "cooking", source="wiktextract:topic")
            assert get_thematic_tags(conn, lid) == ["topic::cooking"]

    def test_dedup_category_and_topic(self, temp_db: Path) -> None:
        """Both 'Cooking' (category) and 'cooking' (topic) → single topic::cooking."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn, "cucinare")
            did = _insert_definition(conn, lid)
            _insert_definition_tag(
                conn, did, "Cooking", source="wiktextract:category", kind="other"
            )
            _insert_definition_tag(conn, did, "cooking", source="wiktextract:topic")
            result = get_thematic_tags(conn, lid)
            assert result == ["topic::cooking"]

    def test_ancestor_excluded(self, temp_db: Path) -> None:
        """Ancestor topics 'sciences', 'lifestyle' → excluded."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn)
            did = _insert_definition(conn, lid)
            _insert_definition_tag(conn, did, "sciences", source="wiktextract:topic")
            _insert_definition_tag(conn, did, "lifestyle", source="wiktextract:topic")
            assert get_thematic_tags(conn, lid) == []

    def test_noise_category_excluded(self, temp_db: Path) -> None:
        """Noise category 'Italian onomatopoeias' → excluded."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn)
            did = _insert_definition(conn, lid)
            _insert_definition_tag(
                conn, did, "Italian onomatopoeias", source="wiktextract:category", kind="other"
            )
            assert get_thematic_tags(conn, lid) == []

    def test_place_kind_excluded(self, temp_db: Path) -> None:
        """kind='place' category → excluded (existing behavior)."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn)
            did = _insert_definition(conn, lid)
            _insert_definition_tag(conn, did, "Rome", source="wiktextract:category", kind="place")
            assert get_thematic_tags(conn, lid) == []

    def test_rename_football(self, temp_db: Path) -> None:
        """'Football (soccer)' → topic::football via rename."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn, "calciare")
            did = _insert_definition(conn, lid)
            _insert_definition_tag(
                conn, did, "Football (soccer)", source="wiktextract:category", kind="other"
            )
            assert get_thematic_tags(conn, lid) == ["topic::football"]

    def test_no_tags(self, temp_db: Path) -> None:
        """Lemma with no tags → empty list."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn)
            assert get_thematic_tags(conn, lid) == []

    def test_multiple_tags_sorted(self, temp_db: Path) -> None:
        """Multiple mapped tags → sorted alphabetically."""
        with get_connection(temp_db) as conn:
            lid = _insert_lemma(conn)
            did = _insert_definition(conn, lid)
            _insert_definition_tag(conn, did, "music", source="wiktextract:topic")
            _insert_definition_tag(conn, did, "cooking", source="wiktextract:topic")
            _insert_definition_tag(conn, did, "anatomy", source="wiktextract:topic")
            result = get_thematic_tags(conn, lid)
            assert result == ["topic::anatomy", "topic::cooking", "topic::music"]
