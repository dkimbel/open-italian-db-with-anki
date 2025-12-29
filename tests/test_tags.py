"""Tests for tag parsing functions."""

from italian_anki.tags import parse_verb_tags, should_filter_form


class TestShouldFilterForm:
    """Tests for should_filter_form with archaic/obsolete/dated tags."""

    def test_filters_archaic_forms(self) -> None:
        """Test that forms with archaic tag are filtered."""
        assert should_filter_form(["archaic", "indicative", "present"]) is True
        assert should_filter_form(["archaic"]) is True

    def test_filters_obsolete_forms(self) -> None:
        """Test that forms with obsolete tag are filtered."""
        assert should_filter_form(["obsolete", "participle", "past"]) is True
        assert should_filter_form(["obsolete"]) is True

    def test_filters_dated_forms(self) -> None:
        """Test that forms with dated tag are filtered."""
        assert should_filter_form(["dated", "subjunctive", "imperfect"]) is True
        assert should_filter_form(["dated"]) is True

    def test_allows_rare_forms(self) -> None:
        """Test that forms with rare tag are NOT filtered (kept with label)."""
        assert should_filter_form(["rare", "indicative", "present"]) is False

    def test_allows_literary_forms(self) -> None:
        """Test that forms with literary tag are NOT filtered (kept with label)."""
        assert should_filter_form(["literary", "participle", "past"]) is False

    def test_allows_uncommon_forms(self) -> None:
        """Test that forms with uncommon tag are NOT filtered (kept with label)."""
        assert should_filter_form(["uncommon", "gerund"]) is False

    def test_allows_poetic_forms(self) -> None:
        """Test that forms with poetic tag are NOT filtered (kept with label)."""
        assert should_filter_form(["poetic", "infinitive"]) is False

    def test_filters_misspelling(self) -> None:
        """Test that existing misspelling filter still works."""
        assert should_filter_form(["misspelling"]) is True

    def test_allows_standard_form(self) -> None:
        """Test that standard forms without labels are allowed."""
        assert should_filter_form(["indicative", "present", "singular"]) is False


class TestParseVerbTagsParticipleFilter:
    """Tests for filtering buggy participle entries with person tags."""

    def test_filters_participle_with_first_person(self) -> None:
        """Test that participles with first-person tag are filtered.

        This catches Wiktextract bugs like empiùto from empiere with tags
        ['first-person', 'participle', 'past', 'present', 'singular'].
        """
        tags = ["first-person", "participle", "past", "present", "singular"]
        result = parse_verb_tags(tags)
        assert result.should_filter is True

    def test_filters_participle_with_second_person(self) -> None:
        """Test that participles with second-person tag are filtered."""
        tags = ["second-person", "participle", "past"]
        result = parse_verb_tags(tags)
        assert result.should_filter is True

    def test_filters_participle_with_third_person(self) -> None:
        """Test that participles with third-person tag are filtered."""
        tags = ["third-person", "participle", "past"]
        result = parse_verb_tags(tags)
        assert result.should_filter is True

    def test_allows_participle_without_person(self) -> None:
        """Test that valid participles without person tags are allowed."""
        tags = ["participle", "past"]
        result = parse_verb_tags(tags)
        assert result.should_filter is False
        assert result.mood == "participle"

    def test_allows_participle_with_labels(self) -> None:
        """Test that participles with usage labels are allowed."""
        tags = ["participle", "past", "uncommon"]
        result = parse_verb_tags(tags)
        assert result.should_filter is False
        assert result.mood == "participle"
        assert result.labels == "uncommon"

    def test_allows_finite_verb_with_person(self) -> None:
        """Test that finite verbs with person tags are still allowed."""
        tags = ["first-person", "indicative", "present", "singular"]
        result = parse_verb_tags(tags)
        assert result.should_filter is False
        assert result.mood == "indicative"
        assert result.person == 1


class TestParseVerbTagsBasic:
    """Basic tests for parse_verb_tags."""

    def test_extracts_mood(self) -> None:
        """Test that mood is extracted correctly."""
        tags = ["indicative", "present", "first-person", "singular"]
        result = parse_verb_tags(tags)
        assert result.mood == "indicative"

    def test_extracts_tense(self) -> None:
        """Test that tense is extracted correctly."""
        tags = ["indicative", "present", "first-person", "singular"]
        result = parse_verb_tags(tags)
        assert result.tense == "present"

    def test_extracts_person(self) -> None:
        """Test that person is extracted correctly."""
        tags = ["indicative", "present", "first-person", "singular"]
        result = parse_verb_tags(tags)
        assert result.person == 1

    def test_extracts_number(self) -> None:
        """Test that number is extracted correctly."""
        tags = ["indicative", "present", "first-person", "singular"]
        result = parse_verb_tags(tags)
        assert result.number == "singular"

    def test_participle_past_tense(self) -> None:
        """Test that past participles have tense='past' for queryability."""
        tags = ["participle", "past"]
        result = parse_verb_tags(tags)
        assert result.mood == "participle"
        assert result.tense == "past"

    def test_extracts_gender_for_participle(self) -> None:
        """Test that gender can be extracted for participles."""
        tags = ["participle", "past", "masculine", "singular"]
        result = parse_verb_tags(tags)
        assert result.gender == "m"
        assert result.number == "singular"

    def test_passato_remoto(self) -> None:
        """Test passato remoto detection (past + historic -> remote)."""
        tags = ["indicative", "past", "historic", "first-person", "singular"]
        result = parse_verb_tags(tags)
        assert result.tense == "remote"
