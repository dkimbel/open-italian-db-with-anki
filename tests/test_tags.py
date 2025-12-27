"""Tests for tag parsing functions."""

from italian_anki.tags import parse_verb_tags


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
