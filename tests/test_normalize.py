"""Tests for text normalization utilities."""

from italian_anki.normalize import normalize, tokenize


class TestNormalize:
    """Tests for the normalize function."""

    def test_strips_grave_accent(self) -> None:
        assert normalize("può") == "puo"
        assert normalize("città") == "citta"
        assert normalize("è") == "e"

    def test_strips_acute_accent(self) -> None:
        assert normalize("perché") == "perche"

    def test_strips_pedagogical_stress_marks(self) -> None:
        # Wiktextract uses these to show stress position
        assert normalize("pàrlo") == "parlo"
        assert normalize("parlàto") == "parlato"
        assert normalize("reìtero") == "reitero"

    def test_lowercases(self) -> None:
        assert normalize("Mangiare") == "mangiare"
        assert normalize("PARLARE") == "parlare"

    def test_preserves_base_characters(self) -> None:
        assert normalize("parlare") == "parlare"
        assert normalize("abc") == "abc"

    def test_handles_empty_string(self) -> None:
        assert normalize("") == ""

    def test_handles_multiple_accents(self) -> None:
        assert normalize("così") == "cosi"
        assert normalize("perciò") == "percio"


class TestTokenize:
    """Tests for the tokenize function."""

    def test_splits_on_spaces(self) -> None:
        assert tokenize("lui può parlare") == ["lui", "può", "parlare"]

    def test_removes_punctuation(self) -> None:
        assert tokenize("Ciao, come stai?") == ["ciao", "come", "stai"]
        assert tokenize("Lui può parlare.") == ["lui", "può", "parlare"]

    def test_preserves_apostrophes_within_words(self) -> None:
        assert tokenize("dov'è il libro") == ["dov'è", "il", "libro"]
        assert tokenize("l'uomo") == ["l'uomo"]

    def test_lowercases_tokens(self) -> None:
        assert tokenize("Lui Può Parlare") == ["lui", "può", "parlare"]

    def test_handles_empty_string(self) -> None:
        assert tokenize("") == []

    def test_handles_multiple_spaces(self) -> None:
        assert tokenize("uno   due    tre") == ["uno", "due", "tre"]

    def test_handles_numbers_as_separators(self) -> None:
        # Numbers break words but aren't included
        assert tokenize("abc123def") == ["abc", "def"]

    def test_strips_leading_trailing_apostrophes(self) -> None:
        assert tokenize("'ciao'") == ["ciao"]
        assert tokenize("'test") == ["test"]
