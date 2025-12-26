"""Tests for text normalization utilities."""

from italian_anki.normalize import derive_written_from_stressed, normalize, tokenize


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


class TestDeriveWrittenFromStressed:
    """Tests for derive_written_from_stressed function."""

    def test_strips_non_final_accent(self) -> None:
        assert derive_written_from_stressed("pàrlo") == "parlo"
        assert derive_written_from_stressed("bèlla") == "bella"
        assert derive_written_from_stressed("parlàre") == "parlare"

    def test_keeps_final_accent_polysyllable(self) -> None:
        assert derive_written_from_stressed("parlò") == "parlò"
        assert derive_written_from_stressed("città") == "città"
        assert derive_written_from_stressed("perché") == "perché"

    def test_whitelist_monosyllables(self) -> None:
        assert derive_written_from_stressed("dà") == "dà"
        assert derive_written_from_stressed("è") == "è"
        assert derive_written_from_stressed("più") == "più"
        assert derive_written_from_stressed("sì") == "sì"

    def test_strips_non_whitelist_monosyllables(self) -> None:
        assert derive_written_from_stressed("fù") == "fu"
        assert derive_written_from_stressed("blù") == "blu"

    def test_blacklist_never_accented(self) -> None:
        # qua/qui should never have accents even if source has them
        assert derive_written_from_stressed("quà") == "qua"
        assert derive_written_from_stressed("quì") == "qui"

    def test_no_accent_returns_unchanged(self) -> None:
        assert derive_written_from_stressed("parlo") == "parlo"
        assert derive_written_from_stressed("casa") == "casa"

    def test_empty_returns_none(self) -> None:
        assert derive_written_from_stressed("") is None

    def test_multi_word_phrase(self) -> None:
        assert derive_written_from_stressed("volùto dìre") == "voluto dire"
        assert derive_written_from_stressed("andàre giù") == "andare giù"
        assert derive_written_from_stressed("èssere in sé") == "essere in sé"

    def test_multi_word_with_unaccented_words(self) -> None:
        assert derive_written_from_stressed("il bèllo") == "il bello"
        assert derive_written_from_stressed("la càsa") == "la casa"
