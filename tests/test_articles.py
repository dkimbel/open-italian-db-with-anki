"""Tests for Italian article determination."""

from italian_anki.articles import (
    ArticleSelector,
    derive_indefinite,
    derive_partitive,
    get_definite,
)


class TestGetDefinite:
    """Tests for the get_definite function."""

    # Basic masculine singular
    def test_masculine_singular_consonant(self):
        """il libro"""
        article, source = get_definite("libro", "m", "singular")
        assert article == "il"
        assert source == "inferred"

    def test_masculine_singular_vowel(self):
        """l'amico"""
        article, source = get_definite("amico", "m", "singular")
        assert article == "l'"
        assert source == "inferred"

    # Masculine singular with lo-triggers
    def test_masculine_singular_z(self):
        """lo zaino"""
        article, source = get_definite("zaino", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_s_consonant(self):
        """lo studente"""
        article, source = get_definite("studente", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_gn(self):
        """lo gnomo"""
        article, source = get_definite("gnomo", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_ps(self):
        """lo psicologo"""
        article, source = get_definite("psicologo", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_pn(self):
        """lo pneumatico"""
        article, source = get_definite("pneumatico", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_x(self):
        """lo xilofono"""
        article, source = get_definite("xilofono", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_y(self):
        """lo yogurt"""
        article, source = get_definite("yogurt", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_i_vowel(self):
        """lo iodio - i + vowel is a semiconsonant"""
        article, source = get_definite("iodio", "m", "singular")
        assert article == "lo"
        assert source == "inferred"

    def test_masculine_singular_i_consonant(self):
        """l'italiano - i + consonant is a vowel"""
        article, source = get_definite("italiano", "m", "singular")
        assert article == "l'"
        assert source == "inferred"

    # Masculine plural
    def test_masculine_plural_consonant(self):
        """i libri"""
        article, source = get_definite("libri", "m", "plural")
        assert article == "i"
        assert source == "inferred"

    def test_masculine_plural_vowel(self):
        """gli amici"""
        article, source = get_definite("amici", "m", "plural")
        assert article == "gli"
        assert source == "inferred"

    def test_masculine_plural_s_consonant(self):
        """gli studenti"""
        article, source = get_definite("studenti", "m", "plural")
        assert article == "gli"
        assert source == "inferred"

    def test_masculine_plural_z(self):
        """gli zaini"""
        article, source = get_definite("zaini", "m", "plural")
        assert article == "gli"
        assert source == "inferred"

    # Feminine singular
    def test_feminine_singular_consonant(self):
        """la casa"""
        article, source = get_definite("casa", "f", "singular")
        assert article == "la"
        assert source == "inferred"

    def test_feminine_singular_vowel(self):
        """l'amica"""
        article, source = get_definite("amica", "f", "singular")
        assert article == "l'"
        assert source == "inferred"

    def test_feminine_singular_lo_trigger(self):
        """la scuola - feminine uses la even for lo-triggers"""
        article, source = get_definite("scuola", "f", "singular")
        assert article == "la"
        assert source == "inferred"

    # Feminine plural (always le)
    def test_feminine_plural_consonant(self):
        """le case"""
        article, source = get_definite("case", "f", "plural")
        assert article == "le"
        assert source == "inferred"

    def test_feminine_plural_vowel(self):
        """le amiche - no elision in feminine plural"""
        article, source = get_definite("amiche", "f", "plural")
        assert article == "le"
        assert source == "inferred"

    def test_feminine_plural_lo_trigger(self):
        """le scuole"""
        article, source = get_definite("scuole", "f", "plural")
        assert article == "le"
        assert source == "inferred"

    # Accented vowels
    def test_accented_initial_vowel(self):
        """l'ètà - accented vowel treated as vowel"""
        article, source = get_definite("età", "f", "singular")
        assert article == "l'"
        assert source == "inferred"


class TestExceptions:
    """Tests for exception dictionary handling."""

    # Silent H loanwords
    def test_hotel_singular(self):
        """l'hotel - silent H"""
        article, source = get_definite("hotel", "m", "singular")
        assert article == "l'"
        assert source == "exception:silent_h"

    def test_hotel_plural(self):
        """gli hotel"""
        article, source = get_definite("hotel", "m", "plural")
        assert article == "gli"
        assert source == "exception:silent_h"

    def test_hobby(self):
        """l'hobby"""
        article, source = get_definite("hobby", "m", "singular")
        assert article == "l'"
        assert source == "exception:silent_h"

    def test_hamburger(self):
        """l'hamburger"""
        article, source = get_definite("hamburger", "m", "singular")
        assert article == "l'"
        assert source == "exception:silent_h"

    # W as consonant
    def test_web(self):
        """il web"""
        article, source = get_definite("web", "m", "singular")
        assert article == "il"
        assert source == "exception:w_consonant"

    def test_weekend(self):
        """il weekend"""
        article, source = get_definite("weekend", "m", "singular")
        assert article == "il"
        assert source == "exception:w_consonant"

    def test_whisky(self):
        """il whisky"""
        article, source = get_definite("whisky", "m", "singular")
        assert article == "il"
        assert source == "exception:w_consonant"

    # Historical exception
    def test_dei_plural_of_dio(self):
        """gli dèi - historical exception"""
        article, source = get_definite("dèi", "m", "plural")
        assert article == "gli"
        assert source == "exception:historical"

    def test_dei_alternate_spelling(self):
        """gli dei - alternate spelling"""
        article, source = get_definite("dei", "m", "plural")
        assert article == "gli"
        assert source == "exception:historical"

    # Case insensitivity
    def test_exception_case_insensitive(self):
        """Exceptions should match regardless of case."""
        article, source = get_definite("HOTEL", "m", "singular")
        assert article == "l'"
        assert source == "exception:silent_h"


class TestDeriveIndefinite:
    """Tests for deriving indefinite articles."""

    def test_from_il(self):
        assert derive_indefinite("il", "m") == "un"

    def test_from_lo(self):
        assert derive_indefinite("lo", "m") == "uno"

    def test_from_la(self):
        assert derive_indefinite("la", "f") == "una"

    def test_from_elided_masculine(self):
        assert derive_indefinite("l'", "m") == "un"

    def test_from_elided_feminine(self):
        assert derive_indefinite("l'", "f") == "un'"

    def test_plural_returns_none(self):
        assert derive_indefinite("i", "m") is None
        assert derive_indefinite("gli", "m") is None
        assert derive_indefinite("le", "f") is None


class TestDerivePartitive:
    """Tests for deriving partitive articles."""

    def test_from_il(self):
        assert derive_partitive("il") == "del"

    def test_from_lo(self):
        assert derive_partitive("lo") == "dello"

    def test_from_elided(self):
        assert derive_partitive("l'") == "dell'"

    def test_from_la(self):
        assert derive_partitive("la") == "della"

    def test_from_i(self):
        assert derive_partitive("i") == "dei"

    def test_from_gli(self):
        assert derive_partitive("gli") == "degli"

    def test_from_le(self):
        assert derive_partitive("le") == "delle"


class TestArticleSelector:
    """Tests for the ArticleSelector class."""

    def test_basic_definite(self):
        """Basic definite article lookup."""
        selector = ArticleSelector()
        article, source = selector.get_definite("libro", "m", "singular")
        assert article == "il"
        assert source == "inferred"

    def test_indefinite(self):
        """Indefinite article through selector."""
        selector = ArticleSelector()
        article, source = selector.get_indefinite("libro", "m")
        assert article == "un"
        assert source == "inferred"

    def test_partitive(self):
        """Partitive article through selector."""
        selector = ArticleSelector()
        article, source = selector.get_partitive("libro", "m", "singular")
        assert article == "del"
        assert source == "inferred"

    def test_extra_exceptions(self):
        """Custom exceptions can be added."""
        selector = ArticleSelector(extra_exceptions={"customword": ("vowel", "exception:custom")})
        article, source = selector.get_definite("customword", "m", "singular")
        assert article == "l'"
        assert source == "exception:custom"


class TestEdgeCases:
    """Tests for specific edge cases mentioned in requirements."""

    def test_italiano_not_semiconsonant(self):
        """italiano starts with i+consonant, so it's a vowel, not semiconsonant."""
        article, _ = get_definite("italiano", "m", "singular")
        assert article == "l'"

    def test_iato_semiconsonant(self):
        """iato starts with i+vowel (ia), so it's a semiconsonant -> lo."""
        article, _ = get_definite("iato", "m", "singular")
        assert article == "lo"

    def test_ione_semiconsonant(self):
        """ione starts with i+vowel (io), so it's a semiconsonant -> lo."""
        article, _ = get_definite("ione", "m", "singular")
        assert article == "lo"

    def test_sbaglio_s_consonant(self):
        """sbaglio has s+consonant -> lo."""
        article, _ = get_definite("sbaglio", "m", "singular")
        assert article == "lo"

    def test_sport_s_consonant(self):
        """sport has s+consonant -> lo."""
        article, _ = get_definite("sport", "m", "singular")
        assert article == "lo"

    def test_strada_s_consonant(self):
        """strada has s+consonant -> lo."""
        # But strada is feminine, so it's "la"
        article, _ = get_definite("strada", "f", "singular")
        assert article == "la"

    def test_s_vowel_not_lo_trigger(self):
        """sole starts with s+vowel, not lo-trigger."""
        article, _ = get_definite("sole", "m", "singular")
        assert article == "il"

    def test_stressed_form_works(self):
        """Articles work with stressed forms (with accent marks)."""
        article, _ = get_definite("càmera", "f", "singular")
        assert article == "la"

        article, _ = get_definite("àlbero", "m", "singular")
        assert article == "l'"


class TestCaching:
    """Tests to verify caching behavior."""

    def test_same_input_same_output(self):
        """Same inputs should produce same outputs."""
        result1 = get_definite("libro", "m", "singular")
        result2 = get_definite("libro", "m", "singular")
        assert result1 == result2

    def test_different_number_different_output(self):
        """Different numbers should produce different articles."""
        sing = get_definite("libro", "m", "singular")
        plur = get_definite("libri", "m", "plural")
        assert sing[0] == "il"
        assert plur[0] == "i"
