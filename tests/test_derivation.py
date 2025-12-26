"""Tests for derivation functions."""

from italian_anki.derivation import derive_participle_forms


class TestDeriveParticipleForms:
    """Tests for derive_participle_forms function."""

    def test_regular_are_verb(self) -> None:
        """Test regular -are verb participle (parlare -> parlato)."""
        forms = derive_participle_forms("parlàto")
        assert forms == [
            ("parlàta", "feminine", "singular"),
            ("parlàti", "masculine", "plural"),
            ("parlàte", "feminine", "plural"),
        ]

    def test_regular_ere_verb(self) -> None:
        """Test regular -ere verb participle (credere -> creduto)."""
        forms = derive_participle_forms("credùto")
        assert forms == [
            ("credùta", "feminine", "singular"),
            ("credùti", "masculine", "plural"),
            ("credùte", "feminine", "plural"),
        ]

    def test_regular_ire_verb(self) -> None:
        """Test regular -ire verb participle (dormire -> dormito)."""
        forms = derive_participle_forms("dormìto")
        assert forms == [
            ("dormìta", "feminine", "singular"),
            ("dormìti", "masculine", "plural"),
            ("dormìte", "feminine", "plural"),
        ]

    def test_irregular_fatto(self) -> None:
        """Test irregular participle fatto (fare)."""
        forms = derive_participle_forms("fàtto")
        assert forms == [
            ("fàtta", "feminine", "singular"),
            ("fàtti", "masculine", "plural"),
            ("fàtte", "feminine", "plural"),
        ]

    def test_irregular_scritto(self) -> None:
        """Test irregular participle scritto (scrivere)."""
        forms = derive_participle_forms("scrìtto")
        assert forms == [
            ("scrìtta", "feminine", "singular"),
            ("scrìtti", "masculine", "plural"),
            ("scrìtte", "feminine", "plural"),
        ]

    def test_irregular_aperto(self) -> None:
        """Test irregular participle aperto (aprire)."""
        forms = derive_participle_forms("apèrto")
        assert forms == [
            ("apèrta", "feminine", "singular"),
            ("apèrti", "masculine", "plural"),
            ("apèrte", "feminine", "plural"),
        ]

    def test_no_accent(self) -> None:
        """Test form without accent (still works)."""
        forms = derive_participle_forms("parlato")
        assert forms == [
            ("parlata", "feminine", "singular"),
            ("parlati", "masculine", "plural"),
            ("parlate", "feminine", "plural"),
        ]

    def test_non_o_ending_returns_empty(self) -> None:
        """Test that non-o endings return empty list (e.g., clitic forms)."""
        # Clitic forms like 'creatosi' end in 'i', not 'o'
        assert derive_participle_forms("creatòsi") == []
        assert derive_participle_forms("datàne") == []
        assert derive_participle_forms("impostaglì") == []

    def test_empty_string_returns_empty(self) -> None:
        """Test empty string returns empty list."""
        assert derive_participle_forms("") == []

    def test_single_char_o(self) -> None:
        """Test edge case of single character 'o'."""
        # Should work but produce unusual forms
        forms = derive_participle_forms("o")
        assert forms == [
            ("a", "feminine", "singular"),
            ("i", "masculine", "plural"),
            ("e", "feminine", "plural"),
        ]

    def test_preserves_stress_marks(self) -> None:
        """Test that stress marks are preserved in derived forms."""
        forms = derive_participle_forms("mangìato")
        assert all("ì" in f[0] for f in forms)

    def test_double_consonant(self) -> None:
        """Test participle with double consonant before -o."""
        forms = derive_participle_forms("dètto")
        assert forms == [
            ("dètta", "feminine", "singular"),
            ("dètti", "masculine", "plural"),
            ("dètte", "feminine", "plural"),
        ]
