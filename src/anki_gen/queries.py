"""Database queries for Anki deck generation.

This module provides read-only queries against the Italian language database.
All functions take a SQLAlchemy Connection and return dataclasses.
"""

from dataclasses import dataclass

from sqlalchemy import Connection, select, text

from italian_db.db import (
    frequencies,
    lemmas,
    verb_forms,
)
from italian_db.enums import POS


@dataclass(frozen=True)
class Verb:
    """A verb lemma with its metadata."""

    lemma_id: int
    written: str  # Real Italian orthography
    stressed: str  # With pedagogical stress marks
    ipa: str | None  # IPA pronunciation


@dataclass(frozen=True)
class VerbForm:
    """A single conjugated verb form."""

    written: str | None  # Real Italian orthography
    stressed: str  # With pedagogical stress marks
    person: int  # 1, 2, 3
    number: str  # "singular" or "plural"
    ipa: str | None = None  # IPA pronunciation (from verb_forms.ipa)


@dataclass(frozen=True)
class ExampleSentence:
    """An Italian sentence with optional English translation."""

    italian: str
    english: str | None


def get_verb_by_lemma(conn: Connection, written: str) -> Verb | None:
    """Look up a verb by its written form.

    Args:
        conn: Database connection
        written: The written infinitive (e.g., "parlare")

    Returns:
        Verb dataclass or None if not found
    """
    # Join with verb_forms to get IPA from the citation form (infinitive)
    stmt = (
        select(
            lemmas.c.id,
            lemmas.c.written,
            lemmas.c.stressed,
            verb_forms.c.ipa,
        )
        .select_from(
            lemmas.outerjoin(
                verb_forms,
                (verb_forms.c.lemma_id == lemmas.c.id) & (verb_forms.c.is_citation_form == True),  # noqa: E712
            )
        )
        .where(
            lemmas.c.written == written,
            lemmas.c.pos == POS.VERB,
        )
    )

    row = conn.execute(stmt).fetchone()
    if row is None:
        return None

    return Verb(
        lemma_id=row.id,
        written=row.written,
        stressed=row.stressed,
        ipa=row.ipa,
    )


def get_present_indicative_forms(conn: Connection, lemma_id: int) -> list[VerbForm]:
    """Get all present indicative forms for a verb, including IPA pronunciations.

    Args:
        conn: Database connection
        lemma_id: The lemma ID

    Returns:
        List of VerbForm dataclasses, ordered by person and number.
        Each form includes IPA pronunciation if available in the database.
    """
    stmt = (
        select(
            verb_forms.c.written,
            verb_forms.c.stressed,
            verb_forms.c.person,
            verb_forms.c.number,
            verb_forms.c.ipa,
        )
        .where(
            verb_forms.c.lemma_id == lemma_id,
            verb_forms.c.mood == "indicative",
            verb_forms.c.tense == "present",
            verb_forms.c.labels.is_(None),  # Standard forms only
            verb_forms.c.is_formal.is_(False),  # Exclude Lei/Loro
        )
        .order_by(verb_forms.c.number, verb_forms.c.person)
    )

    rows = conn.execute(stmt).fetchall()
    return [
        VerbForm(
            written=row.written,
            stressed=row.stressed,
            person=row.person,
            number=row.number,
            ipa=row.ipa,
        )
        for row in rows
    ]


# Mapping from internal mood names to Universal Dependencies values
MOOD_TO_UD: dict[str, str] = {
    "indicative": "Ind",
    "subjunctive": "Sub",
    "conditional": "Cnd",
    "imperative": "Imp",
}

# Mapping from internal tense names to Universal Dependencies values
TENSE_TO_UD: dict[str, str] = {
    "present": "Pres",
    "imperfect": "Imp",
    "remote": "Past",  # passato remoto
    "future": "Fut",
}

# Mapping from tense_id (card generator) to (mood, tense) for morphological lookup
TENSE_ID_TO_MOOD_TENSE: dict[str, tuple[str, str]] = {
    "presente_indicativo": ("indicative", "present"),
    "imperfetto": ("indicative", "imperfect"),
    "passato_remoto": ("indicative", "remote"),
    "futuro_semplice": ("indicative", "future"),
    # Future additions:
    # "presente_congiuntivo": ("subjunctive", "present"),
    # "condizionale_presente": ("conditional", "present"),
}


def get_morphological_example_sentence(
    conn: Connection,
    lemma: str,
    *,
    mood: str | None = None,
    tense: str | None = None,
    max_length: int = 120,
    min_words: int = 7,
    fallback_min_words: int = 3,
) -> ExampleSentence | None:
    """Find an example sentence with a specific morphological form from ParTUT.

    This searches the morphologically-tagged ParTUT corpus for sentences
    containing the verb lemma in a specific mood and/or tense.

    Args:
        conn: Database connection
        lemma: The verb lemma to search for (e.g., "essere")
        mood: Optional mood filter (indicative, subjunctive, conditional, imperative)
        tense: Optional tense filter (present, imperfect, remote, future)
        max_length: Maximum sentence length in characters
        min_words: Preferred minimum number of words in sentence
        fallback_min_words: Fallback minimum if no sentence with min_words found

    Returns:
        ExampleSentence with Italian text and optional English translation,
        or None if no suitable sentence found
    """
    # Convert internal mood/tense names to UD values
    ud_mood = MOOD_TO_UD.get(mood, mood) if mood else None
    ud_tense = TENSE_TO_UD.get(tense, tense) if tense else None

    params: dict[str, str | int] = {
        "lemma": lemma,
        "max_length": max_length,
        "min_words": min_words,
    }

    # Build mood/tense filter conditions
    # These are safe because values come from our internal MOOD_TO_UD/TENSE_TO_UD mappings
    mood_filter = " AND st.mood = :mood" if ud_mood else ""
    tense_filter = " AND st.tense = :tense" if ud_tense else ""

    if ud_mood:
        params["mood"] = ud_mood
    if ud_tense:
        params["tense"] = ud_tense

    # Query with translation join
    # Note: mood_filter and tense_filter are built from hardcoded strings, not user input
    # sentence_tokens.sentence_id and translations.*_sentence_id reference sentences.id (surrogate)
    query = text(
        "SELECT DISTINCT s.id, s.text, eng.text as english "
        "FROM sentence_tokens st "
        "JOIN sentences s ON st.sentence_id = s.id "
        "LEFT JOIN translations t ON t.ita_sentence_id = s.id "
        "LEFT JOIN sentences eng ON t.eng_sentence_id = eng.id "
        "WHERE st.lemma = :lemma" + mood_filter + tense_filter + " AND s.source = 'partut'"
        " AND s.lang = 'ita'"
        " AND length(s.text) <= :max_length"
        " AND (length(s.text) - length(replace(s.text, ' ', '')) + 1) >= :min_words"
        " ORDER BY"
        " CASE WHEN eng.text IS NOT NULL THEN 0 ELSE 1 END,"
        " length(s.text)"
        " LIMIT 1"
    )

    row = conn.execute(query, params).fetchone()

    # Fallback with shorter minimum
    if row is None and fallback_min_words < min_words:
        params["min_words"] = fallback_min_words
        row = conn.execute(query, params).fetchone()

    if row is None:
        return None

    return ExampleSentence(italian=row[1], english=row[2])


def get_example_sentence_with_fallback(
    conn: Connection,
    verb_written: str,
    *,
    mood: str | None = None,
    tense: str | None = None,
    conjugated_forms: list[str] | None = None,
    max_length: int = 120,
    min_words: int = 7,
    fallback_min_words: int = 3,
) -> ExampleSentence | None:
    """Find an example sentence with morphological preference and FTS fallback.

    This function first tries to find a sentence with exact morphological
    matching (mood/tense) from ParTUT. If no suitable sentence is found,
    it falls back to FTS search across all sentences (Tatoeba + ParTUT).

    Args:
        conn: Database connection
        verb_written: The verb infinitive (e.g., "parlare")
        mood: Optional mood filter for morphological search
        tense: Optional tense filter for morphological search
        conjugated_forms: Optional list of specific forms to search for in FTS
        max_length: Maximum sentence length in characters
        min_words: Preferred minimum number of words in sentence
        fallback_min_words: Fallback minimum if no sentence with min_words found

    Returns:
        ExampleSentence with Italian text and optional English translation,
        or None if no suitable sentence found
    """
    # First, try morphological match from ParTUT if mood or tense specified
    if mood or tense:
        result = get_morphological_example_sentence(
            conn,
            verb_written,
            mood=mood,
            tense=tense,
            max_length=max_length,
            min_words=min_words,
            fallback_min_words=fallback_min_words,
        )
        if result:
            return result

    # Fall back to existing FTS search
    return get_example_sentence(
        conn,
        verb_written,
        conjugated_forms=conjugated_forms,
        max_length=max_length,
        min_words=min_words,
        fallback_min_words=fallback_min_words,
    )


def get_example_sentence(
    conn: Connection,
    verb_written: str,
    *,
    conjugated_forms: list[str] | None = None,
    max_length: int = 120,
    min_words: int = 7,
    fallback_min_words: int = 3,
) -> ExampleSentence | None:
    """Find an example sentence containing the verb.

    Uses FTS5 full-text search to find Italian sentences containing the verb.
    If conjugated_forms are provided, searches for those specific forms.
    Otherwise falls back to stem matching.

    Strongly prefers sentences with English translations available.

    Fallback chain:
        1. 7+ words with translation
        2. 7+ words without translation
        3. 3+ words with translation
        4. 3+ words without translation

    Args:
        conn: Database connection
        verb_written: The verb infinitive (e.g., "parlare")
        conjugated_forms: Optional list of specific forms to search for
        max_length: Maximum sentence length in characters
        min_words: Preferred minimum number of words in sentence
        fallback_min_words: Fallback minimum if no sentence with min_words found

    Returns:
        ExampleSentence with Italian text and optional English translation,
        or None if no suitable sentence found
    """
    # Build search query
    if conjugated_forms:
        # Search for any of the conjugated forms
        # Use FTS5 OR operator to match any form
        fts_query = " OR ".join(f'"{form}"' for form in conjugated_forms if form)
    else:
        # Fallback: search for verb stem with prefix matching
        stem = verb_written
        for suffix in ("are", "ere", "ire", "rre", "rsi"):
            if verb_written.endswith(suffix):
                stem = verb_written[: -len(suffix)]
                break
        fts_query = f"{stem}*"

    # Query that joins with translations and returns both Italian and English
    # Orders by: translation availability first (sentences WITH translations come first),
    # then by sentence length (shortest first)
    # sentences_fts.id and translations.*_sentence_id reference sentences.id (surrogate)
    stmt = text("""
        SELECT s.id, s.text, eng.text as english
        FROM sentences_fts fts
        JOIN sentences s ON fts.id = s.id
        LEFT JOIN translations t ON t.ita_sentence_id = s.id
        LEFT JOIN sentences eng ON t.eng_sentence_id = eng.id
        WHERE fts.text MATCH :query
          AND s.lang = 'ita'
          AND length(s.text) <= :max_length
          AND (length(s.text) - length(replace(s.text, ' ', '')) + 1) >= :min_words
        ORDER BY
          CASE WHEN eng.text IS NOT NULL THEN 0 ELSE 1 END,
          length(s.text)
        LIMIT 1
    """)

    # Try with preferred min_words first
    row = conn.execute(
        stmt, {"query": fts_query, "max_length": max_length, "min_words": min_words}
    ).fetchone()

    # Fall back to shorter sentences if needed
    if row is None and fallback_min_words < min_words:
        row = conn.execute(
            stmt, {"query": fts_query, "max_length": max_length, "min_words": fallback_min_words}
        ).fetchone()

    if row is None:
        return None

    italian_text = row[1]
    english_text = row[2]  # May be None

    return ExampleSentence(italian=italian_text, english=english_text)


def get_verb_frequency(conn: Connection, lemma_id: int) -> float | None:
    """Get the Zipf frequency score for a verb.

    Args:
        conn: Database connection
        lemma_id: The lemma ID

    Returns:
        Zipf frequency score or None if not available
    """
    stmt = select(frequencies.c.freq_zipf).where(frequencies.c.lemma_id == lemma_id)
    row = conn.execute(stmt).fetchone()
    return row[0] if row else None


def get_frequency_rank_in_pos(conn: Connection, lemma_id: int) -> int | None:
    """Get the frequency rank within the lemma's part of speech.

    Returns the pre-computed freq_rank_in_pos from the frequencies table,
    which represents the lemma's rank among all lemmas of the same POS
    (e.g., "5th most frequent noun").

    Args:
        conn: Database connection
        lemma_id: The lemma ID

    Returns:
        Rank as integer (1-based), or None if lemma has no frequency data
    """
    stmt = select(frequencies.c.freq_rank_in_pos).where(frequencies.c.lemma_id == lemma_id)
    row = conn.execute(stmt).fetchone()
    return row[0] if row else None


def _rank_to_pos_band(rank: int | None, pos: str) -> str:
    """Convert a POS rank to a frequency band tag.

    Uses finer granularity than global ranks since there are fewer
    lemmas per POS (~4,186 verbs vs ~18,000+ total).

    Bands:
        - freq-{pos}::top-100: ranks 1-100
        - freq-{pos}::top-500: ranks 101-500
        - freq-{pos}::top-1000: ranks 501-1000
        - freq-{pos}::top-2000: ranks 1001-2000
        - freq-{pos}::other: ranks > 2000 or no frequency data

    Args:
        rank: POS-specific rank (1-based), or None if no data
        pos: Part of speech (e.g., "verb", "noun")

    Returns:
        POS-specific frequency band tag string
    """
    if rank is None:
        return f"freq-{pos}::other"
    elif rank <= 100:
        return f"freq-{pos}::top-100"
    elif rank <= 500:
        return f"freq-{pos}::top-500"
    elif rank <= 1000:
        return f"freq-{pos}::top-1000"
    elif rank <= 2000:
        return f"freq-{pos}::top-2000"
    else:
        return f"freq-{pos}::other"


def get_frequency_bands(conn: Connection, lemma_id: int) -> list[str]:
    """Get POS-specific frequency band tag for a lemma.

    With the hybrid frequency approach (PAISA for verbs, OpenSubtitles for
    nouns/adjectives), global frequency rankings are not meaningful since
    different corpora are used for different POS. Only POS-specific rankings
    are returned.

    Args:
        conn: Database connection
        lemma_id: The lemma ID

    Returns:
        List with single tag string: [pos_band]
        e.g., ["freq-verb::top-100"]
    """
    # Get POS rank and POS
    stmt = text("""
        SELECT f.freq_rank_in_pos, l.pos
        FROM frequencies f
        JOIN lemmas l ON f.lemma_id = l.id
        WHERE f.lemma_id = :id
    """)
    row = conn.execute(stmt, {"id": lemma_id}).fetchone()

    if row is None:
        # No frequency data - return "other"
        # We need to get the POS from lemmas table
        pos_stmt = select(lemmas.c.pos).where(lemmas.c.id == lemma_id)
        pos_row = conn.execute(pos_stmt).fetchone()
        pos = pos_row[0].value if pos_row else "unknown"
        return [f"freq-{pos}::other"]

    pos_rank = row[0]
    pos = row[1]  # Raw SQL returns string directly

    # Get POS-specific band
    pos_band = _rank_to_pos_band(pos_rank, pos)

    return [pos_band]


# Irregular English verb conjugations for prompt generation
# Maps: infinitive -> {tense_id -> 1st person singular}
_IRREGULAR_ENGLISH_VERBS: dict[str, dict[str, str]] = {
    "be": {
        "presente_indicativo": "am",
        "imperfetto": "was being",
        "passato_remoto": "was",
        "futuro_semplice": "will be",
    },
    "have": {
        "presente_indicativo": "have",
        "imperfetto": "was having",
        "passato_remoto": "had",
        "futuro_semplice": "will have",
    },
    "go": {
        "presente_indicativo": "go",
        "imperfetto": "was going",
        "passato_remoto": "went",
        "futuro_semplice": "will go",
    },
    "do": {
        "presente_indicativo": "do",
        "imperfetto": "was doing",
        "passato_remoto": "did",
        "futuro_semplice": "will do",
    },
    "say": {
        "presente_indicativo": "say",
        "imperfetto": "was saying",
        "passato_remoto": "said",
        "futuro_semplice": "will say",
    },
    "make": {
        "presente_indicativo": "make",
        "imperfetto": "was making",
        "passato_remoto": "made",
        "futuro_semplice": "will make",
    },
    "see": {
        "presente_indicativo": "see",
        "imperfetto": "was seeing",
        "passato_remoto": "saw",
        "futuro_semplice": "will see",
    },
    "come": {
        "presente_indicativo": "come",
        "imperfetto": "was coming",
        "passato_remoto": "came",
        "futuro_semplice": "will come",
    },
    "know": {
        "presente_indicativo": "know",
        "imperfetto": "was knowing",
        "passato_remoto": "knew",
        "futuro_semplice": "will know",
    },
    "take": {
        "presente_indicativo": "take",
        "imperfetto": "was taking",
        "passato_remoto": "took",
        "futuro_semplice": "will take",
    },
    "give": {
        "presente_indicativo": "give",
        "imperfetto": "was giving",
        "passato_remoto": "gave",
        "futuro_semplice": "will give",
    },
    "think": {
        "presente_indicativo": "think",
        "imperfetto": "was thinking",
        "passato_remoto": "thought",
        "futuro_semplice": "will think",
    },
    "find": {
        "presente_indicativo": "find",
        "imperfetto": "was finding",
        "passato_remoto": "found",
        "futuro_semplice": "will find",
    },
    "tell": {
        "presente_indicativo": "tell",
        "imperfetto": "was telling",
        "passato_remoto": "told",
        "futuro_semplice": "will tell",
    },
    "become": {
        "presente_indicativo": "become",
        "imperfetto": "was becoming",
        "passato_remoto": "became",
        "futuro_semplice": "will become",
    },
    "leave": {
        "presente_indicativo": "leave",
        "imperfetto": "was leaving",
        "passato_remoto": "left",
        "futuro_semplice": "will leave",
    },
    "feel": {
        "presente_indicativo": "feel",
        "imperfetto": "was feeling",
        "passato_remoto": "felt",
        "futuro_semplice": "will feel",
    },
    "put": {
        "presente_indicativo": "put",
        "imperfetto": "was putting",
        "passato_remoto": "put",
        "futuro_semplice": "will put",
    },
    "bring": {
        "presente_indicativo": "bring",
        "imperfetto": "was bringing",
        "passato_remoto": "brought",
        "futuro_semplice": "will bring",
    },
    "begin": {
        "presente_indicativo": "begin",
        "imperfetto": "was beginning",
        "passato_remoto": "began",
        "futuro_semplice": "will begin",
    },
    "keep": {
        "presente_indicativo": "keep",
        "imperfetto": "was keeping",
        "passato_remoto": "kept",
        "futuro_semplice": "will keep",
    },
    "hold": {
        "presente_indicativo": "hold",
        "imperfetto": "was holding",
        "passato_remoto": "held",
        "futuro_semplice": "will hold",
    },
    "write": {
        "presente_indicativo": "write",
        "imperfetto": "was writing",
        "passato_remoto": "wrote",
        "futuro_semplice": "will write",
    },
    "stand": {
        "presente_indicativo": "stand",
        "imperfetto": "was standing",
        "passato_remoto": "stood",
        "futuro_semplice": "will stand",
    },
    "hear": {
        "presente_indicativo": "hear",
        "imperfetto": "was hearing",
        "passato_remoto": "heard",
        "futuro_semplice": "will hear",
    },
    "let": {
        "presente_indicativo": "let",
        "imperfetto": "was letting",
        "passato_remoto": "let",
        "futuro_semplice": "will let",
    },
    "mean": {
        "presente_indicativo": "mean",
        "imperfetto": "was meaning",
        "passato_remoto": "meant",
        "futuro_semplice": "will mean",
    },
    "set": {
        "presente_indicativo": "set",
        "imperfetto": "was setting",
        "passato_remoto": "set",
        "futuro_semplice": "will set",
    },
    "meet": {
        "presente_indicativo": "meet",
        "imperfetto": "was meeting",
        "passato_remoto": "met",
        "futuro_semplice": "will meet",
    },
    "run": {
        "presente_indicativo": "run",
        "imperfetto": "was running",
        "passato_remoto": "ran",
        "futuro_semplice": "will run",
    },
    "read": {
        "presente_indicativo": "read",
        "imperfetto": "was reading",
        "passato_remoto": "read",
        "futuro_semplice": "will read",
    },
    "grow": {
        "presente_indicativo": "grow",
        "imperfetto": "was growing",
        "passato_remoto": "grew",
        "futuro_semplice": "will grow",
    },
    "lose": {
        "presente_indicativo": "lose",
        "imperfetto": "was losing",
        "passato_remoto": "lost",
        "futuro_semplice": "will lose",
    },
    "fall": {
        "presente_indicativo": "fall",
        "imperfetto": "was falling",
        "passato_remoto": "fell",
        "futuro_semplice": "will fall",
    },
    "send": {
        "presente_indicativo": "send",
        "imperfetto": "was sending",
        "passato_remoto": "sent",
        "futuro_semplice": "will send",
    },
    "build": {
        "presente_indicativo": "build",
        "imperfetto": "was building",
        "passato_remoto": "built",
        "futuro_semplice": "will build",
    },
    "understand": {
        "presente_indicativo": "understand",
        "imperfetto": "was understanding",
        "passato_remoto": "understood",
        "futuro_semplice": "will understand",
    },
    "draw": {
        "presente_indicativo": "draw",
        "imperfetto": "was drawing",
        "passato_remoto": "drew",
        "futuro_semplice": "will draw",
    },
    "break": {
        "presente_indicativo": "break",
        "imperfetto": "was breaking",
        "passato_remoto": "broke",
        "futuro_semplice": "will break",
    },
    "spend": {
        "presente_indicativo": "spend",
        "imperfetto": "was spending",
        "passato_remoto": "spent",
        "futuro_semplice": "will spend",
    },
    "cut": {
        "presente_indicativo": "cut",
        "imperfetto": "was cutting",
        "passato_remoto": "cut",
        "futuro_semplice": "will cut",
    },
    "rise": {
        "presente_indicativo": "rise",
        "imperfetto": "was rising",
        "passato_remoto": "rose",
        "futuro_semplice": "will rise",
    },
    "speak": {
        "presente_indicativo": "speak",
        "imperfetto": "was speaking",
        "passato_remoto": "spoke",
        "futuro_semplice": "will speak",
    },
    "buy": {
        "presente_indicativo": "buy",
        "imperfetto": "was buying",
        "passato_remoto": "bought",
        "futuro_semplice": "will buy",
    },
    "lead": {
        "presente_indicativo": "lead",
        "imperfetto": "was leading",
        "passato_remoto": "led",
        "futuro_semplice": "will lead",
    },
    "sit": {
        "presente_indicativo": "sit",
        "imperfetto": "was sitting",
        "passato_remoto": "sat",
        "futuro_semplice": "will sit",
    },
    "drive": {
        "presente_indicativo": "drive",
        "imperfetto": "was driving",
        "passato_remoto": "drove",
        "futuro_semplice": "will drive",
    },
    "eat": {
        "presente_indicativo": "eat",
        "imperfetto": "was eating",
        "passato_remoto": "ate",
        "futuro_semplice": "will eat",
    },
    "drink": {
        "presente_indicativo": "drink",
        "imperfetto": "was drinking",
        "passato_remoto": "drank",
        "futuro_semplice": "will drink",
    },
    "sleep": {
        "presente_indicativo": "sleep",
        "imperfetto": "was sleeping",
        "passato_remoto": "slept",
        "futuro_semplice": "will sleep",
    },
    "wear": {
        "presente_indicativo": "wear",
        "imperfetto": "was wearing",
        "passato_remoto": "wore",
        "futuro_semplice": "will wear",
    },
    "win": {
        "presente_indicativo": "win",
        "imperfetto": "was winning",
        "passato_remoto": "won",
        "futuro_semplice": "will win",
    },
    "teach": {
        "presente_indicativo": "teach",
        "imperfetto": "was teaching",
        "passato_remoto": "taught",
        "futuro_semplice": "will teach",
    },
    "catch": {
        "presente_indicativo": "catch",
        "imperfetto": "was catching",
        "passato_remoto": "caught",
        "futuro_semplice": "will catch",
    },
    "throw": {
        "presente_indicativo": "throw",
        "imperfetto": "was throwing",
        "passato_remoto": "threw",
        "futuro_semplice": "will throw",
    },
    "choose": {
        "presente_indicativo": "choose",
        "imperfetto": "was choosing",
        "passato_remoto": "chose",
        "futuro_semplice": "will choose",
    },
    "fight": {
        "presente_indicativo": "fight",
        "imperfetto": "was fighting",
        "passato_remoto": "fought",
        "futuro_semplice": "will fight",
    },
    "forget": {
        "presente_indicativo": "forget",
        "imperfetto": "was forgetting",
        "passato_remoto": "forgot",
        "futuro_semplice": "will forget",
    },
    "sing": {
        "presente_indicativo": "sing",
        "imperfetto": "was singing",
        "passato_remoto": "sang",
        "futuro_semplice": "will sing",
    },
    "fly": {
        "presente_indicativo": "fly",
        "imperfetto": "was flying",
        "passato_remoto": "flew",
        "futuro_semplice": "will fly",
    },
    "lie": {
        "presente_indicativo": "lie",
        "imperfetto": "was lying",
        "passato_remoto": "lay",
        "futuro_semplice": "will lie",
    },
    "hide": {
        "presente_indicativo": "hide",
        "imperfetto": "was hiding",
        "passato_remoto": "hid",
        "futuro_semplice": "will hide",
    },
    "bear": {
        "presente_indicativo": "bear",
        "imperfetto": "was bearing",
        "passato_remoto": "bore",
        "futuro_semplice": "will bear",
    },
    "sell": {
        "presente_indicativo": "sell",
        "imperfetto": "was selling",
        "passato_remoto": "sold",
        "futuro_semplice": "will sell",
    },
}


def generate_english_prompt(english_infinitive: str | None, tense_id: str) -> str:
    """Generate the English prompt for a verb in a specific tense.

    Converts an English infinitive like "to speak" into a 1st person singular
    prompt like "I speak" or "I was speaking" depending on the tense.

    Handles irregular English verbs (be, have, go, etc.) and falls back to
    regular conjugation patterns for unknown verbs.

    Args:
        english_infinitive: The English infinitive (e.g., "to speak", "to be")
        tense_id: The tense identifier (e.g., "presente_indicativo")

    Returns:
        English prompt like "I speak", "I am", "I was speaking"
    """
    if not english_infinitive:
        # Fallback to generic prompts if no translation available
        fallbacks = {
            "presente_indicativo": "I ___",
            "imperfetto": "I was ___-ing",
            "passato_remoto": "I ___-ed",
            "futuro_semplice": "I will ___",
        }
        return fallbacks.get(tense_id, "I ___")

    # Extract base verb from "to X" or "to X, to Y"
    # Take just the first verb if there are multiple
    first_part = english_infinitive.split(",")[0].strip()
    base_verb = first_part[3:].strip() if first_part.startswith("to ") else first_part

    # Check for irregular verbs
    if base_verb in _IRREGULAR_ENGLISH_VERBS:
        verb_forms = _IRREGULAR_ENGLISH_VERBS[base_verb]
        if tense_id in verb_forms:
            return f"I {verb_forms[tense_id]}"

    # Regular verb conjugation patterns
    if tense_id == "presente_indicativo":
        return f"I {base_verb}"
    elif tense_id == "imperfetto":
        # Handle -e endings: "love" -> "loving", not "loveing"
        if base_verb.endswith("e") and not base_verb.endswith("ee"):
            return f"I was {base_verb[:-1]}ing"
        # Handle consonant doubling for short verbs: "run" -> "running"
        # (simplified - just add -ing for now)
        return f"I was {base_verb}ing"
    elif tense_id == "passato_remoto":
        # Regular past: add -ed (or -d if ends in e)
        if base_verb.endswith("e"):
            return f"I {base_verb}d"
        return f"I {base_verb}ed"
    elif tense_id == "futuro_semplice":
        return f"I will {base_verb}"
    else:
        return f"I {base_verb}"


def get_english_infinitive(conn: Connection, lemma_id: int) -> str | None:
    """Get the English infinitive translation for a verb.

    Finds the first definition gloss that starts with "to " (e.g., "to speak").
    Prefers simple definitions without parentheses, semicolons, or category
    references. Falls back to shortest definition if none are simple.

    Args:
        conn: Database connection
        lemma_id: The lemma ID

    Returns:
        English infinitive like "to speak", or None if not found
    """
    # Prefer simple definitions (no parentheses, semicolons, or "See Category:")
    # Order by word count in first alternative (before comma), then by id as tiebreaker
    # e.g., "to talk, to speak" counts as 2 words (just "to talk")
    stmt = text("""
        SELECT gloss FROM definitions
        WHERE lemma_id = :id
          AND gloss LIKE 'to %'
          AND gloss NOT LIKE '%(%)%'
          AND gloss NOT LIKE '%;%'
          AND gloss NOT LIKE '%See Category:%'
        ORDER BY (
            length(substr(gloss, 1, instr(gloss || ',', ',') - 1))
            - length(replace(substr(gloss, 1, instr(gloss || ',', ',') - 1), ' ', ''))
            + 1
        ), id
        LIMIT 1
    """)
    row = conn.execute(stmt, {"id": lemma_id}).fetchone()
    if row:
        return row[0]

    # Fallback: any "to ..." definition, prefer fewer words in first alternative
    stmt = text("""
        SELECT gloss FROM definitions
        WHERE lemma_id = :id AND gloss LIKE 'to %'
        ORDER BY (
            length(substr(gloss, 1, instr(gloss || ',', ',') - 1))
            - length(replace(substr(gloss, 1, instr(gloss || ',', ',') - 1), ' ', ''))
            + 1
        ), id
        LIMIT 1
    """)
    row = conn.execute(stmt, {"id": lemma_id}).fetchone()
    return row[0] if row else None


def validate_verb_list(conn: Connection, verb_lemmas: list[str]) -> tuple[list[str], list[str]]:
    """Validate that all verbs in the list exist in the database.

    Args:
        conn: Database connection
        verb_lemmas: List of verb infinitives to validate

    Returns:
        Tuple of (found_verbs, missing_verbs)
    """
    found: list[str] = []
    missing: list[str] = []

    for lemma in verb_lemmas:
        verb = get_verb_by_lemma(conn, lemma)
        if verb is not None:
            found.append(lemma)
        else:
            missing.append(lemma)

    return found, missing
