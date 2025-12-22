"""Database schema definition using SQLAlchemy Core."""

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

# Master lemma table
lemmas = Table(
    "lemmas",
    metadata,
    Column("lemma_id", Integer, primary_key=True, autoincrement=True),
    Column("lemma", Text, nullable=False, unique=True),  # normalized (lowercase, no accents)
    Column("lemma_stressed", Text, nullable=False),  # with stress mark (e.g., "parlàre")
    Column("pos", String(20), default="verb"),
    Column("ipa", Text),  # IPA pronunciation from Wiktextract
)

# Frequency data from corpora (separate table for versioning)
frequencies = Table(
    "frequencies",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False, primary_key=True),
    Column("corpus", String(20), nullable=False, primary_key=True),  # 'itwac', 'colfis'
    Column("freq_raw", Integer),  # raw count
    Column("freq_zipf", Float),  # type: ignore[arg-type] # zipf score (normalized)
    Column("corpus_version", String(20)),  # e.g., '2.1.0', '2024-01'
)

# Verb conjugations with explicit grammatical features
verb_forms = Table(
    "verb_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False),
    Column("form", Text),  # real Italian spelling from Morph-it! (NULL if not found)
    Column("form_stressed", Text, nullable=False),  # with stress marks
    # Grammatical features
    Column(
        "mood", Text, nullable=False
    ),  # indicative, subjunctive, conditional, imperative, infinitive, participle, gerund
    Column("tense", Text),  # present, imperfect, remote, future (NULL for non-finite)
    Column("person", Integer),  # 1, 2, 3 (NULL for non-finite)
    Column("number", Text),  # singular, plural (NULL for some non-finite)
    Column("gender", Text),  # masculine, feminine (for participles only)
    # Modifiers
    Column("is_formal", Boolean, default=False),  # Lei/Loro forms
    Column("is_negative", Boolean, default=False),  # negative imperative
    # Usage labels (comma-separated if multiple)
    Column("labels", Text),  # NULL=standard, or "archaic", "archaic,literary", etc.
)

# Noun forms with grammatical features
noun_forms = Table(
    "noun_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False),
    Column("form", Text),  # real Italian spelling
    Column("form_stressed", Text, nullable=False),  # with stress marks
    Column("gender", String(1)),  # 'm' or 'f' (per-form, for nouns like paio/paia)
    Column("number", Text, nullable=False),  # singular, plural
    Column("labels", Text),  # NULL=standard, or comma-separated labels
    Column("is_diminutive", Boolean, default=False),
    Column("is_augmentative", Boolean, default=False),
)

# Adjective forms with grammatical features
adjective_forms = Table(
    "adjective_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False),
    Column("form", Text),  # real Italian spelling
    Column("form_stressed", Text, nullable=False),  # with stress marks
    Column("gender", Text, nullable=False),  # masculine, feminine
    Column("number", Text, nullable=False),  # singular, plural
    Column("degree", Text, default="positive"),  # positive, comparative, superlative
    Column("labels", Text),  # NULL=standard, or comma-separated labels
)

# Lookup table for matching forms in sentences (with POS awareness)
form_lookup = Table(
    "form_lookup",
    metadata,
    Column("form_normalized", Text, nullable=False, primary_key=True),  # accent-stripped
    Column("pos", Text, nullable=False, primary_key=True),  # verb, noun, adjective
    Column("form_id", Integer, nullable=False, primary_key=True),  # references *_forms.id
)

# English definitions
definitions = Table(
    "definitions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False),
    Column("gloss", Text, nullable=False),
    Column("tags", Text),  # JSON array (e.g., ["transitive"])
)

# Tatoeba sentences
sentences = Table(
    "sentences",
    metadata,
    Column("sentence_id", Integer, primary_key=True),  # Tatoeba's ID, not autoincrement
    Column("lang", String(3), nullable=False),  # 'ita' or 'eng'
    Column("text", Text, nullable=False),
)

# Translation links
translations = Table(
    "translations",
    metadata,
    Column("ita_sentence_id", Integer, primary_key=True),
    Column("eng_sentence_id", Integer, primary_key=True),
)

# Sentence-to-lemma linking (for frequency + examples)
sentence_lemmas = Table(
    "sentence_lemmas",
    metadata,
    Column(
        "sentence_id",
        Integer,
        ForeignKey("sentences.sentence_id"),
        nullable=False,
        primary_key=True,
    ),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False, primary_key=True),
    Column("form_found", Text),  # the inflected form matched
)

# Verb-specific metadata (auxiliary and transitivity)
verb_metadata = Table(
    "verb_metadata",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), primary_key=True),
    Column("auxiliary", String(20)),  # 'avere', 'essere', 'both', NULL
    Column("transitivity", String(20)),  # 'transitive', 'intransitive', 'both', NULL
)

# Indexes (defined separately for clarity)
Index("idx_verb_metadata_auxiliary", verb_metadata.c.auxiliary)
# verb_forms indexes
Index("idx_verb_forms_lemma", verb_forms.c.lemma_id)
Index("idx_verb_forms_mood_tense", verb_forms.c.mood, verb_forms.c.tense)
Index("idx_verb_forms_labels", verb_forms.c.labels)
Index("idx_verb_forms_form", verb_forms.c.form)
# New noun_forms indexes
Index("idx_noun_forms_lemma", noun_forms.c.lemma_id)
Index("idx_noun_forms_form", noun_forms.c.form)
Index("idx_noun_forms_gender", noun_forms.c.gender)
# New adjective_forms indexes
Index("idx_adjective_forms_lemma", adjective_forms.c.lemma_id)
Index("idx_adjective_forms_form", adjective_forms.c.form)
# New form_lookup indexes
Index("idx_form_lookup_form_id", form_lookup.c.form_id)
# Other indexes
Index("idx_definitions_lemma", definitions.c.lemma_id)
Index("idx_frequencies_lemma", frequencies.c.lemma_id)
Index("idx_sentences_lang", sentences.c.lang)
Index("idx_sentence_lemmas_lemma", sentence_lemmas.c.lemma_id)
Index("idx_sentence_lemmas_sentence", sentence_lemmas.c.sentence_id)
Index("idx_translations_ita", translations.c.ita_sentence_id)


def init_db(engine: Engine) -> None:
    """Initialize the database schema.

    Creates all tables and indexes if they don't exist.
    Safe to call multiple times (uses checkfirst=True by default).
    """
    metadata.create_all(engine)
