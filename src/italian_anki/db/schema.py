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
    UniqueConstraint,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

# Master lemma table
lemmas = Table(
    "lemmas",
    metadata,
    Column("lemma_id", Integer, primary_key=True, autoincrement=True),
    Column("normalized", Text, nullable=False),  # accent-stripped for lookup (e.g., "citta")
    Column("written", Text),  # actual written form from Morphit (e.g., "città"), NULL if unknown
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "città", "parlàre")
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
    Column("written", Text),  # actual written form from Morphit (e.g., "parlò"), NULL if unknown
    Column("written_source", Text),  # "morphit", NULL if not found
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "parlò", "pàrlo")
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
    # Form origin tracking - how we determined this form exists
    Column("form_origin", Text),  # 'wiktextract', 'inferred:singular', etc.
)

# Noun forms with grammatical features
noun_forms = Table(
    "noun_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False),
    Column("written", Text),  # actual written form from Morphit (e.g., "città"), NULL if unknown
    Column("written_source", Text),  # "morphit", NULL if not found
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "città", "càsa")
    Column(
        "gender", Text, nullable=False
    ),  # 'masculine' or 'feminine' (per-form, for nouns like paio/paia)
    Column("number", Text, nullable=False),  # singular, plural
    Column("labels", Text),  # NULL=standard, or comma-separated labels
    Column("is_diminutive", Boolean, default=False),
    Column("is_augmentative", Boolean, default=False),
    Column("meaning_hint", Text),  # e.g., 'anatomical', 'figurative' for braccio-type plurals
    # Article columns (computed from orthography)
    Column("def_article", Text),  # 'il', 'lo', 'la', "l'", 'i', 'gli', 'le'
    Column("article_source", Text),  # 'inferred' or 'exception:<reason>'
    # Form origin tracking - how we determined this form exists
    Column("form_origin", Text),  # 'wiktextract', 'inferred:singular', etc.
)

# Adjective forms with grammatical features
#
# Note on adjective_forms storage:
# ================================
# We store one row per (lemma_id, stressed, gender, number, degree) combination.
# Even when form text is identical across genders (invariable adjectives like "blu"),
# we store 4 separate rows because:
#
# 1. Each combination requires a different definite article (il/la/i/le)
# 2. This correctly models Italian's gender agreement grammar
# 3. It enables efficient queries like "show all feminine plural forms"
# 4. It supports substantivized adjectives ("il blu", "la bella")
#
# form_origin tracking values:
# - 'wiktextract': Direct from Wiktextract forms array
# - 'inferred:singular': Added missing singular tag (gender-only forms in Wiktextract)
# - 'inferred:two_form': Generated both genders for 2-form adjective (e.g., facile)
# - 'inferred:base_form': From lemma word field when forms array empty
# - 'inferred:invariable': Generated all 4 forms for inv:1 flagged adjectives
# - 'morphit': Fallback from Morphit for adjectives with missing forms
#
adjective_forms = Table(
    "adjective_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False),
    Column("written", Text),  # actual written form from Morphit (e.g., "bella"), NULL if unknown
    Column("written_source", Text),  # "morphit", NULL if not found
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "bèlla")
    Column("gender", Text, nullable=False),  # masculine, feminine
    Column("number", Text, nullable=False),  # singular, plural
    Column("degree", Text, default="positive"),  # positive, comparative, superlative
    Column("labels", Text),  # NULL=standard, or comma-separated labels
    # Article columns (computed from orthography)
    Column("def_article", Text),  # 'il', 'lo', 'la', "l'", 'i', 'gli', 'le'
    Column("article_source", Text),  # 'inferred' or 'exception:<reason>'
    # Form origin tracking - how we determined this form exists (see documentation above)
    Column("form_origin", Text),
    # Unique constraint: allows allomorphs (bel/bello/bell') but prevents true duplicates
    UniqueConstraint(
        "lemma_id", "stressed", "gender", "number", "degree", name="uq_adjective_forms_entry"
    ),
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
    # Optional linkage to specific forms (for nouns with meaning-dependent gender/plurals)
    Column("form_gender", Text),  # NULL (all), 'masculine', 'feminine'
    Column("form_number", Text),  # NULL (all), 'singular', 'plural'
    Column("form_meaning_hint", Text),  # matches noun_forms.meaning_hint
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

# Noun-specific metadata (gender classification, number behavior, and links)
noun_metadata = Table(
    "noun_metadata",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), primary_key=True),
    # Gender classification (mutually exclusive):
    # 'm' = masculine only, 'f' = feminine only,
    # 'common_gender_fixed' = both genders, identical forms (cantante),
    # 'common_gender_variable' = both genders, forms can differ (collega)
    Column("gender_class", Text, nullable=False),
    # Number behavior (mutually exclusive):
    # 'standard' = has both singular and plural,
    # 'pluralia_tantum' = plural only (forbici),
    # 'singularia_tantum' = singular only/uncountable (latte),
    # 'invariable' = same form for both (città)
    Column("number_class", Text, default="standard"),
    # Links to related lemmas
    Column(
        "counterpart_lemma_id", Integer, ForeignKey("lemmas.lemma_id")
    ),  # professore↔professoressa
    Column("base_lemma_id", Integer, ForeignKey("lemmas.lemma_id")),  # tavolino→tavola
    Column("derivation_type", Text),  # 'diminutive', 'augmentative', 'pejorative'
)

# Adjective-specific metadata (inflection class and links)
adjective_metadata = Table(
    "adjective_metadata",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), primary_key=True),
    # Inflection class (mutually exclusive):
    # '4-form' = standard (bello/bella/belli/belle)
    # '2-form' = same form for m/f (facile/facile/facili/facili)
    # 'invariable' = same form for all (blu)
    Column("inflection_class", Text),
    # Links to related lemmas (for comparative/superlative)
    Column("base_lemma_id", Integer, ForeignKey("lemmas.lemma_id")),  # migliore→buono
    Column("degree_relationship", Text),  # 'comparative_of', 'superlative_of'
    Column(
        "degree_relationship_source", Text
    ),  # 'wiktextract', 'wiktextract:canonical', 'hardcoded'
)

# Indexes (defined separately for clarity)
Index("idx_lemmas_normalized_pos", lemmas.c.normalized, lemmas.c.pos)  # For lookups by word+POS
Index("idx_verb_metadata_auxiliary", verb_metadata.c.auxiliary)
# noun_metadata indexes
Index("idx_noun_metadata_gender_class", noun_metadata.c.gender_class)
Index("idx_noun_metadata_counterpart", noun_metadata.c.counterpart_lemma_id)
Index("idx_noun_metadata_base", noun_metadata.c.base_lemma_id)
# verb_forms indexes
Index("idx_verb_forms_lemma", verb_forms.c.lemma_id)
Index("idx_verb_forms_mood_tense", verb_forms.c.mood, verb_forms.c.tense)
Index("idx_verb_forms_labels", verb_forms.c.labels)
Index("idx_verb_forms_written", verb_forms.c.written)
# noun_forms indexes
Index("idx_noun_forms_lemma", noun_forms.c.lemma_id)
Index("idx_noun_forms_written", noun_forms.c.written)
Index("idx_noun_forms_gender", noun_forms.c.gender)
Index("idx_noun_forms_meaning_hint", noun_forms.c.meaning_hint)
# adjective_forms indexes
Index("idx_adjective_forms_lemma", adjective_forms.c.lemma_id)
Index("idx_adjective_forms_written", adjective_forms.c.written)
Index("idx_adjective_forms_origin", adjective_forms.c.form_origin)
# adjective_metadata indexes
Index("idx_adjective_metadata_base", adjective_metadata.c.base_lemma_id)
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
