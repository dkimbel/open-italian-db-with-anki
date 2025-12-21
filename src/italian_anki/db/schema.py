"""Database schema definition using SQLAlchemy Core."""

from sqlalchemy import (
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
    Column("auxiliary", String(20)),  # 'avere', 'essere', 'both', NULL
    Column("transitivity", String(20)),  # 'transitive', 'intransitive', 'both', NULL
    Column("ipa", Text),  # infinitive IPA from Wiktextract
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

# Inflected forms (verbs, nouns, adjectives)
forms = Table(
    "forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False),
    Column("form", Text),  # real Italian spelling from Morph-it! (NULL if not found)
    Column("form_stressed", Text, nullable=False),  # pedagogical with stress marks
    Column("tags", Text, nullable=False),  # JSON array
)

# Lookup table for matching forms in sentences
form_lookup = Table(
    "form_lookup",
    metadata,
    Column("form_normalized", Text, nullable=False, primary_key=True),  # accent-stripped
    Column("form_id", Integer, ForeignKey("forms.id"), nullable=False, primary_key=True),
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
sentence_verbs = Table(
    "sentence_verbs",
    metadata,
    Column(
        "sentence_id",
        Integer,
        ForeignKey("sentences.sentence_id"),
        nullable=False,
        primary_key=True,
    ),
    Column("lemma_id", Integer, ForeignKey("lemmas.lemma_id"), nullable=False, primary_key=True),
    Column("form_found", Text),  # the conjugated form matched
)

# Indexes (defined separately for clarity)
Index("idx_forms_lemma", forms.c.lemma_id)
Index("idx_forms_form", forms.c.form)
Index("idx_form_lookup_form_id", form_lookup.c.form_id)
Index("idx_definitions_lemma", definitions.c.lemma_id)
Index("idx_frequencies_lemma", frequencies.c.lemma_id)
Index("idx_sentences_lang", sentences.c.lang)
Index("idx_sentence_verbs_lemma", sentence_verbs.c.lemma_id)


def init_db(engine: Engine) -> None:
    """Initialize the database schema.

    Creates all tables and indexes if they don't exist.
    Safe to call multiple times (uses checkfirst=True by default).
    """
    metadata.create_all(engine)
