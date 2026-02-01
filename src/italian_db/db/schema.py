"""Database schema definition using SQLAlchemy Core."""

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

# =============================================================================
# Master Lemma Table
# =============================================================================
#
# DEFINITION OF "LEMMA" IN THIS DATABASE
# ======================================
#
# A lemma corresponds to a Wiktextract entry that meets one of these criteria:
#
# 1. Has at least one sense WITHOUT form_of (has independent meaning)
#    - Example: "cagnolino" has senses for "puppy" and "dog paddle" (independent)
#    - Example: "casa" has its own meanings, not derived from another word
#
# 2. Is a clipping (tagged "clipping" in Wiktextract)
#    - Example: "bici" (clipping of "bicicletta")
#    - Example: "auto" (clipping of "automobile")
#    - These are widely-used vocabulary items that deserve their own lemmas
#
# 3. Is filtered and imported as a form of another lemma
#    - Example: "professoressa" is NOT a lemma - its only sense says
#      "female equivalent of professore", so it's imported as a form
#
# HOMONYMS vs POLYSEMY
# ====================
#
# - Homonyms: Same spelling, different etymology → separate lemma rows with
#   different etymology_number (e.g., "scordare" meaning "to put out of tune"
#   vs "to forget" are separate lemmas)
#
# - Polysemes: Same spelling, same etymology → single lemma row with multiple
#   definition rows (e.g., "casa" meaning both "house" and "home" is one lemma
#   with two definitions)
#
# RELATIONSHIPS
# =============
#
# Lemmas can relate to each other via two mechanisms:
#
# 1. lemma_relationships table ("Tier 1"): When ALL definitions inherit the
#    relationship (e.g., clippings, comparatives, reflexive verbs)
#
# 2. definitions.derived_from_lemma_id ("Tier 2"): When only SOME definitions
#    derive from another lemma (e.g., diminutives with independent meanings)
#
# See also:
# - _is_pos_lemma() in wiktextract.py for the filtering logic
# - docs/lemmas-definitions-ipa-refactor.md for detailed specification
#
lemmas = Table(
    "lemmas",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("written", Text),  # actual written form (e.g., "città"), populated from citation form
    Column(
        "written_source", Text
    ),  # provenance: "from:verb_forms", "derived:orthography_rule", etc.
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "città", "parlàre")
    Column("pos", String(20), default="verb"),
    # Etymology data from Wiktextract
    # etymology_number: Wiktextract's section index (1, 2, 3...) when a word has multiple
    # distinct etymologies. NULL for single-etymology entries (the common case).
    Column("etymology_number", Integer),
    Column("etymology_text", Text),  # Human-readable etymology description
)

# Frequency data from corpora (separate table for versioning)
frequencies = Table(
    "frequencies",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), nullable=False, primary_key=True),
    Column("corpus", String(20), nullable=False, primary_key=True),  # 'stanza'
    Column("freq_raw", Integer),  # raw count
    Column("freq_zipf", Float),  # type: ignore[arg-type] # zipf score (normalized)
    Column("corpus_version", String(20)),  # e.g., '2.1.0', '2024-01'
    Column("freq_rank_in_pos", Integer),  # Rank within POS (1 = most frequent)
)

# Verb conjugations with explicit grammatical features
verb_forms = Table(
    "verb_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), nullable=False),
    Column("written", Text),  # actual written form (e.g., "parlò"), NULL if unknown
    Column("written_source", Text),  # "wiktionary", "derived:orthography_rule", etc.
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "parlò", "pàrlo")
    # Grammatical features
    Column(
        "mood", Text, nullable=False
    ),  # indicative, subjunctive, conditional, imperative, infinitive, participle, gerund
    Column("tense", Text),  # present, imperfect, remote, future (NULL for non-finite)
    Column(
        "aspect", Text
    ),  # 'perfective' (past participle), 'imperfective' (present participle), NULL
    Column("person", Integer),  # 1, 2, 3 (NULL for non-finite)
    Column("number", Text),  # singular, plural (NULL for some non-finite)
    Column("gender", Text),  # 'm', 'f' (for participles only)
    # Modifiers
    Column("is_formal", Boolean, default=False),  # Lei/Loro forms
    Column("is_negative", Boolean, default=False),  # negative imperative
    # Usage labels (JSON array)
    Column(
        "labels", JSON(none_as_null=True)
    ),  # NULL=standard, or ["archaic"], ["archaic", "literary"], etc.
    # Form origin tracking - how we determined this form exists
    Column("form_origin", Text),  # 'wiktextract', 'inferred:singular', etc.
    # Citation form marker - True for the canonical/dictionary form (infinitive for verbs)
    Column("is_citation_form", Boolean, default=False),
    # IPA pronunciation (form-level storage - see docs/lemmas-definitions-ipa-refactor.md)
    # The lemma's IPA is stored on the citation form (is_citation_form=True)
    Column("ipa", Text),
    Column("ipa_source", Text),  # 'wiktextract', 'propagated:lemma'
    # Unique constraint via expression index (created in init_db) handles NULLs by using
    # COALESCE. App-level deduplication via seen_verb_forms still runs for performance,
    # but the DB constraint ensures integrity.
)

# Noun forms with grammatical features
noun_forms = Table(
    "noun_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), nullable=False),
    Column("written", Text),  # actual written form (e.g., "città"), NULL if unknown
    Column("written_source", Text),  # "wiktionary", "derived:orthography_rule", etc.
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "città", "càsa")
    Column("gender", Text, nullable=False),  # 'm' or 'f' (per-form, for nouns like paio/paia)
    Column("number", Text, nullable=False),  # singular, plural
    Column("labels", JSON(none_as_null=True)),  # NULL=standard, or JSON array of labels
    Column("derivation_type", Text),  # NULL, 'diminutive', 'augmentative', 'pejorative'
    Column("meaning_hint", Text),  # e.g., 'anatomical', 'figurative' for braccio-type plurals
    # Article columns (computed from orthography)
    Column("definite_article", Text),  # 'il', 'lo', 'la', "l'", 'i', 'gli', 'le'
    Column("article_source", Text),  # 'inferred' or 'exception:<reason>'
    # Form origin tracking - how we determined this form exists
    # Values: 'wiktextract', 'inferred:base_form', 'inferred:head_template', 'inferred:invariable'
    Column("form_origin", Text),
    # Citation form marker - True for the canonical/dictionary form
    # (singular for standard nouns, plural for pluralia tantum)
    Column("is_citation_form", Boolean, default=False),
    # IPA pronunciation (form-level storage - see docs/lemmas-definitions-ipa-refactor.md)
    # The lemma's IPA is stored on the citation form (is_citation_form=True)
    Column("ipa", Text),
    Column("ipa_source", Text),  # 'wiktextract', 'propagated:lemma'
    # Unique constraint: prevents duplicate forms for the same lemma
    # Note: Uses stressed (not written) because written is nullable and NULL != NULL in SQL
    UniqueConstraint("lemma_id", "stressed", "gender", "number", name="uq_noun_forms_entry"),
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
#
adjective_forms = Table(
    "adjective_forms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), nullable=False),
    Column("written", Text),  # actual written form (e.g., "bella"), NULL if unknown
    Column("written_source", Text),  # "wiktionary", "derived:orthography_rule", etc.
    Column("stressed", Text, nullable=False),  # with stress marks (e.g., "bèlla")
    Column("gender", Text, nullable=False),  # 'm', 'f'
    Column("number", Text, nullable=False),  # singular, plural
    Column("degree", Text, default="positive"),  # positive, comparative, superlative
    Column("labels", JSON(none_as_null=True)),  # NULL=standard, or JSON array of labels
    # Article columns (computed from orthography)
    Column("definite_article", Text),  # 'il', 'lo', 'la', "l'", 'i', 'gli', 'le'
    Column("article_source", Text),  # 'inferred' or 'exception:<reason>'
    # Form origin tracking - how we determined this form exists (see documentation above)
    Column("form_origin", Text),
    # Citation form marker - True for the canonical/dictionary form (masculine singular)
    Column("is_citation_form", Boolean, default=False),
    # IPA pronunciation (form-level storage - see docs/lemmas-definitions-ipa-refactor.md)
    # The lemma's IPA is stored on the citation form (is_citation_form=True)
    Column("ipa", Text),
    Column("ipa_source", Text),  # 'wiktextract', 'propagated:lemma'
    # Unique constraint: allows allomorphs (bel/bello/bell') but prevents true duplicates
    UniqueConstraint(
        "lemma_id", "stressed", "gender", "number", "degree", name="uq_adjective_forms_entry"
    ),
)

# English definitions
#
# Definition-to-Lemma Derivation ("Tier 2" of the Two-Tier Relationship Model)
# ===========================================================================
#
# When a lemma has SOME definitions that derive from another lemma (but not ALL),
# we track this at the definition level using derived_from_lemma_id.
#
# Example: "cagnolino" has three senses:
#   - "little dog" → derived_from_lemma_id = <cane>, derivation_type = "diminutive"
#   - "puppy" → derived_from_lemma_id = NULL (independent meaning)
#   - "dog paddle" → derived_from_lemma_id = NULL (independent meaning)
#
# This is "Tier 2" of our relationship model. For relationships where ALL
# definitions of a lemma relate to another (e.g., clippings), use the
# lemma_relationships table instead ("Tier 1").
#
# See docs/lemmas-definitions-ipa-refactor.md for full documentation.
#
definitions = Table(
    "definitions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), nullable=False),
    Column("gloss", Text, nullable=False),
    Column("tags", JSON(none_as_null=True)),  # JSON array (e.g., ["transitive"])
    # Optional linkage to specific forms (for nouns with meaning-dependent plurals)
    # e.g., "braccio" has different meanings for "braccia" (arms) vs "bracci" (crane arms)
    Column("form_meaning_hint", Text),  # matches noun_forms.meaning_hint
    # Definition-level derivation tracking (Tier 2 of relationship model)
    # For definitions that derive from another lemma (e.g., diminutive sense of cagnolino → cane)
    Column(
        "derived_from_lemma_id", Integer, ForeignKey("lemmas.id"), nullable=True
    ),  # The base lemma this definition derives from
    Column(
        "derivation_type", Text, nullable=True
    ),  # DerivationType enum value: diminutive, augmentative, pejorative, endearing
)

# Sentences (Tatoeba)
#
# Uses a surrogate primary key (id) with a composite unique constraint on
# (source, lang, sentence_id). This allows each source to use its native IDs
# without collision, even when the same ID appears in different languages
# (e.g., OpenSubtitles line-aligned pairs share line numbers).
#
sentences = Table(
    "sentences",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),  # Surrogate key for FKs
    Column("sentence_id", Integer, nullable=False),  # Native ID from source (Tatoeba)
    Column("lang", String(3), nullable=False),  # 'ita' or 'eng'
    Column("text", Text, nullable=False),
    Column("source", Text, nullable=False),  # 'tatoeba'
    UniqueConstraint("source", "lang", "sentence_id", name="uq_sentences_source_lang_id"),
)

# Translation links
# WITHOUT ROWID: All columns are in PK, so no need for hidden rowid
# Note: ita_sentence_id and eng_sentence_id reference sentences.id (the surrogate key)
translations = Table(
    "translations",
    metadata,
    Column(
        "ita_sentence_id",
        Integer,
        ForeignKey("sentences.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "eng_sentence_id",
        Integer,
        ForeignKey("sentences.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    sqlite_with_rowid=False,
)

# Sentence tags (from Tatoeba)
#
# Normalized storage for Tatoeba sentence tags. Each sentence can have multiple tags.
# Used for:
# - Quality filtering: exclude sentences with problematic tags (@change, @needs native check, etc.)
# - Tense matching: find sentences with specific tense tags (presente, imperfetto, etc.)
# - Content preference: prefer proverbs in example sentence ranking
#
# WITHOUT ROWID: Composite PK covers all columns, no need for hidden rowid
sentence_tags = Table(
    "sentence_tags",
    metadata,
    Column(
        "sentence_id",
        Integer,
        ForeignKey("sentences.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("tag", Text, primary_key=True),
    sqlite_with_rowid=False,
)

# Sentence tokens (from Stanza POS tagging)
#
# Token-level annotations from Stanza NLP pipeline. Each sentence has multiple tokens
# with POS tags, morphological features, and dependency parse information.
#
# Used for:
# - Filtering example sentences by grammatical features (noun vs adjective uses of "giovane")
# - Finding sentences with specific verb tense/mood/person for conjugation examples
# - Lemma-based sentence search (find sentences containing a specific lemma)
#
# Key morphological features are stored as individual columns for efficient querying.
# Less common features (Degree, PronType, Polarity, etc.) are stored in feats_extra JSON.
#
# WITHOUT ROWID: Composite PK is the clustering key for efficient range scans by sentence.
sentence_tokens = Table(
    "sentence_tokens",
    metadata,
    Column(
        "sentence_id",
        Integer,
        ForeignKey("sentences.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("token_index", Integer, nullable=False),  # 0-indexed position in sentence
    Column("text", Text, nullable=False),  # Surface form
    Column("lemma", Text, nullable=False),  # Stanza lemma
    Column("upos", String(10), nullable=False),  # Universal POS (VERB, NOUN, ADJ, etc.)
    # Key morphological features as individual columns (NULL if not applicable)
    Column("verbform", String(10)),  # Fin, Inf, Part, Ger
    Column("mood", String(10)),  # Ind, Sub, Cnd, Imp (only for VerbForm=Fin)
    Column("tense", String(10)),  # Pres, Past, Imp, Fut
    Column("person", Integer),  # 1, 2, 3
    Column("number", String(10)),  # Sing, Plur
    Column("gender", String(10)),  # Masc, Fem (participles, nouns, adjectives)
    # Compound tense features (resolved from AUX dependent at import time)
    # For VERB tokens with VerbForm=Part,Tense=Past that have an AUX dependent,
    # these store the AUX's mood/tense to enable compound tense sentence matching.
    Column("compound_mood", String(10)),  # AUX mood for compound tenses (Ind, Sub, Cnd)
    Column("compound_tense", String(10)),  # AUX tense for compound tenses (Pres, Imp, Past, Fut)
    # Extra features as JSON for less common attributes
    Column("feats_extra", Text),  # JSON object for Degree, PronType, etc.
    # Dependency info
    Column("head", Integer),  # Dependency head index (0 = root)
    Column("deprel", String(20)),  # Dependency relation
    PrimaryKeyConstraint("sentence_id", "token_index"),
    sqlite_with_rowid=False,
)

# Verb-specific metadata (auxiliary, transitivity, and pronominal verb classification)
#
# Pronominal verbs (ending in -si/-rsi like lavarsi, pentirsi) are classified by type:
#
# - pronominal_type: Classifies the type of pronominal construction:
#   - 'reflexive': Subject acts on itself (lavarsi = wash oneself)
#   - 'inherent': Verb only exists in pronominal form (pentirsi, accorgersi)
#   - NULL: Not a pronominal verb
#
# For the relationship to base verbs (lavarsi → lavare), see the lemma_relationships
# table with relationship_type = 'reflexive_of'.
#
# Note: Both base verb and pronominal verb keep their full conjugations, since
# pronominal forms include clitics (mi lavo, ti lavi) and may use different
# auxiliaries (avere vs essere).
#
verb_metadata = Table(
    "verb_metadata",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), primary_key=True),
    Column("auxiliary", String(20)),  # 'avere', 'essere', 'both', NULL
    Column("transitivity", String(20)),  # 'transitive', 'intransitive', 'both', NULL
    Column("pronominal_type", Text),  # 'reflexive', 'inherent', NULL
    # Note: base_verb_lemma_id moved to lemma_relationships table (reflexive_of)
)

# Noun-specific metadata (gender classification and number behavior)
#
# For noun relationships:
# - Gender counterpart pairs (professore↔professoressa): See lemma_relationships
#   table with relationship_type = 'gender_counterpart'
# - Morphological derivations (cagnolino→cane): See definitions.derived_from_lemma_id
#   with derivation_type = 'diminutive', 'augmentative', 'pejorative', 'endearing'
#
noun_metadata = Table(
    "noun_metadata",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), primary_key=True),
    # Gender classification (mutually exclusive):
    # 'm' = masculine only, 'f' = feminine only,
    # 'common_gender_fixed' = both genders, identical forms (cantante),
    # 'common_gender_variable' = both genders, forms can differ (collega),
    # 'by_sense' = gender depends on meaning (il fine=goal vs la fine=end)
    Column("gender_class", Text, nullable=False),
    # Number behavior (mutually exclusive):
    # 'standard' = has both singular and plural,
    # 'pluralia_tantum' = plural only (forbici),
    # 'singularia_tantum' = singular only/uncountable (latte),
    # 'invariable' = same form for both (città)
    Column("number_class", Text, default="standard"),
    # How number_class was determined:
    # 'wiktextract' = detected via # marker, category, or identical forms
    # 'inferred:accented_ending' = word ends in accented vowel (à, è, ì, ò, ù)
    # 'inferred:greek_si' = word ends in -si (Greek-origin invariables)
    # 'default' = no signal, defaulted to 'standard'
    Column("number_class_source", Text),
    # Note: counterpart_lemma_id moved to lemma_relationships table (gender_counterpart)
    # Note: base_lemma_id and derivation_type moved to definitions table
)

# Adjective-specific metadata (inflection class)
#
# For degree relationships (migliore→buono, ottimo→buono): See lemma_relationships
# table with relationship_type = 'comparative_of' or 'superlative_of'.
#
# Unlike nouns, adjectives don't need counterpart_lemma_id because Italian
# adjectives inflect for gender within a single lemma (bello/bella/belli/belle)
# rather than having separate masculine and feminine lemmas.
#
adjective_metadata = Table(
    "adjective_metadata",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), primary_key=True),
    # Inflection class (mutually exclusive):
    # '4-form' = standard (bello/bella/belli/belle)
    # '2-form' = same form for m/f (facile/facile/facili/facili)
    # 'invariable' = same form for all (blu)
    Column("inflection_class", Text),
    # Note: base_lemma_id, degree_relationship moved to lemma_relationships table
)

# =============================================================================
# Lemma Relationships Table
# =============================================================================
#
# This table captures lemma-to-lemma relationships where ALL definitions of the
# source lemma inherit the relationship. This is the "Tier 1" relationship model.
#
# For relationships that apply to only SOME definitions (e.g., diminutives with
# independent meanings), use definitions.derived_from_lemma_id instead ("Tier 2").
#
# Two-Tier Relationship Model:
# ============================
#
# Tier 1 (lemma_relationships): When ALL definitions inherit the relationship
#   - clipping_of: bici → bicicletta (all senses are "clipping of bicycle")
#   - gender_counterpart: professore ↔ professoressa
#   - comparative_of: migliore → buono (all senses are comparative)
#   - reflexive_of: lavarsi → lavare
#
# Tier 2 (definitions.derived_from_lemma_id): When only SOME definitions relate
#   - diminutive: cagnolino has "little dog" (→ cane) AND "puppy" (independent)
#   - augmentative: similar pattern
#   - pejorative: similar pattern
#
# Direction convention:
#   - source_lemma_id: The derived/dependent lemma (e.g., bici)
#   - target_lemma_id: The base/canonical lemma (e.g., bicicletta)
#
# Why two tiers?
#   - Prevents duplication (no need for both lemma_relationships AND per-def links)
#   - Prevents illegal states (can't have conflicting relationship types)
#   - Matches the linguistic reality: some relationships are whole-lemma, some per-sense
#
lemma_relationships = Table(
    "lemma_relationships",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_lemma_id", Integer, ForeignKey("lemmas.id"), nullable=False),
    Column("target_lemma_id", Integer, ForeignKey("lemmas.id"), nullable=False),
    Column("relationship_type", Text, nullable=False),  # LemmaRelationshipType enum value
    Column("source", Text, nullable=False),  # 'wiktextract', 'hardcoded', 'inferred'
    Column("bidirectional", Boolean, default=False),
    Column("notes", Text),
    UniqueConstraint(
        "source_lemma_id",
        "target_lemma_id",
        "relationship_type",
        name="uq_lemma_relationship",
    ),
)


# Verb irregularity pattern classification
#
# This table classifies irregular verbs by their conjugation patterns across
# different tense domains. A verb can have irregularities in multiple tenses
# (e.g., venire has g_insertion in present, strong_nn in remote, syncopated_rr
# in future).
#
# NULL in any pattern column means the verb is regular in that tense domain.
# This table only contains entries for verbs with at least one irregular pattern.
#
# Pattern enum values are defined in italian_db/enums.py:
# - PresentPattern: g_insertion, suppletive_essere, modal_potere, etc.
# - RemotePattern: strong_ss, strong_nn, suppletive_essere, etc.
# - FuturePattern: syncopated_rr, syncopated_dr, contracted_base, suppletive
# - ParticiplePattern: strong_tto, strong_so, strong_to_into, etc.
# - SubjunctivePattern: suppletive_sia, suppletive_abbia, etc.
#
verb_irregularity = Table(
    "verb_irregularity",
    metadata,
    Column("lemma_id", Integer, ForeignKey("lemmas.id"), primary_key=True),
    # Pattern columns (NULL = regular in this tense domain)
    Column("present_pattern", Text),  # PresentPattern enum value
    Column("remote_pattern", Text),  # RemotePattern enum value
    Column("future_pattern", Text),  # FuturePattern enum value
    Column("participle_pattern", Text),  # ParticiplePattern enum value
    Column("subjunctive_pattern", Text),  # SubjunctivePattern enum value
    # Metadata
    Column("classification_source", Text),  # 'manual', 'inferred', etc.
    Column("notes", Text),  # Optional notes for edge cases
)

# Indexes (defined separately for clarity)
Index(
    "idx_lemmas_stressed_pos", lemmas.c.stressed, lemmas.c.pos
)  # For lookups by stressed form+POS
Index(
    "idx_lemmas_written_pos", lemmas.c.written, lemmas.c.pos
)  # For lookups by written form+POS (used by anki_gen)
Index("idx_verb_metadata_auxiliary", verb_metadata.c.auxiliary)
# noun_metadata indexes
Index("idx_noun_metadata_gender_class", noun_metadata.c.gender_class)
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
# lemma_relationships indexes
Index("idx_lemma_rel_source", lemma_relationships.c.source_lemma_id)
Index("idx_lemma_rel_target", lemma_relationships.c.target_lemma_id)
Index("idx_lemma_rel_type", lemma_relationships.c.relationship_type)
# verb_irregularity indexes
Index("idx_verb_irregularity_present", verb_irregularity.c.present_pattern)
Index("idx_verb_irregularity_remote", verb_irregularity.c.remote_pattern)
Index("idx_verb_irregularity_future", verb_irregularity.c.future_pattern)
Index("idx_verb_irregularity_participle", verb_irregularity.c.participle_pattern)
Index("idx_verb_irregularity_subjunctive", verb_irregularity.c.subjunctive_pattern)
# Other indexes
Index("idx_definitions_lemma", definitions.c.lemma_id)
Index("idx_definitions_derived_from", definitions.c.derived_from_lemma_id)
Index("idx_frequencies_lemma", frequencies.c.lemma_id)
Index("idx_sentences_lang", sentences.c.lang)
Index("idx_sentences_source", sentences.c.source)
Index("idx_sentences_sentence_id", sentences.c.sentence_id)  # For lookups by native ID
Index("idx_translations_ita", translations.c.ita_sentence_id)
# sentence_tags indexes for tag-based filtering
Index("idx_sentence_tags_tag", sentence_tags.c.tag)
# sentence_tokens indexes for common queries
Index("idx_sentence_tokens_lemma", sentence_tokens.c.lemma)
Index("idx_sentence_tokens_upos", sentence_tokens.c.upos)
Index("idx_sentence_tokens_lemma_upos", sentence_tokens.c.lemma, sentence_tokens.c.upos)
# Composite index for verb form lookups
Index(
    "idx_sentence_tokens_verb_forms",
    sentence_tokens.c.lemma,
    sentence_tokens.c.mood,
    sentence_tokens.c.tense,
    sentence_tokens.c.person,
    sentence_tokens.c.number,
)
# Composite index for compound tense lookups (passato prossimo, trapassato, etc.)
Index(
    "idx_sentence_tokens_compound_verb",
    sentence_tokens.c.lemma,
    sentence_tokens.c.compound_mood,
    sentence_tokens.c.compound_tense,
)


def init_db(engine: Engine) -> None:
    """Initialize the database schema.

    Creates all tables and indexes if they don't exist.
    Safe to call multiple times (uses checkfirst=True by default).
    """
    metadata.create_all(engine)

    # Create FTS5 virtual table for sentence search (can't be done via SQLAlchemy Table)
    # id is UNINDEXED (stored but not searchable) for joining to sentences table
    with engine.connect() as conn:
        conn.execute(
            text("""
                CREATE VIRTUAL TABLE IF NOT EXISTS sentences_fts USING fts5(
                    id UNINDEXED,
                    text
                )
            """)
        )

        # Create unique index for verb_forms using COALESCE to handle NULLs
        # (SQLAlchemy Index doesn't support expression-based unique constraints)
        conn.execute(
            text("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_verb_forms_entry ON verb_forms(
                    lemma_id, stressed, mood,
                    COALESCE(tense, ''), COALESCE(aspect, ''),
                    COALESCE(person, 0), COALESCE(number, ''),
                    COALESCE(gender, ''), is_formal, is_negative
                )
            """)
        )

        conn.commit()
