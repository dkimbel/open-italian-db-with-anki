"""Interactive DuckDB session for querying raw ParTUT CoNLL-U data.

ParTUT is a Universal Dependencies treebank with Italian, English, and French
parallel translations. The Italian corpus contains ~2,090 sentences with
full morphological annotation.

CoNLL-U format fields:
    1. ID: Word index (1-indexed)
    2. FORM: Word form / surface text
    3. LEMMA: Dictionary form
    4. UPOS: Universal POS tag (VERB, NOUN, ADJ, etc.)
    5. XPOS: Language-specific POS tag
    6. FEATS: Morphological features (Mood=Ind|Tense=Pres|...)
    7. HEAD: Head of dependency relation
    8. DEPREL: Dependency relation
    9. DEPS: Enhanced dependency graph
    10. MISC: Miscellaneous annotation
"""

import duckdb

conn = duckdb.connect()

# Read all Italian CoNLL-U files into a single table
# We use read_csv with a custom column specification for CoNLL-U format
conn.execute("""
    CREATE VIEW ita_tokens_raw AS
    SELECT
        column0 as id,
        column1 as form,
        column2 as lemma,
        column3 as upos,
        column4 as xpos,
        column5 as feats,
        column6 as head,
        column7 as deprel,
        column8 as deps,
        column9 as misc,
        filename
    FROM read_csv(
        'data/partut/it_partut-ud-*.conllu',
        delim='\t',
        header=false,
        quote='',
        escape='',
        comment='#',
        ignore_errors=true,
        filename=true
    )
    WHERE column0 NOT LIKE '%-%'  -- Exclude multi-word token ranges like "1-2"
""")

# Create a view with parsed morphological features
conn.execute("""
    CREATE VIEW ita_tokens AS
    SELECT
        id::INTEGER as token_idx,
        form,
        lemma,
        upos,
        xpos,
        feats,
        regexp_extract(feats, 'Mood=(\\w+)', 1) as mood,
        regexp_extract(feats, 'Tense=(\\w+)', 1) as tense,
        regexp_extract(feats, 'Person=(\\d)', 1)::INTEGER as person,
        regexp_extract(feats, 'Number=(\\w+)', 1) as number,
        regexp_extract(feats, 'Gender=(\\w+)', 1) as gender,
        regexp_extract(feats, 'VerbForm=(\\w+)', 1) as verb_form,
        head::INTEGER as head_idx,
        deprel,
        filename
    FROM ita_tokens_raw
    WHERE id NOT LIKE '%-%'  -- Double-check: exclude multi-word ranges
""")

# Read English tokens for translation matching
conn.execute("""
    CREATE VIEW eng_tokens_raw AS
    SELECT
        column0 as id,
        column1 as form,
        column2 as lemma,
        column3 as upos,
        column4 as xpos,
        column5 as feats,
        column6 as head,
        column7 as deprel,
        column8 as deps,
        column9 as misc,
        filename
    FROM read_csv(
        'data/partut/en_partut-ud-*.conllu',
        delim='\t',
        header=false,
        quote='',
        escape='',
        comment='#',
        ignore_errors=true,
        filename=true
    )
    WHERE column0 NOT LIKE '%-%'
""")

# Create verb-specific view for easy querying
conn.execute("""
    CREATE VIEW verbs AS
    SELECT
        form,
        lemma,
        mood,
        tense,
        person,
        number,
        verb_form,
        feats,
        filename
    FROM ita_tokens
    WHERE upos = 'VERB'
""")

# Summary statistics
conn.execute("""
    CREATE VIEW stats AS
    SELECT
        upos,
        COUNT(*) as token_count,
        COUNT(DISTINCT lemma) as unique_lemmas
    FROM ita_tokens
    GROUP BY upos
    ORDER BY token_count DESC
""")

print("DuckDB session ready with ParTUT data.")
print()
print("Available views:")
print("  ita_tokens     - Italian tokens with parsed morphological features")
print("  ita_tokens_raw - Raw Italian CoNLL-U data")
print("  eng_tokens_raw - Raw English CoNLL-U data")
print("  verbs          - Italian verbs only, with mood/tense/person/number")
print("  stats          - Token counts by POS")
print()
print("Key columns in ita_tokens:")
print("  token_idx, form, lemma, upos, mood, tense, person, number, gender, verb_form")
print()
print("Universal Dependencies mood/tense values:")
print("  mood:  Ind (indicative), Sub (subjunctive), Cnd (conditional), Imp (imperative)")
print("  tense: Pres (present), Past (remote), Fut (future), Imp (imperfect)")
print()
print("Example queries:")
print("  conn.sql('SELECT * FROM stats')")
print("  conn.sql(\"SELECT * FROM verbs WHERE lemma = 'essere' LIMIT 20\")")
print("  conn.sql(\"SELECT DISTINCT lemma, mood, tense FROM verbs WHERE mood = 'Sub' LIMIT 20\")")
print(
    "  conn.sql(\"SELECT form, lemma, mood, tense, person, number FROM verbs WHERE lemma = 'avere'\")"
)
print(
    '  conn.sql("SELECT COUNT(*) as n, mood, tense FROM verbs GROUP BY mood, tense ORDER BY n DESC")'
)
