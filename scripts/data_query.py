"""Interactive DuckDB session with all source data loaded."""

import duckdb

conn = duckdb.connect()

# =============================================================================
# WIKTEXTRACT
# =============================================================================
conn.execute("""
    CREATE VIEW wikt AS
    SELECT * FROM read_json_auto('data/wiktextract/kaikki.org-dictionary-Italian.jsonl',
        format='newline_delimited',
        maximum_object_size=10485760)
""")

# =============================================================================
# TATOEBA
# =============================================================================
conn.execute("""
    CREATE VIEW ita_sentences AS
    SELECT * FROM read_csv_auto('data/tatoeba/ita_sentences.tsv',
        delim='\t',
        header=false,
        columns={'sentence_id': 'INTEGER', 'lang': 'VARCHAR', 'text': 'VARCHAR'})
""")

conn.execute("""
    CREATE VIEW eng_sentences AS
    SELECT * FROM read_csv_auto('data/tatoeba/eng_sentences.tsv',
        delim='\t',
        header=false,
        columns={'sentence_id': 'INTEGER', 'lang': 'VARCHAR', 'text': 'VARCHAR'})
""")

conn.execute("""
    CREATE VIEW tatoeba_links AS
    SELECT * FROM read_csv_auto('data/tatoeba/ita_eng_links.tsv',
        delim='\t',
        header=false,
        columns={'ita_id': 'INTEGER', 'eng_id': 'INTEGER'})
""")

conn.execute("""
    CREATE VIEW tatoeba_audio AS
    SELECT * FROM read_csv_auto('data/tatoeba/sentences_with_audio.csv',
        delim='\t',
        header=false,
        columns={'sentence_id': 'INTEGER', 'audio_id': 'INTEGER', 'username': 'VARCHAR', 'license': 'VARCHAR', 'url': 'VARCHAR'})
""")

conn.execute("""
    CREATE VIEW translations AS
    SELECT
        i.sentence_id as ita_id,
        i.text as italian,
        e.sentence_id as eng_id,
        e.text as english
    FROM ita_sentences i
    JOIN tatoeba_links l ON i.sentence_id = l.ita_id
    JOIN eng_sentences e ON l.eng_id = e.sentence_id
""")

# =============================================================================
# FREQUENCY DATA (PAISA + OpenSubtitles)
# =============================================================================
conn.execute("""
    CREATE VIEW paisa AS
    SELECT
        column0 as lemma,
        column1::INTEGER as freq
    FROM read_csv_auto('data/paisa/lemma-frequencies-paisa.txt',
        delim=',', header=false, skip=2)
""")

conn.execute("""
    CREATE VIEW opensub AS
    SELECT
        column0 as word,
        column1::INTEGER as freq,
        ROW_NUMBER() OVER (ORDER BY column1::INTEGER DESC) as rank
    FROM read_csv_auto('data/opensubtitles/it_full.txt',
        delim=' ', header=false)
""")

# =============================================================================
# PARTUT (Universal Dependencies Italian treebank)
# =============================================================================
conn.execute("""
    CREATE VIEW partut_ita_tokens AS
    SELECT
        column0 as id,
        column1 as form,
        column2 as lemma,
        column3 as upos,
        column5 as feats,
        regexp_extract(column5, 'Mood=(\\w+)', 1) as mood,
        regexp_extract(column5, 'Tense=(\\w+)', 1) as tense,
        regexp_extract(column5, 'Person=(\\d)', 1)::INTEGER as person,
        regexp_extract(column5, 'Number=(\\w+)', 1) as number,
        regexp_extract(column5, 'VerbForm=(\\w+)', 1) as verb_form,
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
    WHERE column0 NOT LIKE '%-%'
""")

conn.execute("""
    CREATE VIEW partut_verbs AS
    SELECT form, lemma, mood, tense, person, number, verb_form
    FROM partut_ita_tokens
    WHERE upos = 'VERB'
""")

# =============================================================================
print("DuckDB session ready with ALL source data.")
print()
print("WIKTEXTRACT:")
print("  wikt            - Full dictionary entries (word, pos, senses, forms, ...)")
print()
print("TATOEBA:")
print("  ita_sentences   - Italian sentences (sentence_id, lang, text)")
print("  eng_sentences   - English sentences (sentence_id, lang, text)")
print("  tatoeba_links   - Translation links (ita_id, eng_id)")
print("  tatoeba_audio   - Sentences with audio (sentence_id, audio_id, ...)")
print("  translations    - Pre-joined Italian-English pairs")
print()
print("FREQUENCY DATA:")
print("  paisa           - PAISA lemma frequencies (lemma, freq) - used for verbs")
print("  opensub         - OpenSubtitles frequencies (word, freq, rank) - used for nouns/adj")
print()
print("PARTUT:")
print("  partut_ita_tokens - Italian tokens with morphological features")
print("  partut_verbs      - Italian verbs (form, lemma, mood, tense, person, number)")
print()
print("Cross-source query examples:")
print("  # Check if a Wiktextract word has OpenSubtitles frequency")
print(
    "  conn.sql(\"SELECT w.word, o.freq, o.rank FROM wikt w LEFT JOIN opensub o ON w.word = o.word WHERE w.word = 'casa'\")"
)
