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
print("Cross-source query examples:")
print("  conn.sql(\"SELECT w.word, w.pos FROM wikt w WHERE w.word = 'casa'\")")
