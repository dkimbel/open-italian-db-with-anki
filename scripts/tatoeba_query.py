"""Interactive DuckDB session for querying raw Tatoeba data."""

import duckdb

conn = duckdb.connect()

# Italian sentences
conn.execute("""
    CREATE VIEW ita_sentences AS
    SELECT * FROM read_csv_auto('data/tatoeba/ita_sentences.tsv',
        delim='\t',
        header=false,
        columns={'sentence_id': 'INTEGER', 'lang': 'VARCHAR', 'text': 'VARCHAR'})
""")

# English sentences
conn.execute("""
    CREATE VIEW eng_sentences AS
    SELECT * FROM read_csv_auto('data/tatoeba/eng_sentences.tsv',
        delim='\t',
        header=false,
        columns={'sentence_id': 'INTEGER', 'lang': 'VARCHAR', 'text': 'VARCHAR'})
""")

# Links between Italian and English sentences
conn.execute("""
    CREATE VIEW links AS
    SELECT * FROM read_csv_auto('data/tatoeba/ita_eng_links.tsv',
        delim='\t',
        header=false,
        columns={'ita_id': 'INTEGER', 'eng_id': 'INTEGER'})
""")

# Sentences with audio
conn.execute("""
    CREATE VIEW audio AS
    SELECT * FROM read_csv_auto('data/tatoeba/sentences_with_audio.csv',
        delim='\t',
        header=false,
        columns={'sentence_id': 'INTEGER', 'audio_id': 'INTEGER', 'username': 'VARCHAR', 'license': 'VARCHAR', 'url': 'VARCHAR'})
""")

# Convenience view: Italian sentences with English translations
conn.execute("""
    CREATE VIEW translations AS
    SELECT
        i.sentence_id as ita_id,
        i.text as italian,
        e.sentence_id as eng_id,
        e.text as english
    FROM ita_sentences i
    JOIN links l ON i.sentence_id = l.ita_id
    JOIN eng_sentences e ON l.eng_id = e.sentence_id
""")

print("DuckDB session ready. Available views:")
print("  ita_sentences  - Italian sentences (sentence_id, lang, text)")
print("  eng_sentences  - English sentences (sentence_id, lang, text)")
print("  links          - Translation links (ita_id, eng_id)")
print("  audio          - Sentences with audio (sentence_id, audio_id, username, ...)")
print("  translations   - Pre-joined Italian-English pairs (ita_id, italian, eng_id, english)")
print()
print("Example queries:")
print("  conn.sql(\"SELECT * FROM translations WHERE italian LIKE '%casa%' LIMIT 5\")")
print('  conn.sql("SELECT COUNT(*) FROM ita_sentences")')
print('  conn.sql("SELECT * FROM translations t JOIN audio a ON t.ita_id = a.sentence_id LIMIT 5")')
