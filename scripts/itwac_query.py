"""Interactive DuckDB session for querying raw ItWaC frequency data."""

import duckdb

conn = duckdb.connect()

# Verbs (has extra mode and POS2 columns)
conn.execute("""
    CREATE VIEW itwac_verbs AS
    SELECT * FROM read_csv_auto('data/itwac/itwac_verbs_lemmas_notail_2_1_0.csv',
        encoding='latin-1')
""")

# Nouns
conn.execute("""
    CREATE VIEW itwac_nouns AS
    SELECT * FROM read_csv_auto('data/itwac/itwac_nouns_lemmas_notail_2_0_0.csv',
        encoding='latin-1')
""")

# Adjectives
conn.execute("""
    CREATE VIEW itwac_adj AS
    SELECT * FROM read_csv_auto('data/itwac/itwac_adj_lemmas_notail_2_1_0.csv',
        encoding='latin-1')
""")

# Unified view with consistent columns
conn.execute("""
    CREATE VIEW itwac AS
    SELECT Form, Freq, lemma, 'verb' as pos, fpmw, Zipf FROM itwac_verbs
    UNION ALL
    SELECT Form, Freq, lemma, 'noun' as pos, fpmw, Zipf FROM itwac_nouns
    UNION ALL
    SELECT Form, Freq, lemma, 'adj' as pos, fpmw, Zipf FROM itwac_adj
""")

print("DuckDB session ready. Available views:")
print("  itwac_verbs - Verb forms (Form, Freq, lemma, POS, mode, POS2, fpmw, Zipf)")
print("  itwac_nouns - Noun forms (Form, Freq, lemma, POS, fpmw, Zipf)")
print("  itwac_adj   - Adjective forms (Form, Freq, lemma, POS, fpmw, Zipf)")
print("  itwac       - Unified view (Form, Freq, lemma, pos, fpmw, Zipf)")
print()
print("Example queries:")
print("  conn.sql(\"SELECT * FROM itwac WHERE lemma = 'parlare' ORDER BY Freq DESC\")")
print(
    '  conn.sql("SELECT lemma, SUM(Freq) as total FROM itwac GROUP BY lemma ORDER BY total DESC LIMIT 20")'
)
print('  conn.sql("SELECT * FROM itwac WHERE Zipf > 5 ORDER BY Zipf DESC LIMIT 20")')
