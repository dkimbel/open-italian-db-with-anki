"""Interactive DuckDB session for querying raw MorphIt data."""

import duckdb

conn = duckdb.connect()

conn.execute("""
    CREATE VIEW morphit AS
    SELECT * FROM read_csv_auto('data/morphit/morph-it.txt',
        delim='\t',
        header=false,
        encoding='latin-1',
        columns={'surface_form': 'VARCHAR', 'lemma': 'VARCHAR', 'pos_tag': 'VARCHAR'},
        ignore_errors=true)
""")

print("DuckDB session ready. The 'morphit' view is available.")
print("  morphit - (surface_form, lemma, pos_tag)")
print()
print("POS tag prefixes: VER: (verbs), NOUN- (nouns), ADJ: (adjectives)")
print()
print("Example queries:")
print("  conn.sql(\"SELECT * FROM morphit WHERE lemma = 'parlare' LIMIT 20\")")
print("  conn.sql(\"SELECT DISTINCT pos_tag FROM morphit WHERE pos_tag LIKE 'VER:%'\")")
print("  conn.sql(\"SELECT * FROM morphit WHERE surface_form LIKE '%ò' AND pos_tag LIKE 'VER:%'\")")
print(
    '  conn.sql("SELECT lemma, COUNT(*) as forms FROM morphit GROUP BY lemma ORDER BY forms DESC LIMIT 10")'
)
