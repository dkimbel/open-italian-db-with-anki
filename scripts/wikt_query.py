"""Interactive DuckDB session for querying raw Wiktextract data."""

import duckdb

conn = duckdb.connect()
conn.execute("""
    CREATE VIEW wikt AS
    SELECT * FROM read_json_auto('data/wiktextract/kaikki.org-dictionary-Italian.jsonl',
        format='newline_delimited',
        maximum_object_size=10485760)
""")

print("DuckDB session ready. The 'wikt' view is available.")
print("Example queries:")
print("  conn.sql('SELECT pos, COUNT(*) FROM wikt GROUP BY pos')")
print("  conn.sql('SELECT * FROM wikt WHERE word = \"signora\"')")
