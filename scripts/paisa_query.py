"""Interactive DuckDB session for querying PAISA lemma frequency data.

Data source: PAISA corpus (CC-BY-NC-SA 4.0 - NonCommercial!)
Large web corpus from Italian .it domain (2010).

WARNING: NC license - use for evaluation/comparison only, not as primary data source.

Format: CSV with 2 comment lines (starting with #), columns: lemma, frequency
Note: Includes punctuation tokens like ',', '.', '"', '(', ')' as separate entries.
"""

import duckdb

conn = duckdb.connect()

# Load PAISA data, skipping the 2 comment header lines
conn.execute("""
    CREATE VIEW paisa_raw AS
    SELECT
        column0 as lemma,
        column1::INTEGER as freq
    FROM read_csv_auto('data/paisa/lemma-frequencies-paisa.txt',
        delim=',', header=false, skip=2)
""")

# Filtered view excluding punctuation tokens
conn.execute("""
    CREATE VIEW paisa AS
    SELECT
        lemma,
        freq,
        ROW_NUMBER() OVER (ORDER BY freq DESC) as rank
    FROM paisa_raw
    WHERE lemma NOT IN (',', '.', '"', '(', ')', ':', ';', '?', '!', '-', '\'', '/')
      AND length(lemma) > 0
""")

print("=" * 70)
print("WARNING: PAISA has CC-BY-NC-SA license (NonCommercial)")
print("         Use for evaluation/comparison only!")
print("=" * 70)
print()
print("DuckDB session ready. Available views:")
print("  paisa_raw - All PAISA data including punctuation (lemma, freq)")
print("  paisa     - Filtered to exclude punctuation (lemma, freq, rank)")
print()
print("Source: PAISA corpus - Italian web text from .it domain (2010)")
print()
print("Example queries:")
print("  conn.sql('SELECT * FROM paisa LIMIT 20')  # Top 20 lemmas")
print("  conn.sql(\"SELECT * FROM paisa WHERE lemma = 'essere'\")")
print("  conn.sql(\"SELECT * FROM paisa WHERE lemma IN ('ciao', 'mamma', 'decreto')\")")
print()
print("Note: PAISA uses lemmas (base forms), not surface forms.")
print("      E.g., 'essere' instead of 'è', 'sono', 'era', etc.")
