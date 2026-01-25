"""Interactive DuckDB session for querying OpenSubtitles frequency data.

Data source: hermitdave/FrequencyWords (CC-BY-SA 4.0)
Derived from OpenSubtitles2018 corpus (conversational/dialogue text).

Format: Space-separated 'word count' pairs, no header.
"""

import duckdb

conn = duckdb.connect()

# Top 50K words (smaller file, faster loading)
conn.execute("""
    CREATE VIEW opensub_50k AS
    SELECT
        column0 as word,
        column1::INTEGER as freq
    FROM read_csv_auto('data/opensubtitles/it_50k.txt',
        delim=' ', header=false)
""")

# Full word list (may take longer to load)
conn.execute("""
    CREATE VIEW opensub_full AS
    SELECT
        column0 as word,
        column1::INTEGER as freq
    FROM read_csv_auto('data/opensubtitles/it_full.txt',
        delim=' ', header=false)
""")

# Add rank columns for analysis
conn.execute("""
    CREATE VIEW opensub AS
    SELECT
        word,
        freq,
        ROW_NUMBER() OVER (ORDER BY freq DESC) as rank
    FROM opensub_full
""")

print("DuckDB session ready. Available views:")
print("  opensub_50k  - Top 50K words (word, freq)")
print("  opensub_full - Complete word list (word, freq)")
print("  opensub      - Full list with rank (word, freq, rank)")
print()
print("Source: OpenSubtitles2018 via hermitdave/FrequencyWords (CC-BY-SA 4.0)")
print("        Conversational/dialogue text from movie subtitles")
print()
print("Example queries:")
print("  conn.sql(\"SELECT * FROM opensub WHERE word = 'ciao'\")")
print('  conn.sql("SELECT * FROM opensub ORDER BY freq DESC LIMIT 20")')
print("  conn.sql(\"SELECT * FROM opensub WHERE word IN ('ciao', 'mamma', 'decreto')\")")
