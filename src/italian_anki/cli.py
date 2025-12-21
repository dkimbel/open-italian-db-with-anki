"""Command-line interface for Italian Anki deck generator."""

import argparse
import sys
from pathlib import Path

from italian_anki.db import get_connection, get_engine, init_db
from italian_anki.importers import import_wiktextract

DEFAULT_WIKTEXTRACT_PATH = Path("data/wiktextract/kaikki.org-dictionary-Italian.jsonl")
DEFAULT_DB_PATH = Path("italian_anki.db")


def cmd_import_wiktextract(args: argparse.Namespace) -> int:
    """Run the Wiktextract import command."""
    jsonl_path = Path(args.input)
    db_path = Path(args.database)

    if not jsonl_path.exists():
        print(f"Error: Input file not found: {jsonl_path}", file=sys.stderr)
        return 1

    print(f"Initializing database: {db_path}")
    engine = get_engine(db_path)
    init_db(engine)

    print(f"Importing from: {jsonl_path}")
    print(f"Filtering to: {args.pos}")
    print()

    with get_connection(db_path) as conn:
        stats = import_wiktextract(conn, jsonl_path, pos_filter=args.pos)

    print()
    print("Import complete!")
    print(f"  Lemmas:      {stats['lemmas']:,}")
    print(f"  Forms:       {stats['forms']:,}")
    print(f"  Definitions: {stats['definitions']:,}")
    print(f"  Skipped:     {stats['skipped']:,}")

    return 0


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="italian-anki",
        description="Generate Anki flashcard decks for learning Italian",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # import-wiktextract subcommand
    import_parser = subparsers.add_parser(
        "import-wiktextract",
        help="Import data from Wiktextract JSONL dump",
    )
    import_parser.add_argument(
        "-i",
        "--input",
        type=str,
        default=str(DEFAULT_WIKTEXTRACT_PATH),
        help=f"Path to Wiktextract JSONL file (default: {DEFAULT_WIKTEXTRACT_PATH})",
    )
    import_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to output SQLite database (default: {DEFAULT_DB_PATH})",
    )
    import_parser.add_argument(
        "--pos",
        type=str,
        default="verb",
        choices=["verb", "noun", "adjective", "adverb"],
        help="Part of speech to import (default: verb)",
    )
    import_parser.set_defaults(func=cmd_import_wiktextract)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
