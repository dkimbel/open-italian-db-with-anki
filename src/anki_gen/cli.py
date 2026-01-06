"""Command-line interface for Anki deck generation."""

import argparse
import sys
from pathlib import Path

from anki_gen.generator import generate_deck
from anki_gen.preview import write_preview
from italian_db.db import get_connection

DEFAULT_DB_PATH = Path("italian.db")
DEFAULT_OUTPUT_PATH = Path("output/italian.apkg")
DEFAULT_PREVIEW_PATH = Path("output/preview.html")


def cmd_generate_deck(args: argparse.Namespace) -> int:
    """Generate an Anki deck from the database."""
    db_path = Path(args.database)
    output_path = Path(args.output)

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"Generating Anki deck from: {db_path}")
    print(f"Output: {output_path}")
    print()

    with get_connection(db_path) as conn:
        stats = generate_deck(
            conn,
            output_path=output_path,
            tenses=["presente_indicativo"],
        )

    print()
    print("=" * 50)
    print("Generation complete!")
    print("=" * 50)
    print(f"  Verbs processed:  {stats.verbs_processed}")
    print(f"  Cards generated:  {stats.cards_generated}")
    print(f"  Verbs skipped:    {stats.verbs_skipped}")
    if stats.skipped_reasons:
        print()
        print("Skipped reasons:")
        for reason in stats.skipped_reasons[:10]:
            print(f"  - {reason}")
        if len(stats.skipped_reasons) > 10:
            print(f"  ... and {len(stats.skipped_reasons) - 10} more")
    print()
    print(f"Output written to: {output_path}")

    return 0


def cmd_preview(args: argparse.Namespace) -> int:
    """Generate an HTML preview of a card."""
    db_path = Path(args.database)
    output_path = Path(args.output)
    verb = args.verb

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        return 1

    with get_connection(db_path) as conn:
        result_path = write_preview(conn, verb, output_path)

    print(f"Preview written to: {result_path}")
    return 0


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="anki-gen",
        description="Generate Anki flashcard decks for learning Italian",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate subcommand
    gen_parser = subparsers.add_parser(
        "generate",
        help="Generate an Anki deck",
    )
    gen_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    gen_parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_PATH),
        help=f"Path for output .apkg file (default: {DEFAULT_OUTPUT_PATH})",
    )
    gen_parser.set_defaults(func=cmd_generate_deck)

    # preview subcommand
    preview_parser = subparsers.add_parser(
        "preview",
        help="Generate an HTML preview of a card",
    )
    preview_parser.add_argument(
        "verb",
        type=str,
        help="Verb infinitive to preview (e.g., 'parlare')",
    )
    preview_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    preview_parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=str(DEFAULT_PREVIEW_PATH),
        help=f"Path for output HTML file (default: {DEFAULT_PREVIEW_PATH})",
    )
    preview_parser.set_defaults(func=cmd_preview)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
