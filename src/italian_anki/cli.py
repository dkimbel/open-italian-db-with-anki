"""Command-line interface for Italian Anki deck generator."""

import argparse
import sys
from pathlib import Path

from italian_anki.db import get_connection, get_engine, init_db
from italian_anki.download import (
    download_all,
    download_itwac,
    download_morphit,
    download_tatoeba,
    download_wiktextract,
)
from italian_anki.importers import (
    import_itwac,
    import_morphit,
    import_tatoeba,
    import_wiktextract,
)
from italian_anki.importers.itwac import ITWAC_CSV_FILES
from italian_anki.importers.wiktextract import enrich_from_form_of

DEFAULT_WIKTEXTRACT_PATH = Path("data/wiktextract/kaikki.org-dictionary-Italian.jsonl")
DEFAULT_MORPHIT_PATH = Path("data/morphit/morph-it.txt")
DEFAULT_ITWAC_DIR = Path("data/itwac")
DEFAULT_ITA_SENTENCES_PATH = Path("data/tatoeba/ita_sentences.tsv")
DEFAULT_ENG_SENTENCES_PATH = Path("data/tatoeba/eng_sentences.tsv")
DEFAULT_LINKS_PATH = Path("data/tatoeba/ita_eng_links.tsv")
DEFAULT_DB_PATH = Path("italian.db")


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
    if stats["cleared"] > 0:
        print(f"  Cleared:     {stats['cleared']:,} existing lemmas")
    print(f"  Lemmas:      {stats['lemmas']:,}")
    print(f"  Forms:       {stats['forms']:,}")
    print(f"  Definitions: {stats['definitions']:,}")
    print(f"  Skipped:     {stats['skipped']:,}")
    if args.pos == "noun":
        print(f"  With gender: {stats.get('nouns_with_gender', 0):,}")
        print(f"  No gender:   {stats.get('nouns_no_gender', 0):,}")

    return 0


def cmd_enrich_formof(args: argparse.Namespace) -> int:
    """Run the form-of enrichment command."""
    jsonl_path = Path(args.input)
    db_path = Path(args.database)

    if not jsonl_path.exists():
        print(f"Error: Input file not found: {jsonl_path}", file=sys.stderr)
        return 1

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    print(f"Enriching forms from form-of entries: {db_path}")
    print(f"Using Wiktextract data from: {jsonl_path}")
    print(f"Filtering to: {args.pos}")
    print()

    with get_connection(db_path) as conn:
        stats = enrich_from_form_of(conn, jsonl_path, pos_filter=args.pos)

    print()
    print("Enrichment complete!")
    print(f"  Form-of entries scanned: {stats['scanned']:,}")
    print(f"  With label tags:         {stats['with_labels']:,}")
    print(f"  Forms updated:           {stats['updated']:,}")
    print(f"  Not found:               {stats['not_found']:,}")

    return 0


def cmd_import_morphit(args: argparse.Namespace) -> int:
    """Run the Morph-it! enrichment command."""
    morphit_path = Path(args.input)
    db_path = Path(args.database)

    if not morphit_path.exists():
        print(f"Error: Input file not found: {morphit_path}", file=sys.stderr)
        return 1

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    print(f"Enriching database: {db_path}")
    print(f"Using Morph-it! data from: {morphit_path}")
    print(f"Filtering to: {args.pos}")
    print()

    with get_connection(db_path) as conn:
        stats = import_morphit(conn, morphit_path, pos_filter=args.pos)

    print()
    print("Enrichment complete!")
    print(f"  Forms updated:    {stats['updated']:,}")
    print(f"  Forms not found:  {stats['not_found']:,}")
    print(f"  Lookup entries:   {stats['lookup_added']:,}")

    return 0


def cmd_import_itwac(args: argparse.Namespace) -> int:
    """Run the ItWaC frequency import command."""
    db_path = Path(args.database)

    # Determine CSV path: use explicit --input, or derive from --pos
    if args.input:
        csv_path = Path(args.input)
    else:
        csv_filename = ITWAC_CSV_FILES.get(args.pos)
        if csv_filename is None:
            print(f"Error: No ItWaC file configured for POS '{args.pos}'", file=sys.stderr)
            return 1
        csv_path = DEFAULT_ITWAC_DIR / csv_filename

    if not csv_path.exists():
        print(f"Error: Input file not found: {csv_path}", file=sys.stderr)
        return 1

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    print(f"Importing frequencies to: {db_path}")
    print(f"Using ItWaC data from: {csv_path}")
    print(f"Filtering to: {args.pos}")
    print()

    with get_connection(db_path) as conn:
        stats = import_itwac(conn, csv_path, pos_filter=args.pos)

    print()
    print("Import complete!")
    print(f"  Lemmas matched:     {stats['matched']:,}")
    print(f"  Lemmas not found:   {stats['not_found']:,}")

    return 0


def cmd_import_tatoeba(args: argparse.Namespace) -> int:
    """Run the Tatoeba sentences import command."""
    ita_path = Path(args.ita_sentences)
    eng_path = Path(args.eng_sentences)
    links_path = Path(args.links)
    db_path = Path(args.database)

    for path, name in [
        (ita_path, "Italian sentences"),
        (eng_path, "English sentences"),
        (links_path, "links"),
    ]:
        if not path.exists():
            print(f"Error: {name} file not found: {path}", file=sys.stderr)
            return 1

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    print(f"Importing Tatoeba sentences to: {db_path}")
    print(f"  Italian sentences: {ita_path}")
    print(f"  English sentences: {eng_path}")
    print(f"  Links: {links_path}")
    print()

    with get_connection(db_path) as conn:
        stats = import_tatoeba(conn, ita_path, eng_path, links_path)

    print()
    print("Import complete!")
    if stats["cleared"] > 0:
        print(f"  Cleared:          {stats['cleared']:,} existing sentences")
    print(f"  Italian sentences: {stats['ita_sentences']:,}")
    print(f"  English sentences: {stats['eng_sentences']:,}")
    print(f"  Translations:      {stats['translations']:,}")
    print(f"  Sentence-lemma links: {stats['sentence_lemmas']:,}")

    return 0


def cmd_download_wiktextract(args: argparse.Namespace) -> int:
    """Download Wiktextract Italian dictionary."""
    stats = download_wiktextract(force=args.force)
    if stats["downloaded"] > 0:
        print("Download complete!")
    return 0


def cmd_download_morphit(args: argparse.Namespace) -> int:
    """Download Morph-it! morphological lexicon."""
    stats = download_morphit(force=args.force)
    if stats["downloaded"] > 0:
        print("Download complete!")
    return 0


def cmd_download_itwac(args: argparse.Namespace) -> int:
    """Download ItWaC frequency lists."""
    stats = download_itwac(force=args.force)
    print(f"Downloaded: {stats['downloaded']} files, Skipped: {stats['skipped']} files")
    return 0


def cmd_download_tatoeba(args: argparse.Namespace) -> int:
    """Download Tatoeba sentences and links."""
    stats = download_tatoeba(force=args.force)
    print(f"Downloaded: {stats['downloaded']} files, Skipped: {stats['skipped']} files")
    return 0


def cmd_download_all(args: argparse.Namespace) -> int:
    """Download all data sources."""
    download_all(force=args.force)
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

    # enrich-formof subcommand
    enrich_parser = subparsers.add_parser(
        "enrich-formof",
        help="Enrich forms with labels from form-of entries",
    )
    enrich_parser.add_argument(
        "-i",
        "--input",
        type=str,
        default=str(DEFAULT_WIKTEXTRACT_PATH),
        help=f"Path to Wiktextract JSONL file (default: {DEFAULT_WIKTEXTRACT_PATH})",
    )
    enrich_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    enrich_parser.add_argument(
        "--pos",
        type=str,
        default="verb",
        choices=["verb", "noun", "adjective"],
        help="Part of speech to enrich (default: verb)",
    )
    enrich_parser.set_defaults(func=cmd_enrich_formof)

    # import-morphit subcommand
    morphit_parser = subparsers.add_parser(
        "import-morphit",
        help="Enrich forms with real Italian spelling from Morph-it!",
    )
    morphit_parser.add_argument(
        "-i",
        "--input",
        type=str,
        default=str(DEFAULT_MORPHIT_PATH),
        help=f"Path to Morph-it! file (default: {DEFAULT_MORPHIT_PATH})",
    )
    morphit_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    morphit_parser.add_argument(
        "--pos",
        type=str,
        default="verb",
        choices=["verb", "noun", "adjective"],
        help="Part of speech to enrich (default: verb)",
    )
    morphit_parser.set_defaults(func=cmd_import_morphit)

    # import-itwac subcommand
    itwac_parser = subparsers.add_parser(
        "import-itwac",
        help="Import frequency data from ItWaC corpus",
    )
    itwac_parser.add_argument(
        "-i",
        "--input",
        type=str,
        default=None,
        help="Path to ItWaC CSV file (auto-detected from --pos if not specified)",
    )
    itwac_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    itwac_parser.add_argument(
        "--pos",
        type=str,
        default="verb",
        choices=["verb", "noun", "adjective"],
        help="Part of speech to import (default: verb)",
    )
    itwac_parser.set_defaults(func=cmd_import_itwac)

    # import-tatoeba subcommand
    tatoeba_parser = subparsers.add_parser(
        "import-tatoeba",
        help="Import Tatoeba sentences and link to verbs",
    )
    tatoeba_parser.add_argument(
        "--ita-sentences",
        type=str,
        default=str(DEFAULT_ITA_SENTENCES_PATH),
        help=f"Path to Italian sentences TSV (default: {DEFAULT_ITA_SENTENCES_PATH})",
    )
    tatoeba_parser.add_argument(
        "--eng-sentences",
        type=str,
        default=str(DEFAULT_ENG_SENTENCES_PATH),
        help=f"Path to English sentences TSV (default: {DEFAULT_ENG_SENTENCES_PATH})",
    )
    tatoeba_parser.add_argument(
        "--links",
        type=str,
        default=str(DEFAULT_LINKS_PATH),
        help=f"Path to Italian-English links TSV (default: {DEFAULT_LINKS_PATH})",
    )
    tatoeba_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    tatoeba_parser.set_defaults(func=cmd_import_tatoeba)

    # download-wiktextract subcommand
    dl_wikt_parser = subparsers.add_parser(
        "download-wiktextract",
        help="Download Wiktextract Italian dictionary",
    )
    dl_wikt_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if file already exists",
    )
    dl_wikt_parser.set_defaults(func=cmd_download_wiktextract)

    # download-morphit subcommand
    dl_morphit_parser = subparsers.add_parser(
        "download-morphit",
        help="Download Morph-it! morphological lexicon",
    )
    dl_morphit_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if file already exists",
    )
    dl_morphit_parser.set_defaults(func=cmd_download_morphit)

    # download-itwac subcommand
    dl_itwac_parser = subparsers.add_parser(
        "download-itwac",
        help="Download ItWaC frequency lists",
    )
    dl_itwac_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if files already exist",
    )
    dl_itwac_parser.set_defaults(func=cmd_download_itwac)

    # download-tatoeba subcommand
    dl_tatoeba_parser = subparsers.add_parser(
        "download-tatoeba",
        help="Download Tatoeba sentences and links",
    )
    dl_tatoeba_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if files already exist",
    )
    dl_tatoeba_parser.set_defaults(func=cmd_download_tatoeba)

    # download-all subcommand
    dl_all_parser = subparsers.add_parser(
        "download-all",
        help="Download all data sources",
    )
    dl_all_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if files already exist",
    )
    dl_all_parser.set_defaults(func=cmd_download_all)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
