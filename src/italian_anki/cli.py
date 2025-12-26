"""Command-line interface for Italian Anki deck generator."""

import argparse
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import Connection

from italian_anki.db import (
    adjective_forms,
    frequencies,
    get_connection,
    get_engine,
    init_db,
    lemmas,
    noun_forms,
    sentences,
    verb_forms,
)
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
from italian_anki.importers.morphit import (
    apply_orthography_fallback,
    apply_unstressed_fallback,
    enrich_lemma_written,
    fill_missing_adjective_forms,
)
from italian_anki.importers.wiktextract import (
    enrich_form_spelling_from_form_of,
    enrich_from_form_of,
    generate_gendered_participles,
    import_adjective_allomorphs,
)

DEFAULT_WIKTEXTRACT_PATH = Path("data/wiktextract/kaikki.org-dictionary-Italian.jsonl")
DEFAULT_MORPHIT_PATH = Path("data/morphit/morph-it.txt")
DEFAULT_ITWAC_DIR = Path("data/itwac")
DEFAULT_ITA_SENTENCES_PATH = Path("data/tatoeba/ita_sentences.tsv")
DEFAULT_ENG_SENTENCES_PATH = Path("data/tatoeba/eng_sentences.tsv")
DEFAULT_LINKS_PATH = Path("data/tatoeba/ita_eng_links.tsv")
DEFAULT_DB_PATH = Path("italian.db")

# Mapping from singular POS values to plural forms for display
POS_PLURAL = {"verb": "verbs", "noun": "nouns", "adjective": "adjectives", "adverb": "adverbs"}


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
    print(f"Filtering to: {POS_PLURAL.get(args.pos, args.pos)}")
    print()

    with get_connection(db_path) as conn:
        _run_wiktextract_import(conn, jsonl_path, args.pos)

    print()
    print("Import complete!")
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
    print(f"Filtering to: {POS_PLURAL.get(args.pos, args.pos)}")
    print()

    with get_connection(db_path) as conn:
        _run_formof_enrichment(conn, jsonl_path, args.pos)

    print()
    print("Enrichment complete!")
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
    print(f"Filtering to: {POS_PLURAL.get(args.pos, args.pos)}")
    print()

    with get_connection(db_path) as conn:
        _run_morphit_import(conn, morphit_path, args.pos)

    print()
    print("Enrichment complete!")
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
    print(f"Filtering to: {POS_PLURAL.get(args.pos, args.pos)}")
    print()

    with get_connection(db_path) as conn:
        _run_itwac_import(conn, csv_path, args.pos)

    print()
    print("Import complete!")
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
        _run_tatoeba_import(conn, ita_path, eng_path, links_path)

    print()
    print("Import complete!")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Print database statistics."""
    from sqlalchemy import func, select

    db_path = Path(args.database)

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        return 1

    with get_connection(db_path) as conn:
        # Lemma counts
        total_lemmas = conn.execute(select(func.count()).select_from(lemmas)).scalar()
        n_verbs = conn.execute(
            select(func.count()).select_from(lemmas).where(lemmas.c.pos == "verb")
        ).scalar()
        n_nouns = conn.execute(
            select(func.count()).select_from(lemmas).where(lemmas.c.pos == "noun")
        ).scalar()
        n_adjectives = conn.execute(
            select(func.count()).select_from(lemmas).where(lemmas.c.pos == "adjective")
        ).scalar()

        # Form counts (separate tables)
        n_verb_forms = conn.execute(select(func.count()).select_from(verb_forms)).scalar() or 0
        n_noun_forms = conn.execute(select(func.count()).select_from(noun_forms)).scalar() or 0
        n_adj_forms = conn.execute(select(func.count()).select_from(adjective_forms)).scalar() or 0
        total_forms = n_verb_forms + n_noun_forms + n_adj_forms

        # Forms with real spelling
        verb_with_spelling = (
            conn.execute(
                select(func.count()).select_from(verb_forms).where(verb_forms.c.written.isnot(None))
            ).scalar()
            or 0
        )
        noun_with_spelling = (
            conn.execute(
                select(func.count()).select_from(noun_forms).where(noun_forms.c.written.isnot(None))
            ).scalar()
            or 0
        )
        adj_with_spelling = (
            conn.execute(
                select(func.count())
                .select_from(adjective_forms)
                .where(adjective_forms.c.written.isnot(None))
            ).scalar()
            or 0
        )
        forms_with_spelling = verb_with_spelling + noun_with_spelling + adj_with_spelling

        # Metadata
        nouns_with_gender = conn.execute(
            select(func.count()).select_from(noun_forms).where(noun_forms.c.gender.isnot(None))
        ).scalar()
        lemmas_with_freq = conn.execute(
            select(func.count(func.distinct(frequencies.c.lemma_id)))
        ).scalar()

        # Sentences
        ita_sentences = conn.execute(
            select(func.count()).select_from(sentences).where(sentences.c.lang == "ita")
        ).scalar()
        eng_sentences = conn.execute(
            select(func.count()).select_from(sentences).where(sentences.c.lang == "eng")
        ).scalar()

    print(f"Database: {db_path}")
    print()
    print("Lemmas:")
    print(f"  Total:      {total_lemmas:,}")
    print(f"  Verbs:      {n_verbs:,}")
    print(f"  Nouns:      {n_nouns:,}")
    print(f"  Adjectives: {n_adjectives:,}")
    print()
    print("Forms:")
    print(f"  Total:         {total_forms:,}")
    print(f"  With spelling: {forms_with_spelling:,}")
    print()
    print("Metadata:")
    print(f"  Noun forms with gender: {nouns_with_gender:,}")
    print(f"  Lemmas with frequency:  {lemmas_with_freq:,}")
    print()
    print("Sentences:")
    print(f"  Italian:     {ita_sentences:,}")
    print(f"  English:     {eng_sentences:,}")

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


def _print_progress(current: int, total: int, desc: str = "Processing") -> None:
    """Print progress in-place using carriage return."""
    if total == 0:
        return
    pct = current * 100 // total
    print(f"\r  {desc}... {pct}% ({current:,} / {total:,})", end="", flush=True)
    if current >= total:
        print()  # newline when done


def _make_progress_callback(desc: str = "Processing"):
    """Create a progress callback for import functions."""

    def callback(current: int, total: int) -> None:
        _print_progress(current, total, desc)

    return callback


# --- Shared import helpers ---
# These encapsulate the import logic + output formatting, used by both
# standalone commands and cmd_import_all.


def _run_wiktextract_import(
    conn: Connection, jsonl_path: Path, pos: str, indent: str = "  "
) -> dict[str, Any]:
    """Run wiktextract import and print stats."""
    stats = import_wiktextract(
        conn, jsonl_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    if stats["cleared"] > 0:
        print(f"{indent}Cleared:     {stats['cleared']:,} existing lemmas")
    print(f"{indent}Lemmas:      {stats['lemmas']:,}")
    print(f"{indent}Forms:       {stats['forms']:,}")
    print(f"{indent}Definitions: {stats['definitions']:,}")
    print(f"{indent}Skipped:     {stats['skipped']:,}")
    if stats.get("nouns_without_gender", 0) > 0:
        print(f"{indent}  No gender: {stats['nouns_without_gender']:,}")
    return stats


def _run_formof_enrichment(
    conn: Connection, jsonl_path: Path, pos: str, indent: str = "  "
) -> dict[str, Any]:
    """Run form-of enrichment and print stats."""
    stats = enrich_from_form_of(
        conn, jsonl_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    print(f"{indent}Form-of entries scanned: {stats['scanned']:,}")
    print(f"{indent}With label tags:         {stats['with_labels']:,}")
    print(f"{indent}Forms updated:           {stats['updated']:,}")
    print(f"{indent}Not found:               {stats['not_found']:,}")
    return stats


def _run_morphit_import(
    conn: Connection, morphit_path: Path, pos: str, indent: str = "  "
) -> dict[str, Any]:
    """Run Morph-it! enrichment and print stats."""
    stats = import_morphit(
        conn, morphit_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    print(f"{indent}Forms updated:    {stats['updated']:,}")
    print(f"{indent}Forms not found:  {stats['not_found']:,}")
    print(f"{indent}Lookup entries:   {stats['lookup_added']:,}")
    return stats


def _run_formof_spelling_enrichment(
    conn: Connection, jsonl_path: Path, pos: str, indent: str = "  "
) -> dict[str, Any]:
    """Run form-of spelling enrichment and print stats."""
    stats = enrich_form_spelling_from_form_of(
        conn, jsonl_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    print(f"{indent}Form-of entries scanned: {stats['scanned']:,}")
    print(f"{indent}Forms updated:           {stats['updated']:,}")
    print(f"{indent}Already had spelling:    {stats['already_filled']:,}")
    print(f"{indent}Not found:               {stats['not_found']:,}")
    return stats


def _run_itwac_import(
    conn: Connection, csv_path: Path, pos: str, indent: str = "  "
) -> dict[str, Any] | None:
    """Run ItWaC frequency import and print stats. Returns None if file doesn't exist."""
    if not csv_path.exists():
        return None
    stats = import_itwac(
        conn, csv_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    print(f"{indent}Lemmas matched:     {stats['matched']:,}")
    print(f"{indent}Lemmas not found:   {stats['not_found']:,}")
    return stats


def _run_tatoeba_import(
    conn: Connection, ita_path: Path, eng_path: Path, links_path: Path, indent: str = "  "
) -> dict[str, Any]:
    """Run Tatoeba import and print stats."""
    stats = import_tatoeba(conn, ita_path, eng_path, links_path)
    print()
    if stats["cleared"] > 0:
        print(f"{indent}Cleared:          {stats['cleared']:,} existing sentences")
    print(f"{indent}Italian sentences: {stats['ita_sentences']:,}")
    print(f"{indent}English sentences: {stats['eng_sentences']:,}")
    print(f"{indent}Translations:      {stats['translations']:,}")
    return stats


def cmd_import_all(args: argparse.Namespace) -> int:
    """Run the full import pipeline for all parts of speech."""
    db_path = Path(args.database)
    jsonl_path = DEFAULT_WIKTEXTRACT_PATH
    morphit_path = DEFAULT_MORPHIT_PATH
    ita_path = DEFAULT_ITA_SENTENCES_PATH
    eng_path = DEFAULT_ENG_SENTENCES_PATH
    links_path = DEFAULT_LINKS_PATH

    # Validate input files exist
    for path, name in [
        (jsonl_path, "Wiktextract JSONL"),
        (morphit_path, "Morph-it!"),
        (ita_path, "Italian sentences"),
        (eng_path, "English sentences"),
        (links_path, "Links"),
    ]:
        if not path.exists():
            print(f"Error: {name} file not found: {path}", file=sys.stderr)
            print("Run 'download-all' first to download data files.", file=sys.stderr)
            return 1

    # Initialize database
    print(f"Initializing database: {db_path}")
    engine = get_engine(db_path)
    init_db(engine)
    print()

    pos_list = ["verb", "noun", "adjective"]
    total_pos = len(pos_list)
    indent = "    "

    # Import each POS
    for pos_idx, pos in enumerate(pos_list, 1):
        pos_plural = POS_PLURAL[pos]
        print("=" * 80)
        print(f"Importing {pos_plural} (Step {pos_idx} of {total_pos})")
        print("=" * 80)
        print()

        # Determine step count:
        # - adjectives: 10 steps (extra: fill-missing, allomorphs)
        # - verbs: 9 steps (extra: gendered participles)
        # - nouns: 8 steps (base)
        # All POS have: wiktextract, form-of, morphit forms, morphit lemmas, form-of spelling,
        #               unstressed fallback, orthography fallback, itwac
        total_steps = 10 if pos == "adjective" else (9 if pos == "verb" else 8)

        with get_connection(db_path) as conn:
            # Step 1: Wiktextract import
            print(f"[1/{total_steps}] Importing from Wiktextract...")
            _run_wiktextract_import(conn, jsonl_path, pos, indent=indent)
            print()

            # Step 2 (verb only): Generate gendered participles
            if pos == "verb":
                print(f"[2/{total_steps}] Generating gendered participle forms...")
                stats = generate_gendered_participles(
                    conn, progress_callback=_make_progress_callback()
                )
                print()
                print(f"{indent}Participles found:     {stats['participles_found']:,}")
                print(f"{indent}Forms generated:       {stats['forms_generated']:,}")
                print(f"{indent}Duplicates skipped:    {stats['duplicates_skipped']:,}")
                print()

            # Step 2 (or 3 for verbs): Form-of enrichment
            step_formof_enrich = 3 if pos == "verb" else 2
            print(f"[{step_formof_enrich}/{total_steps}] Enriching from form-of entries...")
            _run_formof_enrichment(conn, jsonl_path, pos, indent=indent)
            print()

            # Step 3 (or 4 for verbs): Morph-it! form enrichment
            step_morphit = 4 if pos == "verb" else 3
            print(f"[{step_morphit}/{total_steps}] Enriching forms with Morph-it! spelling...")
            _run_morphit_import(conn, morphit_path, pos, indent=indent)
            print()

            # Step 4 (or 5 for verbs): Morph-it! lemma enrichment
            step_morphit_lemma = 5 if pos == "verb" else 4
            print(
                f"[{step_morphit_lemma}/{total_steps}] Enriching lemmas with Morph-it! spelling..."
            )
            stats = enrich_lemma_written(
                conn, morphit_path, pos_filter=pos, progress_callback=_make_progress_callback()
            )
            print()
            print(f"{indent}Lemmas updated:   {stats['updated']:,}")
            print(f"{indent}Exact matched:    {stats['exact_matched']:,}")
            print(f"{indent}Not in Morphit:   {stats['not_found']:,}")
            print()

            # Step 5 (adjective only): Fill missing forms from Morphit
            if pos == "adjective":
                print(f"[5/{total_steps}] Filling missing adjective forms from Morphit...")
                stats = fill_missing_adjective_forms(
                    conn, morphit_path, progress_callback=_make_progress_callback()
                )
                print()
                print(f"{indent}Adjectives checked:   {stats['adjectives_checked']:,}")
                print(f"{indent}Forms added:          {stats['forms_added']:,}")
                print(f"{indent}Completed:            {stats['adjectives_completed']:,}")
                print(f"{indent}Not in Morphit:       {stats['not_in_morphit']:,}")
                print(f"{indent}Elided added:         {stats['elided_added']:,}")
                print(f"{indent}Combos skipped:       {stats['combos_skipped']:,}")
                print()

            # Step 6 (adjective only): Import allomorphs from alt_of entries
            # Must run AFTER fill_missing so allomorphs don't prevent grandi from being added
            if pos == "adjective":
                print(f"[6/{total_steps}] Importing allomorphs (apocopic/elided forms)...")
                stats = import_adjective_allomorphs(
                    conn, jsonl_path, progress_callback=_make_progress_callback()
                )
                print()
                print(f"{indent}Entries scanned:      {stats['scanned']:,}")
                print(f"{indent}Allomorphs found:     {stats['allomorphs_added']:,}")
                print(f"{indent}Forms added:          {stats['forms_added']:,}")
                print(f"{indent}Already in parent:    {stats['already_in_parent']:,}")
                print(f"{indent}Duplicates skipped:   {stats['duplicates_skipped']:,}")
                print(f"{indent}Parent not found:     {stats['parent_not_found']:,}")
                print(f"{indent}Hardcoded added:      {stats['hardcoded_added']:,}")
                print()

            # Step 5 (verb: 6, adjective: 7): Form-of spelling fallback
            step_formof = 7 if pos == "adjective" else (6 if pos == "verb" else 5)
            print(f"[{step_formof}/{total_steps}] Enriching form spelling from form-of entries...")
            _run_formof_spelling_enrichment(conn, jsonl_path, pos, indent=indent)
            print()

            # Step 6 (verb: 7, adjective: 8): Unstressed fallback (for all POS)
            step_unstressed = 8 if pos == "adjective" else (7 if pos == "verb" else 6)
            print(f"[{step_unstressed}/{total_steps}] Applying unstressed form fallback...")
            stats = apply_unstressed_fallback(conn, pos_filter=pos)
            print(f"{indent}Forms updated: {stats['updated']:,}")
            print()

            # Step 7 (verb: 8, adjective: 9): Orthography-based written derivation
            step_ortho = 9 if pos == "adjective" else (8 if pos == "verb" else 7)
            print(f"[{step_ortho}/{total_steps}] Applying orthography-based written derivation...")
            stats = apply_orthography_fallback(conn, pos_filter=pos)
            print(f"{indent}Forms updated: {stats['updated']:,}")
            print(f"{indent}Loanwords:     {stats['loanwords']:,}")
            if stats["failed"] > 0:
                print(f"{indent}Failed:        {stats['failed']:,}")
            print()

            # Step 8 (verb: 9, adjective: 10): ItWaC frequency import
            step_itwac = 10 if pos == "adjective" else (9 if pos == "verb" else 8)
            csv_filename = ITWAC_CSV_FILES.get(pos)
            if csv_filename:
                csv_path = DEFAULT_ITWAC_DIR / csv_filename
                print(f"[{step_itwac}/{total_steps}] Importing ItWaC frequencies...")
                result = _run_itwac_import(conn, csv_path, pos, indent=indent)
                if result is None:
                    print(f"{indent}Skipped: ItWaC file not found")
            else:
                print(f"[{step_itwac}/{total_steps}] Skipped: No ItWaC file for this POS")
            print()

    # Final step: Tatoeba sentences (for all POS)
    print("=" * 80)
    print("Importing Tatoeba sentences")
    print("=" * 80)
    print()
    print("Importing sentences...")

    with get_connection(db_path) as conn:
        _run_tatoeba_import(conn, ita_path, eng_path, links_path, indent="  ")
    print()

    print("=" * 80)
    print("Import pipeline complete!")
    print("=" * 80)

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

    # import-all subcommand
    import_all_parser = subparsers.add_parser(
        "import-all",
        help="Run full import pipeline for all parts of speech",
    )
    import_all_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    import_all_parser.set_defaults(func=cmd_import_all)

    # stats subcommand
    stats_parser = subparsers.add_parser(
        "stats",
        help="Show database statistics",
    )
    stats_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    stats_parser.set_defaults(func=cmd_stats)

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
