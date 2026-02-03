"""Command-line interface for Italian Anki deck generator."""

import argparse
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import Connection

from italian_db.db import (
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
from italian_db.download import (
    download_all,
    download_nvdb,
    download_opensubtitles,
    download_profilo,
    download_tatoeba,
    download_wiktextract,
)
from italian_db.enums import POS
from italian_db.importers import (
    compute_pos_frequency_ranks,
    create_sentence_indexes,
    create_sentence_token_indexes,
    drop_sentence_indexes,
    drop_sentence_token_indexes,
    import_nvdb,
    import_profilo,
    import_sentence_tokens,
    import_tatoeba,
    import_verb_irregularity,
    import_wiktextract,
)
from italian_db.importers.wiktextract import (
    enrich_from_form_of_entries,
    enrich_missing_feminine_plurals,
    generate_gendered_participles,
    import_adjective_allomorphs,
    import_form_ipa,
    import_noun_allomorphs,
)
from italian_db.importers.written_enrichment import (
    apply_orthography_fallback,
    apply_unstressed_fallback,
    enrich_lemma_written,
)
from italian_db.verify import verify_database

DEFAULT_WIKTEXTRACT_PATH = Path("data/wiktextract/kaikki.org-dictionary-Italian.jsonl")
DEFAULT_ITA_SENTENCES_PATH = Path("data/tatoeba/ita_sentences.tsv")
DEFAULT_ENG_SENTENCES_PATH = Path("data/tatoeba/eng_sentences.tsv")
DEFAULT_LINKS_PATH = Path("data/tatoeba/ita_eng_links.tsv")
DEFAULT_TAGS_PATH = Path("data/tatoeba/tags.csv")
DEFAULT_SENTENCES_IN_LISTS_PATH = Path("data/tatoeba/sentences_in_lists.csv")
DEFAULT_TATOEBA_SENTENCE_TOKENS_PATH = Path("data/tatoeba/ita_sentences_pos.jsonl")
DEFAULT_OPENSUBTITLES_SENTENCE_TOKENS_PATH = Path("data/opensubtitles/it_sentences_pos.jsonl")
DEFAULT_OPENSUBTITLES_ITA_PATH = Path("data/opensubtitles/it_sentences.tsv")
DEFAULT_OPENSUBTITLES_ENG_PATH = Path("data/opensubtitles/en_sentences.tsv")
DEFAULT_OPENSUBTITLES_LINKS_PATH = Path("data/opensubtitles/links.tsv")
DEFAULT_PROFILO_DIR = Path("data/profilo")
DEFAULT_NVDB_DIR = Path("data/nvdb")
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
    print(f"Filtering to: {POS(args.pos).plural}")
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
    print(f"Filtering to: {POS(args.pos).plural}")
    print()

    with get_connection(db_path) as conn:
        _run_formof_combined_enrichment(conn, jsonl_path, args.pos)

    print()
    print("Enrichment complete!")
    return 0


def cmd_import_frequencies(args: argparse.Namespace) -> int:
    """Compute frequency data from Stanza sentence tokens."""
    db_path = Path(args.database)

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    print(f"Computing frequencies from sentence tokens: {db_path}")
    print()

    from italian_db.importers.frequency_from_tokens import compute_frequencies_from_tokens

    with get_connection(db_path) as conn:
        stats = compute_frequencies_from_tokens(conn, progress_callback=_make_progress_callback())
        print()
        print(f"  Total tokens counted:   {stats['total_tokens']:,}")
        print(f"  Lemmas matched:         {stats['matched']:,}")
        print(f"  Lemmas not in DB:       {stats['not_found']:,}")
        print()

        # Compute per-POS rankings
        print("Computing per-POS frequency rankings...")
        rank_stats = compute_pos_frequency_ranks(conn, "stanza")
        for pos_name, count in sorted(rank_stats.items()):
            print(f"  {pos_name.capitalize()}: {count:,} ranked")

    print()
    print("Import complete!")
    return 0


def cmd_import_tatoeba(args: argparse.Namespace) -> int:
    """Run the Tatoeba sentences import command."""
    ita_path = Path(args.ita_sentences)
    eng_path = Path(args.eng_sentences)
    links_path = Path(args.links)
    tags_path = Path(args.tags) if args.tags else None
    sentences_in_lists_path = Path(args.sentences_in_lists) if args.sentences_in_lists else None
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
    if tags_path and tags_path.exists():
        print(f"  Tags: {tags_path}")
    if sentences_in_lists_path and sentences_in_lists_path.exists():
        print(f"  CK whitelist: {sentences_in_lists_path}")
    print()

    with get_connection(db_path, bulk=True) as conn:
        print("Dropping sentence indexes for bulk insert...")
        drop_sentence_indexes(conn)
        try:
            _run_tatoeba_import(
                conn,
                ita_path,
                eng_path,
                links_path,
                tags_path=tags_path,
                sentences_in_lists_path=sentences_in_lists_path,
            )
        finally:
            print("Recreating sentence indexes...")
            create_sentence_indexes(conn)

    print()
    print("Import complete!")
    return 0


def cmd_import_opensubtitles_sentences(args: argparse.Namespace) -> int:
    """Run the OpenSubtitles sentences import command."""
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

    print(f"Importing OpenSubtitles sentences to: {db_path}")
    print(f"  Italian sentences: {ita_path}")
    print(f"  English sentences: {eng_path}")
    print(f"  Links: {links_path}")
    print()

    from italian_db.importers.opensubtitles_sentences import import_opensubtitles_sentences

    with get_connection(db_path, bulk=True) as conn:
        print("Dropping sentence indexes for bulk insert...")
        drop_sentence_indexes(conn)
        try:
            stats = import_opensubtitles_sentences(
                conn,
                ita_path,
                eng_path,
                links_path,
                progress_callback=_make_progress_callback(),
            )
            print()
            if stats["cleared"] > 0:
                print(f"  Cleared:           {stats['cleared']:,} existing sentences")
            print(f"  Italian sentences: {stats['ita_sentences']:,}")
            print(f"  English sentences: {stats['eng_sentences']:,}")
            print(f"  Translations:      {stats['translations']:,}")
        finally:
            print("Recreating sentence indexes...")
            create_sentence_indexes(conn)

    print()
    print("Import complete!")
    return 0


def cmd_import_verb_irregularity(args: argparse.Namespace) -> int:
    """Run the verb irregularity pattern import command."""
    db_path = Path(args.database)

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    # Ensure verb_irregularity table exists (may be added after initial schema)
    engine = get_engine(db_path)
    init_db(engine)

    print(f"Importing verb irregularity patterns to: {db_path}")
    print()

    with get_connection(db_path) as conn:
        stats = import_verb_irregularity(conn, progress_callback=_make_progress_callback())
        print()
        print(f"  Total classifications:  {stats.total:,}")
        print(f"  Matched:                {stats.matched:,}")
        print(f"  Not found:              {stats.not_found:,}")
        if stats.not_found > 0:
            if len(stats.not_found_list) <= 10:
                print(f"    Missing verbs: {', '.join(stats.not_found_list)}")
            else:
                print(f"    First 10 missing: {', '.join(stats.not_found_list[:10])}")

    print()
    print("Import complete!")
    return 0


def cmd_import_sentence_tokens(args: argparse.Namespace) -> int:
    """Run the sentence tokens import command."""
    db_path = Path(args.database)
    source = args.source

    # Determine JSONL path based on source
    if args.jsonl:
        jsonl_path = Path(args.jsonl)
    elif source == "opensubtitles":
        jsonl_path = DEFAULT_OPENSUBTITLES_SENTENCE_TOKENS_PATH
    else:
        jsonl_path = DEFAULT_TATOEBA_SENTENCE_TOKENS_PATH

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    if not jsonl_path.exists():
        print(f"Error: JSONL file not found: {jsonl_path}", file=sys.stderr)
        print("Run 'task stanza-pos-tag' first to generate POS-tagged tokens.", file=sys.stderr)
        return 1

    # Ensure sentence_tokens table exists
    engine = get_engine(db_path)
    init_db(engine)

    print(f"Importing sentence tokens to: {db_path}")
    print(f"  From: {jsonl_path}")
    print(f"  Source: {source}")
    print()

    with get_connection(db_path, bulk=True) as conn:
        print("\r  Dropping indexes for bulk insert...", end="", flush=True)
        drop_sentence_token_indexes(conn)
        try:
            stats = import_sentence_tokens(
                conn,
                jsonl_path,
                source=source,
                progress_callback=_make_progress_callback(),
                status_callback=_make_status_callback(),
            )
            print()
            print(f"  Sentences processed:  {stats.sentences_processed:,}")
            print(f"  Tokens inserted:      {stats.tokens_inserted:,}")
            if stats.sentences_not_found > 0:
                print(f"  Sentences not found:  {stats.sentences_not_found:,}")
        finally:
            print("  Recreating indexes...")
            create_sentence_token_indexes(conn)

    print()
    print("Import complete!")
    return 0


def cmd_import_profilo(args: argparse.Namespace) -> int:
    """Import Profilo CEFR level data."""
    profilo_dir = Path(args.profilo_dir)
    db_path = Path(args.database)

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    # Check that at least one HTML file exists
    if not any(profilo_dir.glob("liste_lessicali_*.html")):
        print(f"Error: No Profilo HTML files found in: {profilo_dir}", file=sys.stderr)
        print("Run 'download-profilo' first to download the data.", file=sys.stderr)
        return 1

    # Ensure cefr_levels table exists
    engine = get_engine(db_path)
    init_db(engine)

    print(f"Importing Profilo CEFR levels to: {db_path}")
    print(f"  From: {profilo_dir}")
    print()

    with get_connection(db_path) as conn:
        _run_profilo_import(conn, profilo_dir)

    print()
    print("Import complete!")
    return 0


def cmd_download_profilo(args: argparse.Namespace) -> int:
    """Download Profilo della lingua italiana CEFR word lists."""
    stats = download_profilo(force=args.force)
    if stats["downloaded"] > 0:
        print("Download complete!")
    return 0


def cmd_import_nvdb(args: argparse.Namespace) -> int:
    """Import NVdB usage tier data."""
    nvdb_dir = Path(args.nvdb_dir)
    db_path = Path(args.database)

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("Run 'import-wiktextract' first to create the database.", file=sys.stderr)
        return 1

    nvdb_path = nvdb_dir / "nvdb.html"
    if not nvdb_path.exists():
        print(f"Error: NVdB HTML file not found: {nvdb_path}", file=sys.stderr)
        print("Run 'download-nvdb' first to download the data.", file=sys.stderr)
        return 1

    # Ensure nvdb_tiers table exists
    engine = get_engine(db_path)
    init_db(engine)

    print(f"Importing NVdB usage tiers to: {db_path}")
    print(f"  From: {nvdb_path}")
    print()

    with get_connection(db_path) as conn:
        _run_nvdb_import(conn, nvdb_path)

    print()
    print("Import complete!")
    return 0


def cmd_download_nvdb(args: argparse.Namespace) -> int:
    """Download NVdB vocabulary list."""
    stats = download_nvdb(force=args.force)
    if stats["downloaded"] > 0:
        print("Download complete!")
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
            select(func.count()).select_from(lemmas).where(lemmas.c.pos == POS.VERB)
        ).scalar()
        n_nouns = conn.execute(
            select(func.count()).select_from(lemmas).where(lemmas.c.pos == POS.NOUN)
        ).scalar()
        n_adjectives = conn.execute(
            select(func.count()).select_from(lemmas).where(lemmas.c.pos == POS.ADJECTIVE)
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

        # Sentences by source
        tatoeba_ita = conn.execute(
            select(func.count())
            .select_from(sentences)
            .where(sentences.c.lang == "ita", sentences.c.source == "tatoeba")
        ).scalar()
        tatoeba_eng = conn.execute(
            select(func.count())
            .select_from(sentences)
            .where(sentences.c.lang == "eng", sentences.c.source == "tatoeba")
        ).scalar()

        # OpenSubtitles sentences
        opensub_ita = conn.execute(
            select(func.count())
            .select_from(sentences)
            .where(sentences.c.lang == "ita", sentences.c.source == "opensubtitles")
        ).scalar()
        opensub_eng = conn.execute(
            select(func.count())
            .select_from(sentences)
            .where(sentences.c.lang == "eng", sentences.c.source == "opensubtitles")
        ).scalar()

        # Sentence tags count (Tatoeba)
        from italian_db.db.schema import sentence_tags, sentence_tokens

        tag_count = conn.execute(select(func.count()).select_from(sentence_tags)).scalar() or 0
        unique_tags = (
            conn.execute(select(func.count(func.distinct(sentence_tags.c.tag)))).scalar() or 0
        )

        # Sentence tokens count (Stanza POS tagging)
        token_count = conn.execute(select(func.count()).select_from(sentence_tokens)).scalar() or 0
        sentences_with_tokens = (
            conn.execute(select(func.count(func.distinct(sentence_tokens.c.sentence_id)))).scalar()
            or 0
        )

        # IPA statistics
        verb_with_ipa = (
            conn.execute(
                select(func.count()).select_from(verb_forms).where(verb_forms.c.ipa.isnot(None))
            ).scalar()
            or 0
        )
        noun_with_ipa = (
            conn.execute(
                select(func.count()).select_from(noun_forms).where(noun_forms.c.ipa.isnot(None))
            ).scalar()
            or 0
        )
        adj_with_ipa = (
            conn.execute(
                select(func.count())
                .select_from(adjective_forms)
                .where(adjective_forms.c.ipa.isnot(None))
            ).scalar()
            or 0
        )

        # CEFR level statistics
        from italian_db.db.schema import cefr_levels, nvdb_tiers

        cefr_total = conn.execute(select(func.count()).select_from(cefr_levels)).scalar() or 0
        cefr_by_level: dict[str, int] = {}
        if cefr_total > 0:
            for level_val in ["A1", "A2", "B1", "B2"]:
                cnt = (
                    conn.execute(
                        select(func.count())
                        .select_from(cefr_levels)
                        .where(cefr_levels.c.level == level_val)
                    ).scalar()
                    or 0
                )
                cefr_by_level[level_val] = cnt

        # NVdB tier statistics
        nvdb_total = conn.execute(select(func.count()).select_from(nvdb_tiers)).scalar() or 0
        nvdb_by_tier: dict[str, int] = {}
        if nvdb_total > 0:
            for tier_val in ["FO", "AU", "AD"]:
                cnt = (
                    conn.execute(
                        select(func.count())
                        .select_from(nvdb_tiers)
                        .where(nvdb_tiers.c.tier == tier_val)
                    ).scalar()
                    or 0
                )
                nvdb_by_tier[tier_val] = cnt

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
    print("IPA Coverage:")
    verb_pct = (verb_with_ipa / n_verb_forms * 100) if n_verb_forms else 0
    noun_pct = (noun_with_ipa / n_noun_forms * 100) if n_noun_forms else 0
    adj_pct = (adj_with_ipa / n_adj_forms * 100) if n_adj_forms else 0
    print(f"  Verb forms:      {verb_with_ipa:,} ({verb_pct:.1f}%)")
    print(f"  Noun forms:      {noun_with_ipa:,} ({noun_pct:.1f}%)")
    print(f"  Adjective forms: {adj_with_ipa:,} ({adj_pct:.1f}%)")
    print()
    print("Metadata:")
    print(f"  Noun forms with gender: {nouns_with_gender:,}")
    print(f"  Lemmas with frequency:  {lemmas_with_freq:,}")
    print()
    print("Sentences (Tatoeba):")
    print(f"  Italian:     {tatoeba_ita:,}")
    print(f"  English:     {tatoeba_eng:,}")
    if tag_count > 0:
        print(f"  Tags:        {tag_count:,} ({unique_tags:,} unique)")
    if opensub_ita:
        print()
        print("Sentences (OpenSubtitles):")
        print(f"  Italian:     {opensub_ita:,}")
        print(f"  English:     {opensub_eng:,}")
    if token_count > 0:
        print()
        print("Sentence Tokens (Stanza):")
        print(f"  Tokens:      {token_count:,} ({sentences_with_tokens:,} sentences)")
    if cefr_total > 0:
        print()
        print("CEFR Levels (Profilo):")
        print(f"  Total:       {cefr_total:,}")
        for level_val, cnt in cefr_by_level.items():
            print(f"  {level_val}:          {cnt:,}")
    if nvdb_total > 0:
        print()
        print("NVdB Usage Tiers:")
        print(f"  Total:       {nvdb_total:,}")
        for tier_val, cnt in nvdb_by_tier.items():
            print(f"  {tier_val}:          {cnt:,}")

    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Verify database integrity and consistency."""
    db_path = Path(args.database)

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"Database Verification: {db_path}")

    with get_connection(db_path) as conn:
        report = verify_database(conn, verbose=args.verbose)

    print(report.summary(verbose=args.verbose))

    return 0 if report.all_passed else 1


def cmd_download_wiktextract(args: argparse.Namespace) -> int:
    """Download Wiktextract Italian dictionary."""
    stats = download_wiktextract(force=args.force)
    if stats["downloaded"] > 0:
        print("Download complete!")
    return 0


def cmd_download_tatoeba(args: argparse.Namespace) -> int:
    """Download Tatoeba sentences and links."""
    stats = download_tatoeba(force=args.force)
    print(f"Downloaded: {stats['downloaded']} files, Skipped: {stats['skipped']} files")
    return 0


def cmd_download_opensubtitles(args: argparse.Namespace) -> int:
    """Download OpenSubtitles parallel sentences from OPUS."""
    stats = download_opensubtitles(force=args.force)
    if stats["downloaded"] > 0:
        print("Download complete!")
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


def _make_status_callback():
    """Create a status callback that prints phase messages with carriage return."""

    def callback(message: str) -> None:
        print(f"\r  {message}\033[K", end="", flush=True)

    return callback


# --- Shared import helpers ---
# These encapsulate the import logic + output formatting, used by both
# standalone commands and cmd_import_all.


def _run_wiktextract_import(
    conn: Connection, jsonl_path: Path, pos: POS, indent: str = "  "
) -> dict[str, Any]:
    """Run wiktextract import and print stats."""
    stats = import_wiktextract(
        conn, jsonl_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    if stats["cleared"] > 0:
        print(f"{indent}Cleared:          {stats['cleared']:,} existing lemmas")
    print(f"{indent}Lemmas:           {stats['lemmas']:,}")
    print(f"{indent}Forms:            {stats['forms']:,}")
    print(f"{indent}Definitions:      {stats['definitions']:,}")
    print(f"{indent}Etymology text:   {stats['etymology_has_text']:,}")
    print(f"{indent}Multi-etymology:  {stats['etymology_has_number']:,}")
    if pos == POS.VERB:
        print(f"{indent}Stress synced:    {stats.get('lemma_stress_synced', 0):,}")
    print(f"{indent}Skipped:          {stats['skipped']:,}")
    # Show skip reason breakdown (only non-zero counts)
    if stats.get("blocklisted_lemmas", 0) > 0:
        print(f"{indent}  Blocklisted:        {stats['blocklisted_lemmas']:,}")
    if stats.get("misspellings_skipped", 0) > 0:
        print(f"{indent}  Misspellings:       {stats['misspellings_skipped']:,}")
    if stats.get("alt_forms_skipped", 0) > 0:
        print(f"{indent}  Alt-forms:          {stats['alt_forms_skipped']:,}")
    if stats.get("skipped_plural_duplicate", 0) > 0:
        print(f"{indent}  Duplicate plurals:  {stats['skipped_plural_duplicate']:,}")
    if stats.get("nouns_skipped_no_gender", 0) > 0:
        print(f"{indent}  No gender:          {stats['nouns_skipped_no_gender']:,}")
    if stats.get("counterpart_wrong_gender", 0) > 0:
        print(f"{indent}  Wrong gender:       {stats['counterpart_wrong_gender']:,}")

    # Print relationship statistics (from post-processing in import_wiktextract)
    _print_relationship_stats(stats, pos, indent)

    return stats


def _print_relationship_stats(stats: dict[str, Any], pos: POS, indent: str = "  ") -> None:
    """Print relationship statistics from wiktextract import."""
    has_any = False

    # Verb: pronominal linking (reflexive verbs)
    if pos == POS.VERB and stats.get("pronominal_verbs", 0) > 0:
        if not has_any:
            print(f"{indent}Relationships:")
            has_any = True
        print(f"{indent}  Pronominal verbs:   {stats['pronominal_verbs']:,}")
        print(f"{indent}    Linked (reflexive_of):  {stats.get('pronominal_linked', 0):,}")
        print(f"{indent}    Inherent (no base):     {stats.get('pronominal_inherent', 0):,}")

    # Noun: counterpart pairs (gender relationships)
    if pos == POS.NOUN and stats.get("counterparts_found", 0) > 0:
        if not has_any:
            print(f"{indent}Relationships:")
            has_any = True
        print(f"{indent}  Counterpart pairs:  {stats['counterparts_found']:,}")
        bi = stats.get("counterparts_linked_bidirectional", 0)
        uni = stats.get("counterparts_linked_unidirectional", 0)
        print(f"{indent}    Linked (bidirectional):   {bi:,}")
        print(f"{indent}    Linked (unidirectional):  {uni:,}")
        if stats.get("counterparts_base_not_found", 0) > 0:
            print(f"{indent}    Base not found:           {stats['counterparts_base_not_found']:,}")

    # Noun: derivation linking (diminutives, augmentatives, etc.)
    if pos == POS.NOUN and stats.get("derivations_found", 0) > 0:
        if not has_any:
            print(f"{indent}Relationships:")
            has_any = True
        print(f"{indent}  Noun derivations:   {stats['derivations_found']:,}")
        print(f"{indent}    Linked:                   {stats.get('derivations_linked', 0):,}")
        dim = stats.get("derivations_diminutive", 0)
        aug = stats.get("derivations_augmentative", 0)
        pej = stats.get("derivations_pejorative", 0)
        if dim > 0:
            print(f"{indent}      Diminutive:   {dim:,}")
        if aug > 0:
            print(f"{indent}      Augmentative: {aug:,}")
        if pej > 0:
            print(f"{indent}      Pejorative:   {pej:,}")
        if stats.get("derivations_base_not_found", 0) > 0:
            print(f"{indent}    Base not found:           {stats['derivations_base_not_found']:,}")

    # Adjective: degree linking (comparatives, superlatives)
    if pos == POS.ADJECTIVE and stats.get("degree_linked", 0) > 0:
        if not has_any:
            print(f"{indent}Relationships:")
            has_any = True
        print(f"{indent}  Degree relationships:  {stats['degree_linked']:,}")
        if stats.get("degree_base_not_found", 0) > 0:
            print(f"{indent}    Base not found:      {stats['degree_base_not_found']:,}")

    # All POS: definition-level derivations (from form_of senses)
    if stats.get("definition_derivations_found", 0) > 0:
        if not has_any:
            print(f"{indent}Relationships:")
            has_any = True
        print(f"{indent}  Definition derivations: {stats['definition_derivations_found']:,}")
        print(
            f"{indent}    Linked:               {stats.get('definition_derivations_linked', 0):,}"
        )
        if stats.get("definition_derivations_target_not_found", 0) > 0:
            print(
                f"{indent}    Target not found:     "
                f"{stats['definition_derivations_target_not_found']:,}"
            )


def _run_formof_combined_enrichment(
    conn: Connection, jsonl_path: Path, pos: POS, indent: str = "  "
) -> dict[str, Any]:
    """Run combined form-of enrichment (labels + spelling) and print stats."""
    stats = enrich_from_form_of_entries(
        conn, jsonl_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    print(f"{indent}Form-of entries scanned: {stats['scanned']:,}")
    print(f"{indent}Labels:")
    print(f"{indent}  With tags:     {stats['labels_with_tags']:,}")
    print(f"{indent}  Updated:       {stats['labels_updated']:,}")
    print(f"{indent}  Not found:     {stats['labels_not_found']:,}")
    print(f"{indent}Spelling:")
    print(f"{indent}  Updated:       {stats['spelling_updated']:,}")
    print(f"{indent}  Already set:   {stats['spelling_already_filled']:,}")
    print(f"{indent}  Not found:     {stats['spelling_not_found']:,}")
    return stats


def _run_tatoeba_import(
    conn: Connection,
    ita_path: Path,
    eng_path: Path,
    links_path: Path,
    *,
    tags_path: Path | None = None,
    sentences_in_lists_path: Path | None = None,
    indent: str = "  ",
) -> dict[str, Any]:
    """Run Tatoeba import and print stats."""
    stats = import_tatoeba(
        conn,
        ita_path,
        eng_path,
        links_path,
        tags_path=tags_path,
        sentences_in_lists_path=sentences_in_lists_path,
        progress_callback=_make_progress_callback(),
    )
    print()
    if stats["cleared"] > 0:
        print(f"{indent}Cleared:           {stats['cleared']:,} existing sentences")
    if stats.get("ck_whitelist_size", 0) > 0:
        print(f"{indent}CK whitelist:      {stats['ck_whitelist_size']:,} English sentences")
    print(f"{indent}Italian sentences: {stats['ita_sentences']:,}")
    print(f"{indent}English sentences: {stats['eng_sentences']:,}")
    print(f"{indent}Translations:      {stats['translations']:,}")
    if stats.get("tags", 0) > 0:
        print(f"{indent}Tags:              {stats['tags']:,}")
    return stats


def _run_profilo_import(conn: Connection, profilo_dir: Path, indent: str = "  ") -> dict[str, Any]:
    """Run Profilo CEFR import and print stats."""
    stats = import_profilo(conn, profilo_dir, progress_callback=_make_progress_callback())
    print()
    if stats["cleared"] > 0:
        print(f"{indent}Cleared:            {stats['cleared']:,} existing CEFR rows")
    print(f"{indent}Total entries:      {stats['total_entries']:,}")
    print(f"{indent}Matched:            {stats['matched']:,}")
    print(f"{indent}Unmatched:          {stats['unmatched']:,}")
    print(f"{indent}Skipped (multiword):{stats['skipped_multiword']:,}")
    print(f"{indent}Skipped (POS):      {stats['skipped_pos']:,}")
    print(f"{indent}Per level:")
    for level in ["A1", "A2", "B1", "B2"]:
        count = stats[f"level_{level}"]
        print(f"{indent}  {level}: {count:,}")
    return stats


def _run_nvdb_import(conn: Connection, nvdb_path: Path, indent: str = "  ") -> dict[str, Any]:
    """Run NVdB import and print stats."""
    stats = import_nvdb(conn, nvdb_path, progress_callback=_make_progress_callback())
    print()
    if stats["cleared"] > 0:
        print(f"{indent}Cleared:            {stats['cleared']:,} existing NVdB rows")
    print(f"{indent}Total entries:      {stats['total_entries']:,}")
    print(f"{indent}Matched:            {stats['matched']:,}")
    print(f"{indent}Unmatched:          {stats['unmatched']:,}")
    print(f"{indent}Skipped (multiword):{stats['skipped_multiword']:,}")
    print(f"{indent}Skipped (POS):      {stats['skipped_pos']:,}")
    print(f"{indent}Per tier:")
    for tier in ["FO", "AU", "AD"]:
        count = stats[f"tier_{tier}"]
        print(f"{indent}  {tier}: {count:,}")
    return stats


def _run_verb_irregularity_import(conn: Connection, indent: str = "  ") -> dict[str, Any]:
    """Run verb irregularity import and print stats."""
    stats = import_verb_irregularity(conn, progress_callback=_make_progress_callback())
    print()
    print(f"{indent}Total classifications:  {stats.total:,}")
    print(f"{indent}Matched:                {stats.matched:,}")
    print(f"{indent}Not found:              {stats.not_found:,}")
    return {"total": stats.total, "matched": stats.matched, "not_found": stats.not_found}


def _run_ipa_import(
    conn: Connection, jsonl_path: Path, pos: POS, indent: str = "  "
) -> dict[str, Any]:
    """Run IPA import and print stats."""
    stats = import_form_ipa(
        conn, jsonl_path, pos_filter=pos, progress_callback=_make_progress_callback()
    )
    print()
    print(f"{indent}Entries scanned:    {stats['entries_scanned']:,}")
    print(f"{indent}Entries with IPA:   {stats['entries_with_ipa']:,}")
    print(f"{indent}Lemma IPA updated:  {stats['lemma_ipa_updated']:,}")
    print(f"{indent}Form IPA updated:   {stats['form_ipa_updated']:,}")
    if stats["lemma_not_found"] > 0:
        print(f"{indent}Lemma not found:    {stats['lemma_not_found']:,}")
    if stats["form_not_found"] > 0:
        print(f"{indent}Form not found:     {stats['form_not_found']:,}")
    return stats


def _run_sentence_tokens_import(
    conn: Connection, jsonl_path: Path, *, source: str, indent: str = "  "
) -> dict[str, Any]:
    """Run sentence tokens import and print stats."""
    stats = import_sentence_tokens(
        conn,
        jsonl_path,
        source=source,
        progress_callback=_make_progress_callback(),
        status_callback=_make_status_callback(),
    )
    print()
    print(f"{indent}Sentences processed:  {stats.sentences_processed:,}")
    print(f"{indent}Tokens inserted:      {stats.tokens_inserted:,}")
    if stats.sentences_not_found > 0:
        print(f"{indent}Sentences not found:  {stats.sentences_not_found:,}")
    return {
        "sentences_processed": stats.sentences_processed,
        "tokens_inserted": stats.tokens_inserted,
        "sentences_not_found": stats.sentences_not_found,
    }


def cmd_import_all(args: argparse.Namespace) -> int:
    """Run the full import pipeline for all parts of speech."""
    db_path = Path(args.database)
    jsonl_path = DEFAULT_WIKTEXTRACT_PATH
    ita_path = DEFAULT_ITA_SENTENCES_PATH
    eng_path = DEFAULT_ENG_SENTENCES_PATH
    links_path = DEFAULT_LINKS_PATH

    # Validate input files exist
    for path, name in [
        (jsonl_path, "Wiktextract JSONL"),
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

    pos_list = list(POS)
    # Determine total phases:
    # 3 POS + post-processing + Tatoeba + OpenSubtitles(optional) + sentence tokens + frequency
    has_opensub = DEFAULT_OPENSUBTITLES_ITA_PATH.exists()
    has_profilo = any(DEFAULT_PROFILO_DIR.glob("liste_lessicali_*.html"))
    has_nvdb = (DEFAULT_NVDB_DIR / "nvdb.html").exists()
    has_tatoeba_tokens = DEFAULT_TATOEBA_SENTENCE_TOKENS_PATH.exists()
    has_opensub_tokens = DEFAULT_OPENSUBTITLES_SENTENCE_TOKENS_PATH.exists()
    has_any_tokens = has_tatoeba_tokens or has_opensub_tokens

    # Count phases: 3 POS + post-processing + [Profilo] + [NVdB] + Tatoeba + [OpenSubtitles] + [tokens] + [frequencies]
    total_phases = 3 + 1 + 1  # POS + post-processing + Tatoeba
    if has_profilo:
        total_phases += 1
    if has_nvdb:
        total_phases += 1
    if has_opensub:
        total_phases += 1
    if has_any_tokens:
        total_phases += 1
    if has_any_tokens:
        total_phases += 1  # frequency computation (needs tokens)

    indent = "    "
    current_phase = 0

    # Import each POS
    for pos_idx, pos in enumerate(pos_list, 1):
        current_phase = pos_idx
        pos_plural = pos.plural
        print("=" * 80)
        print(f"Importing {pos_plural} (Step {current_phase} of {total_phases})")
        print("=" * 80)
        print()

        # Determine step count:
        # - adjectives: 7 steps (wiktextract, form-of, lemma-written, allomorphs,
        #                        unstressed, orthography, ipa)
        # - nouns: 7 steps (wiktextract, form-of, lemma-written, allomorphs,
        #                   unstressed, orthography, ipa)
        # - verbs: 5 steps (wiktextract, participles, lemma-written, form-of,
        #                   verb-irregularity, ipa)
        if pos == POS.ADJECTIVE:
            total_steps = 7
        elif pos == POS.VERB:
            total_steps = 6
        else:
            total_steps = 7

        with get_connection(db_path, bulk=True) as conn:
            # Step 1: Wiktextract import
            print(f"[1/{total_steps}] Importing from Wiktextract...")
            _run_wiktextract_import(conn, jsonl_path, pos, indent=indent)
            print()

            # Step 2 (verb only): Generate gendered participles
            if pos == POS.VERB:
                print(f"[2/{total_steps}] Generating gendered participle forms...")
                stats = generate_gendered_participles(
                    conn, progress_callback=_make_progress_callback()
                )
                print()
                print(f"{indent}Participles found:     {stats['participles_found']:,}")
                print(f"{indent}Forms generated:       {stats['forms_generated']:,}")
                print(f"{indent}Duplicates skipped:    {stats['duplicates_skipped']:,}")
                print()

            # Step 2 (noun/adjective only): Form-of enrichment (labels + spelling)
            if pos != POS.VERB:
                print(f"[2/{total_steps}] Enriching from form-of entries...")
                _run_formof_combined_enrichment(conn, jsonl_path, pos, indent=indent)
                print()

            # Step 3 (verb/noun/adjective): Lemma written enrichment (from citation forms)
            step_lemma_written = 3
            print(f"[{step_lemma_written}/{total_steps}] Enriching lemmas with written spelling...")
            stats = enrich_lemma_written(
                conn, pos_filter=pos, progress_callback=_make_progress_callback()
            )
            print()
            print(f"{indent}Lemmas updated:   {stats['updated']:,}")
            print(f"{indent}From citation:    {stats['from_form']:,}")
            print(f"{indent}Derived:          {stats['derived']:,}")
            print(f"{indent}No citation form: {stats['no_citation_form']:,}")
            print()

            # Step 4 (noun only): Import noun allomorphs from alt_of entries
            if pos == POS.NOUN:
                print(f"[4/{total_steps}] Importing allomorphs (apocopic forms)...")
                stats = import_noun_allomorphs(
                    conn, jsonl_path, progress_callback=_make_progress_callback()
                )
                print()
                print(f"{indent}Entries scanned:      {stats['scanned']:,}")
                print(f"{indent}Allomorphs found:     {stats['allomorphs_added']:,}")
                print(f"{indent}Forms added:          {stats['forms_added']:,}")
                print(f"{indent}Already in parent:    {stats['already_in_parent']:,}")
                print(f"{indent}Parent not found:     {stats['parent_not_found']:,}")
                print(f"{indent}Hardcoded added:      {stats['hardcoded_added']:,}")
                print()

            # Step 4 (adjective only): Import allomorphs from alt_of entries
            if pos == POS.ADJECTIVE:
                print(f"[4/{total_steps}] Importing allomorphs (apocopic/elided forms)...")
                stats = import_adjective_allomorphs(
                    conn, jsonl_path, progress_callback=_make_progress_callback()
                )
                print()
                print(f"{indent}Entries scanned:      {stats['scanned']:,}")
                print(f"{indent}Alt_of filtered:      {stats['alt_of_filtered']:,}")
                print(f"{indent}Allomorphs found:     {stats['allomorphs_added']:,}")
                print(f"{indent}Forms added:          {stats['forms_added']:,}")
                print(f"{indent}Already in parent:    {stats['already_in_parent']:,}")
                print(f"{indent}Duplicates skipped:   {stats['duplicates_skipped']:,}")
                print(f"{indent}Parent not found:     {stats['parent_not_found']:,}")
                print(f"{indent}Hardcoded added:      {stats['hardcoded_added']:,}")
                print()

            # Form-of enrichment for verbs only at step 4
            if pos == POS.VERB:
                print(f"[4/{total_steps}] Enriching from form-of entries...")
                _run_formof_combined_enrichment(conn, jsonl_path, pos, indent=indent)
                print()

            # Unstressed fallback (noun/adjective only)
            if pos != POS.VERB:
                step_unstressed = 5
                print(f"[{step_unstressed}/{total_steps}] Applying unstressed form fallback...")
                stats = apply_unstressed_fallback(conn, pos_filter=pos)
                print(f"{indent}Forms updated: {stats['updated']:,}")
                print()

            # Orthography-based written derivation (noun/adjective only)
            if pos != POS.VERB:
                step_ortho = 6
                print(
                    f"[{step_ortho}/{total_steps}] Applying orthography-based written derivation..."
                )
                stats = apply_orthography_fallback(conn, pos_filter=pos)
                print(f"{indent}Forms updated: {stats['updated']:,}")
                print(f"{indent}Loanwords:     {stats['loanwords']:,}")
                if stats["failed"] > 0:
                    print(f"{indent}Failed:        {stats['failed']:,}")
                print()

            # Step 5 (verb only): Import verb irregularity patterns
            if pos == POS.VERB:
                print(f"[5/{total_steps}] Importing verb irregularity patterns...")
                _run_verb_irregularity_import(conn, indent=indent)
                print()

            # IPA import (final step for each POS)
            step_ipa = 6 if pos == POS.VERB else 7
            print(f"[{step_ipa}/{total_steps}] Importing IPA pronunciations...")
            _run_ipa_import(conn, jsonl_path, pos, indent=indent)
            print()

    # Post-processing: Cross-POS enrichments
    current_phase += 1
    print("=" * 80)
    print(f"Post-processing enrichments (Step {current_phase} of {total_phases})")
    print("=" * 80)
    print()

    with get_connection(db_path) as conn:
        # Synthesize missing feminine plurals for CGV nouns
        print("Synthesizing missing feminine plural forms...")
        stats = enrich_missing_feminine_plurals(conn, progress_callback=_make_progress_callback())
        print()
        print(f"  f.sg forms processed:  {stats['total_f_sg']:,}")
        print(f"  Synthesized:           {stats['synthesized']:,}")
        print(f"  Added (invariable):    {stats['added_invariable']:,}")
        print(f"  Skipped (exists):      {stats['skipped_already_exists']:,}")
        print(f"  Skipped (blocklisted): {stats['skipped_blocklisted']:,}")
        print(f"  Skipped (multi-word):  {stats['skipped_multiword']:,}")
        print(f"  Skipped (typo):        {stats['skipped_typo']:,}")
    print()

    # Profilo CEFR levels (optional - only if HTML files exist)
    if has_profilo:
        current_phase += 1
        print("=" * 80)
        print(f"Importing Profilo CEFR levels (Step {current_phase} of {total_phases})")
        print("=" * 80)
        print()

        with get_connection(db_path) as conn:
            _run_profilo_import(conn, DEFAULT_PROFILO_DIR, indent="  ")
        print()

    # NVdB usage tiers (optional - only if HTML file exists)
    if has_nvdb:
        current_phase += 1
        print("=" * 80)
        print(f"Importing NVdB usage tiers (Step {current_phase} of {total_phases})")
        print("=" * 80)
        print()

        with get_connection(db_path) as conn:
            _run_nvdb_import(conn, DEFAULT_NVDB_DIR / "nvdb.html", indent="  ")
        print()

    # Tatoeba sentences
    current_phase += 1
    print("=" * 80)
    print(f"Importing Tatoeba sentences (Step {current_phase} of {total_phases})")
    print("=" * 80)
    print()

    # Check for optional files
    tags_path = DEFAULT_TAGS_PATH if DEFAULT_TAGS_PATH.exists() else None
    sentences_in_lists_path = (
        DEFAULT_SENTENCES_IN_LISTS_PATH if DEFAULT_SENTENCES_IN_LISTS_PATH.exists() else None
    )

    if sentences_in_lists_path:
        print("Using CK whitelist filtering (List 907)")
    if tags_path:
        print("Importing sentence tags for tense matching")

    with get_connection(db_path, bulk=True) as conn:
        print("Dropping sentence indexes for bulk insert...")
        drop_sentence_indexes(conn)
        try:
            print("Importing sentences...")
            _run_tatoeba_import(
                conn,
                ita_path,
                eng_path,
                links_path,
                tags_path=tags_path,
                sentences_in_lists_path=sentences_in_lists_path,
                indent="  ",
            )
            print()

            # OpenSubtitles sentences (optional - only if download files exist)
            if has_opensub:
                current_phase += 1
                print("=" * 80)
                print(f"Importing OpenSubtitles sentences (Step {current_phase} of {total_phases})")
                print("=" * 80)
                print()

                from italian_db.importers.opensubtitles_sentences import (
                    import_opensubtitles_sentences,
                )

                stats = import_opensubtitles_sentences(
                    conn,
                    DEFAULT_OPENSUBTITLES_ITA_PATH,
                    DEFAULT_OPENSUBTITLES_ENG_PATH,
                    DEFAULT_OPENSUBTITLES_LINKS_PATH,
                    progress_callback=_make_progress_callback(),
                )
                print()
                if stats["cleared"] > 0:
                    print(f"  Cleared:           {stats['cleared']:,} existing sentences")
                print(f"  Italian sentences: {stats['ita_sentences']:,}")
                print(f"  English sentences: {stats['eng_sentences']:,}")
                print(f"  Translations:      {stats['translations']:,}")
                print()
        finally:
            print("Recreating sentence indexes...")
            create_sentence_indexes(conn)
            print()

    # Sentence tokens (optional - only if JSONL exists for either source)
    if has_any_tokens:
        current_phase += 1
        print("=" * 80)
        print(f"Importing sentence token annotations (Step {current_phase} of {total_phases})")
        print("=" * 80)
        print()

        with get_connection(db_path, bulk=True) as conn:
            print("Dropping indexes for bulk insert...")
            drop_sentence_token_indexes(conn)
            try:
                if has_tatoeba_tokens:
                    print(
                        f"Importing Tatoeba tokens from {DEFAULT_TATOEBA_SENTENCE_TOKENS_PATH}..."
                    )
                    _run_sentence_tokens_import(
                        conn,
                        DEFAULT_TATOEBA_SENTENCE_TOKENS_PATH,
                        source="tatoeba",
                        indent="  ",
                    )
                    print()

                if has_opensub_tokens:
                    print(
                        f"Importing OpenSubtitles tokens from "
                        f"{DEFAULT_OPENSUBTITLES_SENTENCE_TOKENS_PATH}..."
                    )
                    _run_sentence_tokens_import(
                        conn,
                        DEFAULT_OPENSUBTITLES_SENTENCE_TOKENS_PATH,
                        source="opensubtitles",
                        indent="  ",
                    )
                    print()
            finally:
                print("Recreating indexes...")
                create_sentence_token_indexes(conn)
                print()

    # Frequency computation from sentence tokens
    if has_any_tokens:
        current_phase += 1
        print("=" * 80)
        print(f"Computing frequencies from tokens (Step {current_phase} of {total_phases})")
        print("=" * 80)
        print()

        from italian_db.importers.frequency_from_tokens import compute_frequencies_from_tokens

        with get_connection(db_path) as conn:
            freq_stats = compute_frequencies_from_tokens(
                conn, progress_callback=_make_progress_callback()
            )
            print()
            print(f"  Total tokens counted:   {freq_stats['total_tokens']:,}")
            print(f"  Lemmas matched:         {freq_stats['matched']:,}")
            print(f"  Lemmas not in DB:       {freq_stats['not_found']:,}")
            print()

            # Compute per-POS rankings
            print("  Computing per-POS frequency rankings...")
            rank_stats = compute_pos_frequency_ranks(conn, "stanza")
            for pos_name, count in sorted(rank_stats.items()):
                print(f"    {pos_name.capitalize()}: {count:,} ranked")
        print()

    print("=" * 80)
    print("Import pipeline complete!")
    print("=" * 80)

    return 0


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="italian-db",
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
        type=POS,
        default=POS.VERB,
        choices=list(POS),
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
        type=POS,
        default=POS.VERB,
        choices=list(POS),
        help="Part of speech to enrich (default: verb)",
    )
    enrich_parser.set_defaults(func=cmd_enrich_formof)

    # import-frequencies subcommand
    freq_parser = subparsers.add_parser(
        "import-frequencies",
        help="Compute frequency data from Stanza sentence tokens",
    )
    freq_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    freq_parser.set_defaults(func=cmd_import_frequencies)

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
        "--tags",
        type=str,
        default=str(DEFAULT_TAGS_PATH),
        help=f"Path to tags CSV (default: {DEFAULT_TAGS_PATH})",
    )
    tatoeba_parser.add_argument(
        "--sentences-in-lists",
        type=str,
        default=str(DEFAULT_SENTENCES_IN_LISTS_PATH),
        help=f"Path to sentences_in_lists CSV for CK whitelist (default: {DEFAULT_SENTENCES_IN_LISTS_PATH})",
    )
    tatoeba_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    tatoeba_parser.set_defaults(func=cmd_import_tatoeba)

    # import-opensubtitles-sentences subcommand
    opensub_parser = subparsers.add_parser(
        "import-opensubtitles-sentences",
        help="Import OpenSubtitles parallel sentences",
    )
    opensub_parser.add_argument(
        "--ita-sentences",
        type=str,
        default=str(DEFAULT_OPENSUBTITLES_ITA_PATH),
        help=f"Path to Italian sentences TSV (default: {DEFAULT_OPENSUBTITLES_ITA_PATH})",
    )
    opensub_parser.add_argument(
        "--eng-sentences",
        type=str,
        default=str(DEFAULT_OPENSUBTITLES_ENG_PATH),
        help=f"Path to English sentences TSV (default: {DEFAULT_OPENSUBTITLES_ENG_PATH})",
    )
    opensub_parser.add_argument(
        "--links",
        type=str,
        default=str(DEFAULT_OPENSUBTITLES_LINKS_PATH),
        help=f"Path to links TSV (default: {DEFAULT_OPENSUBTITLES_LINKS_PATH})",
    )
    opensub_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    opensub_parser.set_defaults(func=cmd_import_opensubtitles_sentences)

    # import-verb-irregularity subcommand
    irreg_parser = subparsers.add_parser(
        "import-verb-irregularity",
        help="Import verb irregularity pattern classifications",
    )
    irreg_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    irreg_parser.set_defaults(func=cmd_import_verb_irregularity)

    # import-sentence-tokens subcommand
    tokens_parser = subparsers.add_parser(
        "import-sentence-tokens",
        help="Import POS-tagged sentence tokens from Stanza JSONL",
    )
    tokens_parser.add_argument(
        "--jsonl",
        type=str,
        default=None,
        help="Path to POS-tagged JSONL file (auto-detected from --source if not set)",
    )
    tokens_parser.add_argument(
        "--source",
        type=str,
        default="tatoeba",
        choices=["tatoeba", "opensubtitles"],
        help="Sentence source to import tokens for (default: tatoeba)",
    )
    tokens_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    tokens_parser.set_defaults(func=cmd_import_sentence_tokens)

    # import-profilo subcommand
    profilo_parser = subparsers.add_parser(
        "import-profilo",
        help="Import Profilo CEFR level data",
    )
    profilo_parser.add_argument(
        "--profilo-dir",
        type=str,
        default=str(DEFAULT_PROFILO_DIR),
        help=f"Path to Profilo HTML directory (default: {DEFAULT_PROFILO_DIR})",
    )
    profilo_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    profilo_parser.set_defaults(func=cmd_import_profilo)

    # import-nvdb subcommand
    nvdb_parser = subparsers.add_parser(
        "import-nvdb",
        help="Import NVdB usage tier data",
    )
    nvdb_parser.add_argument(
        "--nvdb-dir",
        type=str,
        default=str(DEFAULT_NVDB_DIR),
        help=f"Path to NVdB HTML directory (default: {DEFAULT_NVDB_DIR})",
    )
    nvdb_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    nvdb_parser.set_defaults(func=cmd_import_nvdb)

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

    # verify subcommand
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify database integrity and consistency",
    )
    verify_parser.add_argument(
        "-d",
        "--database",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    verify_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed breakdown and metrics",
    )
    verify_parser.set_defaults(func=cmd_verify)

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

    # download-opensubtitles subcommand
    dl_opensub_parser = subparsers.add_parser(
        "download-opensubtitles",
        help="Download OpenSubtitles parallel sentences from OPUS",
    )
    dl_opensub_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if files already exist",
    )
    dl_opensub_parser.set_defaults(func=cmd_download_opensubtitles)

    # download-profilo subcommand
    dl_profilo_parser = subparsers.add_parser(
        "download-profilo",
        help="Download Profilo CEFR word lists",
    )
    dl_profilo_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if files already exist",
    )
    dl_profilo_parser.set_defaults(func=cmd_download_profilo)

    # download-nvdb subcommand
    dl_nvdb_parser = subparsers.add_parser(
        "download-nvdb",
        help="Download NVdB vocabulary list",
    )
    dl_nvdb_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if file already exists",
    )
    dl_nvdb_parser.set_defaults(func=cmd_download_nvdb)

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
