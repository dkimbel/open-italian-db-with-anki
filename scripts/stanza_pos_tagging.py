#!/usr/bin/env python3
"""Stanza POS tagging for Italian sentences.

This is a one-off preprocessing script that generates POS-tagged tokens for
Italian sentences from Tatoeba or OpenSubtitles. The output JSONL is then
imported into the database by the sentence_tokens importer.

Usage:
    # Tag Tatoeba sentences (default)
    python scripts/stanza_pos_tagging.py

    # Tag OpenSubtitles sentences
    python scripts/stanza_pos_tagging.py --source opensubtitles

    # Tag only CK-whitelisted Tatoeba sentences (recommended for initial testing)
    python scripts/stanza_pos_tagging.py --ck-only

    # Custom paths
    python scripts/stanza_pos_tagging.py --input data/tatoeba/ita_sentences.tsv \
        --output data/tatoeba/ita_sentences_pos.jsonl

Output JSONL format (one line per sentence):
    {
        "sentence_id": 4369,
        "text": "Devo andare a dormire.",
        "tokens": [
            {
                "id": 1,
                "text": "Devo",
                "lemma": "dovere",
                "upos": "AUX",
                "feats": {"Mood": "Ind", "Number": "Sing", ...},
                "head": 2,
                "deprel": "aux"
            },
            ...
        ]
    }

Requirements:
    - stanza>=1.8 (install with: uv sync --all-extras)
    - Download Italian model first: python -c "import stanza; stanza.download('it')"
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

# Stanza import is deferred to allow --help without the heavy import
CK_LIST_ID = 907

# Default paths by source
SOURCE_DEFAULTS = {
    "tatoeba": {
        "input": Path("data/tatoeba/ita_sentences.tsv"),
        "output": Path("data/tatoeba/ita_sentences_pos.jsonl"),
    },
    "opensubtitles": {
        "input": Path("data/opensubtitles/it_sentences.tsv"),
        "output": Path("data/opensubtitles/it_sentences_pos.jsonl"),
    },
}

DEFAULT_SENTENCES_IN_LISTS = Path("data/tatoeba/sentences_in_lists.csv")
DEFAULT_LINKS = Path("data/tatoeba/ita_eng_links.tsv")


def parse_sentences_tsv(path: Path) -> dict[int, str]:
    """Parse sentences TSV file (Tatoeba-compatible format).

    Format: sentence_id<TAB>lang<TAB>text (no header)
    """
    result: dict[int, str] = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 3:
                try:
                    sentence_id = int(parts[0])
                    text = parts[2]
                    result[sentence_id] = text
                except ValueError:
                    continue
    return result


def load_ck_whitelist(path: Path, list_id: int = CK_LIST_ID) -> set[int]:
    """Load English sentence IDs from CK whitelist."""
    sentence_ids: set[int] = set()
    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                try:
                    file_list_id = int(parts[0])
                    sentence_id = int(parts[1])
                    if file_list_id == list_id:
                        sentence_ids.add(sentence_id)
                except ValueError:
                    continue
    return sentence_ids


def get_ita_ids_with_ck_translation(
    links_path: Path, ck_whitelist: set[int], all_ita_ids: set[int]
) -> set[int]:
    """Get Italian sentence IDs that have translation to CK-whitelisted English."""
    ita_ids: set[int] = set()
    with links_path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                try:
                    ita_id = int(parts[0])
                    eng_id = int(parts[1])
                    if ita_id in all_ita_ids and eng_id in ck_whitelist:
                        ita_ids.add(ita_id)
                except ValueError:
                    continue
    return ita_ids


def parse_feats(feats_str: str | None) -> dict[str, str]:
    """Parse Stanza feats string into dict."""
    if not feats_str:
        return {}
    result: dict[str, str] = {}
    for feat in feats_str.split("|"):
        if "=" in feat:
            key, value = feat.split("=", 1)
            result[key] = value
    return result


def process_document(doc: Any) -> list[dict[str, Any]]:
    """Extract token info from a Stanza Document."""
    tokens: list[dict[str, Any]] = []
    for sentence in doc.sentences:
        for word in sentence.words:
            token: dict[str, Any] = {
                "id": word.id,
                "text": word.text,
                "lemma": word.lemma,
                "upos": word.upos,
                "feats": parse_feats(word.feats),
                "head": word.head,
                "deprel": word.deprel,
            }
            tokens.append(token)
    return tokens


def run_pos_tagging(
    sentences: dict[int, str],
    output_path: Path,
    batch_size: int = 100,
) -> None:
    """Run Stanza POS tagging on sentences and write JSONL output."""
    # Import stanza here to avoid slow import on --help
    import stanza

    print("Loading Stanza Italian model...")
    nlp = stanza.Pipeline(
        "it",
        processors="tokenize,mwt,pos,lemma,depparse",
        verbose=False,
    )

    total = len(sentences)
    processed = 0
    start_time = time.time()

    # Prepare batches
    sentence_items = list(sentences.items())

    with output_path.open("w", encoding="utf-8") as f:
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch = sentence_items[batch_start:batch_end]

            # Process batch with Stanza
            texts = [text for _, text in batch]
            docs = nlp.bulk_process(texts)

            # Write results
            for (sentence_id, text), doc in zip(batch, docs, strict=True):
                record = {
                    "sentence_id": sentence_id,
                    "text": text,
                    "tokens": process_document(doc),
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            processed += len(batch)

            # Progress report
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            pct = processed * 100 // total
            print(
                f"\r  Processing... {pct}% ({processed:,}/{total:,}) "
                f"[{rate:.1f} sent/s, ETA: {eta / 60:.1f}m]",
                end="",
                flush=True,
            )

    print()  # newline after progress
    elapsed = time.time() - start_time
    print(f"  Completed in {elapsed / 60:.1f} minutes ({processed / elapsed:.1f} sent/s)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Stanza POS tagging on Italian sentences",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage:")[0],  # Show the docstring preamble
    )
    parser.add_argument(
        "--source",
        type=str,
        default="tatoeba",
        choices=["tatoeba", "opensubtitles"],
        help="Sentence source to process (default: tatoeba)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Path to Italian sentences TSV (auto-detected from --source if not set)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to output JSONL file (auto-detected from --source if not set)",
    )
    parser.add_argument(
        "--ck-only",
        action="store_true",
        help="Only process sentences with CK-whitelisted English translations (Tatoeba only)",
    )
    parser.add_argument(
        "--sentences-in-lists",
        type=Path,
        default=DEFAULT_SENTENCES_IN_LISTS,
        help=f"Path to sentences_in_lists.csv (default: {DEFAULT_SENTENCES_IN_LISTS})",
    )
    parser.add_argument(
        "--links",
        type=Path,
        default=DEFAULT_LINKS,
        help=f"Path to links TSV (default: {DEFAULT_LINKS})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of sentences to process per batch (default: 100)",
    )

    args = parser.parse_args()

    # Resolve defaults based on source
    defaults = SOURCE_DEFAULTS[args.source]
    input_path = args.input or defaults["input"]
    output_path = args.output or defaults["output"]

    # Validate input file
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        return 1

    # CK-only filtering is only valid for Tatoeba
    if args.ck_only and args.source != "tatoeba":
        print("Error: --ck-only is only valid for --source tatoeba", file=sys.stderr)
        return 1

    print(f"Source: {args.source}")
    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")

    # Load sentences
    print("Loading Italian sentences...")
    sentences = parse_sentences_tsv(input_path)
    print(f"  Loaded {len(sentences):,} sentences")

    # Filter to CK-whitelisted if requested (Tatoeba only)
    if args.ck_only:
        if not args.sentences_in_lists.exists():
            print(
                f"Error: CK whitelist file not found: {args.sentences_in_lists}",
                file=sys.stderr,
            )
            return 1
        if not args.links.exists():
            print(f"Error: Links file not found: {args.links}", file=sys.stderr)
            return 1

        print("Loading CK whitelist...")
        ck_whitelist = load_ck_whitelist(args.sentences_in_lists)
        print(f"  CK whitelist: {len(ck_whitelist):,} English sentences")

        print("Filtering to sentences with CK translations...")
        all_ita_ids = set(sentences.keys())
        ck_ita_ids = get_ita_ids_with_ck_translation(args.links, ck_whitelist, all_ita_ids)
        sentences = {sid: text for sid, text in sentences.items() if sid in ck_ita_ids}
        print(f"  After filtering: {len(sentences):,} sentences")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Run POS tagging
    print()
    print("Running Stanza POS tagging...")
    run_pos_tagging(sentences, output_path, batch_size=args.batch_size)

    print()
    print(f"Output written to: {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
