"""Import ParTUT Universal Dependencies corpus with morphological annotations.

ParTUT is a multilingual parallel treebank that includes Italian and English.
It provides 1:1 sentence alignments with full morphological tagging, enabling
precise example sentence matching by grammatical features (mood, tense, person, number).

CoNLL-U format reference: https://universaldependencies.org/format.html

The importer:
1. Parses all CoNLL-U files for both Italian and English
2. Matches sentences by sent_id (e.g., "train-s1" in both corpora)
3. Inserts Italian sentences with source='partut'
4. Inserts English translations
5. Populates sentence_tokens with morphological annotations for Italian

Sentence IDs start at 10,000,000 to avoid collision with Tatoeba IDs.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, text

from italian_db.db.schema import sentence_tokens, sentences, translations
from italian_db.download import PARTUT_DIR, PARTUT_ENG_FILES, PARTUT_ITA_FILES

# Text corrections for known issues in ParTUT source data
PARTUT_TEXT_OVERRIDES: dict[str, str] = {
    # Missing closing quote in original
    'And we are not just talking about the so-called "quants.': 'And we are not just talking about the so-called "quants".',
}


@dataclass
class Token:
    """A single token from a CoNLL-U file."""

    index: int  # 1-indexed position in sentence
    form: str  # Surface form
    lemma: str  # Dictionary form
    upos: str  # Universal POS tag (VERB, NOUN, ADJ, etc.)
    feats: dict[str, str]  # Morphological features


def _empty_token_list() -> list[Token]:
    """Return an empty list of tokens (factory for dataclass default)."""
    return []


@dataclass
class Sentence:
    """A sentence from a CoNLL-U file."""

    sent_id: str  # Unique identifier (e.g., "train-s1")
    text: str  # Full sentence text
    tokens: list[Token] = field(default_factory=_empty_token_list)


def _parse_feats(feats_str: str) -> dict[str, str]:
    """Parse CoNLL-U morphological features string.

    Format: "Key1=Val1|Key2=Val2|..."
    Returns empty dict for "_" (no features).
    """
    if feats_str == "_" or not feats_str:
        return {}
    result: dict[str, str] = {}
    for pair in feats_str.split("|"):
        if "=" in pair:
            key, val = pair.split("=", 1)
            result[key] = val
    return result


def _parse_conllu_file(path: Path) -> list[Sentence]:
    """Parse a CoNLL-U file into a list of Sentence objects.

    CoNLL-U format:
    - Comment lines start with #
    - # sent_id = xxx identifies the sentence
    - # text = xxx provides the full sentence text
    - Token lines are tab-separated with 10 columns
    - Blank lines separate sentences
    """
    sentences_list: list[Sentence] = []
    current_sent_id: str | None = None
    current_text: str | None = None
    current_tokens: list[Token] = []

    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")

            if not line:
                # Blank line: end of sentence
                if current_sent_id and current_text:
                    sentences_list.append(
                        Sentence(sent_id=current_sent_id, text=current_text, tokens=current_tokens)
                    )
                current_sent_id = None
                current_text = None
                current_tokens = []
                continue

            if line.startswith("#"):
                # Comment line - extract metadata
                if line.startswith("# sent_id = "):
                    current_sent_id = line[12:].strip()
                elif line.startswith("# text = "):
                    current_text = line[9:].strip()
                continue

            # Token line
            parts = line.split("\t")
            if len(parts) < 10:
                continue

            # Skip multi-word token ranges (e.g., "1-2") and empty nodes (e.g., "1.1")
            token_id = parts[0]
            if "-" in token_id or "." in token_id:
                continue

            try:
                token_index = int(token_id)
            except ValueError:
                continue

            form = parts[1]
            lemma = parts[2]
            upos = parts[3]
            feats_str = parts[5]

            current_tokens.append(
                Token(
                    index=token_index,
                    form=form,
                    lemma=lemma,
                    upos=upos,
                    feats=_parse_feats(feats_str),
                )
            )

    # Handle final sentence if file doesn't end with blank line
    if current_sent_id and current_text:
        sentences_list.append(
            Sentence(sent_id=current_sent_id, text=current_text, tokens=current_tokens)
        )

    return sentences_list


def _clear_partut_data(conn: Connection) -> int:
    """Clear all existing ParTUT data.

    Deletes ParTUT sentences (cascades to translations and tokens).
    Returns the number of sentences cleared.
    """
    # Count existing ParTUT sentences
    result = conn.execute(text("SELECT COUNT(*) FROM sentences WHERE source = 'partut'"))
    existing_count = result.scalar() or 0

    if existing_count == 0:
        return 0

    # Delete sentence_tokens first (FK constraint references sentences.id)
    conn.execute(
        text("""
            DELETE FROM sentence_tokens
            WHERE sentence_id IN (SELECT id FROM sentences WHERE source = 'partut')
        """)
    )

    # Delete translations (FK constraint references sentences.id)
    conn.execute(
        text("""
            DELETE FROM translations
            WHERE ita_sentence_id IN (SELECT id FROM sentences WHERE source = 'partut')
               OR eng_sentence_id IN (SELECT id FROM sentences WHERE source = 'partut')
        """)
    )

    # Delete from FTS index (references sentences.id)
    conn.execute(
        text("""
            DELETE FROM sentences_fts
            WHERE id IN (SELECT id FROM sentences WHERE source = 'partut')
        """)
    )

    # Delete sentences
    conn.execute(text("DELETE FROM sentences WHERE source = 'partut'"))

    return existing_count


def import_partut(
    conn: Connection,
    partut_dir: Path | None = None,
    *,
    batch_size: int = 500,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import ParTUT corpus with morphological annotations.

    This function is idempotent: it clears existing ParTUT data before importing.

    Args:
        conn: SQLAlchemy connection
        partut_dir: Path to ParTUT data directory (default: data/partut/)
        batch_size: Number of rows to insert per batch
        progress_callback: Optional callback for progress reporting (current, total)

    Returns:
        Statistics dict with counts
    """
    if partut_dir is None:
        partut_dir = PARTUT_DIR

    # Clear existing ParTUT data first (idempotency)
    cleared = _clear_partut_data(conn)

    stats: dict[str, int] = {
        "cleared": cleared,
        "ita_sentences": 0,
        "eng_sentences": 0,
        "translations": 0,
        "tokens": 0,
    }

    # Parse all Italian files
    # sent_ids have language prefix (e.g., "it_partut-ud-3"), we normalize to just "partut-ud-3"
    ita_sentences: dict[str, Sentence] = {}
    for filename in PARTUT_ITA_FILES:
        path = partut_dir / filename
        if path.exists():
            for sent in _parse_conllu_file(path):
                # Normalize: "it_partut-ud-3" -> "partut-ud-3"
                normalized_id = sent.sent_id.removeprefix("it_")
                ita_sentences[normalized_id] = sent

    # Parse all English files
    eng_sentences: dict[str, Sentence] = {}
    for filename in PARTUT_ENG_FILES:
        path = partut_dir / filename
        if path.exists():
            for sent in _parse_conllu_file(path):
                # Normalize: "en_partut-ud-3" -> "partut-ud-3"
                normalized_id = sent.sent_id.removeprefix("en_")
                eng_sentences[normalized_id] = sent

    # Generate native sentence_ids by extracting numeric ID from ParTUT sent_id
    # e.g., "partut-ud-1118" -> 1118
    # These are stored in the sentence_id column; uniqueness is on (source, sentence_id)
    native_id_mapping: dict[str, int] = {}
    for sent_id in sorted(ita_sentences.keys()):
        # Extract numeric ID from "partut-ud-1118" -> 1118
        numeric_id = int(sent_id.split("-")[-1])
        native_id_mapping[sent_id] = numeric_id
    # English sentences get IDs in a separate range (offset by 1,000,000)
    # to keep them unique within source='partut'
    eng_native_id_offset = 1_000_000
    for sent_id in sorted(eng_sentences.keys()):
        if sent_id in ita_sentences:
            native_id_mapping[f"eng:{sent_id}"] = native_id_mapping[sent_id] + eng_native_id_offset

    # Calculate total items for progress
    total_items = (
        len(ita_sentences) + len(eng_sentences) + sum(len(s.tokens) for s in ita_sentences.values())
    )
    processed = 0

    # Insert Italian sentences
    ita_batch: list[dict[str, Any]] = []
    for sent_id, sent in ita_sentences.items():
        native_id = native_id_mapping[sent_id]
        ita_batch.append(
            {"sentence_id": native_id, "lang": "ita", "text": sent.text, "source": "partut"}
        )
        if len(ita_batch) >= batch_size:
            conn.execute(sentences.insert(), ita_batch)
            stats["ita_sentences"] += len(ita_batch)
            processed += len(ita_batch)
            if progress_callback:
                progress_callback(processed, total_items)
            ita_batch = []
    if ita_batch:
        conn.execute(sentences.insert(), ita_batch)
        stats["ita_sentences"] += len(ita_batch)
        processed += len(ita_batch)

    # Insert English sentences (only those that have Italian counterparts)
    eng_batch: list[dict[str, Any]] = []
    eng_sent_ids_with_ita: set[str] = set()
    for sent_id, sent in eng_sentences.items():
        if sent_id in ita_sentences:
            native_id = native_id_mapping[f"eng:{sent_id}"]
            # Apply text corrections for known issues
            corrected_text = PARTUT_TEXT_OVERRIDES.get(sent.text, sent.text)
            eng_batch.append(
                {
                    "sentence_id": native_id,
                    "lang": "eng",
                    "text": corrected_text,
                    "source": "partut",
                }
            )
            eng_sent_ids_with_ita.add(sent_id)
            if len(eng_batch) >= batch_size:
                conn.execute(sentences.insert(), eng_batch)
                stats["eng_sentences"] += len(eng_batch)
                processed += len(eng_batch)
                if progress_callback:
                    progress_callback(processed, total_items)
                eng_batch = []
    if eng_batch:
        conn.execute(sentences.insert(), eng_batch)
        stats["eng_sentences"] += len(eng_batch)
        processed += len(eng_batch)

    # Build mapping from native sentence_id to surrogate id
    # This is needed because translations and tokens reference the surrogate id
    result = conn.execute(text("SELECT id, sentence_id FROM sentences WHERE source = 'partut'"))
    native_to_surrogate: dict[int, int] = {row[1]: row[0] for row in result}

    # Insert translation pairs using surrogate IDs
    trans_batch: list[dict[str, int]] = []
    for sent_id in eng_sent_ids_with_ita:
        ita_native = native_id_mapping[sent_id]
        eng_native = native_id_mapping[f"eng:{sent_id}"]
        ita_surrogate = native_to_surrogate.get(ita_native)
        eng_surrogate = native_to_surrogate.get(eng_native)
        if ita_surrogate is not None and eng_surrogate is not None:
            trans_batch.append({"ita_sentence_id": ita_surrogate, "eng_sentence_id": eng_surrogate})
        if len(trans_batch) >= batch_size:
            conn.execute(translations.insert(), trans_batch)
            stats["translations"] += len(trans_batch)
            trans_batch = []
    if trans_batch:
        conn.execute(translations.insert(), trans_batch)
        stats["translations"] += len(trans_batch)

    # Insert token-level morphological annotations using surrogate IDs
    token_batch: list[dict[str, Any]] = []
    for sent_id, sent in ita_sentences.items():
        native_id = native_id_mapping[sent_id]
        surrogate_id = native_to_surrogate.get(native_id)
        if surrogate_id is None:
            continue
        for token in sent.tokens:
            feats = token.feats
            token_batch.append(
                {
                    "sentence_id": surrogate_id,
                    "token_index": token.index,
                    "form": token.form,
                    "lemma": token.lemma,
                    "upos": token.upos,
                    "mood": feats.get("Mood"),
                    "tense": feats.get("Tense"),
                    "person": int(feats["Person"]) if "Person" in feats else None,
                    "number": feats.get("Number"),
                    "gender": feats.get("Gender"),
                    "verb_form": feats.get("VerbForm"),
                }
            )
            if len(token_batch) >= batch_size:
                conn.execute(sentence_tokens.insert(), token_batch)
                stats["tokens"] += len(token_batch)
                processed += len(token_batch)
                if progress_callback:
                    progress_callback(processed, total_items)
                token_batch = []
    if token_batch:
        conn.execute(sentence_tokens.insert(), token_batch)
        stats["tokens"] += len(token_batch)
        processed += len(token_batch)

    # Final progress
    if progress_callback:
        progress_callback(total_items, total_items)

    # Populate FTS5 index for Italian ParTUT sentences (using surrogate id)
    conn.execute(
        text("""
            INSERT INTO sentences_fts(id, text)
            SELECT id, text FROM sentences
            WHERE source = 'partut' AND lang = 'ita'
        """)
    )

    return stats
