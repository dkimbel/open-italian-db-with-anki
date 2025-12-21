"""Import Tatoeba sentences and link to lemmas."""

from pathlib import Path
from typing import Any

from sqlalchemy import Connection, select

from italian_anki.db.schema import form_lookup, forms, sentence_lemmas, sentences, translations
from italian_anki.normalize import normalize, tokenize


def _clear_existing_data(conn: Connection) -> int:
    """Clear all existing Tatoeba data.

    Deletes in FK-safe order: sentence_lemmas → translations → sentences.
    Returns the number of sentences cleared.
    """
    # Count existing sentences
    result = conn.execute(select(sentences.c.sentence_id))
    existing_count = len(result.fetchall())

    if existing_count == 0:
        return 0

    # Delete in FK-safe order
    conn.execute(sentence_lemmas.delete())
    conn.execute(translations.delete())
    conn.execute(sentences.delete())

    return existing_count


def _parse_sentences_tsv(path: Path) -> dict[int, str]:
    """Parse a Tatoeba sentences TSV file.

    Format: sentence_id<TAB>lang<TAB>text (no header)
    Returns dict mapping sentence_id -> text.
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


def _stream_links(path: Path, italian_ids: set[int]) -> tuple[set[int], list[tuple[int, int]]]:
    """Stream links.csv and filter to Italian→English pairs.

    Returns:
        - Set of English sentence IDs that are translations of Italian sentences
        - List of (ita_id, eng_id) translation pairs
    """
    english_ids: set[int] = set()
    pairs: list[tuple[int, int]] = []

    with path.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                try:
                    id1 = int(parts[0])
                    id2 = int(parts[1])

                    # Check if this is Italian → something
                    if id1 in italian_ids:
                        english_ids.add(id2)
                        pairs.append((id1, id2))
                except ValueError:
                    continue

    return english_ids, pairs


def _build_form_lookup_dict(conn: Connection) -> dict[str, list[int]]:
    """Build a dict mapping normalized_form -> list of lemma_ids."""
    # Get form_lookup entries
    lookup_result = conn.execute(select(form_lookup.c.form_normalized, form_lookup.c.form_id))

    # Get forms to map form_id -> lemma_id
    forms_result = conn.execute(select(forms.c.id, forms.c.lemma_id))
    form_to_lemma: dict[int, int] = {row.id: row.lemma_id for row in forms_result}

    # Build normalized -> lemma_ids dict
    result: dict[str, list[int]] = {}
    for row in lookup_result:
        form_normalized = row.form_normalized
        form_id = row.form_id
        lemma_id = form_to_lemma.get(form_id)
        if lemma_id is not None:
            if form_normalized not in result:
                result[form_normalized] = []
            if lemma_id not in result[form_normalized]:
                result[form_normalized].append(lemma_id)

    return result


def import_tatoeba(
    conn: Connection,
    ita_sentences_path: Path,
    eng_sentences_path: Path,
    links_path: Path,
    *,
    batch_size: int = 1000,
) -> dict[str, int]:
    """Import Tatoeba sentences and link to verb lemmas.

    This function is idempotent: it clears existing Tatoeba data before importing.

    Args:
        conn: SQLAlchemy connection
        ita_sentences_path: Path to Italian sentences TSV
        eng_sentences_path: Path to English sentences TSV
        links_path: Path to links CSV
        batch_size: Number of rows to insert per batch

    Returns:
        Statistics dict with counts
    """
    # Clear existing data first (idempotency)
    cleared = _clear_existing_data(conn)

    stats: dict[str, int] = {
        "cleared": cleared,
        "ita_sentences": 0,
        "eng_sentences": 0,
        "translations": 0,
        "sentence_lemmas": 0,
    }

    # Step 1: Parse Italian sentences
    ita_sentences = _parse_sentences_tsv(ita_sentences_path)
    italian_ids = set(ita_sentences.keys())

    # Step 2: Stream links and find needed English IDs
    needed_eng_ids, translation_pairs = _stream_links(links_path, italian_ids)

    # Step 3: Parse English sentences (only those we need)
    all_eng_sentences = _parse_sentences_tsv(eng_sentences_path)
    eng_sentences = {sid: text for sid, text in all_eng_sentences.items() if sid in needed_eng_ids}

    # Filter translation pairs to only include English sentences we have
    eng_ids_we_have = set(eng_sentences.keys())
    translation_pairs = [(ita, eng) for ita, eng in translation_pairs if eng in eng_ids_we_have]

    # Step 4: Insert Italian sentences
    ita_batch: list[dict[str, Any]] = []
    for sentence_id, text in ita_sentences.items():
        ita_batch.append({"sentence_id": sentence_id, "lang": "ita", "text": text})
        if len(ita_batch) >= batch_size:
            conn.execute(sentences.insert(), ita_batch)
            stats["ita_sentences"] += len(ita_batch)
            ita_batch = []
    if ita_batch:
        conn.execute(sentences.insert(), ita_batch)
        stats["ita_sentences"] += len(ita_batch)

    # Step 5: Insert English sentences
    eng_batch: list[dict[str, Any]] = []
    for sentence_id, text in eng_sentences.items():
        eng_batch.append({"sentence_id": sentence_id, "lang": "eng", "text": text})
        if len(eng_batch) >= batch_size:
            conn.execute(sentences.insert(), eng_batch)
            stats["eng_sentences"] += len(eng_batch)
            eng_batch = []
    if eng_batch:
        conn.execute(sentences.insert(), eng_batch)
        stats["eng_sentences"] += len(eng_batch)

    # Step 6: Insert translation pairs
    trans_batch: list[dict[str, int]] = []
    for ita_id, eng_id in translation_pairs:
        trans_batch.append({"ita_sentence_id": ita_id, "eng_sentence_id": eng_id})
        if len(trans_batch) >= batch_size:
            conn.execute(translations.insert().prefix_with("OR IGNORE"), trans_batch)
            stats["translations"] += len(trans_batch)
            trans_batch = []
    if trans_batch:
        conn.execute(translations.insert().prefix_with("OR IGNORE"), trans_batch)
        stats["translations"] += len(trans_batch)

    # Step 7: Match Italian sentences to verbs
    form_lookup_dict = _build_form_lookup_dict(conn)

    verb_batch: list[dict[str, Any]] = []
    seen_pairs: set[tuple[int, int]] = set()  # (sentence_id, lemma_id)

    for sentence_id, text in ita_sentences.items():
        tokens = tokenize(text)
        for token in tokens:
            normalized_token = normalize(token)
            lemma_ids = form_lookup_dict.get(normalized_token, [])
            for lemma_id in lemma_ids:
                pair = (sentence_id, lemma_id)
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    verb_batch.append(
                        {
                            "sentence_id": sentence_id,
                            "lemma_id": lemma_id,
                            "form_found": token,
                        }
                    )

        if len(verb_batch) >= batch_size:
            conn.execute(sentence_lemmas.insert(), verb_batch)
            stats["sentence_lemmas"] += len(verb_batch)
            verb_batch = []

    if verb_batch:
        conn.execute(sentence_lemmas.insert(), verb_batch)
        stats["sentence_lemmas"] += len(verb_batch)

    return stats
