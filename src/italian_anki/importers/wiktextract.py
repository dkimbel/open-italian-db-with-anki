"""Import Italian verb data from Wiktextract JSONL."""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, select

from italian_anki.db.schema import (
    definitions,
    form_lookup,
    forms,
    frequencies,
    lemmas,
    noun_metadata,
    verb_metadata,
)
from italian_anki.normalize import normalize

# Tags to skip when inserting forms (metadata, not actual forms)
SKIP_TAGS = {"table-tags", "inflection-template"}

# Mapping from our POS names to Wiktextract's abbreviated names
WIKTEXTRACT_POS = {
    "verb": "verb",
    "noun": "noun",
    "adjective": "adj",  # Wiktextract uses "adj"
}


def _parse_entry(line: str) -> dict[str, Any] | None:
    """Parse a JSONL line, returning None if invalid."""
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def _is_pos_lemma(entry: dict[str, Any], pos: str) -> bool:
    """Check if entry is a lemma for the given POS (not an inflected form entry).

    Works for verbs, nouns, and adjectives.
    """
    if entry.get("pos") != pos:
        return False
    # Lemmas have a forms array; form entries have form_of in senses
    if "forms" not in entry:
        return False
    # Check if any sense has form_of (meaning this is a form, not a lemma)
    return all("form_of" not in sense for sense in entry.get("senses", []))


def _extract_auxiliary(entry: dict[str, Any]) -> str | None:
    """Extract auxiliary verb (avere, essere, or both) from forms."""
    auxiliaries: set[str] = set()
    for form in entry.get("forms", []):
        if "auxiliary" in form.get("tags", []):
            aux = normalize(form.get("form", ""))
            if "aver" in aux:
                auxiliaries.add("avere")
            elif "esser" in aux:
                auxiliaries.add("essere")

    if len(auxiliaries) == 2:
        return "both"
    if len(auxiliaries) == 1:
        return auxiliaries.pop()
    return None


def _extract_transitivity(entry: dict[str, Any]) -> str | None:
    """Extract transitivity from senses tags."""
    transitive = False
    intransitive = False

    for sense in entry.get("senses", []):
        tags = sense.get("tags", [])
        if "transitive" in tags:
            transitive = True
        if "intransitive" in tags:
            intransitive = True

    if transitive and intransitive:
        return "both"
    if transitive:
        return "transitive"
    if intransitive:
        return "intransitive"
    return None


def _extract_ipa(entry: dict[str, Any]) -> str | None:
    """Extract IPA pronunciation for the infinitive."""
    for sound in entry.get("sounds", []):
        if "ipa" in sound:
            return sound["ipa"]
    return None


def _extract_gender(entry: dict[str, Any]) -> str | None:
    """Extract grammatical gender for nouns.

    Priority: categories → senses tags → head_templates.
    Returns 'm' for masculine, 'f' for feminine, None if unknown.
    """
    # Check categories first (most reliable)
    categories: list[str | dict[str, Any]] = entry.get("categories", [])
    for cat in categories:
        cat_name = str(cat.get("name", "")) if isinstance(cat, dict) else (str(cat) if cat else "")
        if "Italian masculine nouns" in cat_name:
            return "m"
        if "Italian feminine nouns" in cat_name:
            return "f"

    # Check senses tags as fallback
    for sense in entry.get("senses", []):
        tags = sense.get("tags", [])
        if "masculine" in tags:
            return "m"
        if "feminine" in tags:
            return "f"

    # Check head_templates as last resort
    for template in entry.get("head_templates", []):
        args = template.get("args", {})
        # Common pattern: {"1": "it", "2": "m"} or {"1": "it", "2": "f"}
        gender_arg = args.get("2", "") or args.get("g", "")
        if gender_arg in ("m", "m-s", "m-p"):
            return "m"
        if gender_arg in ("f", "f-s", "f-p"):
            return "f"

    return None


def _extract_lemma_stressed(entry: dict[str, Any]) -> str:
    """Extract the stressed form of the lemma (infinitive)."""
    # First check forms for canonical or infinitive
    for form in entry.get("forms", []):
        tags = form.get("tags", [])
        if "canonical" in tags or "infinitive" in tags:
            return form.get("form", entry["word"])
    # Fallback to word
    return entry["word"]


def _iter_forms(entry: dict[str, Any], pos: str) -> Iterator[tuple[str, list[str]]]:
    """Yield (form_stressed, tags) for each inflected form.

    Args:
        entry: Wiktextract entry dict
        pos: Part of speech (verb, noun, adjective)
    """
    seen: set[tuple[str, tuple[str, ...]]] = set()
    has_singular = False
    has_masc_singular = False

    for form_data in entry.get("forms", []):
        form_stressed = form_data.get("form", "")
        tags = form_data.get("tags", [])

        # Skip metadata entries
        if not form_stressed or set(tags) & SKIP_TAGS:
            continue

        # Skip auxiliary markers (they're metadata, not conjugated forms)
        if "auxiliary" in tags:
            continue

        # Skip canonical form for verbs only (stored separately as lemma_stressed)
        # For nouns/adjectives, canonical is the singular form we want to keep
        if pos == "verb" and "canonical" in tags:
            continue

        # Track whether we've seen the base form
        if pos == "noun" and "singular" in tags:
            has_singular = True
        if pos == "adjective" and "masculine" in tags and "singular" in tags:
            has_masc_singular = True

        # Deduplicate
        key = (form_stressed, tuple(sorted(tags)))
        if key in seen:
            continue
        seen.add(key)

        yield form_stressed, tags

    # Add base form if missing (Wiktextract stores it in 'word', not in 'forms')
    # For nouns: add singular form if not present
    # For adjectives: add masculine singular form if not present
    lemma_stressed = _extract_lemma_stressed(entry)

    if pos == "noun" and not has_singular:
        key = (lemma_stressed, ("singular",))
        if key not in seen:
            yield lemma_stressed, ["singular"]

    if pos == "adjective" and not has_masc_singular:
        key = (lemma_stressed, ("masculine", "singular"))
        if key not in seen:
            yield lemma_stressed, ["masculine", "singular"]


def _iter_definitions(entry: dict[str, Any]) -> Iterator[tuple[str, list[str] | None]]:
    """Yield (gloss, tags) for each definition."""
    for sense in entry.get("senses", []):
        # Skip form-of entries
        if "form_of" in sense:
            continue

        glosses = sense.get("glosses", [])
        if not glosses:
            continue

        # Join multiple glosses
        gloss = "; ".join(glosses)
        tags = sense.get("tags") if sense.get("tags") else None

        yield gloss, tags


def _clear_existing_data(conn: Connection, pos_filter: str) -> int:
    """Clear all existing data for the given POS.

    Deletes in FK-safe order: form_lookup → forms → definitions → frequencies
    → noun_metadata/verb_metadata → lemmas.
    Returns the number of lemmas cleared.
    """
    # Get existing lemma IDs for this POS
    result = conn.execute(select(lemmas.c.lemma_id).where(lemmas.c.pos == pos_filter))
    existing_ids = [row.lemma_id for row in result]

    if not existing_ids:
        return 0

    # Delete in FK-safe order
    # 1. form_lookup (references forms)
    conn.execute(
        form_lookup.delete().where(
            form_lookup.c.form_id.in_(select(forms.c.id).where(forms.c.lemma_id.in_(existing_ids)))
        )
    )
    # 2. forms (references lemmas)
    conn.execute(forms.delete().where(forms.c.lemma_id.in_(existing_ids)))
    # 3. definitions (references lemmas)
    conn.execute(definitions.delete().where(definitions.c.lemma_id.in_(existing_ids)))
    # 4. frequencies (references lemmas)
    conn.execute(frequencies.delete().where(frequencies.c.lemma_id.in_(existing_ids)))
    # 5. POS-specific metadata tables
    if pos_filter == "noun":
        conn.execute(noun_metadata.delete().where(noun_metadata.c.lemma_id.in_(existing_ids)))
    elif pos_filter == "verb":
        conn.execute(verb_metadata.delete().where(verb_metadata.c.lemma_id.in_(existing_ids)))
    # 6. lemmas
    conn.execute(lemmas.delete().where(lemmas.c.lemma_id.in_(existing_ids)))

    return len(existing_ids)


def import_wiktextract(
    conn: Connection,
    jsonl_path: Path,
    *,
    pos_filter: str = "verb",
    batch_size: int = 1000,
) -> dict[str, int]:
    """Import Wiktextract data into the database.

    This function is idempotent: it clears existing data for the POS before importing.
    All operations happen within the caller's transaction, so on failure the database
    rolls back to its original state.

    Args:
        conn: SQLAlchemy connection
        jsonl_path: Path to the Wiktextract JSONL file
        pos_filter: Part of speech to import (default: "verb")
        batch_size: Number of forms to insert per batch

    Returns:
        Statistics dict with counts of imported items
    """
    # Clear existing data first (idempotency)
    cleared = _clear_existing_data(conn, pos_filter)

    stats: dict[str, int] = {
        "lemmas": 0,
        "forms": 0,
        "definitions": 0,
        "skipped": 0,
        "cleared": cleared,
    }
    if pos_filter == "noun":
        stats["nouns_with_gender"] = 0
        stats["nouns_no_gender"] = 0

    form_batch: list[dict[str, Any]] = []
    lookup_batch: list[dict[str, Any]] = []
    definition_batch: list[dict[str, Any]] = []

    def flush_batches() -> None:
        nonlocal form_batch, lookup_batch, definition_batch
        if form_batch:
            result = conn.execute(forms.insert().returning(forms.c.id), form_batch)
            form_ids = [row.id for row in result]

            # Build lookup entries with the returned IDs
            for form_id, form_data in zip(form_ids, form_batch, strict=True):
                form_normalized = normalize(form_data["form_stressed"])
                lookup_batch.append({"form_normalized": form_normalized, "form_id": form_id})

            form_batch = []
            stats["forms"] += len(form_ids)

        if lookup_batch:
            # Use INSERT OR IGNORE for lookup (same normalized form can map to multiple form_ids)
            conn.execute(
                form_lookup.insert().prefix_with("OR IGNORE"),
                lookup_batch,
            )
            lookup_batch = []

        if definition_batch:
            conn.execute(definitions.insert(), definition_batch)
            stats["definitions"] += len(definition_batch)
            definition_batch = []

    # Map to Wiktextract's POS naming
    wiktextract_pos = WIKTEXTRACT_POS.get(pos_filter, pos_filter)

    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            entry = _parse_entry(line)
            if entry is None:
                continue

            # Filter by POS (using Wiktextract's naming)
            if entry.get("pos") != wiktextract_pos:
                continue

            # Only import lemmas, not form entries
            if not _is_pos_lemma(entry, wiktextract_pos):
                stats["skipped"] += 1
                continue

            # Extract lemma data
            word = entry["word"]
            lemma_normalized = normalize(word)
            lemma_stressed = _extract_lemma_stressed(entry)

            # Insert lemma
            try:
                result = conn.execute(
                    lemmas.insert().values(
                        lemma=lemma_normalized,
                        lemma_stressed=lemma_stressed,
                        pos=pos_filter,
                        ipa=_extract_ipa(entry),
                    )
                )
                pk = result.inserted_primary_key
                if pk is None:
                    continue
                lemma_id: int = pk[0]
                stats["lemmas"] += 1
            except Exception:
                # Duplicate lemma - skip
                stats["skipped"] += 1
                continue

            # Insert POS-specific metadata
            if pos_filter == "noun":
                gender = _extract_gender(entry)
                if gender:
                    conn.execute(noun_metadata.insert().values(lemma_id=lemma_id, gender=gender))
                    stats["nouns_with_gender"] += 1
                else:
                    stats["nouns_no_gender"] += 1
            elif pos_filter == "verb":
                auxiliary = _extract_auxiliary(entry)
                transitivity = _extract_transitivity(entry)
                if auxiliary or transitivity:
                    conn.execute(
                        verb_metadata.insert().values(
                            lemma_id=lemma_id,
                            auxiliary=auxiliary,
                            transitivity=transitivity,
                        )
                    )

            # Queue forms for batch insert
            for form_stressed, tags in _iter_forms(entry, pos_filter):
                form_batch.append(
                    {
                        "lemma_id": lemma_id,
                        "form": None,  # Will be filled by Morph-it! importer
                        "form_stressed": form_stressed,
                        "tags": json.dumps(tags),
                    }
                )

                if len(form_batch) >= batch_size:
                    flush_batches()

            # Queue definitions
            for gloss, tags in _iter_definitions(entry):
                definition_batch.append(
                    {
                        "lemma_id": lemma_id,
                        "gloss": gloss,
                        "tags": json.dumps(tags) if tags else None,
                    }
                )

    # Final flush
    flush_batches()

    return stats
