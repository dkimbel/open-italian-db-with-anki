"""Import CEFR level data from Profilo della lingua italiana.

Parses the "liste lessicali" HTML pages from the University for Foreigners
of Perugia (Spinelli & Parizzi, 2010) and matches entries against our
lemmas table to assign CEFR levels (A1-B2).

The word lists are cumulative (A2 includes all A1 words, etc.), so we
compute per-level deltas to assign each word its lowest CEFR level.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from sqlalchemy.engine import Connection

# EN DASH used as separator in Profilo POS strings (e.g., "s.m. \u2013 s.f.")
_EN_DASH = "\u2013"

# POS abbreviation mapping: Profilo abbreviation -> our POS system
POS_MAP: dict[str, str | None] = {
    "v.t.": "verb",
    "v.t": "verb",  # typo variant (missing period)
    "v.int.": "verb",
    "v. int.": "verb",  # space variant
    "v.intr.": "verb",  # variant abbreviation
    "v. intr.": "verb",
    "v.rifl.": "verb",
    "v. rifl.": "verb",
    "v. rifl. recip.": "verb",  # reciprocal reflexive
    "v.t. pron.": "verb",
    "v.t.pron.": "verb",
    "v. t. pron.": "verb",  # space variant
    "v. t.": "verb",  # space variant
    "v.int. pron.": "verb",
    "v.int.pron.": "verb",
    "v. int. pron.": "verb",
    "v.intr. pron.": "verb",
    "s.m.": "noun",
    "s.m": "noun",  # typo variant
    "s.m. pl.": "noun",  # plural noun
    "s.m. agg.": "noun",  # noun-adjective (take first)
    "s.f.": "noun",
    "s.f": "noun",  # typo variant
    "agg.": "adjective",
    # These POS exist in Profilo but don't match our lemma DB (verb/noun/adj only)
    "avv.": "adverb",
    "prep.": "preposition",
    "cong.": "conjunction",
    "pron.": "pronoun",
    "part. pron.": "pronoun",
    "part. pron. luogo": "pronoun",  # ci (place pronoun)
    "art.": "article",
    "inter.": "interjection",
    "loc.": "locution",
    "loc.sost.m.": "locution",  # compound noun locution
    "locuz.": "locution",  # variant abbreviation
}

# POS values that can match our database
MATCHABLE_POS = {"verb", "noun", "adjective"}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ProfiloEntry:
    """A single word entry from the Profilo word lists."""

    word: str  # The word as written in Profilo (may include /a, parenthetical forms)
    clean_word: str  # Normalized for DB matching (stripped of /a, parentheticals)
    pos_raw: str  # Raw POS string from HTML (e.g., "v.t.", "s.m. - s.f.")
    pos_mapped: str | None  # Our POS mapping (verb/noun/adjective/etc.) or None
    cefr_level: str  # Lowest level this word appears at (A1/A2/B1/B2)
    is_multiword: bool = False  # Contains spaces (multiword expression)
    has_reflexive: bool = False  # Word has /si reflexive form


# ---------------------------------------------------------------------------
# Parsing functions
# ---------------------------------------------------------------------------


def _parse_entries(html: str) -> list[tuple[str, str]]:
    """Parse numbered entries from Profilo HTML.

    Each entry looks like:
        NUMBER.\\t<a href="#" onClick="...">WORD</a> (POS)<br>

    Returns list of (word, pos_raw) tuples.
    """
    pattern = re.compile(
        r"\d+\.\s*"  # number and dot
        r"<a[^>]*>"  # opening <a> tag
        r"([^<]+)"  # word text (capture group 1)
        r"</a>"  # closing </a>
        r"\s*"  # optional whitespace
        r"\(([^)]+)\)"  # POS in parentheses (capture group 2)
    )

    entries: list[tuple[str, str]] = []
    for match in pattern.finditer(html):
        word = match.group(1).strip()
        pos_raw = match.group(2).strip()
        entries.append((word, pos_raw))

    return entries


def _clean_word(word: str) -> tuple[str, bool, bool]:
    """Clean a Profilo word for database matching.

    Handles patterns like:
    - "amico/a" -> "amico" (gender variant)
    - "aereo(aeroplano)" -> "aereo" (expanded form)
    - "chiamare/si" -> "chiamare" (reflexive variant)

    Returns (clean_word, is_multiword, has_reflexive).
    """
    is_multiword = False
    has_reflexive = False

    word = word.strip()

    # Handle reflexive /si, /rsi
    if word.endswith("/si") or word.endswith("/rsi"):
        has_reflexive = True
        word = word.split("/")[0]
    # Handle gender variant /a, /o
    elif "/" in word:
        word = word.split("/")[0]

    # Handle parenthetical expansions: "aereo(aeroplano)" -> "aereo"
    word = re.sub(r"\([^)]*\)", "", word).strip()

    # Check for multiword
    if " " in word:
        is_multiword = True

    return word, is_multiword, has_reflexive


def _map_pos(pos_raw: str) -> str | None:
    """Map a raw Profilo POS string to our POS system.

    POS can be compound: "s.m. - s.f." or "v.t. - v.rifl."
    We take the first POS component and map it.
    """
    # Split on en-dash or hyphen (with surrounding spaces)
    parts = re.split(r"\s*[" + _EN_DASH + r"-]\s*", pos_raw)
    first_pos = parts[0].strip()

    # Normalize multiple spaces to single space
    first_pos = re.sub(r"\s+", " ", first_pos)

    # Direct lookup
    if first_pos in POS_MAP:
        return POS_MAP[first_pos]

    # Try adding trailing period (handles typos like "v.t" instead of "v.t.")
    if not first_pos.endswith("."):
        with_period = first_pos + "."
        if with_period in POS_MAP:
            return POS_MAP[with_period]

    return None


def _parse_all_levels(profilo_dir: Path) -> list[ProfiloEntry]:
    """Parse all four CEFR level HTML files and compute per-level deltas.

    Since lists are cumulative, a word appearing in A1 and A2 gets level A1.

    Args:
        profilo_dir: Directory containing the HTML files.

    Returns:
        List of deduplicated ProfiloEntry objects with lowest CEFR level assigned.
    """
    # Track: (clean_word_lower, pos_mapped) -> lowest level seen
    seen: dict[tuple[str, str | None], str] = {}
    entries: dict[tuple[str, str | None], ProfiloEntry] = {}

    for level in ["A1", "A2", "B1", "B2"]:
        html_path = profilo_dir / f"liste_lessicali_{level.lower()}.html"
        if not html_path.exists():
            raise FileNotFoundError(f"Profilo HTML file not found: {html_path}")

        html = html_path.read_text(encoding="utf-8")
        raw_entries = _parse_entries(html)

        for word, pos_raw in raw_entries:
            clean, is_multi, has_refl = _clean_word(word)
            pos = _map_pos(pos_raw)

            key = (clean.lower(), pos)
            if key not in seen:
                seen[key] = level
                entries[key] = ProfiloEntry(
                    word=word,
                    clean_word=clean,
                    pos_raw=pos_raw,
                    pos_mapped=pos,
                    cefr_level=level,
                    is_multiword=is_multi,
                    has_reflexive=has_refl,
                )

    return list(entries.values())


# ---------------------------------------------------------------------------
# Database matching
# ---------------------------------------------------------------------------


def _match_entry(conn: Connection, entry: ProfiloEntry) -> int | None:
    """Try to match a ProfiloEntry against the lemmas table.

    Matching strategy:
    1. Exact match on (written, pos)
    2. Case-insensitive match (handles CD/cd, Natale/natale)
    3. For reflexive verbs, try -si form (chiamare -> chiamarsi)

    Returns lemma_id if matched, None otherwise.
    """
    words_to_try = [entry.clean_word]
    if entry.has_reflexive and entry.pos_mapped == "verb":
        base = entry.clean_word
        if base.endswith("re"):
            words_to_try.append(base[:-1] + "si")  # chiamare -> chiamarsi

    for word in words_to_try:
        # Exact match
        row = conn.execute(
            text("SELECT id FROM lemmas WHERE written = :word AND pos = :pos LIMIT 1"),
            {"word": word, "pos": entry.pos_mapped},
        ).fetchone()

        if not row:
            # Case-insensitive fallback
            row = conn.execute(
                text(
                    "SELECT id FROM lemmas "
                    "WHERE LOWER(written) = LOWER(:word) AND pos = :pos LIMIT 1"
                ),
                {"word": word, "pos": entry.pos_mapped},
            ).fetchone()

        if row:
            return row[0]

    return None


# ---------------------------------------------------------------------------
# Main import function
# ---------------------------------------------------------------------------


def import_profilo(
    conn: Connection,
    profilo_dir: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import Profilo CEFR level data into the cefr_levels table.

    Parses the four HTML files, computes per-level deltas, matches against
    lemmas, and inserts into cefr_levels. Idempotent: clears existing
    source='profilo' rows before inserting.

    Args:
        conn: SQLAlchemy connection.
        profilo_dir: Directory containing liste_lessicali_*.html files.
        progress_callback: Optional (current, total) callback for progress.

    Returns:
        Stats dict with counts and per-level breakdown.
    """
    # Parse all levels
    all_entries = _parse_all_levels(profilo_dir)

    # Clear existing profilo data (idempotent)
    cleared = conn.execute(text("DELETE FROM cefr_levels WHERE source = 'profilo'")).rowcount

    # Filter to matchable entries
    matchable = [e for e in all_entries if e.pos_mapped in MATCHABLE_POS and not e.is_multiword]

    total = len(matchable)
    matched_rows: list[dict[str, object]] = []
    skipped_multiword = sum(1 for e in all_entries if e.is_multiword)
    skipped_pos = sum(
        1 for e in all_entries if e.pos_mapped not in MATCHABLE_POS and not e.is_multiword
    )
    unmatched = 0

    for i, entry in enumerate(matchable):
        lemma_id = _match_entry(conn, entry)
        if lemma_id is not None:
            matched_rows.append(
                {
                    "lemma_id": lemma_id,
                    "level": entry.cefr_level,
                    "source": "profilo",
                    "source_word": entry.word,
                    "source_pos": entry.pos_raw,
                }
            )
        else:
            unmatched += 1

        if progress_callback and (i + 1) % 100 == 0:
            progress_callback(i + 1, total)

    # Batch insert
    if matched_rows:
        conn.execute(
            text("""
                INSERT INTO cefr_levels (lemma_id, level, source, source_word, source_pos)
                VALUES (:lemma_id, :level, :source, :source_word, :source_pos)
            """),
            matched_rows,
        )

    conn.commit()

    if progress_callback:
        progress_callback(total, total)

    # Per-level breakdown
    level_counts: dict[str, int] = {}
    for row in matched_rows:
        lvl = str(row["level"])
        level_counts[lvl] = level_counts.get(lvl, 0) + 1

    return {
        "total_entries": len(all_entries),
        "cleared": cleared,
        "matched": len(matched_rows),
        "unmatched": unmatched,
        "skipped_multiword": skipped_multiword,
        "skipped_pos": skipped_pos,
        "level_A1": level_counts.get("A1", 0),
        "level_A2": level_counts.get("A2", 0),
        "level_B1": level_counts.get("B1", 0),
        "level_B2": level_counts.get("B2", 0),
    }
