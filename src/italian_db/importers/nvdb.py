"""Import NVdB (Nuovo Vocabolario di Base) usage tier data.

Parses the NVdB HTML file (De Mauro, 2016) which classifies ~7,500 Italian
words into three usage tiers based on formatting:
  - <b>bold</b>: FO (fondamentale, ~2,000 words)
  - plain text: AU (alto uso, ~2,750 words)
  - <i>italic</i>: AD (alta disponibilità, ~2,300 words)

Each word is matched against our lemmas table to assign a usage tier.
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


# POS abbreviation mapping: NVdB abbreviation -> our POS system
POS_MAP: dict[str, str | None] = {
    # Verbs
    "v.tr.": "verb",
    "v.intr.": "verb",
    "v.pronom.intr.": "verb",
    "v.pronom.tr.": "verb",
    # Nouns
    "s.m.": "noun",
    "s.f.": "noun",
    "s.m.inv.": "noun",
    "s.f.inv.": "noun",
    # Adjectives
    "agg.": "adjective",
    # Non-matchable POS (tracked for completeness)
    "avv.": "adverb",
    "prep.": "preposition",
    "cong.": "conjunction",
    "pron.": "pronoun",
    "art.": "article",
    "inter.": "interjection",
    "p.pres.": "participle",
    "p.pass.": "participle",
    "num.": "numeral",
}

# POS values that can match our database
MATCHABLE_POS = {"verb", "noun", "adjective"}

# Regex to parse <p> entries: detects bold (FO), italic (AD), or plain (AU)
_ENTRY_RE = re.compile(
    r"<p>"
    r"(?:<b>(.*?)\s*</b>|<i>(.*?)\s*</i>|(.*?))"  # word in bold, italic, or plain
    r"\s*"
    r"((?:[a-z]|\.|\s|,|inv)+\.)"  # POS string ending with period
    r",?\s*</p>",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class NvdbEntry:
    """A single word entry from the NVdB word list."""

    word: str  # The word as found in HTML
    clean_word: str  # Normalized for DB matching
    pos_raw: str  # Raw POS string from HTML (e.g., "v.tr.", "s.m. e f.")
    pos_mapped: str | None  # Our POS mapping (verb/noun/adjective/etc.) or None
    tier: str  # FO, AU, AD
    is_multiword: bool = False  # Contains spaces (multiword expression)
    has_reflexive: bool = False  # Word has reflexive -si form


# ---------------------------------------------------------------------------
# Parsing functions
# ---------------------------------------------------------------------------


def _parse_entries(html: str) -> list[tuple[str, str, str]]:
    """Parse <p> tags from NVdB HTML, detecting tier from formatting.

    Returns list of (word, pos_raw, tier) tuples.

    Tier detection:
    - <b>word</b> → FO (fondamentale)
    - <i>word</i> → AD (alta disponibilità)
    - plain text → AU (alto uso)
    """
    entries: list[tuple[str, str, str]] = []

    for line in html.splitlines():
        line = line.strip()
        if not line.startswith("<p>") or not line.endswith("</p>"):
            continue

        # Extract the content between <p> and </p>
        content = line[3:-4].strip()
        if not content:
            continue

        # Determine tier from formatting
        if content.startswith("<b>") and "</b>" in content:
            tier = "FO"
            # Extract word from <b>word</b>
            b_end = content.index("</b>")
            word = content[3:b_end].strip()
            pos_part = content[b_end + 4 :].strip()
        elif content.startswith("<i>") and "</i>" in content:
            tier = "AD"
            # Extract word from <i>word</i>
            i_end = content.index("</i>")
            word = content[3:i_end].strip()
            pos_part = content[i_end + 4 :].strip()
        else:
            tier = "AU"
            # Plain text: word and POS together
            # Split on first POS-like pattern
            word, pos_part = _split_word_pos(content)

        if not word or not pos_part:
            continue

        # Clean trailing comma from POS
        pos_raw = pos_part.rstrip(",").strip()
        if not pos_raw:
            continue

        entries.append((word, pos_raw, tier))

    return entries


def _split_word_pos(text_content: str) -> tuple[str, str]:
    """Split a plain-text entry into (word, pos_part).

    Handles cases like:
    - "abbandono s.m.," → ("abbandono", "s.m.,")
    - "accederev.intr.," → ("accedere", "v.intr.,") — no space between word and POS
    """
    # Try splitting on known POS prefixes (handles no-space cases like "accederev.intr.")
    pos_prefixes = [
        "v.tr.",
        "v.intr.",
        "v.pronom.",
        "s.m.",
        "s.f.",
        "agg.",
        "avv.",
        "prep.",
        "cong.",
        "pron.",
        "art.",
        "inter.",
        "p.pres.",
        "p.pass.",
        "num.",
    ]

    for prefix in pos_prefixes:
        idx = text_content.find(prefix)
        if idx > 0:
            word = text_content[:idx].strip()
            pos_part = text_content[idx:].strip()
            return word, pos_part

    # Fallback: split on first space before a known POS pattern
    return text_content, ""


def _clean_word(word: str) -> tuple[str, bool, bool]:
    """Clean an NVdB word for database matching.

    Handles patterns like:
    - "amico/a" -> "amico" (gender variant)
    - "chiamare/si" -> "chiamare" (reflexive variant)
    - "aereo(aeroplano)" -> "aereo" (expanded form)

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
    """Map a raw NVdB POS string to our POS system.

    NVdB uses comma-separated compound POS (e.g., "p.pres., agg., s.m.").
    We try each component in order and return the first matchable one
    (verb/noun/adjective). If none are matchable, return the first mapped one.

    NVdB also uses "e" as a separator: "s.m. e f.", "v.intr. e tr."
    """
    # Split on comma to handle compound POS like "p.pres., agg., s.m."
    parts = [p.strip() for p in pos_raw.split(",") if p.strip()]

    first_mapped: str | None = None

    for part in parts:
        # Normalize multiple spaces
        part = re.sub(r"\s+", " ", part)

        # Handle "e" separator: "s.m. e f." → take "s.m."
        if " e " in part:
            sub_parts = part.split(" e ")
            part = sub_parts[0].strip()

        mapped: str | None = None

        # Direct lookup
        if part in POS_MAP:
            mapped = POS_MAP[part]
        # Try adding trailing period
        elif not part.endswith("."):
            with_period = part + "."
            if with_period in POS_MAP:
                mapped = POS_MAP[with_period]

        if mapped is not None:
            if first_mapped is None:
                first_mapped = mapped
            # Prefer matchable POS
            if mapped in MATCHABLE_POS:
                return mapped

    return first_mapped


def _map_all_matchable_pos(pos_raw: str) -> list[str]:
    """Return all matchable POS values from a compound POS string.

    Splits on comma (NVdB's compound delimiter), handles "e" separator
    within each component (takes left side), maps via POS_MAP, and returns
    the deduplicated list of values in MATCHABLE_POS.

    Examples:
        "p.pres., agg., s.m." → ["adjective", "noun"]
        "v.intr. e tr." → ["verb"] (single component, "e" is modifier)
        "avv., inter." → [] (neither matchable)
    """
    parts = [p.strip() for p in pos_raw.split(",") if p.strip()]

    seen: set[str] = set()
    result: list[str] = []

    for part in parts:
        part = re.sub(r"\s+", " ", part)

        # Handle "e" separator: "s.m. e f." → take "s.m."
        if " e " in part:
            sub_parts = part.split(" e ")
            part = sub_parts[0].strip()

        mapped: str | None = None
        if part in POS_MAP:
            mapped = POS_MAP[part]
        elif not part.endswith("."):
            with_period = part + "."
            if with_period in POS_MAP:
                mapped = POS_MAP[with_period]

        if mapped is not None and mapped in MATCHABLE_POS and mapped not in seen:
            seen.add(mapped)
            result.append(mapped)

    return result


def _parse_all_entries(nvdb_path: Path) -> list[NvdbEntry]:
    """Parse the NVdB HTML file and return deduplicated entries.

    Deduplication: keyed on (clean_word_lower, pos_mapped).
    Unlike Profilo, NVdB tiers are non-overlapping (no cumulative lists),
    so the first occurrence wins for any duplicates.

    Args:
        nvdb_path: Path to the nvdb.html file.

    Returns:
        List of deduplicated NvdbEntry objects.
    """
    if not nvdb_path.exists():
        raise FileNotFoundError(f"NVdB HTML file not found: {nvdb_path}")

    html = nvdb_path.read_text(encoding="utf-8")
    raw_entries = _parse_entries(html)

    # Deduplicate: (clean_word_lower, pos_mapped) -> first entry wins
    seen: dict[tuple[str, str | None], NvdbEntry] = {}

    for word, pos_raw, tier in raw_entries:
        clean, is_multi, has_refl = _clean_word(word)

        # Explode compound POS into one entry per matchable POS
        matchable_list = _map_all_matchable_pos(pos_raw)
        if matchable_list:
            for pos in matchable_list:
                key = (clean.lower(), pos)
                if key not in seen:
                    seen[key] = NvdbEntry(
                        word=word,
                        clean_word=clean,
                        pos_raw=pos_raw,
                        pos_mapped=pos,
                        tier=tier,
                        is_multiword=is_multi,
                        has_reflexive=has_refl,
                    )
        else:
            # No matchable POS — create entry with first mapped POS for stats
            pos = _map_pos(pos_raw)
            key = (clean.lower(), pos)
            if key not in seen:
                seen[key] = NvdbEntry(
                    word=word,
                    clean_word=clean,
                    pos_raw=pos_raw,
                    pos_mapped=pos,
                    tier=tier,
                    is_multiword=is_multi,
                    has_reflexive=has_refl,
                )

    return list(seen.values())


# ---------------------------------------------------------------------------
# Database matching
# ---------------------------------------------------------------------------


def _match_entry(conn: Connection, entry: NvdbEntry) -> int | None:
    """Try to match an NvdbEntry against the lemmas table.

    Matching strategy (same as Profilo):
    1. Exact match on (written, pos)
    2. Case-insensitive match
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


def import_nvdb(
    conn: Connection,
    nvdb_path: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """Import NVdB usage tier data into the nvdb_tiers table.

    Parses the HTML file, matches against lemmas, and inserts into nvdb_tiers.
    Idempotent: clears existing nvdb_tiers rows before inserting.

    Args:
        conn: SQLAlchemy connection.
        nvdb_path: Path to nvdb.html file.
        progress_callback: Optional (current, total) callback for progress.

    Returns:
        Stats dict with counts and per-tier breakdown.
    """
    # Parse all entries
    all_entries = _parse_all_entries(nvdb_path)

    # Clear existing data (idempotent)
    cleared = conn.execute(text("DELETE FROM nvdb_tiers")).rowcount

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
                    "tier": entry.tier,
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
                INSERT INTO nvdb_tiers (lemma_id, tier, source_word, source_pos)
                VALUES (:lemma_id, :tier, :source_word, :source_pos)
            """),
            matched_rows,
        )

    conn.commit()

    if progress_callback:
        progress_callback(total, total)

    # Per-tier breakdown
    tier_counts: dict[str, int] = {}
    for row in matched_rows:
        t = str(row["tier"])
        tier_counts[t] = tier_counts.get(t, 0) + 1

    return {
        "total_entries": len(all_entries),
        "cleared": cleared,
        "matched": len(matched_rows),
        "unmatched": unmatched,
        "skipped_multiword": skipped_multiword,
        "skipped_pos": skipped_pos,
        "tier_FO": tier_counts.get("FO", 0),
        "tier_AU": tier_counts.get("AU", 0),
        "tier_AD": tier_counts.get("AD", 0),
    }
