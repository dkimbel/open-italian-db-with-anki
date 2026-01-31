#!/usr/bin/env python3
"""Analyze sentence coverage for morphological matching.

This script determines coverage for example sentences using the sentence_tokens
table (Stanza POS analysis) with English translation requirement.

Run this to inform the Phase 2 implementation strategy:
- How many top verbs x tense combos have matching sentences?
- How many top nouns/adjectives have matching sentences?

Usage:
    python scripts/sentence_coverage_analysis.py

Requirements:
    - Database must be populated with Tatoeba sentences and sentence_tokens
    - Run `task import-all` first (or at least tatoeba + sentence-tokens)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

# Database path
DB_PATH = Path("italian.db")

# Stanza mood/tense values we care about for verbs
VERB_MOOD_TENSE_COMBOS = [
    ("Ind", "Pres"),  # Present indicative
    ("Ind", "Imp"),  # Imperfect indicative
    ("Ind", "Past"),  # Passato remoto
    ("Ind", "Fut"),  # Future
    ("Sub", "Pres"),  # Present subjunctive
    ("Sub", "Imp"),  # Imperfect subjunctive
    ("Cnd", "Pres"),  # Conditional
    ("Imp", "Pres"),  # Imperative
]


@dataclass
class VerbCoverageResult:
    """Coverage result for a single verb x mood/tense combo."""

    lemma: str
    rank: int
    mood: str
    tense: str
    count_with_eng: int  # Sentences with English translation
    count_any: int  # All sentences (with or without translation)

    @property
    def has_sentence_with_eng(self) -> bool:
        return self.count_with_eng > 0

    @property
    def has_sentence_any(self) -> bool:
        return self.count_any > 0


@dataclass
class LemmaCoverageResult:
    """Coverage result for a single noun/adjective lemma."""

    lemma: str
    pos: str
    rank: int
    count_with_eng: int
    count_any: int

    @property
    def has_sentence_with_eng(self) -> bool:
        return self.count_with_eng > 0

    @property
    def has_sentence_any(self) -> bool:
        return self.count_any > 0


def check_database_ready(conn: Connection) -> bool:
    """Check if database has required tables populated."""
    checks = [
        ("sentences", "SELECT COUNT(*) FROM sentences WHERE lang = 'ita'"),
        ("translations", "SELECT COUNT(*) FROM translations"),
        ("sentence_tokens", "SELECT COUNT(*) FROM sentence_tokens"),
        (
            "frequencies (verbs)",
            "SELECT COUNT(*) FROM frequencies f JOIN lemmas l ON f.lemma_id = l.id WHERE l.pos = 'verb'",
        ),
    ]

    print("Checking database readiness...")
    all_ok = True
    for name, query in checks:
        try:
            count = conn.execute(text(query)).scalar() or 0
            status = "✓" if count > 0 else "✗"
            print(f"  {status} {name}: {count:,} rows")
            if count == 0:
                all_ok = False
        except Exception as e:
            print(f"  ✗ {name}: Error - {e}")
            all_ok = False

    return all_ok


def get_top_verbs(conn: Connection, limit: int = 100) -> list[tuple[str, int]]:
    """Get top N verbs by frequency rank.

    Returns list of (lemma, rank) tuples.
    """
    query = text("""
        SELECT l.written, f.freq_rank_in_pos
        FROM frequencies f
        JOIN lemmas l ON f.lemma_id = l.id
        WHERE l.pos = 'verb'
          AND f.freq_rank_in_pos IS NOT NULL
        ORDER BY f.freq_rank_in_pos
        LIMIT :limit
    """)
    rows = conn.execute(query, {"limit": limit}).fetchall()
    return [(row[0], row[1]) for row in rows]


def get_top_lemmas_by_pos(conn: Connection, pos: str, limit: int = 1000) -> list[tuple[str, int]]:
    """Get top N lemmas of a POS by frequency rank.

    Returns list of (lemma, rank) tuples.
    """
    query = text("""
        SELECT l.written, f.freq_rank_in_pos
        FROM frequencies f
        JOIN lemmas l ON f.lemma_id = l.id
        WHERE l.pos = :pos
          AND f.freq_rank_in_pos IS NOT NULL
        ORDER BY f.freq_rank_in_pos
        LIMIT :limit
    """)
    rows = conn.execute(query, {"pos": pos, "limit": limit}).fetchall()
    return [(row[0], row[1]) for row in rows]


def count_verb_sentences(conn: Connection, lemma: str, mood: str, tense: str) -> tuple[int, int]:
    """Count sentences with verb form matching lemma + mood + tense.

    Returns (count_with_english, count_any).
    """
    # Stanza UPOS for verbs can be VERB or AUX
    query_with_eng = text("""
        SELECT COUNT(DISTINCT st.sentence_id)
        FROM sentence_tokens st
        JOIN translations t ON t.ita_sentence_id = st.sentence_id
        WHERE st.lemma = :lemma
          AND st.upos IN ('VERB', 'AUX')
          AND st.mood = :mood
          AND st.tense = :tense
    """)
    query_any = text("""
        SELECT COUNT(DISTINCT st.sentence_id)
        FROM sentence_tokens st
        WHERE st.lemma = :lemma
          AND st.upos IN ('VERB', 'AUX')
          AND st.mood = :mood
          AND st.tense = :tense
    """)
    params = {"lemma": lemma, "mood": mood, "tense": tense}
    count_with_eng = conn.execute(query_with_eng, params).scalar() or 0
    count_any = conn.execute(query_any, params).scalar() or 0
    return (count_with_eng, count_any)


def count_lemma_sentences(conn: Connection, lemma: str, upos: str) -> tuple[int, int]:
    """Count sentences containing a lemma with matching UPOS.

    Returns (count_with_english, count_any).
    """
    query_with_eng = text("""
        SELECT COUNT(DISTINCT st.sentence_id)
        FROM sentence_tokens st
        JOIN translations t ON t.ita_sentence_id = st.sentence_id
        WHERE st.lemma = :lemma
          AND st.upos = :upos
    """)
    query_any = text("""
        SELECT COUNT(DISTINCT st.sentence_id)
        FROM sentence_tokens st
        WHERE st.lemma = :lemma
          AND st.upos = :upos
    """)
    params = {"lemma": lemma, "upos": upos}
    count_with_eng = conn.execute(query_with_eng, params).scalar() or 0
    count_any = conn.execute(query_any, params).scalar() or 0
    return (count_with_eng, count_any)


def analyze_verb_coverage(
    conn: Connection, verbs: list[tuple[str, int]]
) -> list[VerbCoverageResult]:
    """Analyze coverage for verbs x all mood/tense combos."""
    results: list[VerbCoverageResult] = []

    total = len(verbs) * len(VERB_MOOD_TENSE_COMBOS)
    processed = 0

    for lemma, rank in verbs:
        for mood, tense in VERB_MOOD_TENSE_COMBOS:
            count_with_eng, count_any = count_verb_sentences(conn, lemma, mood, tense)
            results.append(
                VerbCoverageResult(
                    lemma=lemma,
                    rank=rank,
                    mood=mood,
                    tense=tense,
                    count_with_eng=count_with_eng,
                    count_any=count_any,
                )
            )
            processed += 1
            if processed % 100 == 0:
                print(f"\r  Verbs: {processed}/{total}", end="", flush=True)

    print(f"\r  Verbs: {total}/{total} done")
    return results


def analyze_noun_adj_coverage(
    conn: Connection, lemmas: list[tuple[str, int]], pos: str, upos: str
) -> list[LemmaCoverageResult]:
    """Analyze coverage for nouns or adjectives."""
    results: list[LemmaCoverageResult] = []

    total = len(lemmas)
    for i, (lemma, rank) in enumerate(lemmas):
        count_with_eng, count_any = count_lemma_sentences(conn, lemma, upos)
        results.append(
            LemmaCoverageResult(
                lemma=lemma,
                pos=pos,
                rank=rank,
                count_with_eng=count_with_eng,
                count_any=count_any,
            )
        )
        if (i + 1) % 100 == 0:
            print(f"\r  {pos.title()}s: {i + 1}/{total}", end="", flush=True)

    print(f"\r  {pos.title()}s: {total}/{total} done")
    return results


def print_verb_coverage_summary(results: list[VerbCoverageResult]) -> None:
    """Print summary tables for verb coverage."""
    # Group by mood/tense
    by_mood_tense: dict[tuple[str, str], list[VerbCoverageResult]] = {}
    for r in results:
        key = (r.mood, r.tense)
        by_mood_tense.setdefault(key, []).append(r)

    print("\n" + "=" * 80)
    print("VERB COVERAGE BY MOOD/TENSE")
    print("=" * 80)

    print(f"\n{'Mood':<6} {'Tense':<6} {'With Eng':>10} {'Any':>10} {'Delta':>8}")
    print("-" * 45)

    for mood, tense in VERB_MOOD_TENSE_COMBOS:
        items = by_mood_tense.get((mood, tense), [])
        if not items:
            continue

        covered_eng = sum(1 for r in items if r.has_sentence_with_eng)
        covered_any = sum(1 for r in items if r.has_sentence_any)
        pct_eng = covered_eng * 100 / len(items) if items else 0
        pct_any = covered_any * 100 / len(items) if items else 0
        delta = pct_any - pct_eng

        print(f"{mood:<6} {tense:<6} {pct_eng:>9.1f}% {pct_any:>9.1f}% {delta:>+7.1f}%")

    # Coverage by frequency tier
    print("\n" + "-" * 80)
    print("VERB COVERAGE BY FREQUENCY TIER (Present Indicative)")
    print("-" * 80)

    pres_ind = [r for r in results if r.mood == "Ind" and r.tense == "Pres"]
    tiers = [
        ("Top 10", 1, 10),
        ("Top 50", 1, 50),
        ("Top 100", 1, 100),
    ]

    print(f"\n{'Tier':<12} {'With Eng':>10} {'Any':>10} {'Delta':>8}")
    print("-" * 45)

    for tier_name, min_rank, max_rank in tiers:
        tier_items = [r for r in pres_ind if min_rank <= r.rank <= max_rank]
        if not tier_items:
            continue
        covered_eng = sum(1 for r in tier_items if r.has_sentence_with_eng)
        covered_any = sum(1 for r in tier_items if r.has_sentence_any)
        pct_eng = covered_eng * 100 / len(tier_items) if tier_items else 0
        pct_any = covered_any * 100 / len(tier_items) if tier_items else 0
        delta = pct_any - pct_eng
        print(f"{tier_name:<12} {pct_eng:>9.1f}% {pct_any:>9.1f}% {delta:>+7.1f}%")

    # Show missing verbs in top 50 (with and without English)
    print("\n" + "-" * 80)
    print("MISSING VERBS (Top 50, Present Indicative)")
    print("-" * 80)

    missing_with_eng = [r for r in pres_ind if r.rank <= 50 and not r.has_sentence_with_eng]
    missing_any = [r for r in pres_ind if r.rank <= 50 and not r.has_sentence_any]

    print(f"\nMissing with English requirement: {len(missing_with_eng)}")
    if missing_with_eng:
        for r in missing_with_eng[:10]:
            extra = f" (has {r.count_any} w/o Eng)" if r.count_any > 0 else ""
            print(f"  #{r.rank}: {r.lemma}{extra}")
        if len(missing_with_eng) > 10:
            print(f"  ... and {len(missing_with_eng) - 10} more")

    print(f"\nMissing even without English requirement: {len(missing_any)}")
    if missing_any:
        for r in missing_any[:10]:
            print(f"  #{r.rank}: {r.lemma}")
        if len(missing_any) > 10:
            print(f"  ... and {len(missing_any) - 10} more")


def print_noun_adj_coverage_summary(
    noun_results: list[LemmaCoverageResult],
    adj_results: list[LemmaCoverageResult],
) -> None:
    """Print summary tables for noun/adjective coverage."""
    print("\n" + "=" * 80)
    print("NOUN/ADJECTIVE COVERAGE")
    print("=" * 80)

    tiers = [
        ("Top 100", 1, 100),
        ("Top 500", 1, 500),
        ("Top 1000", 1, 1000),
    ]

    for pos, results in [("noun", noun_results), ("adjective", adj_results)]:
        upos = "NOUN" if pos == "noun" else "ADJ"
        print(f"\n{pos.upper()}S (UPOS={upos})")
        print(f"{'Tier':<12} {'With Eng':>10} {'Any':>10} {'Delta':>8}")
        print("-" * 45)

        for tier_name, min_rank, max_rank in tiers:
            tier_items = [r for r in results if min_rank <= r.rank <= max_rank]
            if not tier_items:
                continue
            covered_eng = sum(1 for r in tier_items if r.has_sentence_with_eng)
            covered_any = sum(1 for r in tier_items if r.has_sentence_any)
            pct_eng = covered_eng * 100 / len(tier_items) if tier_items else 0
            pct_any = covered_any * 100 / len(tier_items) if tier_items else 0
            delta = pct_any - pct_eng
            print(f"{tier_name:<12} {pct_eng:>9.1f}% {pct_any:>9.1f}% {delta:>+7.1f}%")

    # Show some missing items
    for pos, results in [("noun", noun_results), ("adjective", adj_results)]:
        missing_eng = [r for r in results if r.rank <= 100 and not r.has_sentence_with_eng]
        if missing_eng:
            print(f"\nMissing {pos}s (top 100, with English): {len(missing_eng)}")
            for r in missing_eng[:10]:
                extra = f" (has {r.count_any} w/o Eng)" if r.count_any > 0 else ""
                print(f"  #{r.rank}: {r.lemma}{extra}")
            if len(missing_eng) > 10:
                print(f"  ... and {len(missing_eng) - 10} more")


def main() -> int:
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        print("Run `task import-all` first to populate the database.")
        return 1

    engine = create_engine(f"sqlite:///{DB_PATH}")

    with engine.connect() as conn:
        if not check_database_ready(conn):
            print("\nDatabase is not ready. Please run `task import-all` first.")
            return 1

        print("\n" + "=" * 70)
        print("ANALYZING SENTENCE COVERAGE")
        print("=" * 70)

        # Analyze verbs
        print("\nFetching top 100 verbs...")
        verbs = get_top_verbs(conn, limit=100)
        print(f"  Found {len(verbs)} verbs with frequency data")

        print("\nAnalyzing verb coverage (100 verbs x 8 tenses)...")
        verb_results = analyze_verb_coverage(conn, verbs)

        # Analyze nouns
        print("\nFetching top 1000 nouns...")
        nouns = get_top_lemmas_by_pos(conn, "noun", limit=1000)
        print(f"  Found {len(nouns)} nouns with frequency data")

        print("\nAnalyzing noun coverage...")
        noun_results = analyze_noun_adj_coverage(conn, nouns, "noun", "NOUN")

        # Analyze adjectives
        print("\nFetching top 1000 adjectives...")
        adjectives = get_top_lemmas_by_pos(conn, "adjective", limit=1000)
        print(f"  Found {len(adjectives)} adjectives with frequency data")

        print("\nAnalyzing adjective coverage...")
        adj_results = analyze_noun_adj_coverage(conn, adjectives, "adjective", "ADJ")

        # Print summaries
        print_verb_coverage_summary(verb_results)
        print_noun_adj_coverage_summary(noun_results, adj_results)

    return 0


if __name__ == "__main__":
    sys.exit(main())
