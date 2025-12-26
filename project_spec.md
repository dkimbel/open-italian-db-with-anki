# Italian Anki Deck Generator — Project Specification

## Overview

Build a Python system to generate Anki flashcard decks for learning Italian, using authoritative linguistic databases. The user is a native English speaker, beginner Italian learner, and software engineer.

---

## Data Sources

### Source Roles

| Need | Source | Notes |
|------|--------|-------|
| Orthographic conjugations | Wiktextract | With pedagogical stress marks (pàrlo) |
| Real Italian spelling | Morph-it! | Authoritative orthography without pedagogical marks |
| Definitions | Wiktextract | English glosses |
| Auxiliary (avere/essere) | Wiktextract | In forms array and categories |
| Transitivity | Wiktextract | Tagged in senses |
| IPA (infinitives only) | Wiktextract | ~63% coverage, includes ɛ/ɔ distinction |
| Frequency | ItWaC | 1.5B word web corpus (MIT license) |
| Example sentences | Tatoeba | With translations and audio links |

**Summary**: Wiktextract provides conjugations with stress marks for learning; Morph-it! provides authoritative real Italian spelling; ItWaC provides frequency data for prioritization; Tatoeba provides example sentences.

---

### Primary: Kaikki.org / Wiktextract
- **URL**: https://kaikki.org/dictionary/Italian/
- **Format**: JSONL (one entry per line)
- **Size**: 634 MB (Italian dictionary)
- **Content**: ~13,300+ verbs with:
  - Orthographic conjugated forms with pedagogical stress marks (pàrlo, parlàto)
  - Auxiliary verb (avere/essere) in `forms` array and categories
  - Transitivity tags
  - English definitions (glosses)
  - IPA for infinitive only (~63% coverage, includes ɛ/ɔ)
  - Example sentences (inconsistent coverage)
- **Also contains**: Nouns, adjectives, other POS (for future expansion)
- **License**: CC-BY-SA 3.0 + GFDL
- **Role**: Essential — provides conjugations with stress marks for learning

### Complementary: Morph-it!
- **URL**: https://docs.sslmit.unibo.it/doku.php?id=resources:morph-it
- **Format**: Tab-separated (form, lemma, morphological tags)
- **Size**: ~505,000 inflected forms
- **Content**: Real Italian orthography (no pedagogical stress marks)
- **License**: CC-BY-SA 2.0 + LGPL
- **Role**: Authoritative source for real written Italian spelling

### Frequency: ItWaC
- **URL**: https://github.com/franfranz/Word_Frequency_Lists_ITA
- **Format**: CSV (ISO-8859-1 encoding)
- **Size**: ~1.5 billion word corpus (web Italian)
- **Files**:
  - `itwac_verbs_lemmas_notail_2_1_0.csv`
  - `itwac_nouns_lemmas_notail_2_0_0.csv`
  - `itwac_adj_lemmas_notail_2_1_0.csv`
- **Columns**: Form, Freq, lemma, POS, mode, POS2, fpmw, Zipf
- **License**: MIT
- **Role**: Frequency data for verb prioritization

### Tertiary: Tatoeba
- **URL**: https://tatoeba.org/en/downloads (or per-language: downloads.tatoeba.org/exports/per_language/ita/)
- **Format**: TSV, headerless
- **Content**: ~951k Italian sentences, ~400k with English translations
- **Key files**:
  - `sentences.csv`: `sentence_id`, `lang`, `text`
  - `ita_eng_links.tsv`: `ita_sentence_id`, `eng_sentence_id` (Italian→English only)
  - `sentences_with_audio.csv`: `sentence_id`, `audio_id`, `username`, `license`, `attribution_url`
  - `tags.csv`: `sentence_id`, `tag_name` (sparse coverage)
- **Audio**: Only 1,591 Italian sentences have audio
- **License**: CC-BY 2.0 FR

### Audio: Kaikki.org bulk download
- **URL**: https://kaikki.org/dictionary/rawdata.html (audio section)
- **Size**: 20.4 GB total (all languages); Italian subset estimated 1-3 GB
- **Format**: Individual audio files, URLs referenced in JSONL entries
- **Action**: Download full dump, filter to Italian during ETL

### Nice-to-have: DiPI (Dizionario di Pronuncia Italiana)
- **URL**: https://www.dipionline.it/
- **Content**: Detailed pronunciation for conjugated verb forms
- **Status**: Not yet researched for format/downloadability; defer to later phase

### Validation only: mlconjug3
- **Install**: `pip install mlconjug3`
- **Use**: Cross-reference/validate conjugations, NOT as primary data source
- **GitHub**: https://github.com/Ars-Linguistica/mlconjug3

---

## Lemma Normalization

"Lemma" = dictionary/base form (infinitive for verbs). "Normalization" ensures matching across sources:

1. Lowercase
2. Strip accent marks (à→a, è→e, ì→i, ò→o, ù→u)
3. Consistent UTF-8 encoding

Example: `Mangiare` → `mangiare`, `reiterare` from both LeFFI and Wiktextract → same `lemma_id`

---

## ETL Pipeline

Data flows through a pipeline: Wiktextract provides lemmas, forms, and definitions → Morph-it! enriches forms with real Italian spelling (form_source="morphit") → Form-of fallback fills remaining gaps from Wiktionary (form_source="wiktionary") → ItWaC adds frequency data → Tatoeba links example sentences. Each step is idempotent and can be run with `task import-*` commands. Run `task stats` to see current database state.

---

## Anki Deck Targets

### Verb conjugations (primary)
- ALL tenses: presente, imperfetto, passato remoto, futuro, congiuntivo presente/imperfetto, condizionale, imperativo
- ALL persons: io, tu, lui/lei, noi, voi, loro
- ALL irregular verbs
- Card format: prompt conjugation, show IPA + audio

### Basic vocabulary
- Colors, numbers, months, days, common nouns
- Source: curated word lists → Tatoeba sentence lookup

### Common sentences/phrases
- Filtered from Tatoeba by length, has-translation, optionally has-audio

### Future decks (ideas)
- Prepositions and verb+preposition combos
- Noun gender patterns
- Pronouns (direct/indirect/combined)
- False friends
- Passato prossimo vs imperfetto contrast pairs
- Congiuntivo triggers
- Word families
- Minimal pairs (pronunciation)

---

## Download URLs

| Source | URL |
|--------|-----|
| Wiktextract | https://kaikki.org/dictionary/Italian/kaikki.org-dictionary-Italian.jsonl.gz |
| Morph-it! | https://docs.sslmit.unibo.it/lib/exe/fetch.php?media=resources:morph-it.tgz |
| ItWaC | https://github.com/franfranz/Word_Frequency_Lists_ITA |
| Tatoeba | https://tatoeba.org/en/downloads |
| Kaikki audio | https://kaikki.org/dictionary/rawdata.html (deferred)

---

## Nouns & Adjectives

Nouns and adjectives use the same pipeline as verbs:
- `lemmas.pos` = 'noun' or 'adjective'
- `noun_forms.gender` stores 'm' or 'f' per form (supports nouns like paio/paia that change gender in plural)
- Adjective gender is stored in form columns (gender is inflectional)

---

## Open Questions

1. **DiPI integration**: Format unknown; research later for per-form IPA
2. **Card templates**: Specific Anki note types TBD
3. **Frequency threshold**: Currently importing all; may want to prioritize by frequency for deck ordering
4. **Audio download**: Kaikki audio dump (20.4 GB) deferred to later phase
5. **CoLFIS frequency**: Download infrastructure currently unavailable; using ItWaC only
