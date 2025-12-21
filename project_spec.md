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
  - `links.csv`: `sentence_id`, `translation_id` (bidirectional)
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

## Database Schema

### Core tables

```sql
-- Master lemma table
CREATE TABLE lemmas (
    lemma_id INTEGER PRIMARY KEY AUTOINCREMENT,
    lemma TEXT NOT NULL UNIQUE,       -- normalized (lowercase, no accents)
    lemma_stressed TEXT NOT NULL,     -- with stress mark (e.g., "parlàre")
    pos TEXT DEFAULT 'verb',
    auxiliary TEXT,                   -- 'avere', 'essere', 'both', NULL
    transitivity TEXT,                -- 'transitive', 'intransitive', 'both', NULL
    ipa TEXT                          -- infinitive IPA from Wiktextract
);

-- Frequency data from corpora (separate table for versioning)
CREATE TABLE frequencies (
    lemma_id INTEGER NOT NULL,
    corpus TEXT NOT NULL,             -- 'itwac', 'colfis'
    freq_raw INTEGER,                 -- raw count
    freq_zipf REAL,                   -- zipf score (normalized)
    corpus_version TEXT,              -- e.g., '2.1.0', '2024-01'
    PRIMARY KEY (lemma_id, corpus),
    FOREIGN KEY (lemma_id) REFERENCES lemmas(lemma_id)
);

-- Inflected forms (verbs, nouns, adjectives)
CREATE TABLE forms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lemma_id INTEGER NOT NULL,
    form TEXT,                        -- real Italian spelling from Morph-it! (NULL if not found)
    form_stressed TEXT NOT NULL,      -- pedagogical with stress marks from Wiktextract
    tags TEXT NOT NULL,               -- JSON array
    FOREIGN KEY (lemma_id) REFERENCES lemmas(lemma_id)
);

-- Lookup table for matching forms in sentences
CREATE TABLE form_lookup (
    form_normalized TEXT NOT NULL,    -- accent-stripped lowercase (puo, subito, parlo)
    form_id INTEGER NOT NULL,
    PRIMARY KEY (form_normalized, form_id),
    FOREIGN KEY (form_id) REFERENCES forms(id)
);

-- English definitions
CREATE TABLE definitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lemma_id INTEGER NOT NULL,
    gloss TEXT NOT NULL,
    tags TEXT,                        -- JSON array (e.g., ["transitive"])
    FOREIGN KEY (lemma_id) REFERENCES lemmas(lemma_id)
);

-- Tatoeba sentences
CREATE TABLE sentences (
    sentence_id INTEGER PRIMARY KEY,
    lang TEXT NOT NULL,               -- 'ita' or 'eng'
    text TEXT NOT NULL
);

-- Translation links
CREATE TABLE translations (
    ita_sentence_id INTEGER,
    eng_sentence_id INTEGER,
    PRIMARY KEY (ita_sentence_id, eng_sentence_id)
);

-- Sentence-to-lemma linking (for frequency + examples)
CREATE TABLE sentence_verbs (
    sentence_id INTEGER,
    lemma_id INTEGER,
    form_found TEXT,                  -- the conjugated form matched
    PRIMARY KEY (sentence_id, lemma_id),
    FOREIGN KEY (sentence_id) REFERENCES sentences(sentence_id),
    FOREIGN KEY (lemma_id) REFERENCES lemmas(lemma_id)
);
```

### Indexes

```sql
CREATE INDEX idx_forms_lemma ON forms(lemma_id);
CREATE INDEX idx_forms_form ON forms(form);
CREATE INDEX idx_form_lookup_form_id ON form_lookup(form_id);
CREATE INDEX idx_definitions_lemma ON definitions(lemma_id);
CREATE INDEX idx_frequencies_lemma ON frequencies(lemma_id);
CREATE INDEX idx_sentences_lang ON sentences(lang);
CREATE INDEX idx_sentence_verbs_lemma ON sentence_verbs(lemma_id);
```

---

## Lemma Normalization

"Lemma" = dictionary/base form (infinitive for verbs). "Normalization" ensures matching across sources:

1. Lowercase
2. Strip accent marks (à→a, è→e, ì→i, ò→o, ù→u)
3. Consistent UTF-8 encoding

Example: `Mangiare` → `mangiare`, `reiterare` from both LeFFI and Wiktextract → same `lemma_id`

---

## ETL Workflow

### Phase 1: Download (complete)
- `data/wiktextract/kaikki.org-dictionary-Italian.jsonl` — CC-BY-SA + GFDL
- `data/morphit/morph-it.txt` — CC-BY-SA 2.0 + LGPL
- `data/itwac/*.csv` — MIT
- `data/tatoeba/*.tsv` — CC-BY 2.0 FR

### Phase 2: Import Wiktextract
1. Stream JSONL, filter to `pos == 'verb'`
2. For each entry:
   - Insert into `lemmas` (normalized lemma + stressed form) with auxiliary, transitivity, IPA
   - Insert `forms` from `forms` array (form_stressed from Wiktextract)
   - Insert `definitions` from `senses` array
3. Build initial `form_lookup` from normalized forms

### Phase 3: Enrich with Morph-it!
1. Parse `morph-it.txt` into a lookup dict (normalized → real spelling)
2. Update `forms.form` with real Italian spelling where available
3. Update `form_lookup` with Morph-it! normalized forms

### Phase 4: Import ItWaC frequencies
1. Parse CSV files (convert ISO-8859-1 → UTF-8)
2. Match lemmas by normalized form
3. Insert into `frequencies` table with corpus version

### Phase 5: Import Tatoeba
1. Import Italian + English sentences into `sentences`
2. Import `links.csv` into `translations`
3. Tokenize Italian sentences, match against `form_lookup`
4. Insert matches into `sentence_verbs`

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

## Key Libraries

```
pip install mlconjug3 genanki requests
```

- **genanki**: Generate .apkg files programmatically
- **mlconjug3**: Validation/cross-reference only
- **sqlite3**: Built-in Python

---

## Data Directory Structure

```
data/
├── wiktextract/
│   ├── kaikki.org-dictionary-Italian.jsonl
│   └── LICENSE                    # CC-BY-SA + GFDL
├── morphit/
│   ├── morph-it.txt
│   └── LICENSE                    # CC-BY-SA 2.0 + LGPL
├── itwac/
│   ├── itwac_verbs_lemmas_notail_2_1_0.csv
│   ├── itwac_nouns_lemmas_notail_2_0_0.csv
│   ├── itwac_adj_lemmas_notail_2_1_0.csv
│   └── LICENSE                    # MIT
└── tatoeba/
    ├── ita_sentences.tsv
    ├── eng_sentences.tsv
    ├── links.csv
    ├── sentences_with_audio.csv
    └── LICENSE                    # CC-BY 2.0 FR
```

## Download URLs

| Source | URL |
|--------|-----|
| Wiktextract | https://kaikki.org/dictionary/Italian/kaikki.org-dictionary-Italian.jsonl.gz |
| Morph-it! | https://docs.sslmit.unibo.it/lib/exe/fetch.php?media=resources:morph-it.tgz |
| ItWaC | https://github.com/franfranz/Word_Frequency_Lists_ITA |
| Tatoeba | https://tatoeba.org/en/downloads |
| Kaikki audio | https://kaikki.org/dictionary/rawdata.html (deferred)

---

## Future: Nouns & Adjectives

Schema already supports via:
- `lemmas.pos` = 'noun' or 'adjective'
- `lemmas.gender` for nouns
- `wiktextract_forms` handles plurals, feminine forms via same tag structure
- `definitions` and `pronunciations` work unchanged

Wiktextract JSONL contains all POS — just filter differently during import.

---

## Open Questions / Deferred

1. **DiPI integration**: Format unknown; research later for per-form IPA
2. **Card templates**: Specific Anki note types TBD
3. **Frequency threshold**: Currently importing all; may want to prioritize by frequency for deck ordering
4. **Audio download**: Kaikki audio dump (20.4 GB) deferred to later phase
5. **CoLFIS frequency**: Download infrastructure currently unavailable; using ItWaC only
