# Data Sources

This directory contains linguistic data from multiple sources, each with its own license.

## Licenses

Licenses for all the data described here can be found in `data-licenses`.

## Downloading Data

All data can be downloaded programmatically using the provided tasks:

```bash
# Download all data sources (~2.5 GB total)
task download-all

# Or download individual sources:
task download-wiktextract     # Italian dictionary (634 MB)
task download-tatoeba         # Sentences and links (660 MB)
task download-opensubtitles   # OpenSubtitles v2024 parallel sentences (~1.8 GB zip)
task download-profilo         # Profilo CEFR word lists (4 small HTML files)

# Force re-download (even if files exist):
task download-all FORCE=1
```

After downloading, run the import pipeline:

```bash
# Import all parts of speech (verb, noun, adjective)
task import-all

# Or import a single part of speech
task import-all POS=verb
```

---

## Import Pipeline

The import runs in stages for each part of speech (verb, noun, adjective):

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         WIKTEXTRACT DATA (620k entries)                     │
│                                                                             │
│  Contains TWO types of entries:                                             │
│                                                                             │
│  LEMMA ENTRIES (imported)          FORM-OF ENTRIES (skipped*)               │
│  ┌─────────────────────────┐       ┌─────────────────────────┐              │
│  │ word: "parlare"         │       │ word: "parlo"           │              │
│  │ forms: [                │       │ senses: [{              │              │
│  │   {form: "pàrlo", ...}  │       │   form_of: "parlare",   │              │
│  │   {form: "pàrli", ...}  │       │   tags: ["1st-person"]  │              │
│  │   ... (66 forms)        │       │ }]                      │              │
│  │ ]                       │       │                         │              │
│  │ senses: [{gloss: ...}]  │       │ * Re-scanned in Step 2  │              │
│  └─────────────────────────┘       └─────────────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
    ┌─────────────────────────────────┴─────────────────────────────────┐
    │                                                                   │
    ▼                                                                   ▼
┌─────────────────────────────────┐     ┌─────────────────────────────────────┐
│ STEP 1: Import from Wiktextract │     │ "Skipped" count explained:          │
├─────────────────────────────────┤     │                                     │
│                                 │     │ Verbs:      387k skipped (97%)      │
│ • Extract LEMMA entries only    │     │   → ~50-100 conjugated forms/verb   │
│ • Each lemma has a forms array  │     │                                     │
│ • Forms have stress marks:      │     │ Nouns:       62k skipped (52%)      │
│   "pàrlo", "parlàre", etc.      │     │   → plurals, gender variants        │
│                                 │     │                                     │
│ Creates:                        │     │ Adjectives:  55k skipped (73%)      │
│ • lemmas table                  │     │   → gender/number agreement forms   │
│ • verb_forms (form_stressed)    │     │                                     │
│ • definitions                   │     │ These are expected! We only want    │
│                                 │     │ one entry per word (the lemma).     │
└─────────────────────────────────┘     └─────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: Enrich from Form-of Entries (written_source = "wiktionary")         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Single pass through form-of entries extracting TWO types of data:           │
│                                                                             │
│ A) WRITTEN FORMS - The `word` field of form-of entries has the correct      │
│   written spelling. Example: entry for "parlo" with form_of="parlare"       │
│   tells us the written form of stressed "pàrlo" is "parlo".                 │
│                                                                             │
│   Wiktextract provides STRESSED forms: "pàrlo", "parlàto", "parlerà"        │
│   Form-of entries provide WRITTEN forms: "parlo", "parlato", "parlerà"      │
│                                                                             │
│   The difference: stress marks (for pronunciation) vs actual written        │
│   Italian. Future tense (-rà) and passato remoto (-ò) keep accents.         │
│                                                                             │
│ B) USAGE LABELS - For forms with special tags:                              │
│   "fo"  → form_of: "fare", tags: ["literary", "regional"]                   │
│   "diè" → form_of: "dare", tags: ["archaic"]                                │
│   Only ~0.1% of form-of entries have labels (e.g., 226 of 353k for verbs)   │
│                                                                             │
│ NOTE: VERBS derive spelling using Italian orthography rules during lemma    │
│ enrichment (Step 3), since form-of matching for verbs is less reliable.     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 2.5: Import CEFR Levels (Profilo) [optional]                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Profilo della lingua italiana (Spinelli & Parizzi, 2010):                  │
│   - ~2,700 unique entries across A1-B2 CEFR levels                         │
│   - Expert-curated levels (not corpus-frequency derived)                   │
│   - Matches ~1,800+ lemmas in our database (verbs, nouns, adjectives)      │
│   - Skips multiword expressions and non-matchable POS                      │
│                                                                             │
│ Cumulative lists are converted to per-level deltas: each word gets its     │
│ lowest CEFR level (A1 word appearing in A2 list stays A1).                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: Import Sentences (Tatoeba + OpenSubtitles)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Tatoeba: ~950,000 Italian sentences with English translations.              │
│   - CK whitelist filtering for quality                                      │
│   - Tag-based filtering for tense matching                                  │
│   - Preferred source for example sentences                                  │
│                                                                             │
│ OpenSubtitles v2024: ~5M Italian sentences with English translations.       │
│   - OPUS parallel corpus, Moses format                                      │
│   - Preprocessed: deduped, cleaned, sampled during download                 │
│   - Conversational vocabulary from movie subtitles                          │
│                                                                             │
│ Both use FTS5 for full-text search.                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: Stanza POS Tagging → Sentence Tokens                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Stanza NLP pipeline tags every sentence with token-level annotations:       │
│   - Lemma, UPOS, morphological features (mood, tense, person, number)      │
│   - Dependency parsing (head, deprel) for compound tense resolution         │
│                                                                             │
│ Enables:                                                                    │
│   - Lemma-based sentence search for example sentences                       │
│   - Grammatical feature filtering (find sentences with specific tenses)     │
│   - Unified frequency computation across all POS                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 5: Compute Frequency Data (from Stanza tokens)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Derives word frequency from Stanza-parsed sentence tokens:                  │
│                                                                             │
│ 1. Count tokens by (lemma, UPOS) across all sentence_tokens                │
│ 2. Map UPOS to POS: VERB/AUX → verb, NOUN → noun, ADJ → adjective         │
│ 3. Match Stanza lemmas to our lemmas table                                  │
│ 4. Compute Zipf scores: log10(freq_per_million) + 3                         │
│ 5. Rank within each POS using DENSE_RANK                                    │
│                                                                             │
│ Advantages over pre-computed frequency lists:                               │
│   - Accurate lemmatization for verbs (no surface form collisions)           │
│   - Consistent frequency data across all POS from same corpora              │
│   - Conversational vocabulary from OpenSubtitles                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Why Different POS Use Different Accent Logic

Italian has two types of accents in learning materials:
1. **Required orthographic accents**: Must appear in standard written Italian (e.g., `città`, `parlò`, `perché`)
2. **Pedagogical stress marks**: Show pronunciation but are stripped for normal writing (e.g., `pàrlo` → `parlo`)

### All POS: Form-of entries provide written forms

Wiktextract's form-of entries have a `word` field that contains the correct written form.
For example, the form-of entry for "parlo" (with `form_of: "parlare"`) tells us that
the stressed form "pàrlo" should be written as "parlo".

This approach works for all parts of speech because form-of entries are generated
from the same Wiktionary data that provides the stressed forms, ensuring consistency.

### Verbs: Derive from Wiktextract stressed forms

For verbs, we derive `written` forms directly from Wiktextract's `stressed` values
using Italian orthography rules during the lemma enrichment phase. This is because
verb conjugations are regular enough that the rules apply reliably.

### Nouns and Adjectives: Form-of entries + fallback derivation

For nouns and adjectives:
1. First try form-of entry enrichment (provides written form from `word` field)
2. Then apply Italian orthography rules as a fallback for remaining NULL written values

### Loanword Handling

The orthography derivation algorithm strips non-final accents assuming they're
pedagogical stress marks. However, loanwords from other languages may have
orthographic accents that should be preserved.

**Why only French needs a whitelist:**

| Language | Accent type | In ACCENTED_CHARS? | Example | Handling |
|----------|-------------|-------------------|---------|----------|
| French | é, è, ê, â | Yes (overlap) | rétro | Whitelist |
| German | ö, ü | No | föhn | Pass-through |
| Portuguese | ç, ã | No | Maracanaço | Pass-through |
| Spanish | á, é, í | Yes, but final | colibrì | Final → preserved |
| English | (pedagogical) | Yes | bàrista | Stripped (correct) |

French is the only language where:
1. Accents overlap with Italian accent characters (é, è match àèéìòóù)
2. Accents appear in non-final position (rétro, éclair, élite)
3. Accents are orthographic (part of correct spelling), not pedagogical

The whitelist contains ~40 French loanwords identified from the complete
Wiktextract Italian dictionary. See `FRENCH_LOANWORD_WHITELIST` in
`src/italian_db/normalize.py` for the full list and detailed documentation.

---

## Wiktextract (data/wiktextract/)

**Source:** Kaikki.org Italian dictionary extract from Wiktionary
**URL:** https://kaikki.org/dictionary/Italian/
**Downloaded:** `kaikki.org-dictionary-Italian.jsonl.gz`
**License:** CC-BY-SA 3.0 + GFDL (dual-licensed, your choice)
**Citation:**
> Tatu Ylonen: Wiktextract: Wiktionary as Machine-Readable Structured Data,
> Proceedings of the 13th Conference on Language Resources and Evaluation (LREC),
> pp. 1317-1325, Marseille, 20-25 June 2022.

**Sample Data:**
```jsonl
// Lemma entry - has forms array with stress marks
{"word": "parlare", "pos": "verb", "forms": [
  {"form": "parlàre", "tags": ["canonical"]},
  {"form": "pàrlo", "tags": ["first-person", "present", "singular"]}
], "senses": [{"glosses": ["to speak, to talk"]}]}

// Form-of entry - links to lemma, provides written spelling
{"word": "parlo", "pos": "verb", "senses": [{
  "tags": ["first-person", "form-of", "present", "singular"],
  "form_of": [{"word": "parlare"}]
}]}
```

## OpenSubtitles v2024 Parallel Sentences (data/opensubtitles/)

**Source:** OPUS OpenSubtitles v2024 parallel corpus
**URL:** https://opus.nlpl.eu/OpenSubtitles/v2024/en-it
**Corpus:** Italian-English parallel sentences from movie/TV subtitles
**License:** Attribution (cite the original source)
**Citation:**
> P. Lison and J. Tiedemann, 2016, OpenSubtitles2016: Extracting Large Parallel Corpora
> from Movie and TV Subtitles. In Proceedings of LREC 2016.

**Files** (generated by `download_opensubtitles()`):
- `it_sentences.tsv` - Italian sentences (Tatoeba-compatible format)
- `en_sentences.tsv` - English sentences (Tatoeba-compatible format)
- `links.tsv` - Translation links (1:1 line-aligned pairs)

**Processing during download:**
1. Download Moses-format zip (~1.8 GB) containing line-aligned en/it files
2. Clean: strip HTML tags, remove bracketed/parenthesized annotations, normalize whitespace
3. Filter: skip lines < 3 chars or > 500 chars
4. Deduplicate by Italian text (MD5 hash, keep first occurrence)
5. Sample ~5M pairs (deterministic seed=42)
6. Output TSV in Tatoeba-compatible format (1-indexed line numbers as sentence IDs)

**Why OpenSubtitles for frequency + example sentences:**
OpenSubtitles represents conversational vocabulary much better than formal web corpora.
Combined with Tatoeba and Stanza POS tagging, it provides accurate lemmatized frequency
data for all parts of speech, solving the surface form collision problem.

## Tatoeba (data/tatoeba/)

**Source:** Tatoeba sentence corpus
**URL:** https://tatoeba.org/en/downloads
**License:** CC-BY 2.0 FR (some sentences CC0)
**Files:**
- `ita_sentences.tsv` - Italian sentences
- `eng_sentences.tsv` - English sentences
- `ita_eng_links.tsv` - Translation links between Italian-English sentences
- `sentences_with_audio.csv` - Sentences with audio recordings

**Sample Data:**
```tsv
# ita_sentences.tsv (id, lang, text)
4369	ita	Devo andare a dormire.

# eng_sentences.tsv
1277	eng	I have to go to sleep.

# ita_eng_links.tsv (ita_id, eng_id)
4369	1277
```

## Profilo della lingua italiana (data/profilo/)

**Source:** Profilo della lingua italiana - livelli di riferimento del QCER
**URL:** https://www.unistrapg.it/profilo_lingua_italiana/
**Authors:** Barbara Spinelli and Francesca Parizzi (2010)
**Publisher:** La Nuova Italia / RCS Libri (ISBN 978-88-221-6579-1)
**License:** No explicit open license (published book; word lists hosted publicly)
**Files:** 4 HTML pages (A1, A2, B1, B2 cumulative word lists)

Expert-curated CEFR level assignments for ~2,700 Italian words. Unlike
corpus-frequency-derived approaches (like KELLY), these levels reflect
pedagogical sequencing, producing appropriate assignments (e.g., "gatto" = A1,
not B2).

**Processing:**
1. Parse numbered entries from HTML (`<a>` tags with POS in parentheses)
2. Clean words: strip gender variants (`/a`), parentheticals, reflexive (`/si`)
3. Map Profilo POS abbreviations to our system (verb/noun/adjective)
4. Compute per-level deltas (cumulative lists → lowest level per word)
5. Match against `lemmas` table (exact → case-insensitive → reflexive fallback)

---

Each subdirectory contains a LICENSE file with the full license text.

---

## Evaluated but Not Used

### PAISA Lemma Frequencies

**Source:** PAISA corpus (Paisà - Piattaforma per l'Apprendimento dell'Italiano Su corpora Annotati)
**URL:** https://clarin.eurac.edu/repository/xmlui/handle/20.500.12124/3
**Corpus:** Large Italian web corpus (~250M words from .it domains, 2010)
**License:** CC-BY-NC-SA 4.0 (NonCommercial)
**Evaluated:** January 2025
**Decision:** Replaced by Stanza-derived frequency from sentence tokens

PAISA provided pre-lemmatized frequency data, which was originally used for verb
frequency ranking to avoid surface form collision issues. Replaced by computing
frequency directly from Stanza-tagged sentence tokens (Tatoeba + OpenSubtitles),
which provides accurate lemmatization for all POS using a single unified approach.

### hermitdave/FrequencyWords (OpenSubtitles2018)

**Source:** hermitdave/FrequencyWords (derived from OpenSubtitles2018 corpus)
**URL:** https://github.com/hermitdave/FrequencyWords
**Corpus:** OpenSubtitles2018 (~500M words of conversational Italian)
**License:** CC-BY-SA 4.0
**Evaluated:** January 2025
**Decision:** Replaced by Stanza-derived frequency from OPUS OpenSubtitles v2024 sentences

Surface-form frequency lists required aggregation to lemma level, which worked
for nouns/adjectives but not verbs. Replaced by importing actual OpenSubtitles v2024
sentences from OPUS and computing lemmatized frequency via Stanza POS tagging.

### ParTUT (Universal Dependencies Italian Treebank)

**Source:** Universal Dependencies Italian-ParTUT Treebank
**URL:** https://github.com/UniversalDependencies/UD_Italian-ParTUT
**License:** CC-BY-NC-SA 4.0 (NonCommercial)
**Evaluated:** January 2025
**Decision:** Removed

ParTUT provides ~2,090 Italian sentences with full morphological annotation
(lemma, POS, mood, tense, person, number) derived from the JRC Acquis corpus
of EU legal documents.

**Why not used:**
The source text is overwhelmingly bureaucratic/legalistic EU documents,
making it unsuitable for learning conversational Italian. Example sentences
like "The Commission shall adopt implementing acts" are not helpful for
language learners. Tatoeba provides ~950,000 sentences with more natural,
conversational content.

### ItWaC Frequency Lists

**Source:** Word frequency lists derived from the Italian Web as Corpus (itWaC)
**URL:** https://github.com/franfranz/Word_Frequency_Lists_ITA
**Corpus:** itWaC (~1.5 billion words of web Italian)
**License:** MIT
**Evaluated:** January 2025
**Decision:** Replaced by Stanza-derived frequency from Tatoeba + OpenSubtitles v2024

ItWaC was the original frequency source but analysis revealed significant issues:

1. **Heavy legal/bureaucratic bias**: Web crawling of .it domains captured disproportionate
   amounts of legal and governmental text. Words like "decreto" (decree) and "comma" (clause)
   rank in the top 100, while "ciao" (hello) ranks #289,038.

2. **Poor conversational vocabulary rankings**: Common everyday words are severely underranked
   compared to conversational corpora like OpenSubtitles.

3. **Surface form issues for verbs**: Lemmatization in ItWaC is less reliable, causing
   collisions between homographic forms (e.g., "parte" as noun vs verb).

| Word | ItWaC Rank | OpenSubtitles Rank | Issue |
|------|------------|-------------------|-------|
| decreto | 69 | 16,121 | Bureaucratic bias |
| comma | 27 | 33,158 | Legal jargon |
| ciao | 289,038 | 153 | Missing conversational |
| mamma | 1,725 | 191 | Underranked |

**Replacement:** Stanza-derived frequency from Tatoeba + OpenSubtitles v2024 sentence tokens (lemmatized, all POS).

### KELLY Project Italian CEFR Vocabulary

**Source:** KELLY Project (Keywords for Language Learning for Young and adults alike)
**URL:** https://ssharoff.github.io/kelly/it_m3.xls
**License:** CC BY-ND-NC-SA 2.0
**Evaluated:** January 2025
**Decision:** Not integrated

The KELLY project provides ~5,300 Italian lemmas with CEFR levels (A1-C2). However, investigation revealed the levels are derived from **corpus frequency** rather than pedagogical sequencing, producing unusable results:

| Word | KELLY Level | Expected |
|------|-------------|----------|
| gatto (cat) | B2 | A1 |
| pranzo (lunch) | B2 | A1 |
| inverno (winter) | B1 | A1 |
| madre (mother) | A2 | A1 |
| rendiconto (financial statement) | A1 | B2+ |

The methodology prioritizes newspaper/business vocabulary frequency over learner needs, making it unsuitable for pedagogical CEFR tagging. The Profilo della lingua italiana (see above) provides expert-curated CEFR levels that correctly address these issues.

### Italian WordNet (IWN-OMW)

**Source:** IWN-OMW (Italian WordNet - Open Multilingual Wordnet)
**URL:** https://github.com/valeq/IWN-OMW
**License:** CC-BY-SA 4.0
**Evaluated:** January 2025
**Decision:** Not integrated

IWN-OMW provides ~49K synsets with semantic relations (hypernym/hyponym chains), potentially enabling semantic category tagging (animal, food, etc.) and vocabulary organization.

However, IWN only provides `(written_form, pos)` for matching, creating two insurmountable problems:

1. **Homonyms**: Our database has separate lemmas for homonyms (same spelling, different etymology). "lama" has 4 noun lemmas (llama, blade, monk, mud). IWN can't tell us which synsets belong to which lemma.

2. **Polysemy**: A single lemma may have multiple senses. "banco" maps to 9 synsets (bank, bench, counter, desk, etc.). Any category assignment would reflect ALL senses, not the one being studied - a card for "banco" (bank) could get tagged `category::furniture`.

~31% of matchable lemmas have multiple synsets, making semantic categories unreliable for vocabulary study.

### Morph-it!

**Source:** Morph-it! morphological lexicon for Italian
**URL:** https://docs.sslmit.unibo.it/doku.php?id=resources:morph-it
**Version:** 0.48, February 2009
**License:** CC-BY-SA 2.0 + LGPL (dual-licensed)
**Authors:** Marco Baroni and Eros Zanchetta
**Evaluated:** January 2025
**Decision:** Not used

Morph-it! provides ~500k Italian word forms with morphological annotations. It was initially considered as the primary source for converting stressed forms (e.g., "pàrlo") to written forms (e.g., "parlo").

**Investigation findings:**

1. **Wiktextract form-of entries provide the same data**: Wiktextract's form-of entries have a `word` field containing the correct written form. Analysis showed 100% overlap with Morph-it! for nouns and adjectives, with the only 2 disagreements being Morph-it! errors (e.g., "prìncipi" instead of correct "principi").

2. **Morph-it! has zero verb coverage**: All verb forms are stored without accents (e.g., "parlo" not "parlò"), making it useless for verbs which need orthographic accents on futures and passato remoto.

3. **Known errors**: Contains typos like "toto" instead of "totò" for the Italian lottery game, and incorrect stress marks like "prìncipi" instead of "principi".

**Sample Data:** (TSV: form, lemma, POS+features)
```
parlo	parlare	VER:ind+pres+1+s
parli	parlare	VER:ind+pres+2+s
città	città	NOUN-F:s
```
