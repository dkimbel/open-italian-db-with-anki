# Data Sources

This directory contains linguistic data from multiple sources, each with its own license.

## Licenses

Licenses for all the data described here can be found in `data-licenses`.

## Downloading Data

All data can be downloaded programmatically using the provided tasks:

```bash
# Download all data sources (~1.3 GB total)
task download-all

# Or download individual sources:
task download-wiktextract     # Italian dictionary (634 MB)
task download-paisa           # PAISA lemma frequencies (verbs)
task download-opensubtitles   # OpenSubtitles frequencies (nouns/adj)
task download-tatoeba         # Sentences and links (660 MB)

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
│ STEP 3: Import Frequency Data (PAISA + OpenSubtitles)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Adds word frequency data for prioritizing vocabulary in Anki decks:         │
│                                                                             │
│ VERBS: PAISA corpus (~250M words)                                           │
│   - Already lemmatized, avoiding verb form collision issues                 │
│   - E.g., "parte" as verb vs noun would be confused in surface forms        │
│                                                                             │
│ NOUNS/ADJECTIVES: OpenSubtitles (~500M words)                               │
│   - Surface forms aggregated to lemma level                                 │
│   - Better conversational vocabulary than formal web corpora                │
│   - E.g., "ciao" ranks #153 vs #289K in formal corpora                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: Import Tatoeba Sentences                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Imports ~950,000 Italian sentences with English translations from Tatoeba.  │
│ Uses FTS5 for full-text search and tag-based filtering for tense matching.  │
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

## PAISA Lemma Frequencies (data/paisa/)

**Source:** PAISA corpus (Paisà - Piattaforma per l'Apprendimento dell'Italiano Su corpora Annotati)
**URL:** https://clarin.eurac.edu/repository/xmlui/handle/20.500.12124/3
**Corpus:** Large Italian web corpus (~250M words from .it domains, 2010)
**License:** CC-BY-NC-SA 4.0 (NonCommercial)
**Files:**
- `lemma-frequencies-paisa.txt` - Pre-lemmatized frequency data

**Why PAISA for Verbs:**
PAISA provides already-lemmatized frequency data, avoiding the verb surface form collision
problem. For example, "parte" appears as both a verb form (from "partire") and a common
noun, which would confuse surface-form frequency aggregation.

**Sample Data:** (CSV with 2 comment lines, lemma,frequency format)
```csv
# lemma frequencies for paisa corpus
# source: ...
essere,2500000
avere,1800000
fare,1200000
```

## OpenSubtitles Frequency Data (data/opensubtitles/)

**Source:** hermitdave/FrequencyWords (derived from OpenSubtitles2018 corpus)
**URL:** https://github.com/hermitdave/FrequencyWords
**Corpus:** OpenSubtitles2018 (~500M words of conversational Italian from movie subtitles)
**License:** CC-BY-SA 4.0
**Files:**
- `it_50k.txt` - Top 50K words with frequencies
- `it_full.txt` - Complete word list with frequencies

**Why OpenSubtitles for Nouns/Adjectives:**
OpenSubtitles represents conversational vocabulary much better than formal web corpora.
Common words like "ciao" (hello) rank #153 in OpenSubtitles vs #289,038 in ItWaC.
Surface form aggregation works well for nouns/adjectives since collisions are less
problematic than for verbs.

**Sample Data:** (space-separated word frequency pairs)
```
non 12500000
che 11800000
io 8200000
```

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

---

Each subdirectory contains a LICENSE file with the full license text.

---

## Evaluated but Not Used

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
**Decision:** Replaced by PAISA + OpenSubtitles hybrid approach

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

**Replacement:** PAISA for verbs (lemmatized), OpenSubtitles for nouns/adjectives (conversational).

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

The methodology prioritizes newspaper/business vocabulary frequency over learner needs, making it unsuitable for pedagogical CEFR tagging.

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
