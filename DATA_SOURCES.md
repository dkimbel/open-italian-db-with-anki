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
task download-wiktextract   # Italian dictionary (634 MB)
task download-itwac         # Frequency lists (45 MB)
task download-tatoeba       # Sentences and links (660 MB)

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
│ STEP 3: Import ItWaC Frequencies                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Adds word frequency data from a 1.5 billion word Italian web corpus.        │
│ Used to prioritize common words for Anki deck generation.                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ FINAL STEP: Import Tatoeba Sentences                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Imports Italian sentences with English translations.                        │
│ Links sentences to lemmas via form_lookup table for example sentences.      │
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

## ItWaC Frequency Lists (data/itwac/)

**Source:** Word frequency lists derived from the Italian Web as Corpus (itWaC)
**URL:** https://github.com/franfranz/Word_Frequency_Lists_ITA
**Corpus:** itWaC (~1.5 billion words of web Italian)
**License:** MIT
**Files:**
- `itwac_verbs_lemmas_notail_2_1_0.csv`
- `itwac_nouns_lemmas_notail_2_0_0.csv`
- `itwac_adj_lemmas_notail_2_1_0.csv`

**Note:** Files are encoded in ISO-8859-1 (Latin-1), not UTF-8.

**Sample Data:**
```csv
"Form","Freq","lemma","POS","fpmw","Zipf"
"sono",3317859,"essere","VER",1737.257,6.24
"parte",2068220,"parte","NOUN",1082.936,6.035
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

## ParTUT (data/partut/)

**Source:** Universal Dependencies Italian-ParTUT Treebank
**URL:** https://github.com/UniversalDependencies/UD_Italian-ParTUT
**License:** CC-BY-NC-SA 4.0 (NonCommercial)
**Files:**
- `it_partut-ud-train.conllu` - Italian training set
- `it_partut-ud-dev.conllu` - Italian development set
- `it_partut-ud-test.conllu` - Italian test set
- `en_partut-ud-*.conllu` - Parallel English translations

**Why ParTUT:**
ParTUT provides full morphological annotation for each token, enabling precise
example sentence matching by grammatical features (mood, tense, person, number).
For instance, we can find sentences where "essere" appears specifically in the
subjunctive present, not just any form of "essere".

**Sample Data (CoNLL-U format):**
```conllu
# sent_id = train-s1
# text = Ho una lettera per te.
1	Ho	avere	VERB	_	Mood=Ind|Number=Sing|Person=1|Tense=Pres|VerbForm=Fin	0	root	_	_
2	una	uno	DET	_	Gender=Fem|Number=Sing|PronType=Ind	3	det	_	_
3	lettera	lettera	NOUN	_	Gender=Fem|Number=Sing	1	obj	_	_
4	per	per	ADP	_	_	5	case	_	_
5	te	tu	PRON	_	Number=Sing|Person=2|PronType=Prs	1	obl	_	SpaceAfter=No
6	.	.	PUNCT	_	_	1	punct	_	_
```

**IMPORTANT License Note:**
ParTUT uses a CC-BY-NC-SA 4.0 license which includes a NonCommercial restriction.
Including this data source changes the combined database license to also include
the NC restriction.

---

Each subdirectory contains a LICENSE file with the full license text.

---

## Evaluated but Not Used

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
