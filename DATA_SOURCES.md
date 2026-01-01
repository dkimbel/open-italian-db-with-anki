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
task download-morphit       # Morphological lexicon (19 MB)
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
│  │ senses: [{gloss: ...}]  │       │ * Re-scanned in Step 3  │              │
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
│ STEP 2: Enrich with Morph-it! Spelling (written_source = "morphit")         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Wiktextract provides STRESSED forms: "pàrlo", "parlàto", "parlerà"          │
│ These show pronunciation but aren't how Italian is normally written.        │
│                                                                             │
│ Morph-it! provides WRITTEN forms: "parlo", "parlato", "parlerà"             │
│                                                                             │
│   stressed (from Wiktextract)       │  written (from Morph-it!)             │
│   ─────────────────────────────────│──────────────────────────────────────  │
│   pàrlo                            │  parlo     (stress accent removed)     │
│   parlàto                          │  parlato   (stress accent removed)     │
│   parlerà                          │  parlerà   (accent kept - written!)    │
│   cantò                            │  cantò     (accent kept - written!)    │
│                                                                             │
│ The difference: stress marks vs actual written Italian.                     │
│ Future tense (-rà, -rò) and passato remoto 3rd person (-ò) keep accents.    │
│                                                                             │
│ Morph-it! is an academic resource (higher quality, but dated 2009).         │
│ ~30% of noun/adjective forms are found in Morph-it!.                        │
│                                                                             │
│ NOTE: VERBS SKIP THIS STEP - they have zero Morph-it! coverage because      │
│ Morph-it! stores verbs without accents. Verb spelling is derived directly   │
│ from Wiktextract stressed forms using Italian orthography rules.            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: Enrich from Form-of Entries (combined labels + spelling)            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ Single pass through form-of entries extracting TWO types of data:           │
│                                                                             │
│ A) USAGE LABELS - For forms with special tags:                              │
│   "fo"  → form_of: "fare", tags: ["literary", "regional"]                   │
│   "diè" → form_of: "dare", tags: ["archaic"]                                │
│   Only ~0.1% of form-of entries have labels (e.g., 226 of 353k for verbs)   │
│                                                                             │
│ B) SPELLING FALLBACK (written_source = "wiktionary"):                       │
│   For forms where Morph-it! didn't have the spelling, use the form-of       │
│   entry's word field as the written form.                                   │
│   Example: If "pàrlo" has no Morph-it! match, get "parlo" from form-of.     │
│                                                                             │
│ The written_source column tracks which source provided the spelling:        │
│   - "morphit"    = from Morph-it! (academic, higher quality)                │
│   - "wiktionary" = from Wiktionary form-of entries (fallback)               │
│   - "derived:*"  = from orthography rules or fallback                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: Import ItWaC Frequencies                                            │
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

### Verbs: Derive from Wiktextract stressed forms

Wiktextract provides complete verb conjugations with accurate accent marks, including required orthographic accents like `parlò`, `sarà`, `darà`.

Morph-it! has **zero accented verb forms** - it stores all verbs unaccented (e.g., `parlo` not `parlò`). Therefore we derive verb `written` forms directly from Wiktextract's `stressed` values using Italian orthography rules during the Wiktextract import phase.

### Nouns and Adjectives: Prefer Morph-it!, fallback to derivation

For nouns and adjectives, Wiktextract's stressed values sometimes **lack required orthographic accents** (e.g., `eta` instead of `età`).

Morph-it! has authoritative accented forms for nouns/adjectives (e.g., `città`, `più`, `età`). Therefore we:
1. First try Morph-it! enrichment (authoritative source)
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
`src/italian_anki/normalize.py` for the full list and detailed documentation.

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

## Morph-it! (data/morphit/)

**Source:** Morph-it! morphological lexicon for Italian
**URL:** https://docs.sslmit.unibo.it/doku.php?id=resources:morph-it
**Downloaded:** `morph-it.tgz` (version 0.48, February 2009)
**License:** CC-BY-SA 2.0 + LGPL (dual-licensed, your choice)
**Authors:** Marco Baroni and Eros Zanchetta
**Citation:**
> Baroni, M. and Zanchetta, E. (2005). morph-it! A free corpus-based
> morphological resource for the Italian language.

**Sample Data:** (TSV: form, lemma, POS+features)
```
parlo	parlare	VER:ind+pres+1+s
parli	parlare	VER:ind+pres+2+s
città	città	NOUN-F:s
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

---

Each subdirectory contains a LICENSE file with the full license text.
