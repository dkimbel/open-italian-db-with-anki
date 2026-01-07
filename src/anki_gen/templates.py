"""HTML/CSS templates for Anki cards.

Templates follow these design principles:
- Dark mode support via .night_mode class and prefers-color-scheme
- Tense-specific visual cues (border colors)
- Clean two-column conjugation table layout
- Mobile-friendly sizing
"""

# =============================================================================
# CSS Styles
# =============================================================================

CARD_CSS = """
/* Color variables */
:root {
    --color-text: #1a1a1a;
    --color-background: #ffffff;
    --color-prompt: #555;
    --color-muted: #888;
    --color-secondary: #666;
    --color-tertiary: #999;
    --color-quote: #999;
}

.night_mode {
    --color-text: #e0e0e0;
    --color-background: #1a1a1a;
    --color-prompt: #aaa;
    --color-muted: #808080;
    --color-secondary: #888;
    --color-tertiary: #808080;
    --color-quote: #666;
}

/* Base styles */
.card {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    font-size: 18px;
    text-align: center;
    color: var(--color-text);
    background-color: var(--color-background);
    padding: 20px;
    line-height: 1.5;
    max-width: 450px;
    margin: 0 auto;
}

/* English prompt on front */
.english-prompt {
    font-size: 24px;
    font-style: italic;
    color: var(--color-prompt);
    margin: 20px 0;
}

/* Front card context (tense + infinitive) */
.front-context {
    margin-top: 30px;
    font-size: 13px;
    color: var(--color-muted);
    display: flex;
    justify-content: center;
    gap: 20px;
}

.tense-english {
    text-transform: lowercase;
}

.english-infinitive {
    font-style: italic;
}

/* Infinitive header */
.infinitive {
    font-size: 28px;
    font-weight: bold;
    margin-bottom: 5px;
}

/* IPA pronunciation */
.ipa {
    font-size: 16px;
    color: var(--color-secondary);
    margin-bottom: 20px;
}

/* Conjugation table container */
.conjugation-container {
    display: inline-block;
    text-align: left;
    padding: 15px 20px;
    margin: 15px auto;
}

/* Two-column conjugation table */
.conjugation-table {
    width: 100%;
    border-collapse: collapse;
}

.conjugation-table td {
    padding: 6px 8px;
    vertical-align: baseline;
}

/* Person labels column */
.person-label {
    color: var(--color-secondary);
    font-size: 14px;
    min-width: 50px;
    text-align: right;
    padding-right: 8px;
}

/* Verb forms */
.verb-form {
    font-size: 20px;
    font-weight: 500;
}

/* Separator between singular and plural */
.column-gap {
    width: 30px;
}

/* Example sentence with decorative quote marks */
.example {
    position: relative;
    display: inline-block;
    text-align: left;
    margin-top: 20px;
    padding: 0 8px;
    font-size: 16px;
    font-style: italic;
}

.example::before {
    content: '\\201C';
    position: absolute;
    left: -7%;
    top: 110%;
    transform: translate(-100%, -50%);
    font-size: 112px;
    font-family: Georgia, "Times New Roman", serif;
    color: var(--color-quote);
    opacity: 0.4;
    line-height: 1;
}

.example::after {
    content: '\\201D';
    position: absolute;
    right: 0;
    top: 110%;
    transform: translate(100%, -50%);
    font-size: 112px;
    font-family: Georgia, "Times New Roman", serif;
    color: var(--color-quote);
    opacity: 0.4;
    line-height: 1;
}

.example-italian {
    margin-bottom: 5px;
    text-align: center;
}

.example-english {
    font-size: 14px;
    color: var(--color-secondary);
    text-align: center;
}

/* Technical tense label */
.tense-label {
    margin-top: 15px;
    font-size: 12px;
    color: var(--color-tertiary);
    letter-spacing: 0.5px;
}
"""

# =============================================================================
# Card Templates
# =============================================================================

VERB_FRONT_TEMPLATE = """
<div class="english-prompt">{{EnglishPrompt}}</div>
<div class="front-context">
    <span class="tense-english">{{TenseEnglish}}</span>
    {{#EnglishInfinitive}}<span class="english-infinitive">{{EnglishInfinitive}}</span>{{/EnglishInfinitive}}
</div>
"""

VERB_BACK_TEMPLATE = """
<div class="infinitive">{{Infinitive}}</div>
{{#IPA}}<div class="ipa">{{IPA}}</div>{{/IPA}}

<div class="conjugation-container">
{{ConjugationTable}}
</div>

{{#ExampleSentence}}
<div class="example">
    {{ExampleSentence}}
</div>
{{/ExampleSentence}}

<div class="tense-label">{{TenseTechnical}}</div>
"""

# =============================================================================
# Tense Metadata
# =============================================================================

# Maps database tense identifiers to display info
TENSE_INFO: dict[str, dict[str, str]] = {
    "presente_indicativo": {
        "english_prompt": "I speak",
        "english_name": "present indicative",
        "technical_name": "presente indicativo",
    },
    "imperfetto": {
        "english_prompt": "I was speaking",
        "english_name": "imperfect",
        "technical_name": "imperfetto",
    },
    "passato_remoto": {
        "english_prompt": "I spoke",
        "english_name": "remote past",
        "technical_name": "passato remoto",
    },
    "futuro_semplice": {
        "english_prompt": "I will speak",
        "english_name": "simple future",
        "technical_name": "futuro semplice",
    },
    # More tenses will be added in later phases
}


def build_conjugation_table_html(forms: dict[tuple[int, str], str]) -> str:
    """Build HTML for the two-column conjugation table.

    Args:
        forms: Dict mapping (person, number) to display form with stress marking.
               Keys are like (1, "singular"), (2, "plural"), etc.

    Returns:
        HTML string for the conjugation table
    """
    # Person labels
    person_labels = {
        1: ("io", "noi"),
        2: ("tu", "voi"),
        3: ("lui/lei", "loro"),
    }

    rows: list[str] = []
    for person in [1, 2, 3]:
        sg_label, pl_label = person_labels[person]
        sg_form = forms.get((person, "singular"), "—")
        pl_form = forms.get((person, "plural"), "—")

        row = f"""
        <tr>
            <td class="person-label">{sg_label}</td>
            <td class="verb-form">{sg_form}</td>
            <td class="column-gap"></td>
            <td class="person-label">{pl_label}</td>
            <td class="verb-form">{pl_form}</td>
        </tr>
        """
        rows.append(row)

    return f'<table class="conjugation-table">{"".join(rows)}</table>'
