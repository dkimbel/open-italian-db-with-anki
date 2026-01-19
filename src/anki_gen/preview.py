"""Generate HTML previews of Anki cards for rapid iteration.

This module renders cards as standalone HTML files that can be viewed
in a browser, enabling fast feedback without importing into Anki.

The preview shows a 2x2 grid with front/back in both light and dark modes.
"""

from pathlib import Path

from sqlalchemy import Connection

from anki_gen.generator import build_verb_tags
from anki_gen.queries import (
    TENSE_ID_TO_MOOD_TENSE,
    generate_english_prompt,
    get_english_infinitive,
    get_example_sentence_with_fallback,
    get_present_indicative_forms,
    get_verb_by_lemma,
)
from anki_gen.stress import format_conjugation_with_stress
from anki_gen.templates import CARD_CSS, TENSE_INFO, build_conjugation_table_html


def generate_preview_html(
    conn: Connection,
    verb_lemma: str,
    tense_id: str = "presente_indicativo",
) -> str:
    """Generate a standalone HTML preview of a verb card.

    Shows a 2x2 grid with:
    - Front (Light) | Front (Dark)
    - Back (Light)  | Back (Dark)

    Args:
        conn: Database connection
        verb_lemma: Verb infinitive (e.g., "parlare")
        tense_id: Tense to preview

    Returns:
        Complete HTML document as string
    """
    verb = get_verb_by_lemma(conn, verb_lemma)
    if verb is None:
        return f"<html><body><h1>Verb not found: {verb_lemma}</h1></body></html>"

    tense_info = TENSE_INFO.get(tense_id)
    if tense_info is None:
        return f"<html><body><h1>Tense not found: {tense_id}</h1></body></html>"

    # Get forms
    forms = get_present_indicative_forms(conn, verb.lemma_id)
    forms_dict: dict[tuple[int, str], str] = {}
    conjugated_forms: list[str] = []
    for form in forms:
        # Use CSS-based dot (non-copyable) for stress marking
        display = format_conjugation_with_stress(form.written, form.stressed, use_css=True)
        forms_dict[(form.person, form.number)] = display
        if form.written:
            conjugated_forms.append(form.written)

    table_html = build_conjugation_table_html(forms_dict)

    # Extract mood/tense for morphological sentence matching
    mood, tense = TENSE_ID_TO_MOOD_TENSE.get(tense_id, (None, None))

    # Get example: try morphological match from ParTUT first, then FTS fallback
    example = get_example_sentence_with_fallback(
        conn,
        verb.written,
        mood=mood,
        tense=tense,
        conjugated_forms=conjugated_forms,
    )
    example_section = ""
    if example:
        italian_text = example.italian
        example_section = f'<div class="example">\n    <span class="example-quote">\u201c</span>\n    <span class="example-italian">{italian_text}</span>\n    <span class="example-quote">\u201d</span>\n</div>'
        if example.english:
            example_section += f'\n<div class="example-english">{example.english}</div>'

    ipa_html = f"<div class='ipa'>{verb.ipa}</div>" if verb.ipa else ""
    # Get tags
    tags = build_verb_tags(conn, verb, tense_id)

    # Get English infinitive and generate verb-specific prompt
    english_infinitive = get_english_infinitive(conn, verb.lemma_id)
    english_prompt = generate_english_prompt(english_infinitive, tense_id)
    tense_english = tense_info.get("english_name", tense_id.replace("_", " "))

    # Front card content
    infinitive_span = (
        f'<span class="english-infinitive">{english_infinitive}</span>'
        if english_infinitive
        else ""
    )
    front_content = f"""<div class="english-prompt">{english_prompt}</div>
<div class="front-context">
    <span class="tense-english">{tense_english}</span>
    {infinitive_span}
</div>"""

    # Back card content
    back_content = f"""<div class="infinitive">{verb.written}</div>
{ipa_html}
<div class="conjugation-container">
    {table_html}
</div>
{example_section}
<div class="tense-label">{tense_info["technical_name"]}</div>"""

    # Build complete HTML document
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Card Preview: {verb.written}</title>
    <style>
{CARD_CSS}

/* Preview-specific styles */
body {{
    margin: 0;
    padding: 20px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #121212;
    color: #e0e0e0;
}}

.preview-container {{
    max-width: 900px;
    margin: 0 auto;
}}

.preview-header {{
    text-align: center;
    margin-bottom: 30px;
}}

.preview-header h1 {{
    margin-bottom: 5px;
    color: #e0e0e0;
}}

.preview-header .tense-name {{
    color: #888;
    margin-top: 0;
}}

/* 2x2 grid layout */
.preview-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 30px;
}}

.preview-column {{
    display: flex;
    flex-direction: column;
    gap: 20px;
}}

.column-header {{
    text-align: center;
    font-size: 12px;
    font-weight: bold;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #888;
    margin-bottom: 10px;
}}

.preview-section {{
    border: 1px solid #444;
    border-radius: 8px;
    overflow: hidden;
}}

.preview-label {{
    background: #2a2a2a;
    padding: 8px 15px;
    font-size: 12px;
    font-weight: bold;
    color: #aaa;
    border-bottom: 1px solid #444;
}}

.card {{
    border-radius: 0;
}}

/* Tags display */
.tags-section {{
    margin-top: 30px;
    padding: 15px;
    background: #1a1a1a;
    border-radius: 8px;
    border: 1px solid #333;
}}

.tags-label {{
    font-size: 12px;
    font-weight: bold;
    color: #888;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}

.tags-list {{
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
}}

.tag {{
    background: #2a2a2a;
    color: #aaa;
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 13px;
    font-family: monospace;
}}
    </style>
</head>
<body>
    <div class="preview-container">
        <div class="preview-header">
            <h1>{verb.written}</h1>
            <p class="tense-name">{tense_info["technical_name"]}</p>
        </div>

        <div class="preview-grid">
            <!-- Light Mode Column -->
            <div class="preview-column">
                <div class="column-header">Light Mode</div>

                <div class="preview-section">
                    <div class="preview-label">FRONT</div>
                    <div class="card">
                        {front_content}
                    </div>
                </div>

                <div class="preview-section">
                    <div class="preview-label">BACK</div>
                    <div class="card">
                        {back_content}
                    </div>
                </div>
            </div>

            <!-- Dark Mode Column -->
            <div class="preview-column">
                <div class="column-header">Dark Mode</div>

                <div class="preview-section">
                    <div class="preview-label">FRONT</div>
                    <div class="card night_mode">
                        {front_content}
                    </div>
                </div>

                <div class="preview-section">
                    <div class="preview-label">BACK</div>
                    <div class="card night_mode">
                        {back_content}
                    </div>
                </div>
            </div>
        </div>

        <div class="tags-section">
            <div class="tags-label">Tags</div>
            <div class="tags-list">
                {"".join(f'<span class="tag">{tag}</span>' for tag in tags)}
            </div>
        </div>
    </div>
</body>
</html>
"""
    return html


def write_preview(
    conn: Connection,
    verb_lemma: str,
    output_path: Path | None = None,
    tense_id: str = "presente_indicativo",
) -> Path:
    """Generate and write an HTML preview file.

    Args:
        conn: Database connection
        verb_lemma: Verb infinitive
        output_path: Where to write (default: output/preview.html)
        tense_id: Tense to preview

    Returns:
        Path to written file
    """
    if output_path is None:
        output_path = Path("output") / "preview.html"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    html = generate_preview_html(conn, verb_lemma, tense_id)
    output_path.write_text(html)

    return output_path
