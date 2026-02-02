"""Data importers for Italian Anki deck generator."""

from italian_db.importers.frequency_ranking import compute_pos_frequency_ranks
from italian_db.importers.nvdb import import_nvdb
from italian_db.importers.profilo import import_profilo
from italian_db.importers.sentence_tokens import (
    create_sentence_token_indexes,
    drop_sentence_token_indexes,
    import_sentence_tokens,
)
from italian_db.importers.tatoeba import import_tatoeba
from italian_db.importers.verb_irregularity import import_verb_irregularity
from italian_db.importers.wiktextract import (
    enrich_missing_feminine_plurals,
    generate_gendered_participles,
    import_wiktextract,
)

__all__ = [
    "compute_pos_frequency_ranks",
    "create_sentence_token_indexes",
    "drop_sentence_token_indexes",
    "enrich_missing_feminine_plurals",
    "generate_gendered_participles",
    "import_nvdb",
    "import_profilo",
    "import_sentence_tokens",
    "import_tatoeba",
    "import_verb_irregularity",
    "import_wiktextract",
]
