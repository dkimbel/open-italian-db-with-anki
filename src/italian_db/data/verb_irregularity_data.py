"""Manual classifications of Italian irregular verb patterns.

This module defines the irregularity patterns for Italian verbs across five
tense domains: present, remote (passato remoto), future/conditional, past
participle, and subjunctive.

Each verb is classified by its MOST SALIENT irregularity in each domain.
A value of None means the verb is regular in that domain.
An entry with ALL None values means the verb has been reviewed and confirmed regular.

Pattern enum values are defined in italian_db/enums.py.

Format:
    "written_lemma": (present, remote, future, participle, subjunctive)

Notes:
- Use the written (unaccented) infinitive form as the key
- Examples: "essere", "avere", "andare", "venire", "tenere"
"""

from italian_db.enums import (
    FuturePattern,
    ParticiplePattern,
    PresentPattern,
    RemotePattern,
    SubjunctivePattern,
)

# Type alias for clarity
IrregularityTuple = tuple[
    PresentPattern | None,  # present
    RemotePattern | None,  # remote (passato remoto)
    FuturePattern | None,  # future/conditional
    ParticiplePattern | None,  # past participle
    SubjunctivePattern | None,  # subjunctive
]

# =============================================================================
# VERB IRREGULARITY CLASSIFICATIONS
# =============================================================================
#
# Organized by primary irregularity type for easier maintenance and review.
# Compounds inherit patterns from their base verb and are listed together.

VERB_IRREGULARITY_CLASSIFICATIONS: dict[str, IrregularityTuple] = {
    # =========================================================================
    # HIGHLY SUPPLETIVE / AUXILIARY VERBS
    # =========================================================================
    "essere": (
        PresentPattern.SUPPLETIVE_ESSERE,
        RemotePattern.SUPPLETIVE_ESSERE,
        FuturePattern.SUPPLETIVE,
        ParticiplePattern.SUPPLETIVE,
        SubjunctivePattern.SUPPLETIVE_SIA,
    ),
    "avere": (
        PresentPattern.SUPPLETIVE_AVERE,
        RemotePattern.SUPPLETIVE_AVERE,
        FuturePattern.SYNCOPATED_DR,
        None,  # avuto is regular
        SubjunctivePattern.SUPPLETIVE_ABBIA,
    ),
    # =========================================================================
    # ANDARE family (suppletive present, syncopated future)
    # =========================================================================
    "andare": (
        PresentPattern.SUPPLETIVE_ANDARE,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        SubjunctivePattern.SUPPLETIVE_VADA,
    ),
    "riandare": (
        PresentPattern.SUPPLETIVE_ANDARE,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        SubjunctivePattern.SUPPLETIVE_VADA,
    ),
    # =========================================================================
    # STARE family (suppletive present/subjunctive)
    # =========================================================================
    "stare": (
        PresentPattern.SUPPLETIVE_STARE,
        None,  # stetti is regular-ish
        None,
        None,
        SubjunctivePattern.SUPPLETIVE_STIA,
    ),
    "ristare": (
        PresentPattern.SUPPLETIVE_STARE,
        None,
        None,
        None,
        SubjunctivePattern.SUPPLETIVE_STIA,
    ),
    "sottostare": (
        PresentPattern.SUPPLETIVE_STARE,
        None,
        None,
        None,
        SubjunctivePattern.SUPPLETIVE_STIA,
    ),
    # =========================================================================
    # DARE family (suppletive present/subjunctive)
    # =========================================================================
    "dare": (
        PresentPattern.SUPPLETIVE_DARE,
        None,  # diedi/detti
        None,
        None,
        SubjunctivePattern.SUPPLETIVE_DIA,
    ),
    "ridare": (
        PresentPattern.SUPPLETIVE_DARE,
        None,
        None,
        None,
        SubjunctivePattern.SUPPLETIVE_DIA,
    ),
    # =========================================================================
    # FARE family (contracted from facere)
    # =========================================================================
    "fare": (
        PresentPattern.CONTRACTED_FAC,
        RemotePattern.STRONG_VOWEL_E,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_FACCIA,
    ),
    "disfare": (
        PresentPattern.CONTRACTED_FAC,
        RemotePattern.STRONG_VOWEL_E,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_FACCIA,
    ),
    "rifare": (
        PresentPattern.CONTRACTED_FAC,
        RemotePattern.STRONG_VOWEL_E,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_FACCIA,
    ),
    "soddisfare": (
        PresentPattern.CONTRACTED_FAC,
        RemotePattern.STRONG_VOWEL_E,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_FACCIA,
    ),
    "contraffare": (
        PresentPattern.CONTRACTED_FAC,
        RemotePattern.STRONG_VOWEL_E,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_FACCIA,
    ),
    "stupefare": (
        PresentPattern.CONTRACTED_FAC,
        RemotePattern.STRONG_VOWEL_E,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_FACCIA,
    ),
    "assuefare": (
        PresentPattern.CONTRACTED_FAC,
        RemotePattern.STRONG_VOWEL_E,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_FACCIA,
    ),
    # =========================================================================
    # DIRE family (contracted from dicere)
    # =========================================================================
    "dire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    "contraddire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    "disdire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    "interdire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    "maledire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    "predire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    "ridire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    "benedire": (
        PresentPattern.CONTRACTED_DIC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        SubjunctivePattern.SUPPLETIVE_DICA,
    ),
    # =========================================================================
    # BERE family (contracted from bevere)
    # =========================================================================
    "bere": (
        PresentPattern.CONTRACTED_BEV,
        RemotePattern.STRONG_NN,  # bevvi
        FuturePattern.SYNCOPATED_RR,
        None,  # bevuto is regular
        None,
    ),
    # =========================================================================
    # PORRE family (contracted from ponere)
    # =========================================================================
    "porre": (
        PresentPattern.CONTRACTED_PON,
        None,  # posi
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "comporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "disporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "esporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "imporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "opporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "proporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "supporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "preporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "anteporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "deporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "riporre": (
        PresentPattern.CONTRACTED_PON,
        None,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    # =========================================================================
    # TRARRE family (contracted from trahere)
    # =========================================================================
    "trarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "attrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "contrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "detrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "distrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "estrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "sottrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "protrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    "ritrarre": (
        PresentPattern.CONTRACTED_TRA,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TO_ATTO,
        None,
    ),
    # =========================================================================
    # -DURRE family (contracted from -ducere)
    # =========================================================================
    "condurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "dedurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "indurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "produrre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "ridurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "sedurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "tradurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "addurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "introdurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "riprodurre": (
        PresentPattern.CONTRACTED_DUC,
        RemotePattern.STRONG_SS,
        FuturePattern.CONTRACTED_BASE,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    # =========================================================================
    # VENIRE family (g-insertion, strong -nn-, syncopated -rr-)
    # =========================================================================
    "venire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "avvenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "convenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "divenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "intervenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "pervenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "provenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "sovvenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "svenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "prevenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "rinvenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "contravvenire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    # =========================================================================
    # TENERE family (g-insertion, strong -nn-, syncopated -rr-)
    # =========================================================================
    "tenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "appartenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "contenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "detenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "intrattenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "mantenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "ottenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "ritenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "sostenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "trattenere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "astenersi": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NN,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    # =========================================================================
    # RIMANERE family (g-insertion, syncopated -rr-)
    # =========================================================================
    "rimanere": (
        PresentPattern.G_INSERTION,
        None,
        FuturePattern.SYNCOPATED_RR,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "permanere": (
        PresentPattern.G_INSERTION,
        None,
        FuturePattern.SYNCOPATED_RR,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    # =========================================================================
    # VALERE family (g-insertion, syncopated -rr-)
    # =========================================================================
    "valere": (
        PresentPattern.G_INSERTION,
        None,
        FuturePattern.SYNCOPATED_RR,
        None,  # valso
        None,
    ),
    "prevalere": (
        PresentPattern.G_INSERTION,
        None,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "equivalere": (
        PresentPattern.G_INSERTION,
        None,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    # =========================================================================
    # SALIRE family (g-insertion)
    # =========================================================================
    "salire": (
        PresentPattern.G_INSERTION,
        None,
        None,
        None,
        None,
    ),
    "risalire": (
        PresentPattern.G_INSERTION,
        None,
        None,
        None,
        None,
    ),
    "assalire": (
        PresentPattern.G_INSERTION,
        None,
        None,
        None,
        None,
    ),
    # =========================================================================
    # SCEGLIERE/COGLIERE/TOGLIERE family (g-insertion with lg)
    # =========================================================================
    "scegliere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "cogliere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "togliere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "raccogliere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "accogliere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "sciogliere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "disciogliere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    # =========================================================================
    # DOLERE/VOLERE (modal-like, syncopated -rr-)
    # =========================================================================
    "volere": (
        PresentPattern.MODAL_VOLERE,
        None,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "dolere": (
        PresentPattern.DIPHTHONG_UO,
        None,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    # =========================================================================
    # POTERE (modal, syncopated -dr-)
    # =========================================================================
    "potere": (
        PresentPattern.MODAL_POTERE,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    # =========================================================================
    # DOVERE (modal, syncopated -dr-)
    # =========================================================================
    "dovere": (
        PresentPattern.MODAL_DOVERE,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    # =========================================================================
    # SAPERE (modal-like, syncopated -dr-)
    # =========================================================================
    "sapere": (
        PresentPattern.MODAL_SAPERE,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        SubjunctivePattern.SUPPLETIVE_SAPPIA,
    ),
    # =========================================================================
    # VEDERE family (syncopated -dr-, strong participle)
    # =========================================================================
    "vedere": (
        None,
        RemotePattern.STRONG_VOWEL_I,
        FuturePattern.SYNCOPATED_DR,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "prevedere": (
        None,
        RemotePattern.STRONG_VOWEL_I,
        FuturePattern.SYNCOPATED_DR,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "provvedere": (
        None,
        RemotePattern.STRONG_VOWEL_I,
        FuturePattern.SYNCOPATED_DR,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "rivedere": (
        None,
        RemotePattern.STRONG_VOWEL_I,
        FuturePattern.SYNCOPATED_DR,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "intravedere": (
        None,
        RemotePattern.STRONG_VOWEL_I,
        FuturePattern.SYNCOPATED_DR,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    # =========================================================================
    # CADERE family (syncopated -dr-)
    # =========================================================================
    "cadere": (
        None,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    "accadere": (
        None,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    "ricadere": (
        None,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    "decadere": (
        None,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    "scadere": (
        None,
        None,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    # =========================================================================
    # VIVERE family (syncopated -dr-)
    # =========================================================================
    "vivere": (
        None,
        RemotePattern.STRONG_SS,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    "sopravvivere": (
        None,
        RemotePattern.STRONG_SS,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    "convivere": (
        None,
        RemotePattern.STRONG_SS,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    "rivivere": (
        None,
        RemotePattern.STRONG_SS,
        FuturePattern.SYNCOPATED_DR,
        None,
        None,
    ),
    # =========================================================================
    # PARERE (g-insertion, syncopated -rr-)
    # =========================================================================
    "parere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_RR,
        FuturePattern.SYNCOPATED_RR,
        None,
        None,
    ),
    "apparire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_RR,
        None,
        ParticiplePattern.STRONG_SO,  # apparso
        None,
    ),
    "comparire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_RR,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "scomparire": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_RR,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    # =========================================================================
    # MORIRE family (diphthong, syncopated -rr-)
    # =========================================================================
    "morire": (
        PresentPattern.DIPHTHONG_UO,
        None,
        FuturePattern.SYNCOPATED_RR,
        ParticiplePattern.STRONG_TO_ORTO,
        None,
    ),
    # =========================================================================
    # USCIRE family (esc alternation)
    # =========================================================================
    "uscire": (
        PresentPattern.ESC_ALTERNATION,
        None,
        None,
        None,
        None,
    ),
    "riuscire": (
        PresentPattern.ESC_ALTERNATION,
        None,
        None,
        None,
        None,
    ),
    # =========================================================================
    # PIACERE/TACERE/GIACERE family (strong -cqui)
    # =========================================================================
    "piacere": (
        None,
        RemotePattern.STRONG_CQUI,
        None,
        None,
        None,
    ),
    "dispiacere": (
        None,
        RemotePattern.STRONG_CQUI,
        None,
        None,
        None,
    ),
    "compiacere": (
        None,
        RemotePattern.STRONG_CQUI,
        None,
        None,
        None,
    ),
    "tacere": (
        None,
        RemotePattern.STRONG_CQUI,
        None,
        None,
        None,
    ),
    "giacere": (
        None,
        RemotePattern.STRONG_CQUI,
        None,
        None,
        None,
    ),
    # =========================================================================
    # NASCERE family (strong -cqui)
    # =========================================================================
    "nascere": (
        None,
        RemotePattern.STRONG_CQUI,
        None,
        ParticiplePattern.STRONG_STO,  # nato -> actually just -to
        None,
    ),
    "rinascere": (
        None,
        RemotePattern.STRONG_CQUI,
        None,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    # =========================================================================
    # SCRIVERE family (strong -ss-, strong -tto)
    # =========================================================================
    "scrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "descrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "inscrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "iscrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "prescrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "proscrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "sottoscrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "trascrivere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    # =========================================================================
    # LEGGERE family (strong -ss-, strong -tto)
    # =========================================================================
    "leggere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "rileggere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "eleggere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    # =========================================================================
    # PRENDERE family (strong -si)
    # =========================================================================
    "prendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "apprendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "comprendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "riprendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "sorprendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "intraprendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    # =========================================================================
    # CHIEDERE family (strong -si)
    # =========================================================================
    "chiedere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "richiedere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    # =========================================================================
    # VINCERE family (strong -nsi)
    # =========================================================================
    "vincere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "convincere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "avvincere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    # =========================================================================
    # DIPINGERE/SPINGERE/FINGERE family
    # =========================================================================
    "dipingere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "spingere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "fingere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "tingere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "stingere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "cingere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    "stringere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ETTO,
        None,
    ),
    "costringere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ETTO,
        None,
    ),
    # =========================================================================
    # METTERE family
    # =========================================================================
    "mettere": (
        None,
        RemotePattern.STRONG_SI,  # misi
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "ammettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "commettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "permettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "promettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "smettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "trasmettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "emettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    "sottomettere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_TO_ESSO,
        None,
    ),
    # =========================================================================
    # ROMPERE family
    # =========================================================================
    "rompere": (
        None,
        None,  # ruppi
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "corrompere": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "interrompere": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    "irrompere": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    # =========================================================================
    # RISPONDERE family
    # =========================================================================
    "rispondere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    "corrispondere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_STO,
        None,
    ),
    # =========================================================================
    # APRIRE family (strong -erto participle)
    # =========================================================================
    "aprire": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TO_ERTO,
        None,
    ),
    "riaprire": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TO_ERTO,
        None,
    ),
    "coprire": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TO_ERTO,
        None,
    ),
    "scoprire": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TO_ERTO,
        None,
    ),
    "ricoprire": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TO_ERTO,
        None,
    ),
    "soffrire": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TO_ERTO,
        None,
    ),
    "offrire": (
        None,
        None,
        None,
        ParticiplePattern.STRONG_TO_ERTO,
        None,
    ),
    # =========================================================================
    # CORRERE family (strong -rsi)
    # =========================================================================
    "correre": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "accorrere": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "concorrere": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "occorrere": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "percorrere": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "ricorrere": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "scorrere": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "trascorrere": (
        None,
        RemotePattern.STRONG_RSI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    # =========================================================================
    # GIUNGERE/PUNGERE family
    # =========================================================================
    "giungere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    "aggiungere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    "raggiungere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    "congiungere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    "pungere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    # =========================================================================
    # ASSUMERE family
    # =========================================================================
    "assumere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    "presumere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    "riassumere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    "consumere": (
        None,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_ONTO,
        None,
    ),
    # =========================================================================
    # RISOLVERE family (strong -lsi)
    # =========================================================================
    "risolvere": (
        None,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "assolvere": (
        None,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    "dissolvere": (
        None,
        RemotePattern.STRONG_LSI,
        None,
        ParticiplePattern.STRONG_TO_OLTO,
        None,
    ),
    # =========================================================================
    # ACCENDERE/SCENDERE/SPENDERE family
    # =========================================================================
    "accendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "scendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "discendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "spendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "difendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "offendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "stendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "tendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "attendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "intendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "contendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "pretendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "estendere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    # =========================================================================
    # DECIDERE/RIDERE family
    # =========================================================================
    "decidere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "uccidere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "ridere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "sorridere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    "deridere": (
        None,
        RemotePattern.STRONG_SI,
        None,
        ParticiplePattern.STRONG_SO,
        None,
    ),
    # =========================================================================
    # CUOCERE (strong -ss-)
    # =========================================================================
    "cuocere": (
        None,
        RemotePattern.STRONG_SS,
        None,
        ParticiplePattern.STRONG_TTO,
        None,
    ),
    # =========================================================================
    # SEDERE (diphthong)
    # =========================================================================
    "sedere": (
        PresentPattern.DIPHTHONG_IE,
        None,
        None,
        None,
        None,
    ),
    "risedere": (
        PresentPattern.DIPHTHONG_IE,
        None,
        None,
        None,
        None,
    ),
    "possedere": (
        PresentPattern.DIPHTHONG_IE,
        None,
        None,
        None,
        None,
    ),
    # =========================================================================
    # SPEGNERE/SPENGERE family
    # =========================================================================
    "spegnere": (
        PresentPattern.G_INSERTION,
        RemotePattern.STRONG_NSI,
        None,
        ParticiplePattern.STRONG_TO_INTO,
        None,
    ),
    # =========================================================================
    # UDIRE (suppletive present: odo/odi/ode)
    # =========================================================================
    "udire": (
        PresentPattern.SUPPLETIVE_UDIRE,
        None,
        None,
        None,
        None,
    ),
    # =========================================================================
    # CONOSCERE/CRESCERE family (strong -bbi passato remoto)
    # =========================================================================
    "conoscere": (
        None,
        RemotePattern.STRONG_BBI,
        None,
        None,
        None,
    ),
    "riconoscere": (
        None,
        RemotePattern.STRONG_BBI,
        None,
        None,
        None,
    ),
    "crescere": (
        None,
        RemotePattern.STRONG_BBI,
        None,
        None,
        None,
    ),
    "accrescere": (
        None,
        RemotePattern.STRONG_BBI,
        None,
        None,
        None,
    ),
    # =========================================================================
    # PERDERE (existing patterns, just not previously classified)
    # =========================================================================
    "perdere": (
        None,
        RemotePattern.STRONG_SI,  # persi
        None,
        ParticiplePattern.STRONG_SO,  # perso
        None,
    ),
    # =========================================================================
    # CONFIRMED REGULAR VERBS (all NULL patterns = reviewed and regular)
    # =========================================================================
    # All-NULL entry means "reviewed and confirmed regular."
    #
    # --- KOFI regular verbs (8 total) ---
    "amare": (None, None, None, None, None),
    "credere": (None, None, None, None, None),
    "partire": (None, None, None, None, None),
    "parlare": (None, None, None, None, None),
    "imparare": (None, None, None, None, None),
    "mangiare": (None, None, None, None, None),
    "capire": (None, None, None, None, None),  # -isc- verb
    "finire": (None, None, None, None, None),  # -isc- verb
    #
    # --- Popular regular -are verbs ---
    "trovare": (None, None, None, None, None),  # zipf 5.84
    "pensare": (None, None, None, None, None),  # zipf 5.64
    "portare": (None, None, None, None, None),  # zipf 5.61
    "arrivare": (None, None, None, None, None),  # zipf 5.53
    "cercare": (None, None, None, None, None),  # zipf 5.51
    "lasciare": (None, None, None, None, None),  # zipf 5.49
    "passare": (None, None, None, None, None),  # zipf 5.46
    #
    # --- Popular regular -ere verbs ---
    "temere": (None, None, None, None, None),  # zipf 4.56
    #
    # --- Popular regular -ire verbs (non-isc) ---
    "sentire": (None, None, None, None, None),  # zipf 5.53
    "seguire": (None, None, None, None, None),  # zipf 5.43
    "servire": (None, None, None, None, None),  # zipf 5.24
    #
    # --- Popular regular -ire verbs (-isc-) ---
    "costituire": (None, None, None, None, None),  # zipf 5.46
    "definire": (None, None, None, None, None),  # zipf 5.43
    "stabilire": (None, None, None, None, None),  # zipf 5.36
    "fornire": (None, None, None, None, None),  # zipf 5.35
    "garantire": (None, None, None, None, None),  # zipf 5.27
}

# Count for verification
TOTAL_CLASSIFICATIONS = len(VERB_IRREGULARITY_CLASSIFICATIONS)
