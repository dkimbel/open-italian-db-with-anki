"""Enumeration types for Italian linguistic data.

These StrEnum classes provide type safety while maintaining backward
compatibility with SQLite string storage. Since StrEnum values serialize
as strings, no database migration is needed.
"""

from enum import StrEnum


class POS(StrEnum):
    """Part of speech classification for lemmas."""

    VERB = "verb"
    NOUN = "noun"
    ADJECTIVE = "adjective"

    @property
    def plural(self) -> str:
        """Return the plural form for display (e.g., 'verbs')."""
        return {
            POS.VERB: "verbs",
            POS.NOUN: "nouns",
            POS.ADJECTIVE: "adjectives",
        }[self]


class GenderClass(StrEnum):
    """Gender classification for nouns.

    - M: masculine only (e.g., libro)
    - F: feminine only (e.g., casa)
    - COMMON_GENDER_FIXED: both genders with identical forms (e.g., cantante)
    - COMMON_GENDER_VARIABLE: both genders with different forms (e.g., collega)
    - BY_SENSE: gender depends on meaning (e.g., il fine=goal vs la fine=end)
    """

    M = "m"
    F = "f"
    COMMON_GENDER_FIXED = "common_gender_fixed"
    COMMON_GENDER_VARIABLE = "common_gender_variable"
    BY_SENSE = "by_sense"


class DerivationType(StrEnum):
    """Morphological derivation type for nouns.

    These indicate size/affect modifications from a base noun.
    The field is nullable; None means no derivation.
    """

    DIMINUTIVE = "diminutive"
    AUGMENTATIVE = "augmentative"
    PEJORATIVE = "pejorative"


# =============================================================================
# Verb Irregularity Pattern Enums
# =============================================================================
#
# These enums classify Italian irregular verb patterns by tense domain.
# A verb can have irregularities in multiple tenses (e.g., venire has
# g_insertion in present, strong_nn in remote, syncopated_rr in future).
#
# NULL/None means the verb is regular in that tense domain.


class PresentPattern(StrEnum):
    """Present tense irregularity patterns.

    These patterns describe irregular formations in the present indicative
    (and often subjunctive, which typically derives from the present stem).
    """

    # G-insertion: -go/-gono in 1sg/3pl (most common pattern)
    G_INSERTION = "g_insertion"  # vengo, tengo, rimango, spengo, scelgo, colgo, valgo, salgo
    GG_INSERTION = "gg_insertion"  # traggo, pongo (double-g before -o)

    # Vowel alternations (stressed root diphthongizes)
    DIPHTHONG_IE = "diphthong_ie"  # tieni/tiene, vieni/viene, siedi/siede
    DIPHTHONG_UO = "diphthong_uo"  # muoio/muori/muore, puoi/può, vuoi/vuole

    # Contracted stems (from Latin infinitives)
    CONTRACTED_FAC = "contracted_fac"  # faccio/fai/fa (from facere → fare)
    CONTRACTED_DIC = "contracted_dic"  # dico/dici/dice (from dicere → dire)
    CONTRACTED_BEV = "contracted_bev"  # bevo/bevi/beve (from bevere → bere)
    CONTRACTED_DUC = "contracted_duc"  # traduco/traduci (from traducere → tradurre)
    CONTRACTED_PON = "contracted_pon"  # pongo/poni (from ponere → porre)
    CONTRACTED_TRA = "contracted_tra"  # traggo/trai (from trahere → trarre)

    # Suppletive (completely irregular paradigms)
    SUPPLETIVE_ESSERE = "suppletive_essere"  # sono/sei/è/siamo/siete/sono
    SUPPLETIVE_AVERE = "suppletive_avere"  # ho/hai/ha/abbiamo/avete/hanno
    SUPPLETIVE_ANDARE = "suppletive_andare"  # vado/vai/va/andiamo/andate/vanno
    SUPPLETIVE_STARE = "suppletive_stare"  # sto/stai/sta/stiamo/state/stanno
    SUPPLETIVE_DARE = "suppletive_dare"  # do/dai/dà/diamo/date/danno

    # SC/ESC alternation
    ESC_ALTERNATION = "esc_alternation"  # esco/esci/esce vs usciamo/uscite/escono

    # Modal-like patterns (irregular throughout)
    MODAL_POTERE = "modal_potere"  # posso/puoi/può/possiamo/potete/possono
    MODAL_VOLERE = "modal_volere"  # voglio/vuoi/vuole/vogliamo/volete/vogliono
    MODAL_DOVERE = "modal_dovere"  # devo(debbo)/devi/deve/dobbiamo/dovete/devono
    MODAL_SAPERE = "modal_sapere"  # so/sai/sa/sappiamo/sapete/sanno

    # Suppletive -ire verbs
    SUPPLETIVE_UDIRE = "suppletive_udire"  # odo/odi/ode/udiamo/udite/odono


class RemotePattern(StrEnum):
    """Passato remoto (remote past) irregularity patterns.

    Strong passato remoto has irregular 1sg, 3sg, 3pl with regular 2sg, 1pl, 2pl.
    The pattern describes the consonant/vowel change in the strong forms.
    """

    # Double consonant strong patterns
    STRONG_SS = "strong_ss"  # dissi/disse/dissero (dire, scrivere, leggere, cuocere)
    STRONG_NN = "strong_nn"  # venni/venne/vennero (venire, tenere)
    STRONG_BBI = "strong_bbi"  # conobbi/conobbe/conobbero (conoscere, crescere)

    # Single consonant strong patterns
    STRONG_SI = "strong_si"  # presi/prese/presero (prendere, chiedere, spendere)
    STRONG_LSI = "strong_lsi"  # risolsi/risolse (risolvere, assolvere)
    STRONG_NSI = "strong_nsi"  # vinsi/vinse (vincere, convincere)
    STRONG_RSI = "strong_rsi"  # corsi/corse (correre)

    # Vowel-change strong patterns
    STRONG_VOWEL_I = "strong_vowel_i"  # vidi/vide/videro (vedere)
    STRONG_VOWEL_E = "strong_vowel_e"  # feci/fece/fecero (fare)

    # Q-insertion pattern (stem-final -c- → -cqu-)
    STRONG_CQUI = "strong_cqui"  # piacqui/piacque (piacere, nascere, tacere, giacere)

    # RR-doubling pattern
    STRONG_RR = "strong_rr"  # parvi/parve → appears as irregular (parere)

    # Suppletive patterns
    SUPPLETIVE_ESSERE = "suppletive_essere"  # fui/fosti/fu/fummo/foste/furono
    SUPPLETIVE_AVERE = "suppletive_avere"  # ebbi/avesti/ebbe/avemmo/aveste/ebbero


class FuturePattern(StrEnum):
    """Future/conditional stem irregularity patterns.

    The conditional always uses the same stem as the future, so one pattern
    covers both tenses. The pattern describes how the infinitive stem contracts.
    """

    # Syncopated with -rr- (vowel deleted, r doubles)
    SYNCOPATED_RR = "syncopated_rr"  # verrò, terrò, rimarrò, berrò, parrò, vorrò, morrò

    # Syncopated with -dr- (vowel deleted, no doubling)
    SYNCOPATED_DR = "syncopated_dr"  # andrò, vedrò, potrò, dovrò, saprò, avrò, cadrò, vivrò

    # Contracted base (stem from contracted Latin infinitive)
    CONTRACTED_BASE = "contracted_base"  # farò, dirò, trarró, porrò, condurrò (already short)

    # Suppletive
    SUPPLETIVE = "suppletive"  # sarò (essere)


class ParticiplePattern(StrEnum):
    """Past participle irregularity patterns.

    Regular participles: -are→-ato, -ere→-uto, -ire→-ito.
    These patterns describe irregular past participle formations.
    """

    # Strong -tto pattern
    STRONG_TTO = "strong_tto"  # fatto, detto, scritto, letto, tradotto, rotto, cotto, fritto

    # Strong -sto pattern
    STRONG_STO = "strong_sto"  # chiesto, visto, rimasto, posto, risposto, nascosto

    # Strong -so pattern
    STRONG_SO = "strong_so"  # preso, acceso, sceso, speso, offeso, difeso, riso, deciso, ucciso

    # Strong -to with stem change (by vowel)
    STRONG_TO_ERTO = "strong_to_erto"  # aperto, coperto, offerto, sofferto
    STRONG_TO_ORTO = "strong_to_orto"  # morto, torto, scorto, accorto
    STRONG_TO_INTO = "strong_to_into"  # vinto, dipinto, spinto, tinto, finto
    STRONG_TO_ONTO = "strong_to_onto"  # giunto, punto, assunto, presunto
    STRONG_TO_OLTO = "strong_to_olto"  # risolto, sciolto, tolto, colto, volto, assolto, sepolto
    STRONG_TO_ATTO = "strong_to_atto"  # fatto, tratto, attratto (overlaps with tto)
    STRONG_TO_ETTO = "strong_to_etto"  # stretto, detto (overlaps with tto for some)
    STRONG_TO_OTTO = "strong_to_otto"  # rotto, cotto, dotto (overlaps)
    STRONG_TO_ESSO = "strong_to_esso"  # messo, permesso, commesso

    # Suppletive
    SUPPLETIVE = "suppletive"  # stato (essere)


class SubjunctivePattern(StrEnum):
    """Subjunctive present irregularity patterns.

    Many verbs derive their subjunctive from the present indicative stem.
    These patterns identify verbs with truly suppletive subjunctive forms
    that aren't predictable from the indicative.
    """

    # Suppletive subjunctive stems (not derivable from present indicative)
    SUPPLETIVE_SIA = "suppletive_sia"  # essere: sia/sia/sia/siamo/siate/siano
    SUPPLETIVE_ABBIA = "suppletive_abbia"  # avere: abbia (vs ho)
    SUPPLETIVE_SAPPIA = "suppletive_sappia"  # sapere: sappia (vs so)
    SUPPLETIVE_STIA = "suppletive_stia"  # stare: stia (vs sto)
    SUPPLETIVE_DIA = "suppletive_dia"  # dare: dia (vs do)
    SUPPLETIVE_VADA = "suppletive_vada"  # andare: vada (vs vado - close but distinct)
    SUPPLETIVE_FACCIA = "suppletive_faccia"  # fare: faccia (from present faccio - predictable)
    SUPPLETIVE_DICA = "suppletive_dica"  # dire: dica (from present dico - predictable)
