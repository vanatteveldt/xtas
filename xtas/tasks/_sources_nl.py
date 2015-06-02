from saf.saf import SAF
import collections

SAY_VERBS = {"zeg", "stel", "roep", "schrijf", "denk", "stel_vast"}
VIND_VERBS = {"vind", "meen", "beken", "concludeer", "erken", "waarschuw", "weet"}

VIND_VERBS = SAY_VERBS | {"accepteer", "antwoord", "beaam", "bedenk", "bedoel", "begrijp", "beken", "beklemtoon", "bekrachtig", "belijd", "beluister", "benadruk", "bereken", "bericht", "beschouw", "beschrijf", "besef", "betuig", "bevestig", "bevroed", "beweer", "bewijs", "bezweer", "biecht", "breng", "brul", "concludeer", "confirmeer", "constateer", "debiteer", "declareer", "demonstreer", "denk", "draag_uit", "email", "erken", "expliceer", "expliciteer", "fantaseer", "formuleer", "geef_aan", "geloof", "hoor", "hamer", "herinner", "houd_vol", "kondig_aan", "kwetter", "licht_toe", "maak_bekend", "maak_hard", "meld", "merk", "merk_op", "motiveer", "noem", "nuanceer", "observeer", "onderschrijf", "onderstreep", "onthul", "ontsluier", "ontval", "ontvouw", "oordeel", "parafraseer", "postuleer", "preciseer", "presumeer", "pretendeer", "publiceer", "rapporteer", "realiseer", "redeneer", "refereer", "reken", "roep", "roer_aan", "ruik", "schat", "schets", "schilder", "schreeuw", "schrijf", "signaleer", "snap", "snater", "specificeer", "spreek_uit", "staaf", "stel", "stip_aan", "suggereer", "tater", "teken_aan", "toon_aan", "twitter", "verbaas", "verhaal", "verklaar", "verklap", "verkondig", "vermoed", "veronderstel", "verraad", "vertel", "vertel_na", "verwacht", "verwittig", "verwonder", "verzeker", "vind", "voel", "voel_aan", "waarschuw", "wed", "weet", "wijs_aan", "wind", "zeg", "zet_uiteen", "zie", "twitter"}

VOLGENS = {"volgens", "aldus"}
QUOTES = {'"', "'", "''", "`", "``"}
QPUNC = QUOTES | {":"}


def get_regular_quote_nl(saf, token):
    if token['lemma'] == "blijk": # x blijkt uit y
        quote = saf.get_child(token, "su", "agent")
        uit = saf.get_child(token, "pc", lemma="uit")
        if uit:
            src = saf.get_child(uit, "obj1")
            if src and quote:
                return (src, quote)

    if token['lemma'] in VOLGENS: # volgens x is bla
        src = saf.get_child(token, "obj1")
        rel, quote = saf.get_parent(token)
        if src and rel in {"mod", "tag"}:
            return (src, quote)

    if token['lemma'] in VIND_VERBS: # a zegt: bla (was: say_verbs)
        rel, parent = saf.get_parent(token)
        if rel and rel.strip() == "--" and parent['lemma'].strip() in QPUNC:
            src = saf.get_child(token, "su", "agent")
            quote = saf.get_child(token, "nucl")
            if src and quote:
                return (src, quote)

    if token['lemma'] in VIND_VERBS:
        src = saf.get_child(token, "su", "agent")
        if src:
            dat = saf.get_child(token, "vc", lemma='dat')
            if dat: # a zegt dat bla
                quote = saf.get_child(dat, "body")
                if quote:
                    return (src, quote)
            else: # bla, zegt a
                rel, parent = saf.get_parent(token)
                if rel == "tag":
                    return (src, parent)

def get_colon_quote_nl(saf, sentence):
    for token in saf.get_tokens(sentence):
        if token['lemma'].strip() in QPUNC:  # a: bla
            source = saf.get_child(token, " --")
            if source and source['pos1'] != '.':
                quote =  saf.get_child(source, "tag", "nucl", "sat")
                if quote:
                    return (source, quote)


QUOTE_FUNCTIONS = [_sources.get_token_quotes(get_regular_quote_nl),
                   get_colon_quote_nl]


def add_quotes(saf):
    return _sources.add_quotes(saf, QUOTE_FUNCTIONS)


def add_clauses(saf):
    return _sources.add_clauses(saf)
