from saf.saf import SAF
import collections

SAY_VERBS = {"zeg", "stel", "roep", "schrijf", "denk", "stel_vast"}
VIND_VERBS = {"vind", "meen", "beken", "concludeer", "erken", "waarschuw", "weet"}

VIND_VERBS = SAY_VERBS | {"accepteer", "antwoord", "beaam", "bedenk", "bedoel", "begrijp", "beken", "beklemtoon", "bekrachtig", "belijd", "beluister", "benadruk", "bereken", "bericht", "beschouw", "beschrijf", "besef", "betuig", "bevestig", "bevroed", "beweer", "bewijs", "bezweer", "biecht", "breng", "brul", "concludeer", "confirmeer", "constateer", "debiteer", "declareer", "demonstreer", "denk", "draag_uit", "email", "erken", "expliceer", "expliciteer", "fantaseer", "formuleer", "geef_aan", "geloof", "hoor", "hamer", "herinner", "houd_vol", "kondig_aan", "kwetter", "licht_toe", "maak_bekend", "maak_hard", "meld", "merk", "merk_op", "motiveer", "noem", "nuanceer", "observeer", "onderschrijf", "onderstreep", "onthul", "ontsluier", "ontval", "ontvouw", "oordeel", "parafraseer", "postuleer", "preciseer", "presumeer", "pretendeer", "publiceer", "rapporteer", "realiseer", "redeneer", "refereer", "reken", "roep", "roer_aan", "ruik", "schat", "schets", "schilder", "schreeuw", "schrijf", "signaleer", "snap", "snater", "specificeer", "spreek_uit", "staaf", "stel", "stip_aan", "suggereer", "tater", "teken_aan", "toon_aan", "twitter", "verbaas", "verhaal", "verklaar", "verklap", "verkondig", "vermoed", "veronderstel", "verraad", "vertel", "vertel_na", "verwacht", "verwittig", "verwonder", "verzeker", "vind", "voel", "voel_aan", "waarschuw", "wed", "weet", "wijs_aan", "wind", "zeg", "zet_uiteen", "zie", "twitter"}

VOLGENS = {"volgens", "aldus"}
QUOTES = {'"', "'", "''", "`", "``"}
QPUNC = QUOTES | {":"}


def get_regular_quote(saf, token):
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

def get_colon_quote(saf, sentence):
    for token in saf.get_tokens(sentence):
        if token['lemma'].strip() in QPUNC:  # a: bla 
            source = saf.get_child(token, " --")
            if source and source['pos1'] != '.':
                quote =  saf.get_child(source, "tag", "nucl", "sat")
                if quote:
                    return (source, quote)


# TODO: refactor to get rid of copypasta from sources_en

                    
def get_regular_quotes(saf, sentence):
    for t in saf.get_tokens(sentence):
        q = get_regular_quote(saf, t)
        if q: yield q


def start_quote(saf, sentence):
    t = saf.get_tokens(sentence)
    return t and (t[0]['lemma'] in QUOTES)

def end_quote(saf, sentence):
    t = saf.get_tokens(sentence)
    return t and (t[-1]['lemma'] in QUOTES)

def middle_quote(saf, sentence):
    t = saf.get_tokens(sentence)
    return t and any(t['lemma'] in QUOTES for t in saf.get_tokens(sentence)[1:-1])


def first(seq):
    return next(iter(seq), None)
    
def get_top_subject(saf, sentence):
    subjects = [rel['child'] for rel in saf.saf['dependencies']
                if rel['relation'] in ('su', 'agent')]
    if subjects:
        depths = saf.get_node_depths(sentence)
        if depths:
            return saf.get_token(first(sorted(subjects, key=lambda su: depths.get(su, 99999999))))


def get_multi_quotes(saf, sentence):
    if middle_quote(saf, sentence):
        return # too complicated for now
    if start_quote(saf, sentence):
        previous = sentence - 1
    elif end_quote(saf, sentence) and (start_quote(saf, sentence-1)):
        previous = sentence - 2
    elif end_quote(saf, sentence) and (start_quote(saf, sentence-2)):
        previous = sentence - 3
    elif start_quote(saf, sentence-1) and end_quote(saf, sentence+1):
        previous = sentence - 2
    else:
        return # no quote found
    try:
        root = saf.get_root(sentence)
    except ValueError:
        return# multiple roots - too complicated for now
    quotes = list(get_regular_quotes(saf, previous))
    if quotes: # source of first quote is source
        return (quotes[0][0], root)
    else: # subject of previous sentence is source
        src = get_top_subject(saf, previous)
        if src:
            return src, root


        
def get_quotes(saf):
    for s in saf.get_sentences():
        found = False
        for quote in get_regular_quotes(saf, s):
            found = True
            yield quote
        if not found:
            quote = get_colon_quote(saf, s)
            if quote:
                yield quote
        if not found:
            quote = get_multi_quotes(saf, s)
            if quote:
                yield quote
            


    
def get_clauses(saf):
    if 'dependencies' not in saf.saf: return
    sources = set()
    if 'sources' in saf.saf:
        # skip existing sources
        for quote in saf.saf['sources']:
            sources |= set(quote['source'])

    surels = [rel for rel in saf.saf['dependencies']
              if  rel['relation'] in ('su', 'agent') and rel['child'] not in sources]
    predicates = {rel['parent'] for rel in surels}
    for pred in predicates:
        children = {rel['child'] for rel in surels if rel['parent'] == pred}
        # eliminate children who are descendant of other children
        for child in children:
            others = {c for c in children if c != child}
            if not any(saf.is_descendant(child, c) for c in others):
                yield child, pred


def prune_clauses(saf, clauses):
    def is_contained(node, others):
        for other in others:
            if node == other: continue
            if node in {n['id'] for n in saf.get_descendants(other)}:
                return True

    # if two clauses have the same subject, and the predicate of B is a subset of A, drop B
    clauses_per_subject = collections.defaultdict(list)
    for s, p in clauses:
        clauses_per_subject[s].append(p)
    for s, ps in clauses_per_subject.iteritems():
        for p in ps:
            i = is_contained(p, ps)
            if not is_contained(p, ps):
                yield s, p

def add_clauses(saf):
    if isinstance(saf, dict):
        saf = SAF(saf)
    def make_clause(subj, pred):
        # i.e. in case of tie, node goes to subject, so expand subject first and use all to exclude

        subj = [n['id'] for n in saf.get_descendants(subj, exclude={pred})] if subj else []
        pred = [n['id'] for n in saf.get_descendants(pred, exclude=set(subj))]
        return {"subject": subj, "predicate": pred}
    clauses = get_clauses(saf)
    clauses = prune_clauses(saf, clauses)
    saf.clauses = [make_clause(s, p) for (s,p) in clauses]

    return saf.saf


def add_quotes(saf):
    if isinstance(saf, dict): 
        saf = SAF(saf)
    saf = saf.resolve_passive()
    def expand(node, exclude):
        return [n['id'] for n in saf.get_descendants(node, exclude={exclude['id']})]
    saf.sources = [{"source": expand(src, quote),
                    "quote": expand(quote, src)}
                   for (src, quote) in get_quotes(saf)]
    return saf.saf
