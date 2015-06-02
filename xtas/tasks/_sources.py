import logging
import collections

from saf.saf import SAF

QUOTE_MARKS = {'``', "''", '`', "'", '"'}

def first(seq):
    return next(iter(seq), None)

def get_first_value(dict, keys):
    return first(dict[k] for k in keys if k in dict)


def start_quote(saf, sentence):
    t = saf.get_tokens(sentence)
    return t and (t[0]['lemma'] in QUOTE_MARKS)

def end_quote(saf, sentence):
    t = saf.get_tokens(sentence)
    return t and (t[-1]['lemma'] in QUOTE_MARKS)

def middle_quote(saf, sentence):
    t = saf.get_tokens(sentence)
    return t and any(t['lemma'] in QUOTE_MARKS for t in saf.get_tokens(sentence)[1:-1])


def get_top_subject(saf, sentence):
    subjects = (rel['child'] for rel in saf.saf['dependencies']
                if rel['relation'] in ('nsubj', 'nsubjpass', 'su', 'agent')) # why nsubjpass for en, agent for nl?
    if subjects:
        depths = saf.get_node_depths(sentence)
        if depths:
            return saf.get_token(first(sorted(subjects, key=lambda su: depths.get(su, 99999999))))


def get_multi_quotes(saf, sentence, quotes):
    def get_previous_source(s):
        for src, quote in quotes:
            if src['sentence'] == s:
                return src
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
    root = saf.get_root(sentence)
    src = get_previous_source(previous) # source of last quote is source
    if not src: # subject of previous sentence is source
        src = get_top_subject(saf, previous)
    if src:
        return [(src, root)]

def get_token_quotes(quote_function):
    return lambda saf, s: (quote_function(saf, t) for t in saf.get_tokens(s))


def get_sentence_quotes(saf, s, quote_functions):
    for fn in quote_functions:
        quotes = [q for q in fn(saf, s) if q]
        if quotes:
            return quotes

def get_quotes(saf, quote_functions):
    result = []
    for s in saf.get_sentences():
        try:
            quotes = get_sentence_quotes(saf, s, quote_functions)
            if not quotes:
                quotes = get_multi_quotes(saf, s, result)
            if quotes:
                result += quotes
        except:
            logging.exception("Error on getting quotes, skipping sentence")
    return result

def get_quote_dicts(saf, quotes):
    for src, quote in quotes:
        yield

def add_quotes(saf, quote_functions):
    if isinstance(saf, dict):
        saf = SAF(saf)
    def expand(node, exclude):
        return [n['id'] for n in saf.get_descendants(node, exclude={exclude['id']})]
    saf.sources = [{"source": expand(src, quote),
                    "quote": expand(quote, src)}
                   for (src, quote) in get_quotes(saf, quote_functions)]
    return saf.saf


def get_clauses(saf):
    if 'dependencies' not in saf.saf: return
    sources = set()
    if 'sources' in saf.saf:
        # skip existing sources
        for quote in saf.saf['sources']:
            sources |= set(quote['source'])

    surels = [rel for rel in saf.saf['dependencies']
              if  rel['relation'] in ('su', 'nsubj', 'agent') and rel['child'] not in sources]
    predicates = {rel['parent'] for rel in surels}
    for pred in predicates:
        children = {rel['child'] for rel in surels if rel['parent'] == pred}
        # eliminate children who are descendant of other children
        for child in children:
            others = {c for c in children if c != child}
            if not any(saf.is_descendant(child, c) for c in others):
                yield child, pred

    #add dangling passives
    for rel in saf.saf['dependencies']:
        if (rel['relation'] == 'nsubjpass'
            and rel['child'] not in sources
            and rel['parent'] not in predicates
            and saf.get_token(rel['parent'])['lemma'] not in SAY_VERBS):

            yield None, rel['parent']


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

def add_clauses(saf_dict):
    saf = SAF(saf_dict)
    def make_clause(subj, pred):
        # i.e. in case of tie, node goes to subject, so expand subject first and use all to exclude

        subj = [n['id'] for n in saf.get_descendants(subj, exclude={pred})] if subj else []
        pred = [n['id'] for n in saf.get_descendants(pred, exclude=set(subj))]
        return {"subject": subj, "predicate": pred}
    clauses = get_clauses(saf)
    #clauses = add_nominal_clauses(saf, clauses)
    clauses = prune_clauses(saf, clauses)
    saf.clauses = [make_clause(s, p) for (s,p) in clauses]

    return saf.saf
