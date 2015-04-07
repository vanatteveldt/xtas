from saf import SAF
import collections
import logging

SAY_VERBS = {"tell", "show", " acknowledge", "admit", "affirm", "allege", "announce", "assert", "attest", "avow", "claim", "comment", "concede", "confirm", "declare", "deny", "exclaim", "insist", "mention", "note", "proclaim", "remark", "report", "say", "speak", "state", "suggest", "talk", "tell", "write", "add"}
QUOTE_MARKS = {'``', "''", '`', "'", '"'}

ACTION_NOUNS= {"attack","bombardment","bombing","ambush","strike","raid","invasion","mission","offensive","occupation","assault","aggression","war","kill","massacre","slaughter","assassination","destruction","operation"}
ACTOR_ENTITIES = {"PERSON", "MISC", "ORGANIZATION"}

class SAF(object):
    def __init__(self, saf):
        self.saf = saf
        self._tokens = {t['id']: t for t in saf['tokens']}
        self._children = None # cache token : [(rel, child), ...]


    def get_token(self, token_id):
        return self._tokens[token_id]

    def get_children(self, token):
        if not isinstance(token, int): token = token['id']
        if self._children is None:
            self._children = collections.defaultdict(list)
            for rel in self.saf['dependencies']:
                self._children[rel['parent']].append((rel['relation'], self.get_token(rel['child'])))
        return self._children[token]

    def get_parent(self, token):
        if not isinstance(token, int): token = token['id']
        for rel in self.saf['dependencies']:
            if rel['child'] == token:
                return rel['relation'], self.get_token(rel['parent'])

    def get_tokens(self, sentence):
        return sorted((t for t in self.saf['tokens'] if t['sentence'] == sentence),
                      key = lambda t: int(t['offset']))

    def get_entity(self, token):
        if not isinstance(token, int): token = token['id']
        if 'entities' in self.saf:
            for entity in self.saf['entities']:
                if token in entity['tokens']:
                    return entity['type']



    def get_root(self, sentence):
        parents = {d['child'] : d['parent'] for d in self.saf['dependencies']
                   if self.get_token(d['child'])['sentence'] == sentence}
        # root is a parent that has no parents
        roots = set(parents.values()) - set(parents.keys())
        if len(roots) != 1:
            raise ValueError("Sentence {sentence} has roots {roots}, parents {parents}".format(**locals()))
        return self.get_token(list(roots)[0])

    def get_sentences(self):
        return sorted({t['sentence'] for t in self.saf['tokens']})

    def get_node_depths(self, sentence):
        # return a dict with the dept of each node
        rels = [d for d in self.saf['dependencies']
            if self.get_token(d['child'])['sentence'] == sentence]
        if not rels:
            return {}
        generations = {self.get_root(sentence)['id'] : 0}
        changed = True
        while changed:
            changed = False
            for rel in rels:
                if rel['child'] not in generations and rel['parent'] in generations:
                    generations[rel['child']] = generations[rel['parent']] + 1
                    changed = True
        return generations

    def get_descendants(self, node, exclude=None):
        """
        Yield all descendants (including the node itself),
        stops when a node in exclude is reached
        @param exlude: a set of nodes to exclude
        """
        if isinstance(node, int): node = self.get_token(node)
        if exclude is None: exclude = set()
        exclude = {n if isinstance(n, int) else n['id'] for n in exclude}
        if node['id'] in exclude: return
        exclude.add(node['id'])
        yield node
        for _rel, child in self.get_children(node):
            for descendant in self.get_descendants(child, exclude):
                yield descendant

    def is_descendant(self, node, possible_ancestor):
        return any(node == descendant['id'] for descendant in self.get_descendants(possible_ancestor))

def first(seq):
    return next(iter(seq), None)

def get_first_value(dict, keys):
    return first(dict[k] for k in keys if k in dict)

def get_regular_quote(saf, token):
    c = dict(saf.get_children(token))
    if token['lemma'] in SAY_VERBS:
        src = get_first_value(c, ["nsubj", "agent"])
        quote = get_first_value(c, ["ccomp", "dep", "parataxis", "dobj", "nsubjpass"])
        if src and quote:
            return (src, quote)
        elif src:
            relparent = saf.get_parent(token)
            if relparent:
                if relparent[0] == 'advcl':
                    return (src, relparent[1])

    if 'prepc_according_to' in c and 'pobj' in c:
        return (c['pobj'], token)

def get_regular_quotes(saf, sentence):
    for t in saf.get_tokens(sentence):
        q = get_regular_quote(saf, t)
        if q: yield q


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
                if rel['relation'] in ('nsubj', 'nsubjpass'))
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
    root = saf.get_root(sentence)
    quotes = list(get_regular_quotes(saf, previous))
    if quotes: # source of first quote is source
        return (quotes[0][0], root)
    else: # subject of previous sentence is source
        src = get_top_subject(saf, previous)
        if src:
            return src, root

def get_quotes(saf):
    for s in saf.get_sentences():
        try:

            found = False
            for quote in get_regular_quotes(saf, s):
                found = True
                yield quote
            if not found:
                quote = get_multi_quotes(saf, s)
                if quote:
                    yield quote
        except:
            logging.exception("Error on getting root, skipping sentence")



def get_quote_dicts(saf, quotes):
    for src, quote in quotes:
        yield

def add_quotes(saf_dict):
    saf = SAF(saf_dict)
    def expand(node, exclude):
        return [n['id'] for n in saf.get_descendants(node, exclude={exclude['id']})]
    saf_dict['sources'] = [{"source": expand(src, quote),
                           "quote": expand(quote, src)}
                          for (src, quote) in get_quotes(saf)]
    return saf_dict

def get_clauses(saf):
    if 'dependencies' not in saf.saf: return
    sources = set()
    if 'sources' in saf.saf:
        # skip existing sources
        for quote in saf.saf['sources']:
            sources |= set(quote['source'])

    surels = [rel for rel in saf.saf['dependencies']
              if  rel['relation'] in ('nsubj', 'agent') and rel['child'] not in sources]
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


def add_nominal_clauses(saf, clauses):
    actions = {node['id'] for node in saf.saf['tokens']
               if node['pos1'] == 'N' and node['lemma'] in ACTION_NOUNS}

    def _get_nominal_subject(action):
        children = {rel: child for (rel, child) in saf.get_children(action)}
        if 'poss' in children:
            return children['poss']['id']
        if 'amod' in children and saf.get_entity(children['amod']) in ACTOR_ENTITIES:
            return children['amod']['id']

    def _get_subject(subj):
        if subj in actions:
            nomsubj = _get_nominal_subject(subj)
            if nomsubj:
                return nomsubj
        return subj

    for subj, pred in clauses:
        yield _get_subject(subj), pred
        actions -= {subj}

    for action in actions:
        subj = _get_nominal_subject(action)
        if subj:
            yield subj, action




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
        pred = [n['id'] for n in saf.get_descendants(pred, exclude=subj)]
        return {"subject": subj, "predicate": pred}
    clauses = get_clauses(saf)
    clauses = add_nominal_clauses(saf, clauses)
    clauses = prune_clauses(saf, clauses)
    saf_dict['clauses'] = [make_clause(s, p) for (s,p) in clauses]

    return saf_dict
