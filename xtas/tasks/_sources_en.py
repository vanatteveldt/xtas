from saf import SAF

SAY_VERBS = {"tell", "show", " acknowledge", "admit", "affirm", "allege", "announce", "assert", "attest", "avow", "claim", "comment", "concede", "confirm", "declare", "deny", "exclaim", "insist", "mention", "note", "proclaim", "remark", "report", "say", "speak", "state", "suggest", "talk", "tell", "write", "add"}
QUOTE_MARKS = {'``', "''", '`', "'", '"'}

class SAF(object):
    def __init__(self, saf):
        self.saf = saf
        self._tokens = {t['id']: t for t in saf['tokens']}

    def get_token(self, token_id):
        return self._tokens[token_id]

    def get_children(self, token):
        if not isinstance(token, int): token = token['id']
        return ((rel['relation'], self.get_token(rel['child']))
                for rel in self.saf['dependencies'] if rel['parent'] == token)


    def get_tokens(self, sentence):
        return sorted((t for t in self.saf['tokens'] if t['sentence'] == sentence),
                      key = lambda t: int(t['offset']))


    def get_root(self, sentence):
        parents = {d['child'] : d['parent'] for d in self.saf['dependencies']
                   if self.get_token(d['child'])['sentence'] == sentence}
        # root is a parent that has no parents
        roots = set(parents.values()) - set(parents.keys())
        if len(roots) != 1:
            raise ValueError("Sentence {sentence} has roots {roots}".format(**locals()))
        return self.get_token(list(roots)[0])

    def get_sentences(self):
        return sorted({t['sentence'] for t in self.saf['tokens']})

    def get_node_depths(self, sentence):
        # return a dict with the dept of each node
        rels = [d for d in self.saf['dependencies']
            if self.get_token(d['child'])['sentence'] == sentence]
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
        if exclude is None: exclude = set()
        if node['id'] in exclude: return
        exclude.add(node['id'])
        yield node
        for _rel, child in self.get_children(node):
            for descendant in self.get_descendants(child, exclude):
                yield descendant


def first(seq):
    return next(iter(seq), None)

def get_first_value(dict, keys):
    return first(dict[k] for k in keys if k in dict)

def get_regular_quote(saf, token):
    c = dict(saf.get_children(token))
    if token['lemma'] in SAY_VERBS:
        src = get_first_value(c, ["nsubj", "agent"])
        quote = get_first_value(c, ["ccomp", "dep", "parataxis"])
        if src and quote:
            return (src, quote)
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
    return saf.get_tokens(sentence)[-1]['lemma'] in QUOTE_MARKS

def middle_quote(saf, sentence):
    return any(t['lemma'] in QUOTE_MARKS for t in saf.get_tokens(sentence)[1:-1])


def get_top_subject(saf, sentence):
    subjects = (rel['child'] for rel in saf.saf['dependencies']
                if rel['relation'] in ('nsubj', 'nsubjpass'))
    if subjects:
        depths = saf.get_node_depths(sentence)
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
        found = False
        for quote in get_regular_quotes(saf, s):
            found = True
            yield quote
        if not found:
            quote = get_multi_quotes(saf, s)
            if quote:
                yield quote

def get_quote_dicts(saf, quotes):
    for src, quote in quotes:
        yield

def add_quotes(saf_dict):
    saf = SAF(saf_dict)
    def expand(node, exclude):
        print "EXPANDING", node
        return [n['id'] for n in saf.get_descendants(node, exclude={exclude['id']})]
    saf_dict['sources'] = [{"source": expand(src, quote),
                           "quote": expand(quote, src)}
                          for (src, quote) in get_quotes(saf)]
    return saf_dict
