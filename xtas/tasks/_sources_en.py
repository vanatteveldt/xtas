from saf.saf import SAF
import collections
import logging

import _sources

SAY_VERBS = {"tell", "show", " acknowledge", "admit", "affirm", "allege", "announce", "assert", "attest", "avow", "claim", "comment", "concede", "confirm", "declare", "deny", "exclaim", "insist", "mention", "note", "proclaim", "remark", "report", "say", "speak", "state", "suggest", "talk", "tell", "write", "add"}

def get_regular_quote_en(saf, token):
    c = dict(saf.get_children(token))
    if token['lemma'] in SAY_VERBS:
        src = _sources.get_first_value(c, ["nsubj", "agent"])
        quote = _sources.get_first_value(c, ["ccomp", "dep", "parataxis", "dobj", "nsubjpass"])
        if src and quote:
            return (src, quote)
        elif src:
            relparent = saf.get_parent(token)
            if relparent:
                if relparent[0] == 'advcl':
                    return (src, relparent[1])

    if 'prepc_according_to' in c and 'pobj' in c:
        return (c['pobj'], token)
    # it looks like the corenlp treatment of according to has changed?
    if 'nmod:according_to' in c:
        return (c['nmod:according_to'], token)

QUOTE_FUNCTIONS = [_sources.get_token_quotes(get_regular_quote_en)]

def add_quotes(saf):
    return _sources.add_quotes(saf, QUOTE_FUNCTIONS)


ACTION_NOUNS= {"attack","bombardment","bombing","ambush","strike","raid","invasion","mission","offensive","occupation","assault","aggression","war","kill","massacre","slaughter","assassination","destruction","operation"}
ACTOR_ENTITIES = {"PERSON", "MISC", "ORGANIZATION"}

def add_nominal_clauses(saf, clauses):
    # TODO: is this useful? Is it generalizable?
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

    actions = {node['id'] for node in saf.saf['tokens']
               if node['pos1'] == 'N' and node['lemma'] in ACTION_NOUNS}

    for subj, pred in clauses:
        yield _get_subject(subj), pred
        actions -= {subj}

    for action in actions:
        subj = _get_nominal_subject(action)
        if subj:
            yield subj, action


def add_clauses(saf):
    return _sources.add_clauses(saf)
