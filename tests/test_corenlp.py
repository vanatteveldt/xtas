"""
Test the Stanford corenlp parser and Semafor semantic parser
"""

import json
import os
import csv
import socket
from unittest import SkipTest

from nose.tools import assert_equal, assert_not_equal

def _check_corenlp_home():
    if not os.environ.get("CORENLP_HOME"):
        raise SkipTest("Cannot find CORENLP_HOME")


def _check_semafor():
    from xtas.tasks.semafor import get_settings
    host, port = get_settings()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = s.connect_ex((host, port))
        if(result != 0) :
            raise SkipTest("Semafor web service not listening to {host}:{port}"
                           .format(**locals()))
    finally:
        s.close()

def test_to_conll():
    from xtas.tasks.corenlp import to_conll
    _check_corenlp_home()

    tree = ("(ROOT (S (NP (DT The) (JJ second) (NN miner)) "
            "(VP (VBD was) (VP (VBN found) "
            "(PP (IN in) (NP (DT the) (NN afternoon))))) (. .)))")

    result = to_conll(tree)

    result = [row for row in csv.reader(result.split("\n"), delimiter="\t")
              if row]
    assert_equal(len(result), 9)
    assert_equal(result[0][:4], ['1', 'The', '_', 'DT'])


def test_corenlp_semafor():
    "Try corenlp + semafor pipe. Very slow test due to corenlp loading time."
    from xtas.tasks.corenlp import parse_text
    from xtas.tasks.semafor import get_frames
    _check_semafor()
    _check_corenlp_home()

    sent = "John loves Mary"
    try:
        saf = json.load(open("/tmp/test_corenlp.json"))
    except:
        saf = parse_text(sent)
        json.dump(saf, open("/tmp/test_corenlp.json", "w"))

    assert_equal(set(t['lemma'] for t in saf['tokens']),
                 {'John', 'love', 'Mary'})
    tokens = {token['id'] : token['lemma'] for token in saf['tokens']}
    deps = set((tokens[t['child']], t['relation'], tokens[t['parent']])
               for t in saf['dependencies'])
    assert_equal(set(tokens.values()), {'John', 'love', 'Mary'})
    assert_equal(deps, {('John', 'nsubj', 'love'),
                        ('Mary', 'dobj', 'love')})
    frames = list(get_frames(saf))

    assert_equal(len(frames), 1)
    assert_equal(frames[0]['name'], 'Experiencer_focus')
    assert_equal({tokens[t] for t in frames[0]['target']}, {'love'})

if __name__ == '__main__':
    test_corenlp_semafor()
