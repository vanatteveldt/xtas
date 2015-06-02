"""
Test the English sources and clauses extraction.
"""

import logging
from unittest import SkipTest
import os.path, tempfile, hashlib

#from nose.tools import assert_equal, assert_not_equal, assert_in
def assert_equal(a,b):
    assert a==b, "{a!r} != {b!r}".format(**locals())
def assert_in(a,b):
    #print "{a} in {b}?".format(**locals())
    assert a in b, "{a!r} not in {b!r}".format(**locals())

from saf.saf import SAF
from saf import visualize
from xtas.tasks.corenlp import parse, stanford_to_saf, get_corenlp_version
from xtas.tasks._sources_en import add_quotes, add_clauses
def _check_corenlp():
    v = get_corenlp_version()
    if not v:
        raise SkipTest("CoreNLP not found at CORENLP_HOME")

def get_parse_xml(sentence):
    # cache parsing in tempdir - corenlp is *slow* to start up
    fn = os.path.join(tempfile.gettempdir(),
                      "__xtas__test_sources_en_{}.xml".format(hashlib.md5(sentence).hexdigest()))
    if os.path.exists(fn):
        return open(fn).read()
    else:
        xml = parse(sentence)
        open(fn, 'w').write(xml)
        return xml

def get_saf(sentence):
    saf = stanford_to_saf(get_parse_xml(sentence))
    open("/tmp/tree.png", "w").write(visualize.get_graphviz(SAF(saf)).draw(format='png', prog='dot'))
    return saf
        
def test_sources():
    _check_corenlp()
    def _test(sentence, source, quote=None):
        saf = get_saf(sentence)
        saf = SAF(add_quotes(saf))
        if quote is None:
            assert_equal(len(saf.sources), 0)
        else:
            assert_equal(len(saf.sources), 1)
            src, = saf.sources
            assert_in(source, [saf.get_token(tid)['lemma'] for tid in src['source']])
            assert_in(quote, [saf.get_token(tid)['lemma'] for tid in src['quote']])

    _test("John says he likes Mary.", "John", "like")
    _test("According to embassy sources, the butler stole all the wine", "source", "butler")
    _test("According to the Professor, Mr. Green is fantastic.", "Professor", "fantastic")
    _test("The policeman insisted: he is a crook", "policeman", "crook")
    _test("The policeman insisted on his innocence", None)
    _test("The policeman told me he was a crook", "policeman", "crook")
    #_test("John: 'this has got to stop'", "John", "stop") # Is this a realistic pattern?

def test_clauses():
    _check_corenlp()
    def _test(sentence, subject, predicate=None):
        saf = get_saf(sentence)
        saf = add_quotes(saf)
        saf = SAF(add_clauses(saf))
        if subject is None:
            assert_equal(len(saf.clauses), 0)
        else:
            assert_equal(len(saf.clauses), 1)
            src, = saf.clauses
            assert_in(subject, [saf.get_token(tid)['lemma'] for tid in src['subject']])
            assert_in(predicate, [saf.get_token(tid)['lemma'] for tid in src['predicate']])

    _test("John loves Mary", "John", "Mary")
    _test("John says wonderful things", None)
    _test("John says he loves Mary", "he", "Mary")


def _lemmata(saf, tokens):
    result = set()
    tokens = [(saf.get_token(t) if isinstance(t, int) else t) for t in tokens]
    tokens = sorted(tokens, key=lambda t:(t['sentence'], t['offset']))
    return " ".join(t['word'] for t in tokens)
    
def test_multiline():
    _check_corenlp()
    sent = 'John told me he was confused. "I think he killed me. Why did he do that?"'
    
    saf = SAF(add_quotes(get_saf(sent)))

    assert_equal(len(saf.sources), 3)
    
    quotes = {(_lemmata(saf, src['source']), _lemmata(saf, src['quote'])) for src in saf.sources}
    assert_equal(quotes, {("John", "he was confused"), ("John", 'I think he killed me'), ("John", "Why did he do that")})

    

