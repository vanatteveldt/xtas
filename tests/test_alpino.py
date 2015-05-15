"""
Test the CoreNLP parser/lemmatizer functions and task.
"""

import logging
from unittest import SkipTest
import os.path

from nose.tools import assert_equal, assert_not_equal, assert_in

from xtas.tasks.alpino import parse_text, tokenize, get_alpino_version, parse_raw


def _check_alpino():
    try:
        get_alpino_version()
    except Exception, e:
        raise SkipTest(e)

def test_tokenize():
    _check_alpino()
    x = tokenize("Dit is een test. Zin twee!")
    sents = [s.strip() for s in x.split("\n") if s.strip()]
    assert_equal(len(sents), 2)
    assert_equal(sents[1].split(), ['Zin', 'twee', '!'])

def test_parse_raw():
    _check_alpino()
    triples = parse_raw("Het regent .").split("\n")
    assert_in("regen/[1,2]|verb|hd/su|het/[0,1]|noun|1", triples)

def test_parse():
    _check_alpino()
    saf = parse_text("Het regent .")
    assert_equal(len(saf['tokens']), 3)
    
    
