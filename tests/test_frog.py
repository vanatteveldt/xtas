"""
Test the CoreNLP parser/lemmatizer functions and task.
"""

import logging
from unittest import SkipTest
import os.path
import socket;
    
from nose.tools import assert_equal, assert_in

from xtas.tasks.frog import frog, frog_saf

def _check_frog(host="localhost", port=9887):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((host, port))
    if not result == 0:
        raise SkipTest("No frog server found at {host}:{port}"
                       .format(**locals()))

def test_frog():
    _check_frog()
    tokens, entities = frog("Mark Rutte werkte gisteren nog bij de  Vrije Universiteit in Amsterdam")
    #print "\n".join(map(str, tokens)), "\n", "\n".join(map(str, entities))
    assert_equal(len(tokens), 10)
    assert_equal(tokens[0]['pos1'], 'M')
    assert_equal(tokens[0]['lemma'], 'Mark_Rutte')
    assert_equal(tokens[-1]['word'], 'Amsterdam')
    
    assert_equal(len(entities), 3)
    assert_in({'tokens': [6,7], 'type': 'ORG'}, entities)

def test_frog_saf():
    _check_frog()
    saf = frog_saf("Mark Rutte werkte gisteren nog bij de  Vrije Universiteit in Amsterdam")
    assert_equal(len(saf['entities']), 3)
    assert_equal(len(saf['tokens']), 10)