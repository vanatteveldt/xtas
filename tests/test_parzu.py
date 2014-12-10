"""
Test the CoreNLP parser/lemmatizer functions and task.
"""

import logging
from unittest import SkipTest
import os.path

from nose.tools import assert_equal, assert_not_equal, assert_in

from xtas.tasks.parzu import parse, conll_to_saf, get_parzu_version
from xtas.tasks.single import parzu

def _check_parzu():
    try:
        get_parzu_version()
    except:
        logging.exception("Error on getting parzu version")
        raise SkipTest("ParZu not found at PARZU_HOME")

def test_parse():
    _check_parzu()
    conll = parse("Das ist ein Test.")
    print `conll`
    tokens = conll.strip().split("\n")
    assert_equal(len(tokens), 5)
    assert_equal(tokens[1].split("\t")[2], "sein")

TEST_CONLL = """1	Wer	wer	PRO	PWS	_|Nom|Sg	2	pred	_	_ 
2	bin	sein	V	VAFIN	1|Sg|Pres|Ind	0	root	_	_ 
3	ich	ich	PRO	PPER	1|Sg|_|Nom	2	subj	_	_ 
4	?	?	$.	$.	_	0	root	_	_ 

1	Und	und	KON	KON	_	0	root	_	_ 
2	wo	wo	PWAV	PWAV	_	3	adv	_	_ 
3	bist	sein	V	VAFIN	2|Sg|Pres|Ind	1	cj	_	_ 
4	du	du	PRO	PPER	2|Sg|_|Nom	3	subj	_	_ 
5	?	?	$.	$.	_	0	root	_	_ 
"""

def test_conll_to_saf():
    saf = conll_to_saf(TEST_CONLL)
    assert_equal({t['sentence'] for t in saf['tokens']}, {1,2})
    tokens = {t['word'] : t['id'] for t in saf['tokens']}
    assert_in({'child' : tokens['du'], 'parent': tokens['bist'], 'relation': 'subj'},
              saf['dependencies'])

def test_task():
    _check_parzu()
    saf = parzu(u"Er f\xe4hrt schnell")
    assert_equal({'er', 'fahren', 'schnell'}, {t['lemma'] for t in saf['tokens']})
