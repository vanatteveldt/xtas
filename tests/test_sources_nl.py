"""
Test the Dutch sources and clauses extraction.
"""

import logging
from unittest import SkipTest
import os.path

from nose.tools import assert_equal, assert_not_equal, assert_in

from xtas.tasks.alpino import get_alpino_version, parse_text
from xtas.tasks._sources_nl import get_quotes, add_quotes, add_clauses

from saf.saf import SAF
from saf import visualize

def _check_alpino():
    try:
        get_alpino_version()
    except Exception, e:
        raise SkipTest(e)

def _test_source(sent, source, quote=None):
    saf = SAF(parse_text(sent))
    
    #open("/tmp/tree.png", "w").write(visualize.get_graphviz(saf).draw(format='png', prog='dot'))
    saf = SAF(add_quotes(saf))
    
    if source is None:
        assert_equal(len(saf.sources), 0, "Did not expect source in {sent!r}, found {saf.sources}"
                     .format(**locals()))
    else:
        assert_equal(len(saf.sources), 1, "No source found in {sent!r}".format(**locals()))
        assert_in(source, {saf.get_token(t)['lemma']  for t in saf.sources[0]['source']})
        assert_in(quote, {saf.get_token(t)['lemma']  for t in saf.sources[0]['quote']})
    

def _test_clause(sent, subj, pred=None):
    saf = SAF(parse_text(sent))
    saf = SAF(add_quotes(saf))
    saf = SAF(add_clauses(saf))
    
    #g = visualize.get_graphviz(saf)
    #open("/tmp/tree.png", "w").write(g.draw(format='png', prog='dot'))

    if subj is None:
        assert_equal(len(saf.clauses), 0, "Did not expect clause in {sent!r}, found {saf.clauses}"
                     .format(**locals()))
    else:
        assert_equal(len(saf.clauses), 1, "None or too many clauses found in {sent!r}: {saf.clauses}"
                     .format(**locals()))
        assert_in(subj, {saf.get_token(t)['lemma']  for t in saf.clauses[0]['subject']})
        assert_in(pred, {saf.get_token(t)['lemma']  for t in saf.clauses[0]['predicate']})


        
def test_clauses():
    _check_alpino()
    _test_clause("De dokter zegt dat je de pil moet slikken", "je", "slik")
    _test_clause("Volgens Jan wel", None)
    _test_clause("Piet wordt door Jan geslagen", "Jan", "Piet")
    _test_clause("Jan haat Piet", "Jan", "haat")
        
        
def test_sources():
    _check_alpino()
    _test_source("Door boze tongen wordt beweerd dat Piet stom is", "tong", "stom")
    _test_source("Mozes: leert uw syntax", "Mozes", "leer")
    _test_source("Het is niet eerlijk, zegt Nel", "Nel", "ben")
    _test_source("Het blijkt baarlijke nonsens", None)
    _test_source("Uit vertrouwelijke documenten blijkt dat JSF best duur is", "document", "dat")
    _test_source("Het is onzin, volgens Pietje", "Pietje", "ben")
    _test_source("Volgens mijn vader zou de tuin gesnoeid moeten worden", "vader", "snoei") # zou?
    _test_source("De heer Wilders zegt: ze moeten er allemaal uit", "heer", "moet")
    _test_source("Jan zegt dat Piet stom is", "Jan", "ben")
    _test_source("Wij vinden dat Piet het niet had moeten doen", "wij", "heb")

def test_multiline():
    _check_alpino()
    sent = 'De Jong dreigde om naar de politie te stappen, aldus de advocaat. ,,Joran lachte hem in zijn gezicht uit. Hij heeft hem nooit een cent gegeven."'
    
    saf = SAF(parse_text(sent))
    sources = list(get_quotes(saf))
    assert_equal(len(sources), 3)
    quotes = {(s['lemma'], q['lemma']) for (s,q) in sources}
    assert_equal(quotes, {("advocaat", "dreig"), ("advocaat", '"'), ("advocaat", "heb")})

    _test_source('Wilders wist daar wel raad mee. "Wat een nonsens!"', "Wilders", 'nonsens')

    
