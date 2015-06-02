"""Embarrasingly parallel (per-document) tasks."""

from __future__ import absolute_import

import json
import os
from urllib import urlencode
from urllib2 import urlopen
import socket

import nltk

from .es import fetch
from ..celery import app
from ..downloader import download_stanford_ner


@app.task
def morphy(doc):
    """Lemmatize tokens using morphy, WordNet's lemmatizer."""
    # XXX Results will be better if we do POS tagging first, but then we
    # need to map Penn Treebank tags to WordNet tags.
    nltk.download('wordnet', quiet=False)
    return map(nltk.WordNetLemmatizer().lemmatize,
               _tokenize_if_needed(fetch(doc)))


def _tokenize_if_needed(s):
    if isinstance(s, basestring):
        # XXX building token dictionaries is actually wasteful...
        return [tok['token'] for tok in tokenize(s)]
    return s


_STANFORD_DEFAULT_MODEL = \
    'classifiers/english.all.3class.distsim.crf.ser.gz'


@app.task
def stanford_ner_tag(doc, model=_STANFORD_DEFAULT_MODEL):
    """Named entity recognizer using Stanford NER.

    Parameters
    ----------
    doc : document

    model : str, optional
        Name of model file for Stanford NER tagger, relative to Stanford NER
        installation directory.

    Returns
    -------
    tagged : list of list of pair of string
        For each sentence, a list of (word, tag) pairs.
    """
    import nltk
    from nltk.tag.stanford import NERTagger

    nltk.download('punkt', quiet=False)
    ner_dir = download_stanford_ner()

    doc = fetch(doc)
    sentences = (_tokenize_if_needed(s) for s in nltk.sent_tokenize(doc))

    tagger = NERTagger(os.path.join(ner_dir, model),
                       os.path.join(ner_dir, 'stanford-ner.jar'))
    return tagger.batch_tag(sentences)


@app.task
def pos_tag(tokens, model):
    if model != 'nltk':
        raise ValueError("unknown POS tagger %r" % model)
    return nltk.pos_tag([t["token"] for t in tokens])


@app.task
def tokenize(doc):
    text = fetch(doc)
    return [{"token": t} for t in nltk.word_tokenize(text)]


@app.task
def semanticize(doc):
    text = fetch(doc)

    lang = 'nl'
    if not lang.isalpha():
        raise ValueError("not a valid language: %r" % lang)
    url = 'http://semanticize.uva.nl/api/%s?%s' % (lang,
                                                   urlencode({'text': text}))
    return json.loads(urlopen(url).read())['links']


@app.task
def untokenize(tokens):
    return ' '.join(tokens)

@app.task
def parzu(doc):
    from .parzu import parse, conll_to_saf
    text = fetch(doc)
    return conll_to_saf(parse(text))

    
@app.task(output=['tokens', 'entities', 'dependencies', 'coreferences', 'trees'])
def corenlp(doc):
    # Output: saf article with trees
    # Requires CORENLP_HOME to point to the stanford corenlp folder
    from .corenlp import parse, stanford_to_saf
    text = fetch(doc)
    return stanford_to_saf(parse(text))


@app.task(output=['tokens', 'entities'])
def corenlp_lemmatize(doc):
    # Output: saf article with tokens only
    # Requires CORENLP_HOME to point to the stanford corenlp folder
    from .corenlp import parse, stanford_to_saf
    text = fetch(doc)
    result = parse(text, annotators=["tokenize", "ssplit", "pos", "lemma", "ner"], memory="700M")
    return stanford_to_saf(result)

@app.task
def semafor(saf):
    # Input: saf article with trees (ie corenlp output)
    # Output: saf article with frames
    # Requires CORENLP_HOME to point to the stanford corenlp folder
    # Requires semafor web service to be listening to SEMAFOR_HOST:SEMAFOR:PORT
    from .semafor import add_frames
    if isinstance(saf, (str, unicode)):
        saf = json.loads(saf)
    add_frames(saf)
    return saf



@app.task(output=["tokens", "dependencies"])
def alpino(doc):
    # Output: saf article
    # Requires ALPINO_HOME to point to the stanford corenlp folder
    from .alpino import parse_text
    text = fetch(doc)
    return parse_text(text)


@app.task(output=["tokens", "dependencies", "sources"])
def sources_nl(saf):
    # Input: saf article with dependencies (alpino output)
    # Output: saf article with dependencies and sources
    # Requires syntaxrules to be on the PYTHONATH
    # Requires a sparql server running on http://localhost:3030/x
    # See https://github.com/vanatteveldt/syntaxrules/
    from ._sources_nl import add_quotes
    return add_quotes(saf)

@app.task(output=['tokens', 'entities', 'dependencies', 'coreferences', 'trees', 'sources'])
def sources_en(saf):
    # Input: saf article with dependencies (corenlp output)
    # Output: saf article with dependencies and sources
    from ._sources_en import add_quotes
    add_quotes(saf)
    return saf

@app.task(output=["tokens", "dependencies", "sources", "clauses"])
def clauses_nl(saf):
    # Input: saf article with dependencies (corenlp output) and optional sources
    # Output: saf article with clauses added
    from ._sources_nl import add_clauses
    return add_clauses(saf)
    
@app.task(output=['tokens', 'entities', 'dependencies', 'coreferences', 'trees', 'sources', 'clauses'])
def clauses_en(saf):
    # Input: saf article with dependencies (corenlp output) and optional sources
    # Output: saf article with clauses added
    from ._sources_en import add_clauses
    add_clauses(saf)
    return saf

@app.task(output=["tokens", "entities"])
def frog(doc):
    """
    Run a document through the frog server at localhost:9887
    @return: saf article with tokens+lemmata
    """
    from .frog import frog_saf
    text = fetch(doc)
    return frog_saf(text)

@app.task
def psalm_dummy(doc):
    raise NotImplementedError()
