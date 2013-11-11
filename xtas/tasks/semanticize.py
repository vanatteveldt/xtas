"""Semanticizer task, using the REST endpoint."""

# The official ES client tends to die mysteriously with ZeroDivisionErrors.
#from elasticsearch import Elasticsearch
import requests

from ..taskregistry import task
from ..util import getconf, slashjoin


@task('/semanticize/<index>/<doc_type>/<int:id>')
def semanticize(doc_type, id, index, config):
    sem = getconf(config, 'worker semanticizer', error='raise')
    uri = getconf(sem, 'uri', error='raise')
    lang = getconf(sem, 'lang', error='raise')
    uri = slashjoin([uri, lang])

    #es = Elasticsearch(getconf(config, 'main elasticsearch', error='raise'))
    #doc = es.get(index=index, doc_type=doc_type, id=id)
    #content = doc['_source']['body']

    es = getconf(config, 'main elasticsearch', error='raise')
    doc = requests.get(slashjoin([es, index, doc_type, str(id)]))
    content = doc.json()['_source']['body']

    r = requests.get(uri, params={'text': content})
    if getconf(config, 'main debug'):
        print("Requested %r" % r.url)
    return r.json()
