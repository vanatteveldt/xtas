"""Elasticsearch stuff."""

from __future__ import absolute_import

import re
from datetime import datetime

from elasticsearch import Elasticsearch, client, exceptions

from ..celery import app
from xtas import esconfig
_es = Elasticsearch([{"host": esconfig.ES_HOST, "port": esconfig.ES_PORT}])

_ES_DOC_FIELDS = ('index', 'type', 'id', 'field')

def es_document(idx, typ, id, field):
    """Returns a handle on a document living in the ES store.

    Returns a dict instead of a custom object to ensure JSON serialization
    works.
    """
    return {'index': idx, 'type': typ, 'id': id, 'field': field}


def fetch(doc):
    """Fetch document (if necessary).

    Parameters
    ----------
    doc : {dict, string}
        A dictionary representing a handle returned by es_document, or a plain
        string.
    """
    def get_text(src, field):
        # get the text from the field, default ignore new lines except for paragraph markers
        text = src[field]
        if not (text and text.strip()): return ""
        return "\n".join(re.sub("\s+"," ", para)
                         for para in text.split("\n\n"))
    if isinstance(doc, dict) and set(doc.keys()) == set(_ES_DOC_FIELDS):
        idx, typ, id, field = [doc[k] for k in _ES_DOC_FIELDS]
        if isinstance(field, (str, unicode)):
            field = [field]
        src = _es.get_source(index=idx, doc_type=typ, id=id)
        text = [get_text(src, f) for f in field]
        return "\n".join(t for t in text if t)
    else:
        # Assume simple string
        return doc


@app.task
def fetch_query_batch(idx, typ, query, field='body'):
    """Fetch all documents matching query and return them as a list.

    Returns a list of field contents, with documents that don't have the
    required field silently filtered out.
    """
    r = _es.search(index=idx, doc_type=typ, body={'query': query},
                   _source=[field])
    r = (hit['_source'].get(field, None) for hit in r['hits']['hits'])
    return [hit for hit in r if hit is not None]

CHECKED_MAPPINGS = set()

def _mapping_exists(idx, typ):
    if typ in CHECKED_MAPPINGS:
        return True
    else:
        indices_client = client.indices.IndicesClient(_es)
        result = indices_client.exists_type(idx, typ)
        if result: CHECKED_MAPPINGS.add(typ)
        return result

def _check_mapping(idx, parent_type, child_type, properties):
    """
    Check that a mapping for the child_type exists
    Creates a new mapping with parent_type if needed
    """
    if not _mapping_exists(idx, child_type):
        indices_client = client.indices.IndicesClient(_es)
        props = {prop :{"type" : "object", "enabled" : False}
                 for prop in properties}
        body = {child_type: {"_parent": {"type": parent_type},
                             "properties" : props}}
        indices_client.put_mapping(index=idx, doc_type=child_type,
                                   body=body)
        CHECKED_MAPPINGS.add(child_type)


@app.task
def store_single(data, taskname, properties, idx, typ, id):
    """Store the data as a child document."""
    doc_type = "{typ}__{taskname}".format(**locals())
    _check_mapping(idx, typ, doc_type, properties)
    now = datetime.now().isoformat()
    _es.index(index=idx, doc_type=doc_type, id=id, body=data, parent=id)
    return data

def get_multiple_results(docs, taskname):
    """
    Get all xtas results for the given documents and task name
    returns a sequence of doc, result tuples
    """
    def get_single(docs, prop):
        result = {d[prop] for d in docs}
        if len(result) > 1:
            raise ValueError("All documents need to have the same {prop}"
                             .format(**locals()))
        return result.pop()
    idx, typ = (get_single(docs, p) for p in ["index", "type"])
    doc_type = "{typ}__{taskname}".format(**locals())

    if not _mapping_exists(idx, doc_type):
        # mapping does not exist, so empty results
        for doc in docs:
            yield doc, None
        return

    docdict = {unicode(doc['id']): doc for doc in docs}
    getdocs = [{"_index" : idx, "_id" : doc['id'], "_parent" : doc['id'], "_type" : doc_type}
               for doc in docs]

    results = _es.mget({"docs": getdocs})['docs']
    for d in results:
        typ = d['_type'].split("__")[0]
        doc = docdict[d['_id']]
        result = d['_source'] if d['found'] else None
        yield doc, result


from hashlib import sha224 as hash_class
def adhoc_document(idx, typ, fld, text):
    """
    Retrieve the adhoc document with that text, or create a new one
    @returns: the id of the (existing or created) document
    """
    hash = hash_class(text.encode("utf-8")).hexdigest()
    try:
        _es.get(index=idx, doc_type=typ, id=hash, _source=False)
    except exceptions.NotFoundError:
        _es.index(index=idx, doc_type=typ, id=hash, body={fld: text})
    return es_document(idx, typ, hash, fld)
