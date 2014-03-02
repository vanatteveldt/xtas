"""Elasticsearch stuff."""

from __future__ import absolute_import

import re
from datetime import datetime

from elasticsearch import Elasticsearch, client, exceptions

from ..celery import app

_es = Elasticsearch()


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
    if isinstance(doc, dict) and set(doc.keys()) == set(_ES_DOC_FIELDS):
        idx, typ, id, field = [doc[k] for k in _ES_DOC_FIELDS]
        text = _es.get_source(index=idx, doc_type=typ, id=id)[field]
        text = "\n".join(re.sub("\s+"," ", para)
                         for para in text.split("\n\n"))
        return text
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


def _check_parent_mapping(idx, child_type, parent_type):
    """
    Check that a mapping for the child_type exists
    Creates a new mapping with parent_type if needed
    """
    if not child_type in CHECKED_MAPPINGS:
        indices_client = client.indices.IndicesClient(_es)
        if not indices_client.exists_type(idx, child_type):
            body = {child_type: {"_parent": {"type": parent_type}}}
            indices_client.put_mapping(index=idx, doc_type=child_type,
                                       body=body)
        CHECKED_MAPPINGS.add(child_type)


@app.task
def store_single(data, taskname, idx, typ, id):
    """Store the data as a child document."""
    child_type = "{typ}__{taskname}".format(**locals())
    _check_parent_mapping(idx, child_type, typ)
    now = datetime.now().isoformat()
    doc = {'data': data, 'timestamp': now}
    _es.index(index=idx, doc_type=child_type, id=id, body=doc, parent=id)
    return data


def get_all_results(idx, typ, id):
    """
    Get all xtas results for the document
    Returns a (possibly empty) {taskname : data} dict
    """
    def strip_prefix(child_type):
        """Remove the {typ__} from the beginning of the child type"""
        return child_type[len(typ)+2:]

    body = {"filter": {"has_parent": {
        "filter": {"ids": {"values": [id]}},
        "type": typ
    }}}
    r = _es.search(index=idx, body=body, _source=['data'])
    return {strip_prefix(h['_type']): h['_source']['data']
            for h in r['hits']['hits']}


def get_single_result(taskname, idx, typ, id):
    """Get a single xtas result"""
    child_type = "{typ}__{taskname}".format(**locals())
    try:
        r = _es.get_source(index=idx, doc_type=child_type, id=id, parent=id)
        return r['data']
    except exceptions.TransportError, e:
        if e.status_code != 404:
            raise
