import logging
import json
import time
from Queue import Queue, Empty
from threading import Thread, current_thread

from elasticsearch import Elasticsearch
from elasticsearch.client import indices

from xtas.tasks import es
from xtas.tasks.es import es_document, _check_mapping
from xtas.tasks.pipeline import pipeline, _normalize_pipe, _task_name


def get_filter(setid, doctype):
    """Create a DSL filter dict to filter on set and no existing parser"""
    noparse =  {"not" : {"has_child" : { "type": doctype,
                                         "query" : {"match_all" : {}}}}}
    return {"bool" : {"must" : [{"term" : {"sets" : setid}}, noparse]}}

def get_articles(es, index, doctype, parent_doctype, setid, size=100):
    """Return one or more ranbom uncached articles from the set"""
    body = {"query" : {"function_score" : {"filter" : get_filter(setid, doctype), "random_score" : {}}}}
    result = es.search(index=index, doc_type=parent_doctype, body=body, fields=[], size=size)
    n = result['hits']['total']
    logging.warn("Fetched aids, {n} remaining".format(**locals()))
    return n, [int(r['_id']) for r in result['hits']['hits']]

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=9200)
    parser.add_argument('--index', default='amcat')
    parser.add_argument('--parent-doctype', default='article' )
    parser.add_argument('--field', default='headline,text' )
    parser.add_argument('--n', type=int, default=25)
    parser.add_argument('--verbose',  action='store_true')
    parser.add_argument('--norepeat', action='store_true')
    parser.add_argument('--single', action='store_true', help="Parse a single article: set argument is interpreted as article id")

    parser.add_argument('set', type=int)
    parser.add_argument('modules', nargs="+")

    args = parser.parse_args()

    logging.basicConfig(format='[%(asctime)s %(levelname)s %(name)s:%(lineno)s %(threadName)s] %(message)s', level=logging.INFO if args.verbose else logging.WARN)
    
    fields = [x.strip() for x in args.field.split(",")]
    
    from xtas.celery import app
    app.conf['CELERY_ALWAYS_EAGER'] = True

    logging.warn("Connecting to elastic at {args.host}:{args.port}".format(**locals()))
    es._es = Elasticsearch(hosts=[{"host":args.host, "port": args.port}], timeout=600)

    pipe = list(_normalize_pipe(args.modules))
    doctype = "{typ}__{taskname}".format(typ=args.parent_doctype, taskname=_task_name(pipe))
    _check_mapping(args.index, args.parent_doctype, doctype, pipe[-1]['output'])
    
    while True:
        indices.IndicesClient(es._es).flush()
        if args.single:
            n, aids = 1, [args.set]
        else:
            logging.warn("Retrieving {args.n} articles".format(**locals()))
            try:
                n, aids = list(get_articles(es._es, args.index, doctype, args.parent_doctype, args.set, size=args.n))
            except:
                logging.exception("Error on get_articles, retrying in 10 seconds")
                time.sleep(10)
                continue
        if not aids:
            break
        docs = [es_document(args.index, args.parent_doctype, aid, fields)
                for aid in aids]

        for i, doc in enumerate(docs):
            try:
                logging.warn("Proccesing {doc} ({i}/{m}; {n} left in set)"
                             .format(m=len(docs), **locals()))
                pipeline(doc, pipe)
            except:
                logging.exception("Error on processing {doc}".format(**locals()))

        logging.info("Done with this set!")
        
        if args.norepeat or args.single:
            break
    logging.info("Done!")
