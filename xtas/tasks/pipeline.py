"""
Pipelining with partial caching
"""

import celery

from xtas.tasks.es import _ES_DOC_FIELDS, get_multiple_results, store_single, fetch, adhoc_document
from xtas.celery import app

def _normalize_pipe(pipe):
    """
    Convert a sequence of modules into module/output/arguments dicts
    """
    for task_dict in pipe:
        if isinstance(task_dict, (str, unicode)):
            module, arguments = task_dict, {}
        else:
            module, arguments = task_dict['module'], task_dict.get('arguments', {})
        if isinstance(module, (str, unicode)):
            if "." not in module: module = "xtas.tasks.single.{module}".format(**locals())
            module = app.tasks[module]
        yield dict(module=module, output=module.output, arguments=arguments)

        
def _task_name(tasks):
    return "__".join(t['module'].name.split(".")[-1] for t in tasks)

    
def pipeline_multiple(docs, pipe, store_final=True, store_intermediate=False, block=True, **apply_kwargs):
    """
    Pipeline should be a list of dicts, with members task and argument
    e.g. [{"module" : "tokenize"},
          {"module" : "pos_tag", "arguments" : {"model" : "nltk"}}]
    @param store_final: if True, store the final result
    @param store_intermediate: if True, store all intermediate results as well
    @return a dict of {doc : {failure: boolean, result: result_or_exception}}
    """
    
    def store_task(tasks, doc):
        taskname = _task_name(tasks)
        properties = tasks[-1]['output']
        return store_single.s(taskname, properties, doc['index'], doc['type'], doc['id'])
            
    def get_chained_task(input, tasks, all_tasks, doc=None):
        """
        Get a celery task with the input and the chained tasks
        @param tasks: a list of dictionaries with 'module' and 'arguments']
        """
        head, tail = tasks[0], tasks[1:]
        # Apply result (cached value) to start of chain, add rest if needed
        head = head['module'].s(input, **head['arguments'])
        tail = [x['module'].s(**x['arguments']) for x in tail]
        chain = [head] + tail
        # Store final result
        if doc and store_final:
            chain.append(store_task(all_tasks, doc))
        if doc and store_intermediate:
            offset = len(all_tasks) - len(tasks)  # i.e. number of tasks that are already cached
            for i in range(len(tasks)-1, 0, -1):
                chain.insert(i, store_task(all_tasks[:(i+offset)], doc))
        return celery.chain(*chain)

    tasks = list(_normalize_pipe(pipe))

    todo = {}
    result_dict = {}

    str_docs = [doc for doc in docs if isinstance(doc, (str, unicode))]
    docs = [doc for doc in docs if not isinstance(doc, (str, unicode))]

    for i in range(len(tasks), 0, -1):
        if not docs:
            break
        unseen = []
        taskname = _task_name(tasks[:i])
        for doc, result in get_multiple_results(docs, taskname):
            if result:
                chain = tasks[i:]
                if chain: # need to run one or more modules
                    todo.append(get_chained_task(result, chain, tasks, doc))
                else: # result is fully cached
                    result_dict[doc['id']] = {"state": "SUCCESS", "result": result}
            else:
                unseen.append(doc)
        docs = unseen

    for doc in docs:
        # not done at all
        input=fetch(doc)
        todo[doc['id']] = get_chained_task(input, tasks, tasks, doc).apply_async(**apply_kwargs)
    for doc in str_docs:
        todo[doc['id']] = get_chained_task(doc, tasks, tasks).apply_async(**apply_kwargs)

    if block:
        # fetch all 'todo' tasks
        for docid, asyncres in todo.iteritems():
            try:
                result_dict[docid] = {"state": "SUCCESS", "result": asyncres.get()}
            except:
                # workaround celery bug https://github.com/celery/celery/issues/2458
                while asyncres.result is None:
                    asyncres = asyncres.parent
                result_dict[docid] = {"state": "FAILURE", "result": asyncres.result}
    else:
        for docid, asyncres in todo.iteritems():
            result_dict[docid] = {"state": "PENDING", "result": asyncres}
            
        
    return result_dict
    
def pipeline(doc, pipeline, store_final=True, store_intermediate=False):
    """
    Get the result for a single document.
    """
    results = pipeline_multiple([doc], pipeline, store_final=store_final,
                                store_intermediate=store_intermediate)
    result = results[doc['id']]
    if result["state"] == "FAILURE":
        raise Exception(result['result'])
    else:
        return result['result']


if __name__ == '__main__':
    # provide a command line interface to pipelining
    # Maybe this should go to xtas.__main__ (?)
    # (or as xtas.pipeline)
    import argparse
    import sys
    import json
    import logging

    from xtas.tasks.es import es_document
    from xtas.tasks import app

    

    epilog = '''The modules for the pipeline can be either the names of
                one or more modules, or a json string containing modules
                and arguments, e.g.:
                \'[{"module": "xtas.tasks.single.tokenize"},
                   {"module": "xtas.tasks.single.pos_tag"
                    "arguments": ["ntlk"]}]\'
            '''

    parser = argparse.ArgumentParser(epilog=epilog)

    parser.add_argument("module", nargs="+",
                        help="Name or json list of the module(s) to run")
    parser.add_argument("--always-eager", "-a", help="Don't use celery",
                        action="store_true")
    parser.add_argument("--id", "-i", help="ID of the document to process")
    parser.add_argument("--verbose", "-v", help="Set logging level to INFO")
    parser.add_argument("--index", "-n", help="Elasticsearch index name", default="amcat")
    parser.add_argument("--doctype", "-d", help="Elasticsearch document type", default="article")
    parser.add_argument("--field", "-F", help="Elasticsearch field type", default="text")
    parser.add_argument("--store-intermediate", help="Store intermediate results",
                        action='store_true')
    parser.add_argument("--cache-adhoc", help="Cache ad-hoc (text) queries",
                        action='store_true')
    parser.add_argument("--input-file", "-f", help="Input document name. "
                        "If not given, use ID/index/doctype/field. "
                        "If neither are given, read document text from stdin")
    parser.add_argument("--output-file", "-o",
                    help="Output file. If not given, will write to stdout")
    args = parser.parse_args()

    logging.basicConfig(format='[%(asctime)s %(levelname)s %(name)s:%(lineno)s %(threadName)s] %(message)s', level=logging.INFO if args.verbose else logging.WARN)
    
    fields = [x.strip() for x in args.field.split(",")]

    # input
    if args.input_file is not None:
        doc = open(args.input_file).read().decode("utf-8")
    elif args.id is not None:
        doc = es_document(args.index or "amcat", args.doctype, args.id, fields)
    else:
        doc = sys.stdin.read().decode("utf-8")

    if args.id is None and args.cache_adhoc:
        doc = adhoc_document(args.index or "adhoc", args.doctype, fields, doc)

    # pipeline
    if '[' in args.module[0] or '{' in args.module[0]:
        # json string
        pipe = json.loads(args.module[0])
    else:
        pipe = [{"module": m} for m in args.module]

    # output
    outfile = (open(args.output_file, 'w') if args.output_file is not None
               else sys.stdout)

    if args.always_eager:
        app.conf['CELERY_ALWAYS_EAGER'] = True

    result = pipeline(doc, pipe, store_intermediate=args.store_intermediate)
    json.dump(result, outfile, indent=2)
