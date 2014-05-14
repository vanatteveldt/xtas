"""
Semafor semantic parser

This module requires the semafor web service running at
SEMAFOR_HOST:SEMAFOR_PORT (defaults to localhost:9888).

If called with a penn treebank (?) style parse tree, also
requires CORENLP_HOME to convert it to conll style.

See: https://github.com/sammthomson/semafor

- Clone/download semafor
- Build with mvn package
- Download semafor malt model
- Run the web server:
java -Xms4g -Xmx4g -cp target/Semafor-3.0-alpha-04.jar \
    edu.cmu.cs.lti.ark.fn.SemaforSocketServer \
    model-dir:/home/wva/semafor_malt_model_20121129 port:9888
"""

from __future__ import absolute_import

import datetime
import socket
import json
import os
from cStringIO import StringIO

from xtas.tasks.corenlp import to_conll

def get_settings():
    host = os.environ.get("SEMAFOR_HOST", "localhost")
    port = int(os.environ.get("SEMAFOR_PORT", 9888))
    return host, port

def nc(host, port, input):
    """'netcat' implementation, see http://stackoverflow.com/a/1909355"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect( (host, port ))
    s.sendall(input)
    s.shutdown(socket.SHUT_WR)
    s.settimeout(30)
    result = StringIO()
    while 1:
        data = s.recv(1024)
        if data == "":
            s.close()
            return result.getvalue()
        result.write(data)

def call_semafor(conll_str):
    """
    Use semafor to parse the conll_str. Assumes that semafor is running as a web service on
    localhost:9888, and assumes conll_str to be a string representation of parse trees in conll format
    """
    result = nc("localhost", 9888, conll_str)
    return [json.loads(sent) for sent in result.split("\n") if sent.strip()]


def add_frames(saf_article):
    # assume that the article has penn-style treebanks
    saf_article['frames'] = []
    module = "semafor"
    provenance = {'module': module,
                  'module-version': "3.0a4",
                  "started": datetime.datetime.now().isoformat()}

    saf_article['header']['processed'].append(provenance)
    for t in saf_article['trees']:
        sid = int(t['sentence'])
        tree = t['tree']
        conll = to_conll(tree)
        tokens = sorted((w for w in saf_article['tokens']
                         if w['sentence'] == sid),
                        key=lambda token: int(token['offset']))
        try:
            sent, = call_semafor(conll)
        except socket.timeout, e:
            err = {"module": module, "sentence": sid, "error": unicode(e)}
            saf_article.setdefault('errors', []).append(err)
            continue
        if "error" in sent:
            err = {"module": module, "sentence": sid}
            err.update(sent)
            saf_article.setdefault('errors', []).append(err)
            continue


        frames, sem_tokens = sent["frames"], sent["tokens"]
        assert len(tokens) == len(sem_tokens)

        def get_tokenids(f):
            for span in f["spans"]:
                for i in range(span["start"], span["end"]):
                    yield tokens[i]['id']

        for frame in frames:
            f = {"sentence" : sid,
                 "name" : frame["target"]["name"],
                 "target" : list(get_tokenids(frame["target"])),
                 "elements" : []}
            for a in frame["annotationSets"][0]["frameElements"]:
                f["elements"].append({"name" : a["name"],
                                      "target" : list(get_tokenids(a))})
            saf_article['frames'].append(f)
