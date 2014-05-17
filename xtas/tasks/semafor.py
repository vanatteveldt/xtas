"""
Semafor semantic parser

This module assumes SEMAFOR_HOME to point to the location
where semafor is cloned/installed, and MALT_MODEL_DIR to
the location where the Malt models are downloaded.

If called with a penn treebank (?) style parse tree, also
requires CORENLP_HOME to convert it to conll style.

This module runs semafor in 'interactive' mode, which is added
on the interactive_mode branch of vanatteveldt/semafor.

git clone -b interactive_mode https://github.com/vanatteveldt/semafor

See: https://github.com/sammthomson/semafor
"""

from __future__ import absolute_import

import datetime
import json
import os

from xtas.tasks.corenlp import to_conll


import threading
import subprocess
import tempfile


class Semafor(object):
    def __init__(self):
        self.start_semafor()

    def start_semafor(self):
        semafor_home = os.environ["SEMAFOR_HOME"]
        model_dir = os.environ.get("MALT_MODEL_DIR", semafor_home)
        cp = os.path.join(semafor_home, "target", "Semafor-3.0-alpha-04.jar")
        cmd = ["java","-Xms4g","-Xmx4g","-cp",cp,
               "edu.cmu.cs.lti.ark.fn.SemaforInteractive",
               "model-dir:{model_dir}".format(**locals())]
        self.process = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE)
        list(self.wait_for_prompt())

    def wait_for_prompt(self):
        while True:
            line = self.process.stdout.readline()
            if line == '':
                raise Exception("Unexpected EOF")
            if line.strip() == ">>>":
                break
            yield line

    def call_semafor(self, conll_str):
        self.process.stdin.write(conll_str.strip())
        self.process.stdin.write("\n\n")
        self.process.stdin.flush()
        lines = list(self.wait_for_prompt())
        assert len(lines) == 1
        return json.loads(lines[0])


_SINGLETON_LOCK = threading.Lock()
def call_semafor(conll_str):
    """
    Call semafor on the given conll_str using a thread-safe singleton instance
    """
    with _SINGLETON_LOCK:
        if not hasattr(Semafor, '_singleton'):
            Semafor._singleton = Semafor()
        return Semafor._singleton.call_semafor(conll_str)


def add_frames(saf_article):
    # assume that the article has penn-style treebanks
    saf_article['frames'] = []
    module = "semafor"
    provenance = {'module': module,
                  'module-version': "3.0a4",
                  "started": datetime.datetime.now().isoformat()}

    saf_article['header']['processed'].append(provenance)

    trees = [t['tree'] for t in saf_article['trees']]
    sids = [int(t['sentence']) for t in saf_article['trees']]
    conlls = to_conll(trees)

    assert len(sids) == len(conlls)
    for sid, conll in zip(sids, conlls):
        tokens = sorted((w for w in saf_article['tokens']
                         if w['sentence'] == sid),
                        key=lambda token: int(token['offset']))
        sent = call_semafor(conll)
        if "error" in sent:
            err = {"module": module, "sentence": sid}
            err.update(sent)
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
