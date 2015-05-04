from unidecode import unidecode
import socket
from StringIO import StringIO
from itertools import takewhile
import datetime

_POSMAP = {"VZ" : "P",
           "N" : "N",
           "ADJ" : "A",
           "LET" : ".",
           "VNW" : "O",
           "LID" : "D",
           "SPEC" : "M",
           "TW" : "Q",
           "WW" : "V",
           "BW" : "B",
           "VG" : "C",
           "TSW" : "I",
           "MWU" : "U",
           "" : "?",
}

def _call_tadpole(text, host="localhost", port=9887):
    if not isinstance(text, unicode): text = unicode(text)
    text = unidecode(text).encode("utf-8")
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect( (host, port ))
    s.sendall(text)
    s.shutdown(socket.SHUT_WR)
    return s.makefile('r')


def tadpole(text):
    if not text.endswith("\n"): text = text + "\n"
    sid = 0
    tokens, entities = [], []
    for i, l in enumerate(_call_tadpole(text)):
        l = l.strip('\n')
        if l == 'READY': # end of parse
            break
        elif not l: # end of sentence
            sid += 1
        else:
            tid, token, lemma, morph, pos, conf, ner, chunk, parent_tid, dependency = l.split("\t")
            pos1 = _POSMAP[pos.split("(")[0]]
            tokens.append(dict(id=i, sentence=sid, word=token, lemma=lemma,
                               pos=pos, pos1=pos1, pos_confidence=float(conf)))
            print ">>", ner
            if ner.startswith("B-"):
                type = ner.split("_")[0].split("-")[-1]
                entities.append({'tokens': [], 'type': type})
            if ner != "O":
                entities[-1]['tokens'].append(i)
    return tokens, entities

def tadpole_saf(text):
    tokens, entities = tadpole(text)
    return {"header" : {'format': "SAF",
                      'format-version': "0.0",
                      'processed': [{'module': "tadpole",
                                     "started": datetime.datetime.now().isoformat()}
                                    ]
                    },
            "tokens" : tokens}
    
