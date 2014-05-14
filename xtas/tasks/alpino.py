"""
Python interface for Alpino parser and conversion to xtas/SAF

This module requires Alpino to be installed, and an environment variable
ALPINO_HOME pointing to the folder where it is installed

See http://www.let.rug.nl/vannoord/alp/Alpino/
Download from http://www.let.rug.nl/vannoord/alp/Alpino/binary/versions/
"""

import subprocess
import logging
import os
import unidecode

from xtas.tasks.saf import SAF

log = logging.getLogger(__name__)

CMD_PARSE = ["bin/Alpino", "end_hook=dependencies", "-parse"]
CMD_TOKENIZE = ["Tokenization/tok"]


def parse_text(text):
    alpino_home = os.environ['ALPINO_HOME']

    tokens = tokenize(text, alpino_home)
    parse = parse_raw(tokens, alpino_home)
    return interpret_parse(parse)


def tokenize(text, alpino_home):
    if not isinstance(text, unicode):
        text = text.decode("ascii")
    
    text = unidecode.unidecode(text)    
    text = text.encode("utf-8")
    
    p = subprocess.Popen(CMD_TOKENIZE, shell=False, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, cwd=alpino_home)
    tokens, err = p.communicate(text)
    tokens = tokens.replace("|", "")  # alpino uses | for  'sid | line'
    return tokens


def parse_raw(tokens, alpino_home):
    p = subprocess.Popen(CMD_PARSE, shell=False,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         cwd=alpino_home, env={'ALPINO_HOME': alpino_home})
    parse, err = p.communicate(tokens)
    return parse


def interpret_parse(parse):
    article = SAF()
    article.set_header("xtas.tasks.single.alpino",
                       "Alpino-x86_64-linux-glibc2.5-20214")
    tokens = {}  # {sid, offset: term}

    for line in parse.split("\n"):
        if not line.strip():
            continue
        line = line.strip().split("|")
        sid = int(line[-1])
        if len(line) != 16:
            raise ValueError("Cannot interpret line %r, has %i parts "
                             "(needed 16)" % (line, len(line)))
        parent = interpret_token(tokens, sid, *line[:7])
        child = interpret_token(tokens, sid, *line[8:15])
        func, rel = line[7].split("/")
        dep = dict(child=child['id'], parent=parent['id'], relation=rel)
        article.dependencies.append(dep)
    article.tokens = tokens.values()
    return article


def interpret_token(tokens, sid,
                    lemma, word, begin, _end, major_pos, _pos2, pos):
    begin = int(begin)
    token = tokens.get((sid, begin))
    if not token:
        if pos == "denk_ik":
            major, minor = "verb", None
        elif "(" in pos:
            major, minor = pos.split("(", 1)
            minor = minor[:-1]
        else:
            major, minor = pos, None

        if "_" in major:
            m2 = major.split("_")[-1]
        else:
            m2 = major
        cat = POSMAP.get(m2)
        if not cat:
            raise Exception("Unknown POS: %r (%s/%s/%s/%s)"
                            % (m2, major, begin, word, pos))

        tokenid = len(tokens) + 1
        token = dict(id=tokenid, word=word, lemma=lemma, pos=major_pos,
                     sentence=sid, offset=begin, pos_major=major,
                     pos_minor=minor, pos1=cat)
        tokens[sid, begin] = token
    return token


POSMAP = {"pronoun": 'O',
          "verb": 'V',
          "noun": 'N',
          "preposition": 'P',
          "determiner": "D",
          "comparative": "C",
          "adverb": "B",
          'adv': 'B',
          "adjective": "A",
          "complementizer": "C",
          "punct": ".",
          "conj": "C",
          "tag": "?",
          "particle": "R",
          "name": "M",
          "part": "R",
          "intensifier": "B",
          "number": "Q",
          "cat": "Q",
          "n": "Q",
          "reflexive":  'O',
          "conjunct": 'C',
          "pp": 'P',
          'anders': '?',
          'etc': '?',
          'enumeration': '?',
          'np': 'N',
          'p': 'P',
          'quant': 'Q',
          'sg': '?',
          'zo': '?',
          'max': '?',
          'mogelijk': '?',
          'sbar': '?',
          '--': '?',
          }


if __name__ == '__main__':
    import sys
    import json
    p = parse_text(" ".join(sys.argv[1:]))
    print(json.dumps(p, indent=2))
