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
import re

import unidecode

from xtas.tasks.saf import SAF

log = logging.getLogger(__name__)

CMD_PARSE = ["bin/Alpino", "end_hook=triples_with_frames", "-parse"]
CMD_TOKENIZE = ["Tokenization/tok"]


def parse_text(text):
    alpino_home = os.environ['ALPINO_HOME']

    tokens = tokenize(text, alpino_home)
    parse = parse_raw(tokens, alpino_home)
    return interpret_parse(tokens, parse)


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


def interpret_parse(tokens, parse):
    article = SAF()
    article.set_header("xtas.tasks.single.alpino",
                       "Alpino-x86_64-linux-glibc2.5-20214")

    words = {} # {sid, offset: term}
    
    for sid, sent in enumerate(tokens.split("\n")):
        if sent and sent.strip():
            for i, word in enumerate(sent.split(" ")):
                words[sid+1, i] = word

    tokens = {}  # {sid, offset: term}

    for line in parse.split("\n"):
        if not line.strip():
            continue
                    
        line = line.strip().split("|")
        sid = int(line[-1])
        if len(line) != 6:
            raise ValueError("Cannot interpret line %r, has %i parts "
                             "(needed 6)" % (line, len(line)))
        func, rel = line[2].split("/")
        if func == "top": # ignore the link to 'top'
            continue 
        parent = interpret_token(tokens, words, sid, *line[:2])
        child = interpret_token(tokens, words, sid, *line[3:5])
        dep = dict(child=child['id'], parent=parent['id'], relation=rel)
        article.dependencies.append(dep)
    article.tokens = tokens.values()
    return article


def interpret_token(tokens, words, sid, lemma_position, pos):
    if lemma_position == "top/top":
        return None
    m = re.match(r"(.*)/\[(\d+),(\d+)\]", lemma_position)    
    if not m:
        raise ValueError("Cannot interpret token {lemma_position}, expected format: word/[start,end]"
                         .format(**locals()))
    lemma, begin, end = re.match(r"(.*)/\[(\d+),(\d+)\]", lemma_position).groups()
    begin, end = int(begin), int(end)
    token = tokens.get((sid, begin))
    if not token:
        if pos == "denk_ik":
            pos = "verb"
        word = " ".join(words[sid, i] for i in range(begin, end))
        cat = POSMAP.get(pos)
        if not cat:
            raise Exception("Unknown POS: {pos} (word: {word}, lemma: {lemma}, sid: {sid}, begin: {begin}"
                            .format(**locals()))
        tokenid = len(tokens) + 1
        token = dict(id=tokenid, word=word, lemma=lemma, pos=pos, pos1=cat,
                     sentence=sid, offset=begin)
        tokens[sid, begin] = token
    return token

# see http://www.let.rug.nl/vannoord/alp/Alpino/adt.html#postags
POSMAP = {
    "adj": 'A', #Adjective
    "adv": 'B', #Adverb
    "comp": 'C', #Complementizer
    "comparative": 'C', #Comparative
    "det": 'D', #Determiner
    "fixed": '?', #Fixed part of a fixed expression
    "name": 'M', #Name
    "noun": 'N', #Noun
    "num": 'Q', #Number
    "part": 'R', #Particle
    "pron": 'O', #Pronoun
    "prep": 'P', #Preposition
    "punct": '.', #Punctuation
    "verb": 'V', #Verb
    "vg": 'C', #Conjunction

    # Tags not found in docs:
    "pp": 'P', # 'daaruit'
}
    

if __name__ == '__main__':
    import sys
    import json
    p = parse_text(" ".join(sys.argv[1:]))
    print(json.dumps(p, indent=2))
