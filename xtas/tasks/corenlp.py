"""
Python interface for the Stanford CoreNLP suite and conversion to xtas/SAF

CoreNLP requires CoreNLP to be installed, and an environment variable
CORENLP_HOME pointing to the folder where it is installed.

See http://nlp.stanford.edu/software/corenlp.shtml
Download e.g.
http://nlp.stanford.edu/software/stanford-corenlp-full-2014-01-04.zip
"""

import os
import re
import logging
import pexpect
import itertools
import datetime
import tempfile
import subprocess
from unidecode import unidecode

from cStringIO import StringIO

from .saf import SAF

log = logging.getLogger(__name__)
import threading

PARSER = None
PARSER_LOCK = threading.Lock()
CLASSNAME = "edu.stanford.nlp.pipeline.StanfordCoreNLP"


def parse_text(text, **options):
    """Use a global/persistent corenlp object to parse the given text"""
    global PARSER
    with PARSER_LOCK:
        if PARSER is None:
            PARSER = StanfordCoreNLP(**options)
        parse = PARSER.parse(text)
    return StanfordCoreNLP.stanford_to_saf(parse)


class StanfordCoreNLP(object):

    corenlp_version = os.environ.get("CORENLP_VERSION", "3.3.1")

    def __init__(self, timeout=600, annotators=None, **classpath_args):
        """
        Start the CoreNLP server as a java process. Arguments are used to
        locate the correct jar files.

        @param corenlp_path: the directory containing the jar files
        @param corenlp_version: the yyyy-mm-dd version listed in the jar name
        @param models_version: if different from the main version, the version
                               date of the models jar
        @param timeout: Time to wait for a parse before giving up
        """

        self.timeout = timeout
        cmd = self.get_command(classname=CLASSNAME, memory="3G",
                               annotators=annotators, **classpath_args)

        log.info("Starting the Stanford Core NLP parser.")
        log.debug("Command: {cmd}".format(**locals()))
        self._corenlp_process = pexpect.spawn(cmd)
        self._wait_for_corenlp_init()

    def _wait_for_corenlp_init(self):
        self._corenlp_process.expect("Entering interactive shell.", timeout=600)
        log.info("NLP tools loaded.")

    def _get_results(self):
        """
        Get the raw results from the corenlp process
        """
        buff = StringIO()
        while True:
            try:
                incoming = self._corenlp_process.read_nonblocking(2000, 1)
            except pexpect.TIMEOUT:
                log.debug("Waiting for CoreNLP process; buffer: {!r} "
                          .format(buff.getvalue()))
                continue

            for ch in incoming:
                if ch == "\n":  # return a found line
                    yield buff.getvalue()
                    buff.seek(0)
                    buff.truncate()
                elif ch not in "\r\x07":
                    buff.write(ch)
                    if ch == ">" and buff.getvalue().startswith("NLP>"):
                        return

    def parse(self, text):
        """Call the server and return the raw results."""
        # clean up anything leftover
        while True:
            try:
                self._corenlp_process.read_nonblocking(4000, 0.3)
            except pexpect.TIMEOUT:
                break

        if not isinstance(text, unicode):
            text = text.decode("ascii")
        text = re.sub("\s+", " ", unidecode(text))
        self._corenlp_process.sendline(text)

        return self._get_results()

    @classmethod
    def get_command(cls, classname, argstr="", memory=None, annotators=None,
                    **classpath_args):
        classpath = cls.get_classpath(**classpath_args)
        memory = "" if memory is None else "-Xmx{memory}".format(**locals())
        if isinstance(argstr, list):
            argstr = " ".join(map(str, argstr))
        if annotators:
            if isinstance(annotators, list):
                annotators = ",".join(annotators)
            argstr += " -annotators {annotators} ".format(**locals())
        return ("java {memory} -cp {classpath} {classname} {argstr}"
                .format(**locals()))

    @classmethod
    def get_classpath(cls, corenlp_path=None, corenlp_version=None,
                      models_version=None):
        if corenlp_path is None:
            corenlp_path = os.environ["CORENLP_HOME"]
        if corenlp_version is not None:
            cls.corenlp_version = corenlp_version
        if models_version is None:
            models_version = cls.corenlp_version

        jars = ["stanford-corenlp-{cls.corenlp_version}.jar"
                .format(**locals()),
                "stanford-corenlp-{models_version}-models.jar"
                .format(**locals()),
                "joda-time.jar", "xom.jar", "jollyday.jar"]
        jars = [os.path.join(corenlp_path, jar) for jar in jars]

        # check whether jars exist
        for jar in jars:
            if not os.path.exists(jar):
                raise Exception("Error! Cannot locate {jar}"
                                .format(**locals()))

        return ":".join(jars)

    @classmethod
    def stanford_to_saf(cls, lines):
        """
        Convert stanfords 'interactive' text format to saf
        Unfortunately, stanford cannot return xml in interactive mode, so we
        need to parse their plain text format
        """
        article = SAF()  # collections.defaultdict(list)
        processed = {'module': "corenlp",
                     'module-version':
                     cls.corenlp_version,
                     "started": datetime.datetime.now().isoformat()}
        article.header = {'format': "SAF",
                          'format-version': "0.0",
                          'processed': [processed]}

        lines = iter(lines)
        lines.next()  # skip first line (echo of sentence)
        _parse_article(article, lines)
        return article



CONLL_CLASS = "edu.stanford.nlp.trees.EnglishGrammaticalStructure"

def _get_tree(saf_article, sentence_id):
    for tree in saf_article['trees']:
        if int(tree['sentence']) == int(sentence_id):
            return tree['tree']
    raise ValueError("Sentence {sentence_id} not found in trees {trees}"
                     .format(trees=saf_article['trees'], **locals()))

def to_conll(tree):
    """
    Convert a parse tree from Penn (?) to conll
    """

    xml = ("<root><document><sentences><sentence>{tree}"
           "</sentence></sentences></document></root>"
           .format(**locals()))
    with tempfile.NamedTemporaryFile() as f:
        f.write(xml)
        f.flush()
        cmd = StanfordCoreNLP.get_command(CONLL_CLASS,
                                          ["-conllx", "-treeFile", f.name])
        open("/tmp/tree.xml", "w").write(xml)
        p = subprocess.check_output(cmd, shell=True)

    return p

def _regroups(pattern, text, **kargs):
    m = re.match(pattern, text, **kargs)
    if not m:
        raise Exception("Pattern {pattern!r} did not match text {text!r}"
                        .format(**locals()))
    return m.groups()


def _parse_article(article, lines):
    strip = lambda l: l.strip()
    tokens = {}  # sentence_no, index -> token
    while True:  # parse one sentence
        try:
            line = lines.next()
        except StopIteration:
            break
        while not line.strip():
            line = lines.next()  # skip leading blanks
        if line == "Coreference set:":
            break
        sentence_no = int(_regroups("Sentence #\s*(\d+)\s+", line)[0])
        text = lines.next()

        log.debug("Parsing sentence {sentence_no}: {text!r}"
                  .format(**locals()))

        # Parse tokens
        for i, s in enumerate(re.findall('\[([^\]]+)\]', lines.next())):
            wd = dict(re.findall(r"([^=\s]*)=([^=\s]*)", s))
            tokenid = len(tokens) + 1
            if not "CharacterOffsetBegin" in wd: 
                continue
            token = dict(id=tokenid, word=wd['Text'], lemma=wd.get('Lemma', '?'),
                         pos=wd.get('PartOfSpeech', '?'), sentence=sentence_no,
                         offset=wd["CharacterOffsetBegin"])
            token['pos1'] = POSMAP[token['pos']]
            tokens[sentence_no, i] = token
            article.tokens.append(token)
            if wd.get('NamedEntityTag', 'O') != 'O':
                article.entities.append(dict(tokens=[tokenid],
                                             type=wd['NamedEntityTag']))
        # try to peek ahead to see if we have more than tokens
        lines, copy = itertools.tee(lines)
        try:
            peek = copy.next()
            if peek.startswith("Sentence #"):
                continue
        except StopIteration:
            break

        # Extract original tree
        tree = " ".join(itertools.takewhile(lambda x: x,
                                            itertools.imap(strip, lines)))
        article.trees.append(dict(sentence=sentence_no, tree=tree))

        def parse_dependency(line):
            rfunc, parent, child = _regroups(RE_DEPENDENCY, line)
            if rfunc != 'root':
                parent, child = [tokens[sentence_no, int(j)-1]['id']
                                 for j in (parent, child)]
                article.dependencies.append(dict(child=child, parent=parent,
                                                 relation=rfunc))
        map(parse_dependency, itertools.takewhile(strip, lines))

    # get coreferences
    def get_coreference(lines):
        for line in itertools.takewhile(lambda l: l != "Coreference set:",
                                        lines):
            if line.strip():
                groups = _regroups(RE_COREF, line)
                yield [map(int, re.sub("[^\\d,]", "", s).split(","))
                       for s in groups]
    while True:
        corefs = list(get_coreference(lines))
        if not corefs:
            break
        for coref in corefs:
            sets = []
            for sent_index, head_index, from_index, to_index in coref:
                # take all nodes from .. to, place head first (False<True)
                indices = sorted(range(from_index, to_index),
                                 key=lambda i: (i != head_index, i))
                sets.append([tokens[sent_index, i-1]['id'] for i in indices])
            article.coreferences.append(sets)

RE_DEPENDENCY = "(\w+)\(.+-([0-9']+), .+-([0-9']+)\)"
RE_COREF = r'\s*\((\S+)\) -> \((\S+)\), that is: \".*\" -> \".*\"'
POSMAP = {'CC': 'C',
          'CD': 'Q',
          'DT': 'D',
          'EX': '?',
          'FW': 'N',
          'IN': 'P',
          'JJ': 'A',
          'JJR': 'A',
          'JJS': 'A',
          'LS': 'C',
          'MD': 'V',
          'NN': 'N',
          'NNS': 'N',
          'NNP': 'M',
          'NNPS': 'M',
          'PDT': 'D',
          'POS': 'O',
          'PRP': 'O',
          'PRP$': 'O',
          'RB': 'B',
          'RBR': 'B',
          'RBS': 'B',
          'RP': 'R',
          'SYM': '.',
          'TO': '?',
          'UH': '!',
          'VB': 'V',
          'VBD': 'V',
          'VBG': 'V',
          'VBN': 'V',
          'VBP': 'V',
          'VBZ': 'V',
          'WDT': 'D',
          'WP': 'O',
          'WP$': 'O',
          'WRB': 'B',
          ',' : '.',
          '.' : '.',
          ':' : '.',
          '``' : '.',
          '$' : '.',
          "''" : '.',
          "#" : '.',
          '-LRB-' : '.',
          '-RRB-' : '.',
          }
