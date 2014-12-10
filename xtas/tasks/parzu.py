import os, os.path, subprocess, logging, datetime, collections

def get_parzu_version():
    parzu_home = os.environ.get("PARZU_HOME")
    if not parzu_home:
        raise Exception("PARZU_HOME not set")
    for line in open(os.path.join(parzu_home, 'changelog')):
        if "." in line: return float(line.split()[0])

    
def parse(text):
    if isinstance(text, unicode):
        text = text.encode("utf-8")
    parzu = os.path.join(os.environ["PARZU_HOME"], "parzu")
    p = subprocess.Popen(parzu, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate(text)
    return out

def conll_to_saf(conll):
    header = {'format': "SAF",
              'format-version': "0.0",
              'processed':  {'module': "parzu",
                             'module-version': get_parzu_version(),
                             "started": datetime.datetime.now().isoformat()}
             }
    saf = {'tokens': [], 'dependencies': [], 'header': header}
    
    tokens = {}
    def get_tokenid(sentnr, tokennr):
        return tokens.setdefault((sentnr, tokennr), len(tokens)+1)

    sent = 1
    insent = False
    offset = 0 # offset not given...
    for line in conll.split("\n"):
        if not line.strip():
            if insent:
                sent += 1
                insent = False
            continue
        insent = True
        tokennr, word, lemma, pos, pos2, pos3, parent, rel, _dum, _dum = line.split("\t")
        tokenid = get_tokenid(sent, tokennr)
        parent = get_tokenid(sent, parent)
        saf['tokens'].append(dict(id=tokenid, sentence=sent, offset=offset,
                                  word=word, lemma=lemma, pos=pos, pos2=pos2, extrapos=pos3,
                                  pos1=POSMAP[pos2]))
        saf['dependencies'].append(dict(child=tokenid, parent=parent, relation=rel))
        offset += len(word) + 1
    return saf

# See: http://www.ims.uni-stuttgart.de/forschung/ressourcen/lexika/TagSets/stts-table.html
POSMAP = { 
   'ADJA': 'A',  # attributives Adjektiv
    'ADJD': 'B',  # adverbiales oder pradikatives Adjektiv
    'ADV': 'B',  # Adverb
    'APPR': 'P',  # Praposition; Zirkumposition links
    'APPRART': 'P',  # Praposition mit Artikel
    'APPO': 'P',  # Postposition
    'APZR': 'P',  # Zirkumposition rechts
    'ART': 'D',  # bestimmter oder unbestimmter Artikel
    'CARD': 'Q',  # Kardinalzahl
    'FM': '?',  # Fremdsprachliches Material
    'ITJ': '!',  # Interjektion
    'KOUI': 'C',  # unterordnende Konjunktion mit ``zu'' und Infinitiv
    'KOUS': 'C',  # unterordnende Konjunktion mit Satz
    'KON': 'C',  # nebenordnende Konjunktion
    'KOKOM': 'C',  # Vergleichskonjunktion
    'NN': 'N',  # normales Nomen
    'NE': 'M',  # Eigennamen
    'PDS': 'O',  # substituierendes Demonstrativpronomen
    'PDAT': 'O',  # attribuierendes Demonstrativpronomen
    'PIS': 'O',  # substituierendes Indefinitpronomen
    'PIAT': 'O',  # attribuierendes Indefinitpronomen ohne Determiner
    'PIDAT': 'O',  # attribuierendes Indefinitpronomen mit Determiner
    'PPER': 'O',  # irreflexives Personalpronomen
    'PPOSS': 'O',  # substituierendes Possessivpronomen
    'PPOSAT': 'O',  # attribuierendes Possessivpronomen
    'PRELS': 'O',  # substituierendes Relativpronomen
    'PRELAT': 'O',  # attribuierendes Relativpronomen
    'PRF': 'O',  # reflexives Personalpronomen
    'PWS': 'O',  # substituierendes Interrogativpronomen
    'PWAT': 'O',  # attribuierendes Interrogativpronomen
    'PWAV': 'O',  # adverbiales Interrogativ- oder Relativpronomen
    'PAV': 'O',  # Pronominaladverb
    'PTKZU': 'R',  # ``zu'' vor Infinitiv
    'PTKNEG': 'R',  # Negationspartikel
    'PTKVZ': 'R',  # abgetrennter Verbzusatz
    'PTKANT': 'R',  # Antwortpartikel
    'PTKA': 'R',  # Partikel bei Adjektiv oder Adverb
    'TRUNC': 'C',  # Kompositions-Erstglied
    'VVFIN': 'V',  # finites Verb, voll
    'VVIMP': 'V',  # Imperativ, voll
    'VVINF': 'V',  # Infinitiv, voll
    'VVIZU': 'V',  # Infinitiv mit ``zu'', voll
    'VVPP': 'V',  # Partizip Perfekt, voll
    'VAFIN': 'V',  # finites Verb, aux
    'VAIMP': 'V',  # Imperativ, aux
    'VAINF': 'V',  # Infinitiv, aux
    'VAPP': 'V',  # Partizip Perfekt, aux
    'VMFIN': 'V',  # finites Verb, modal
    'VMINF': 'V',  # Infinitiv, modal
    'VMPP': 'V',  # Partizip Perfekt, modal
    'XY': '?',  # Nichtwort, Sonderzeichen enthaltend
    '$,': '.',  # Komma
    '$.': '.',  # Satzbeendende Interpunktion
    '$(': '.',  # sonstige Satzzeichen; satzintern

    'PROAV': 'B',  # not listed in stts-table, "auch *dabei* geholfen"
}
