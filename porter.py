import re

# https://gist.github.com/Kein1945/9111512

class StemmerPorter:
    """
    Стеммер Портера. Убирает окончания.
    """
    PERFECTIVEGROUND = re.compile(r'((ив|ивши|ившись|ыв|ывши|ывшись)|((?<=[ая])(в|вши|вшись)))$')
    REFLEXIVE = re.compile(r'(с[яь])$')
    ADJECTIVE = re.compile(r'(ее|ие|ые|ое|ими|ыми|ей|ий|ый|ой|ем|им|ым|ом|его|ого|ему|ому|их|ых|ую|юю|ая|яя|ою|ею)$')
    PARTICIPLE = re.compile(r'((ивш|ывш|ующ)|((?<=[ая])(ем|нн|вш|ющ|щ)))$')
    VERB = re.compile(r'((ила|ыла|ена|ейте|уйте|ите|или|ыли|ей|уй|ил|ыл|им|ым|ен|ило|ыло|ено|ят|ует|уют|ит|ыт|ены|ить'
                      r'|ыть|ишь|ую|ю)|((?<=[ая])(ла|на|ете|йте|ли|й|л|ем|н|ло|но|ет|ют|ны|ть|ешь|нно)))$')
    NOUN = re.compile(r'(а|ев|ов|ие|ье|е|иями|ями|ами|еи|ии|и|ией|ей|ой|ий|й|иям|ям|ием|ем|ам|ом|о|у|ах|иях|ях|ы|ь|ию'
                      r'|ью|ю|ия|ья|я)$')
    RVRE = re.compile(r'^(.*?[аеиоуыэюя])(.*)$')
    DERIVATIONAL = re.compile(r'.*[^аеиоуыэюя]+[аеиоуыэюя].*ость?$')
    DER = re.compile(r'ость?$')
    SUPERLATIVE = re.compile(r'(ейше|ейш)$')
    I = re.compile(r'и$')
    P = re.compile(r'ь$')
    NN = re.compile(r'нн$')

    @staticmethod
    def stem(word):
        word = word.lower()
        word = word.replace(u'ё', u'е')
        m = re.match(StemmerPorter.RVRE, word)
        if m and m.groups():
            pre = m.group(1)
            rv = m.group(2)
            temp = StemmerPorter.PERFECTIVEGROUND.sub('', rv, 1)
            if temp == rv:
                rv = StemmerPorter.REFLEXIVE.sub('', rv, 1)
                temp = StemmerPorter.ADJECTIVE.sub('', rv, 1)
                if temp != rv:
                    rv = temp
                    rv = StemmerPorter.PARTICIPLE.sub('', rv, 1)
                else:
                    temp = StemmerPorter.VERB.sub('', rv, 1)
                    if temp == rv:
                        rv = StemmerPorter.NOUN.sub('', rv, 1)
                    else:
                        rv = temp
            else:
                rv = temp

            rv = StemmerPorter.I.sub('', rv, 1)

            if re.match(StemmerPorter.DERIVATIONAL, rv):
                rv = StemmerPorter.DER.sub('', rv, 1)

            temp = StemmerPorter.P.sub('', rv, 1)
            if temp == rv:
                rv = StemmerPorter.SUPERLATIVE.sub('', rv, 1)
                rv = StemmerPorter.NN.sub(u'н', rv, 1)
            else:
                rv = temp
            word = pre+rv
        return word
