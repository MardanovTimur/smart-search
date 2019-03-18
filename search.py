import psycopg2
import numpy
from porter import StemmerPorter
from functools import reduce
from nltk.tokenize import toktok

from collections import OrderedDict
from nltk.stem.snowball import SnowballStemmer

from nltk.corpus import stopwords
from typing import Callable
from pymystem3 import Mystem
from copy import deepcopy


sb_stemmer = SnowballStemmer("russian")

def get_articles(connection, word):
    query = """
    SELECT article_id, count(*) as sort FROM article_term
        INNER JOIN terms_list ON (article_term.term_id = terms_list.term_id)
    WHERE
        term_text = %s GROUP BY article_id ORDER BY sort DESC;
    """
    connection.execute(query, (word, ))
    results = connection.fetchall()
    if results:
        return numpy.array(list(results))[:, 0]
    return []

def lemmatize_search(search: str):
    toktoks = toktok.ToktokTokenizer()
    # tokenize words
    text = list(map(lambda item: item.lower(), toktoks.tokenize(search)))
    # remove stopwords and punct
    stop_words = set(stopwords.words('russian'))
    text = filter(lambda item: not item in stop_words and (item.isalpha() or item.isdigit()), text)
    return list(map(lambda item: sb_stemmer.stem(item), text))


def get_url_by_article_uid(connection, article):
    query = """
    SELECT url FROM articles
        WHERE id = %s
    """
    connection.execute(query, (article,))
    return connection.fetchone()[0]


RELEV_COEF = 1.3

support = lambda x: 1 / pow(x + 1, RELEV_COEF)

def merge(a, b):
    # дает правильную релевантность относительно слияния по AND
    # сделано по фукции 1/x^1.3 которая подстраивает вероятности схожести
    res = []
    func = lambda a: list(map(lambda item: (item[1], support(item[0])), enumerate(a)))
    a_ = dict(func(a))
    b_ = dict(func(b))
    for k in a_.keys():
        a_[k] = a_[k] + b_[k]
    return OrderedDict(sorted(a_.items(), key=lambda x: x[1], reverse=True)).keys()


def set_articles(results):
    result_set = set(results[0])
    for i in range(1, len(results)):
        result_set_new1 = result_set & set(results[i])
        result_set_new2 = set(results[i]) & result_set
        result_set = merge(result_set_new1, result_set_new2)
    return result_set


if __name__ == "__main__":
    connection = psycopg2.connect(database='postgres',
                             user='postgres',
                             password='postgres',
                             host='0.0.0.0',
                             port=4321)
    cursor = connection.cursor()
    try:
        search_string = str(input("Input search word: "))
        lemmas = lemmatize_search(search_string)
        print(lemmas)
        articles_ids = list(map(lambda lem: get_articles(cursor, lem), lemmas))
        #  print('standart', reduce(lambda x, y: set(x) & set(y), articles_ids))
        articles = set_articles(articles_ids)

        print(list(map(lambda item: get_url_by_article_uid(cursor, item), articles)))

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
