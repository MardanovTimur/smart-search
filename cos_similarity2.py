import numpy
from nltk.tokenize import toktok

from nltk.stem.snowball import SnowballStemmer
from search import lemmatize_search, get_articles, get_url_by_article_uid
from connection import PGConnection
from tf_idf import get_idf

from nltk.corpus import stopwords
from typing import Callable
from pymystem3 import Mystem
from copy import deepcopy
import math

sb_stemmer = SnowballStemmer("russian")

scores = {}

def get_tf_idf(connection, article_id):
    query = """
    SELECT term_id, tf_idf FROM article_term
        WHERE article_id = %s
    """
    connection.execute(query, (article_id, ))
    results = dict(connection.fetchall())
    return results


def get_idf(connection):
    # count of documents
    query = """
    select count(*) from articles;
    """
    connection.execute(query)
    document_count = connection.fetchone()
    query = """
    select term_text,
            count(article_id) from article_term
            INNER JOIN terms_list ON article_term.term_id = terms_list.term_id
    group by term_text;
    """
    connection.execute(query)
    results = connection.fetchall()
    idf = {}
    for term_id, count_in_documents in results:
        idf[term_id] = math.log2((document_count[0] - count_in_documents + .5) \
                                 / (count_in_documents + .5))
    return idf


def count_words_in_query(connection, term, article):
    count_words_query =  """
    SELECT count(term), articles_id FROM words_porter
        WHERE
            term = %s
                AND
            articles_id = %s
    GROUP BY articles_id;
    """
    connection.execute(count_words_query, (term, article))
    results = connection.fetchone()
    if results:
        return results[0]
    return 0

def calculate(connection, doc, idf, words):
    # count words in all documents
    query = """
    SELECT count(*) FROM words_porter;
    """
    connection.execute(query)
    average = connection.fetchone()[0]
    query = """
    select count(*) from articles;
    """
    connection.execute(query)
    document_count = connection.fetchone()
    average /= float(document_count[0])
    # count words in document
    for word in words:
        func_count = count_words_in_query(connection, word, doc)
        result = idf.get(word, .0) * func_count * (K + 1)/ \
                (func_count + K * (1 - B + B * document_count[0] / average))
        result = result if result > 0 else 0
        scores[doc] = scores.get(doc, .0) + result



COS = dict()
SCORE = dict()

K = 1.2
B = .75

if __name__ == "__main__":
    with PGConnection() as cursor:
        search_string = str(input("Input search word: "))
        lemmas = lemmatize_search(search_string)
        articles_ids = list(map(lambda lem: get_articles(cursor, lem), lemmas))
        merged_article_ids = set()
        for ids in articles_ids:
            merged_article_ids = merged_article_ids.union(set(ids))
        for article_id in merged_article_ids:
            idf = get_idf(cursor)
            calculate(cursor, article_id, idf, lemmas, )
        results = sorted(scores.items(), key=lambda k: k[1], reverse=True)[:10]
        for id, value in results:
            print(get_url_by_article_uid(cursor, id), value)

