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
import math


sb_stemmer = SnowballStemmer("russian")

def lemmatize_search(search: str):
    toktoks = toktok.ToktokTokenizer()
    # tokenize words
    text = list(map(lambda item: item.lower(), toktoks.tokenize(search)))
    # remove stopwords and punct
    stop_words = set(stopwords.words('russian'))
    text = filter(lambda item: not item in stop_words and (item.isalpha() or item.isdigit()), text)
    return list(map(lambda item: sb_stemmer.stem(item), text))

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

def get_url_by_article_uid(connection, article):
    query = """
    SELECT url FROM articles
        WHERE id = %s
    """
    connection.execute(query, (article,))
    return connection.fetchone()[0]

# new

def get_tf_idf(connection, article_id):
    query = """
    SELECT term_id, tf_idf FROM article_term
        WHERE
            article_id = %s
    """
    connection.execute(query, (article_id, ))
    results = dict(connection.fetchall())
    return results

def get_idf(connection, article_id):
    query = """
    select count(*) from articles;
    """
    connection.execute(query)
    document_count = connection.fetchone()
    query = """
    select term_id,
           count(article_id) from article_term
        WHERE
            article_id = %s
    group by term_id;
    """
    connection.execute(query, (article_id, ))
    results = connection.fetchall()
    idf = dict()
    for term_id, count_in_documents in results:
        idf[term_id] = math.log2(float(document_count[0]) / count_in_documents)
    return idf



COS = dict()

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
        articles_ids = list(map(lambda lem: get_articles(cursor, lem), lemmas))
        #  print('standart', reduce(lambda x, y: set(x) & set(y), articles_ids))
        merged_article_ids = set()
        for ids in articles_ids:
            merged_article_ids = merged_article_ids.union(set(ids))
        for article_id in merged_article_ids:
            tf_idf = get_tf_idf(cursor, article_id)
            idf = get_idf(cursor, article_id)
            numerator = .0
            denominator = 0
            den_A = .0
            den_B = .0
            for term_id, tf_idf in tf_idf.items():
                numerator += tf_idf * idf.get(term_id, 0)
                den_A += idf.get(term_id, 0) ** 2
                den_B += tf_idf ** 2
            denominator = math.sqrt(den_A) * math.sqrt(den_B)
            COS[article_id] = numerator / denominator
        results = sorted(COS.items(), key=lambda k: k[1], reverse=True)[:10]
        for id, value in results:
            print(get_url_by_article_uid(cursor, id), value)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
