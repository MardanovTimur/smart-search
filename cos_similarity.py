import psycopg2
import numpy
from nltk.tokenize import toktok

from nltk.stem.snowball import SnowballStemmer
from search import lemmatize_search, get_articles, get_url_by_article_uid
from tf_idf import get_idf

from nltk.corpus import stopwords
from typing import Callable
from pymystem3 import Mystem
from copy import deepcopy
import math

sb_stemmer = SnowballStemmer("russian")

def get_tf_idf(connection, article_id):
    query = """
    SELECT term_id, tf_idf FROM article_term
        WHERE article_id = %s
    """
    connection.execute(query, (article_id, ))
    results = dict(connection.fetchall())
    return results

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
        merged_article_ids = set()
        for ids in articles_ids:
            merged_article_ids = merged_article_ids.union(set(ids))
        for article_id in merged_article_ids:
            tf_idf = get_tf_idf(cursor, article_id)
            idf = get_idf(cursor)
            numerator = .0; den_A = .0; den_B = .0
            for term_id, tf_idf in tf_idf.items():
                numerator += tf_idf * idf.get(term_id, 0)
                den_A += idf.get(term_id, 0) ** 2
                den_B += tf_idf ** 2
            COS[article_id] = numerator / (math.sqrt(den_A) * math.sqrt(den_B))
        results = sorted(COS.items(), key=lambda k: k[1], reverse=True)[:10]
        for id, value in results:
            print(get_url_by_article_uid(cursor, id), value)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
