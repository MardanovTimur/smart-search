import psycopg2
import numpy
from porter import StemmerPorter
from nltk.tokenize import toktok

from nltk.stem.snowball import SnowballStemmer

from nltk.corpus import stopwords
from typing import Callable
from pymystem3 import Mystem
from copy import deepcopy

tf = {}
idf = {}

def get_articles(connection):
    # count tf
    query = """
    select
        articles_id,
        term_id,
        count(term_id)
    from words_porter
        inner join terms_list on
            terms_list.term_text = words_porter.term
        group by
            articles_id,
            term_id
    order by articles_id;
    """
    connection.execute(query)
    pointer = connection.fetchall()
    del_query = """
    select
        articles_id,
        count(term) from words_porter
    group by articles_id;
    """
    connection.execute(del_query)
    delimeter = dict(connection.fetchall())
    for article_id, term_id, count_term in pointer:
        if not term_id in tf:
            tf[term_id] = {}
        tf[term_id].update({article_id: float(count_term) / delimeter.get(article_id) })
    return tf


def get_idf(connection):
    # count of documents
    query = """
    select count(*) from articles;
    """
    connection.execute(query)
    document_count = connection.fetchone()
    query = """
    select term_id,
            count(article_id)
        from article_term
    group by term_id;
    """
    connection.execute(query)
    results = connection.fetchall()
    for term_id, count_in_documents in results:
        idf[term_id] = float(document_count[0]) / count_in_documents
    return idf


def insert_tf_idf(connection):
    query = """
    UPDATE article_term SET tf_idf=%s
        WHERE
            article_id = %s
                AND
            term_id = %s;
    """
    for term_id, item in tf.items():
        for document, tf_value in item.items():
            idf_value = idf.get(term_id)
            tf_idf = tf_value * idf_value
            connection.execute(query, (tf_idf, document, term_id))

if __name__ == "__main__":
    connection = psycopg2.connect(database='postgres',
                                  user='postgres',
                                  password='postgres',
                                  host='0.0.0.0',
                                  port=4321)
    cursor = connection.cursor()
    try:
        tf = get_articles(cursor)
        idf = get_idf(cursor)
        insert_tf_idf(cursor)
        connection.commit()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
