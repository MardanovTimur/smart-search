from connection import PGConnection
import numpy as np
from nltk.tokenize import toktok
from cos_similarity import get_tf_idf as gti

from nltk.stem.snowball import SnowballStemmer
from search import lemmatize_search, get_articles, get_url_by_article_uid
from tf_idf import get_idf
import math

COS = {}


sb_stemmer = SnowballStemmer("russian")


def get_tf_idf(connection, article_ids):
    query = """
    SELECT term_id, article_id, tf_idf FROM article_term
        WHERE article_id IN %s;
    """
    connection.execute(query, (tuple(article_ids), ))
    results = connection.fetchall()
    tf_idf = {}

    for term, article, t_i in results:
        v = tf_idf.setdefault(term, {})
        v[article] = t_i
    for k, v in tf_idf.items():
        for a_id in article_ids:
            if not a_id[0] in v.keys():
                v[a_id[0]] = .0
    return tf_idf

def sort_by_docs(tf_idf, article_ids):
    results = []
    for term, docs in tf_idf.items():
        zeros = np.zeros(50)
        for ind, d in enumerate(article_ids):
            zeros[ind] = docs.get(d, .0)
        results.append(zeros)
    return results

def tf_idf_as_array(connection, article_ids):
    tf_idf = get_tf_idf(connection, article_ids)
    return sort_by_docs(tf_idf, article_ids)


def get_term_id_by_term(connection, term):
    query = """
    SELECT term_id FROM terms_list
        WHERE term_text = %s;
    """
    connection.execute(query, (term, ))
    result = connection.fetchone()
    return result[0]


if __name__ == '__main__':
    print('7')
    with PGConnection() as connection:
        print('connection')
        search_string = str(input("Input search word: "))
        lemmas = lemmatize_search(search_string)
        articles_ids = list(map(lambda lem: get_articles(connection, lem), lemmas))
        merged_article_ids = set()
        for ids in articles_ids:
            merged_article_ids = merged_article_ids.union(set(ids))
        merged_article_ids = list(merged_article_ids)
        merged_article_ids = sorted(merged_article_ids)
        a = tf_idf_as_array(connection, merged_article_ids)
        u, s, vh = np.linalg.svd(a, full_matrices=False)
        idf = get_idf(connection)
        vh = vh[:5, ]
        idf = get_idf(connection)
        for ind1, article_id in enumerate(merged_article_ids):
            numerator = .0; den_A = .0; den_B = .0
            for ind2, lem in enumerate(lemmas):
                val = vh[ind2, ind1]
                term_id = get_term_id_by_term(connection, lem)
                numerator += val * idf.get(term_id, .0)
                den_A += idf.get(term_id, 0) ** 2
                den_B += val ** 2
            COS[article_id] = numerator / (math.sqrt(den_A) * math.sqrt(den_B))
        results = sorted(COS.items(), key=lambda k: k[1], reverse=True)[:10]
        for id, value in results:
            print(get_url_by_article_uid(connection, id), value)



