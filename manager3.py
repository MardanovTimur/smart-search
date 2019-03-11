import psycopg2
import numpy
from porter import StemmerPorter
from nltk.tokenize import toktok

from nltk.stem.snowball import SnowballStemmer

from nltk.corpus import stopwords
from typing import Callable
from pymystem3 import Mystem
from copy import deepcopy


sb_stemmer = SnowballStemmer("russian")
my_stem_lemmer = Mystem()


def create_terms_table(connection, table_name='terms_list'):
    query = f"""
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    DROP TABLE IF EXISTS {table_name} CASCADE;
    CREATE TABLE IF NOT EXISTS {table_name} (
        term_id UUID PRIMARY KEY DEFAULT uuid_generate_v1(),
        term_text VARCHAR(64) UNIQUE NOT NULL
    );
    """
    connection.execute(query)


def create_article_table(connection, table_name='article_term'):
    query = f"""
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    DROP TABLE IF EXISTS {table_name} CASCADE;
    CREATE TABLE IF NOT EXISTS {table_name} (
        article_id UUID REFERENCES articles (id) ON DELETE CASCADE,
        term_id UUID REFERENCES terms_list (term_id) ON DELETE CASCADE,
        tf_idf FLOAT DEFAULT 0,
        UNIQUE(article_id, term_id)
    );
    """
    connection.execute(query)


def get_porter_words(connection, con):
    query = """
    SELECT term, articles_id FROM words_porter;
    """
    connection.execute(query)
    results = connection.fetchall()
    insert_into_terms(connection, results, con)


def insert_into_terms(connection, results, con):
    query = """
    INSERT INTO terms_list (term_text) VALUES (%s);
    """
    terms = set([item[0] for item in results])
    for term in terms:
        connection.execute(query, (term,))
    query = """
    SELECT term_text, term_id FROM terms_list;
    """
    connection.execute(query)
    res = dict(connection.fetchall())
    con.commit()
    query = """
    INSERT INTO article_term (term_id, article_id) VALUES (%s, %s);
    """
    for item in results:
        uid = res[item[0]]
        try:
            connection.execute(query, (uid, item[1]))
            con.commit()
        except Exception as e:
            con.rollback()


if __name__ == "__main__":
    connection = psycopg2.connect(database='postgres',
                             user='postgres',
                             password='postgres',
                             host='0.0.0.0',
                             port=4321)
    cursor = connection.cursor()
    try:
        create_terms_table(cursor)
        create_article_table(cursor)
        get_porter_words(cursor, connection)
        connection.commit()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
