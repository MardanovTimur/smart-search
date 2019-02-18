import psycopg2
from porter import StemmerPorter
from nltk.tokenize import toktok

from nltk.stem.snowball import SnowballStemmer

from nltk.corpus import stopwords
from typing import Callable
from pymystem3 import Mystem
from copy import deepcopy


sb_stemmer = SnowballStemmer("russian")

my_stem_lemmer = Mystem()

# deprecated
#  porter = lambda word: StemmerPorter.stem(word)
porter = lambda word: sb_stemmer.stem(word)
mystem = lambda word: my_stem_lemmer.lemmatize(word)[0]

def get_articles(connection):
    keywords = ('id', 'title', 'content')
    query = f"""
    SELECT { ','.join(keywords) } FROM articles;
    """
    connection.execute(query);
    fetch = connection.fetchall()
    return list(map(lambda item: dict(zip(keywords, item)) , fetch))

def resolve_article(art, method: Callable):
    article = deepcopy(art)
    toktoks = toktok.ToktokTokenizer()
    # tokenize words
    text = list(map(lambda item: item.lower(), toktoks.tokenize(article['content'])))
    # remove stopwords and punct
    stop_words = set(stopwords.words('russian'))
    text = filter(lambda item: not item in stop_words and (item.isalpha() or item.isdigit()), text)
    article['content'] = list(map(lambda item: method(item), text))
    return article

def resolve_articles(connection, articles: list):
    article_port = map(lambda article: resolve_article(article, porter), articles)
    article_mystem = map(lambda article: resolve_article(article, mystem), articles)
    return article_port, article_mystem

def insert(connection, articles: list, table_name: str):
    query = f"""
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    DROP TABLE IF EXISTS {table_name} CASCADE;
    CREATE TABLE IF NOT EXISTS {table_name} (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v1(),
        term VARCHAR(64) NOT NULL,
        articles_id UUID REFERENCES articles (id) ON DELETE CASCADE
    );
    """
    connection.execute(query)
    query = f"""
    INSERT INTO {table_name} (term, articles_id) VALUES (%s, %s);
    """
    for article in articles:
        for word in article['content']:
            connection.execute(query, (
                word,
                article['id'],
            ))

if __name__ == "__main__":
    connection = psycopg2.connect(database='postgres',
                             user='postgres',
                             password='postgres',
                             host='0.0.0.0',
                             port=4321)
    cursor = connection.cursor()
    try:
        articles = get_articles(cursor)
        p, m = resolve_articles(cursor, articles)
        insert(cursor, m, 'words_mystem')
        insert(cursor, p, 'words_porter')
        connection.commit()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
