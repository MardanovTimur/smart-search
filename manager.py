import requests
import psycopg2
from parsel import Selector


SELECTOR = ".//header[@class='blog-feed__item-header']//a[@class='h1 content-link']/@href"


def generate_urls(url: str) -> list:
    """ Generate urls
    """
    with open('page.html', 'r', encoding='utf-8') as f:
        selector = Selector(f.read())
        return selector.xpath(SELECTOR).getall()


SELECTORS = {
    'body': ".//div[@class='blog-post']",
    'title': "//header[@class='material-item__header']/h1/text()",
    'tags': "//div[@class='material-item__tags-line']/a[@class='link link_color_blue link_size_small']/text()",
    'content': "//div[@class='material-item__content js-mediator-article']/p//text()"}


def GET_SELECTOR(title): return f"{SELECTORS['body']}{SELECTORS[title]}"


def parse_urls(urls):
    for url in urls:
        element = {}
        selector = Selector(requests.get(url).text)
        element['title'] = selector.xpath(GET_SELECTOR('title')).get()
        element['tags'] = ";".join(selector.xpath(GET_SELECTOR('tags')).getall())
        element['url'] = url
        element['content'] = "".join(
            selector.xpath(
                GET_SELECTOR('content')).getall()).replace(
            "'", '\'')
        yield element


def create_student(connection):
    connection.execute("""
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    DROP TABLE IF EXISTS students CASCADE;
    CREATE TABLE IF NOT EXISTS students (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v1(),
        name VARCHAR(32) NOT NULL,
        surname VARCHAR(32) NOT NULL,
        mygroup VARCHAR(6) NOT NULL
    );
    """)


def create_article(connection):
    connection.execute("""
    DROP TABLE IF EXISTS articles CASCADE;
    CREATE TABLE IF NOT EXISTS articles (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v1(),
        title VARCHAR(256) NOT NULL,
        keywords VARCHAR(1000) NOT NULL,
        content TEXT NOT NULL,
        url VARCHAR(1000) NOT NULL,
        student_id UUID REFERENCES students (id) ON DELETE CASCADE
    );
    """)


def insert_student(connection):
    connection.execute("""
    INSERT INTO students (name, surname, mygroup) VALUES ('Timur', 'Mardanov', '11-502');
    """)
    connection.execute("""
    SELECT id FROM students WHERE name='Timur';
    """)
    return connection.fetchone()[0]


def insert_articles(connection, data, student_id, conn_):
    query = """
    INSERT INTO articles (title, keywords, content, url, student_id) VALUES (%s, %s, %s, %s, %s);
    """
    lazy = []
    for item in data:
        print(item['url'])
        try:
            connection.execute(query,
                               (item['title'],
                                item['tags'],
                                item['content'],
                                item['url'],
                                student_id))
            conn_.commit()
        except Exception as e:
            print('Incorrect data on page', e)


if __name__ == "__main__":
    urls = generate_urls('https://www.sports.ru/tribuna/basketball/')[:50]
    conn_ = psycopg2.connect(database='postgres',
                             user='postgres',
                             password='postgres',
                             host='0.0.0.0',
                             port=4321)
    connection = conn_.cursor()
    try:
        create_student(connection)
        create_article(connection)
        uid = insert_student(connection)
        data = parse_urls(urls)
        insert_articles(connection, data, uid, conn_)
        conn_.commit()
    except Exception as e:
        print(e)
    finally:
        conn_.close()
        connection.close()
