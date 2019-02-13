import requests
import psycopg2
from parsel import Selector

URLS = [
    'https://www.sports.ru/tribuna/blogs/personalfoul/2346278.html',
    'https://www.sports.ru/tribuna/blogs/sviridov/2345658.html',
    'https://www.sports.ru/tribuna/blogs/bankshot/2345435.html',
    'https://www.sports.ru/tribuna/blogs/trailblazersnews/2345706.html',
    'https://www.sports.ru/tribuna/blogs/hardenstravel/2344878.html',
    'https://www.sports.ru/tribuna/blogs/basketblog/2343864.html',
    'https://www.sports.ru/tribuna/blogs/makarizm/2343793.html',
    'https://www.sports.ru/tribuna/blogs/midrange/2342837.html',
    'https://www.sports.ru/tribuna/blogs/somenbainfo/2343473.html',
    'https://www.sports.ru/tribuna/blogs/damnit/2342634.html',
]

SELECTORS = {
    'body': ".//div[@class='blog-post']",
    'title': "//header[@class='material-item__header']/h1/text()",
    'tags': "//div[@class='material-item__tags-line']/a[@class='link link_color_blue link_size_small']/text()",
    'content': "//div[@class='material-item__content js-mediator-article']/p//text()"}


def GET_SELECTOR(title): return f"{SELECTORS['body']}{SELECTORS[title]}"


conn_ = psycopg2.connect(database='postgres',
                         user='postgres',
                         password='postgres',
                         host='0.0.0.0',
                         port=4321)
connection = conn_.cursor()


def parse_urls():
    for url in URLS:
        element = {}
        selector = Selector(requests.get(url).text)
        element['title'] = selector.xpath(GET_SELECTOR('title')).get()
        element['tags'] = ",".join(selector.xpath(GET_SELECTOR('tags')).getall())
        element['url'] = url
        element['content'] = "".join(selector.xpath(GET_SELECTOR('content')).getall())
        yield element


def create_student():
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


def create_article():
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


def insert_student():
    connection.execute("""
    INSERT INTO students (name, surname, mygroup) VALUES ('Timur', 'Mardanov', '11-502');
    """)
    connection.execute("""
    SELECT id FROM students WHERE name='Timur';
    """)
    return connection.fetchone()[0]


def insert_articles(data, student_id):
    query = """
    INSERT INTO articles (title, keywords, content, url, student_id) VALUES {lazy};;
    """
    lazy = []
    for item in data:
        lazy.append(
            f"('{item['title']}', '{item['tags']}', '{item['content']}', '{item['url']}', '{student_id}')")
    lazy = ", ".join(lazy)
    connection.execute(query.format(lazy=lazy))


if __name__ == "__main__":
    try:
        create_student()
        create_article()
        uid = insert_student()
        data = parse_urls()
        insert_articles(data, uid)
        conn_.commit()
    except Exception as e:
        print(e)
    finally:
        conn_.close()
        connection.close()
