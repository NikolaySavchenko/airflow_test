from pprint import pprint

import requests
from bs4 import BeautifulSoup
from pathvalidate import sanitize_filename
import pandas as pd
from random import randint

from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine, text


def check_for_redirect_func(response):
    if response.history:
        raise requests.exceptions.HTTPError


def get_book_details_func(book_id=randint(3, 1000)):
    print(book_id)
    url = f'https://tululu.org/b{book_id}/'
    response = requests.get(url)
    response.raise_for_status()
    check_for_redirect_func(response)
    soup = BeautifulSoup(response.text, features='lxml')
    return soup


def get_title_func(soup_content):
    # soup_content = context['task_instance'].xcom_pull(task_ids='get_soup_operator')
    book_title = soup_content.select_one('table h1').text.split('::')[0]
    title = f'{sanitize_filename(book_title.strip())}'
    return title


def get_author_func(soup, title):
    book_author = soup.select_one('table h1').text.split('::')[1]
    author = book_author.strip()
    data_ex = pd.DataFrame({'Название': [title], 'Автор': [author]})
    data_ex.to_excel('file.xlsx', sheet_name='Sheet1', index=False)


# Создаем подключение к Clickhouse
engine = create_engine('clickhouse://localhost:8123/default')

# Создаем сессию
session = make_session(engine)

# Создаем новую базу данных
session.execute(text('CREATE DATABASE IF NOT EXISTS airflow'))

# engine = create_engine('clickhouse://airflow:airflow@localhost:8123/airflow')

session = make_session(engine)

# Проверяем подключение
result = session.execute('SELECT 1')
print(result.fetchall())

# create_table_query = '''
# CREATE TABLE IF NOT EXISTS books_table (
#   id UInt64,
#   author String
#   title String
# ) ENGINE = MergeTree()
# ORDER BY id
# '''
# engine.execute(create_table_query)
#
# data = {'id': [1, 2, 3], 'author': ['John', 'Jane', 'Bob'], 'title': ['81 for', 'Black Swan', 'Potter']}
# df = pd.DataFrame(data)
#
# table_name = 'books'
# df.to_sql(table_name, engine, if_exists='append', index=False)


soup = get_book_details_func()
title = get_title_func(soup)
author = get_author_func(soup, title)


print(author)
print(title)
