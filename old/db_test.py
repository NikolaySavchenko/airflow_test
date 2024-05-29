import pandas as pd
from sqlalchemy import create_engine
from old.utils import *

engine = create_engine('clickhouse+native://airflow:airflow@localhost:9000/default')

# session = make_session(engine)

soup = get_book_details_func()
title = get_title_func(soup)
author = get_author_func(soup, title)

data = {'Author': [author], 'Title': [title]}
df = pd.DataFrame(data)

table_name = 'Books'
df.to_sql(table_name, engine, if_exists='append', index=False)

# Проверяем подключение
# result = session.execute(text('SELECT * FROM Books'))
# print(result.fetchall())

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
