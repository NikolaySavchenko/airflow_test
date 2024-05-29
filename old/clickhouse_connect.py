import pandas as pd
import clickhouse_connect

create_table_sql = """
CREATE TABLE books_table
(
    `id` Int32,
    `title` String,
    `author` String,
)
ENGINE = MergeTree
ORDER BY timeseries
"""


def write_pandas_df():
    client = clickhouse_connect.get_client(host='localhost', port='8123', user='airflow', password='airflow')
    client.command('DROP TABLE IF EXISTS books_table')
    client.command(create_table_sql)
    df = pd.DataFrame({'id': [1],
                       'title': ['Book 1'],
                       'author': ['Author One']})
    client.insert_df('books_table', df)
    result_df = client.query_df('SELECT * FROM books_table')
    print()
    print(result_df.dtypes)
    print()
    print(result_df)


if __name__ == '__main__':
    write_pandas_df()