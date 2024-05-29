import requests
import pandas as pd
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from pathvalidate import sanitize_filename
from random import randint

engine = create_engine('clickhouse+native://airflow:airflow@localhost:9000/default')


def check_for_redirect_func(response):
    if response.history:
        raise requests.exceptions.HTTPError


def get_book_details_func(book_id=randint(3, 1000)):
    url = f'https://tululu.org/b{book_id}/'
    response = requests.get(url)
    response.raise_for_status()
    check_for_redirect_func(response)
    soup = BeautifulSoup(response.text, features='lxml')
    book_title = soup.select_one('table h1').text.split('::')[0]
    book_author = soup.select_one('table h1').text.split('::')[1]
    return (book_title, book_author)


def add_func(**context):
    book_det = context['task_instance'].xcom_pull(task_ids='get_soup_operator')
    title = f'{sanitize_filename(book_det[0].strip())}'
    author = book_det[1].strip()
    data = {'Author': [author], 'Title': [title]}
    df = pd.DataFrame(data)
    table_name = 'Books'
    df.to_sql(table_name, engine, if_exists='append', index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0, 0, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'get_book_dag',
    start_date=days_ago(0, 0, 0, 0, 0),
    default_args=default_args,
    schedule_interval='@daily'
)

get_soup_operator = PythonOperator(
    task_id='get_soup_operator',
    dag=dag,
    python_callable=get_book_details_func,
    provide_context=True
)

add_operator = PythonOperator(
    task_id='add_operator',
    dag=dag,
    python_callable=add_func,
    provide_context=True
)

get_soup_operator >> add_operator
