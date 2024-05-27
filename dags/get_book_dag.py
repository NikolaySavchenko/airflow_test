import requests
import pandas as pd
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
from pathvalidate import sanitize_filename
from random import randint


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


def get_title_func(**context):
    book_det = context['task_instance'].xcom_pull(task_ids='get_soup_operator')
    title = f'{sanitize_filename(book_det[0].strip())}'
    return title


def get_author_func(**context):
    title = context['task_instance'].xcom_pull(task_ids='get_title_operator')
    book_det = context['task_instance'].xcom_pull(task_ids='get_soup_operator')
    author = book_det[1].strip()
    data_ex = pd.DataFrame({'Название': [title], 'Автор': [author]})
    print(data_ex)
    # data_ex.to_excel('file.xlsx', sheet_name='Sheet1', index=False)


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

get_title_operator = PythonOperator(
    task_id='get_title_operator',
    dag=dag,
    python_callable=get_title_func,
    provide_context=True
)

get_author_operator = PythonOperator(
    task_id='get_author_operator',
    dag=dag,
    python_callable=get_author_func,
    provide_context=True
)

get_soup_operator >> get_title_operator >> get_author_operator
