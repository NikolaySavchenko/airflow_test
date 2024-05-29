import requests
from bs4 import BeautifulSoup
from pathvalidate import sanitize_filename
from random import randint


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
    book_title = soup_content.select_one('table h1').text.split('::')[0]
    title = f'{sanitize_filename(book_title.strip())}'
    return title


def get_author_func(soup, title):
    book_author = soup.select_one('table h1').text.split('::')[1]
    author = book_author.strip()
    return author


soup = get_book_details_func()
title = get_title_func(soup)
author = get_author_func(soup, title)

print(author)
print(title)
