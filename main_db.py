import requests
import time
import pymysql
from datetime import datetime, timedelta
import csv
import os
import json
import sys
import re

from bs4 import BeautifulSoup
from category import id_category, id_category_amazon
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from seleniumwire import webdriver


load_dotenv()

HOST = os.getenv('HOST_DATABASE')
USER = os.getenv('USER_DATABASE')
PASSWORD = os.getenv('PASSWORD_DATABASE')
DATABASE = os.getenv('DATABASE')
USERPREFIX = os.getenv('USERPREFIX')

proxies = {
    "http": "http://a21f86:cfc487@95.165.14.53:30935"
}

cookies = {
    'session-id': '138-1031618-7116823',
    'session-id-time': '2082787201l',
    'i18n-prefs': 'USD',
    'sp-cdn': '"L5Z9:RU"',
    'skin': 'noskin',
    'ubid-main': '133-0154677-2221077',
    'session-token': 'MU9QtMbB1xYaPusz9F5R/q+Fm+co46rwn/rAAuNAofMVnYJBp8pDve1kOlFz89S5vuWnsVTgMKN3phqoMISyi6nLQLzz5hH/waCBW/qCNTrC6/EbdH36Zdsmnp9le96+W4jsH3TTnMTHx6IASoGAEy2SSrKbEt/HPAOaOyRn+3m6GPn0+f5XOUpcqiHcW0hbDqUxKvne7LJaQ2ISkvrZJTB4If5xs1KoDQLBxdb5MBA=',
    'csm-hit': 'tb:s-A29N7PY3R9BFG6RV1Z15|1688336166200&t:1688336168215&adb:adblk_yes',
}

headers = {
    'authority': 'www.amazon.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en-US;q=0.9,en;q=0.8,de;q=0.7,zh;q=0.6,zh-TW;q=0.5,zh-CN;q=0.4',
    'cache-control': 'max-age=0',
    # 'cookie': 'session-id=138-1031618-7116823; session-id-time=2082787201l; i18n-prefs=USD; sp-cdn="L5Z9:RU"; skin=noskin; ubid-main=133-0154677-2221077; session-token=MU9QtMbB1xYaPusz9F5R/q+Fm+co46rwn/rAAuNAofMVnYJBp8pDve1kOlFz89S5vuWnsVTgMKN3phqoMISyi6nLQLzz5hH/waCBW/qCNTrC6/EbdH36Zdsmnp9le96+W4jsH3TTnMTHx6IASoGAEy2SSrKbEt/HPAOaOyRn+3m6GPn0+f5XOUpcqiHcW0hbDqUxKvne7LJaQ2ISkvrZJTB4If5xs1KoDQLBxdb5MBA=; csm-hit=tb:s-A29N7PY3R9BFG6RV1Z15|1688336166200&t:1688336168215&adb:adblk_yes',
    'device-memory': '8',
    'dnt': '1',
    'downlink': '10',
    'dpr': '1',
    'ect': '4g',
    'rtt': '50',
    'sec-ch-device-memory': '8',
    'sec-ch-dpr': '1',
    'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-ch-viewport-width': '490',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
    'viewport-width': '490',
}

params = {
    'qid': '1688101270',
    's': 'books',
    'sr': '1-247',
}


category = id_category


def save_post():
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )
    cursor = connection.cursor()
    for file in os.listdir('dle_post'):
        name_category = file[:file.find("_")]
        id_amazon = [k for k, v in id_category_amazon.items() if v == name_category]
        id_amazon = id_amazon[0]
        id_category_book = [k for k, v in id_category.items() if v == id_amazon]
        id_category_book = id_category_book[0]
        path = os.path.join('dle_post', file)
        with open(path, 'r') as table:
            k = 1
            table = table.readlines()
            for colum in table:
                colum = colum.replace('\n', '')
                time_now = datetime.now()
                data = colum[:colum.find(' ')+1] + f"{time_now}" + colum[colum.find('),')+1:]
                data = data.split("', ")
                author = 'Book'
                date_now = data[1][:data[1].find(".")]
                short_story = data[1][data[1].find("'")+1:]
                full_story = data[2][1:]
                title = data[3][1:]
                descr = ''
                alt_name = data[5][data[5].find("'")+1:]
                alt_name = alt_name[:100].replace("#", "")
                xfields = ''
                keyword = 'e-book'
                approve = 1
                data_save = [
                    (author, short_story, full_story, title, descr, id_category_book, alt_name, xfields,
                     keyword, approve)
                ]
                query = """
                INSERT INTO dle_post
                (autor, short_story, full_story, title, descr, category, alt_name, xfields, keywords, approve)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(query, data_save)
                connection.commit()

                cursor.execute(
                    '''
                        SELECT id FROM dle_post
                        ORDER BY ID DESC LIMIT 1
                        ;
                    '''
                )
                last_id = cursor.fetchone()

                query_cat = """
                    INSERT INTO dle_post_extras_cats
                    (news_id, cat_id)
                    VALUES (%s, %s)
                    """
                data_cat = [
                    (last_id, id_category_book)
                ]
                cursor.executemany(query_cat, data_cat)
                connection.commit()

                cursor.execute(
                    '''
                        SELECT id FROM dle_post
                        ORDER BY ID DESC LIMIT 1
                        ;
                    '''
                )
                last_id = cursor.fetchone()
                query_rate = """
                        INSERT INTO dle_post_extras
                        (news_id)
                        VALUES (%s)
                        """

                data_rate = [
                    (last_id,)
                ]
                cursor.executemany(query_rate, data_rate)
                connection.commit()
                print(k)
                k += 1
        print(f"{file} - Done!")
    cursor.close()
    connection.close()


def save_book(title, image, text, price, id_category_book, url_buy):
    # print(title)
    # time.sleep(1000)
    title = str(title).strip()
    # image_tag = f'<p><br></p> <center><img src="{image}"></center>'
    image_link = f'<p> <center><a href="{url_buy}" target="_blank"><img src="{image}" "></a> </center></p>'
    price = price[price.find("$"):].replace(" ", "")
    url_buy = f'<p> <center><a href="{url_buy}" target="_blank">{title}</a></center> </p>'
    price = f'<p><br></p> <p>Price: {price} </p>'
    short_story = f'{image_link}{price}{text[:201]}...'
    full_story = f'{image_link}{price}{text}{url_buy}'
    alt_name = title.replace(",", "").replace("\"", "").replace("'", "").replace(" ", "-")
    alt_name = alt_name.replace("#", "")
    # descr = text[:71]
    date = datetime.now() - timedelta(days=1)
    dle_post = ('Book', date, short_story, full_story, title, '', id_category_book, alt_name, '', "e-book", 1)
    with open('Parenting & Relationships_dle_post.txt', 'a', encoding='utf-8') as post:
        post.write(f'{dle_post}\n')


def get_book():
    for file in os.listdir('books_url'):
        name_category = file[:file.find("_")]
        id_amazon = [k for k, v in id_category_amazon.items() if v == name_category]
        id_amazon = id_amazon[0]
        id_category_book = [k for k, v in id_category.items() if v == id_amazon]
        id_category_book = id_category_book[0]
        path = os.path.join('books_url', file)
        with open(path, 'r') as urls:
            urls = urls.readlines()
            k = 1
            for url in urls:
                if url.find("amazon.com/b?ref=") != -1:
                    continue
                url = url.rstrip('\n')
                id_link = url[url.find("dp/")+3:]
                id_link = id_link[:id_link.find("/")]
                url_buy = f"https://www.amazon.com/dp/{id_link}?&tag=ebooks0d4-20"
                while True:
                    try:
                        response = requests.get(
                            url,
                            headers=headers,
                            proxies=proxies,
                            # cookies=cookies,
                            params=params,
                        ).text
                        soup = BeautifulSoup(response, 'html.parser')

                        try:
                            title = soup.find("span", class_="a-size-extra-large").text
                        except AttributeError:
                            title = soup.find("h1", id="title")
                            # print(soup)
                            # time.sleep(10000)
                        try:
                            image = soup.find("div", class_="image-wrapper").find("img")["src"]
                        except AttributeError:
                            try:
                                image = soup.find("div", id="img-canvas").find_all("img")[-1]["src"]
                            except AttributeError:
                                image = soup.find("div", id="ebooks-img-canvas").find("img")["src"]
                        price = soup.find("div", id="tmmSwatches").find_all("span", class_="a-button-inner")
                        price = price[0].text.replace("\n", "")  # взял первую цену из списка
                        p_text = soup.find("div", class_="a-expander-content")
                        details = []
                        try:
                            product_details = soup.find("div", id="rich_product_information").find_all('li')
                            for li in product_details:
                                details.append(li.text)
                            details = details[:-1]
                        except:
                            pass
                        text = ""
                        for p in p_text:
                            text += p.text
                        text += '<p><strong>' + '<br>'.join(details) + '</strong></p>'
                        save_book(title, image, text, price, id_category_book, url_buy)
                        print(k)
                        k += 1
                    except AttributeError as e:
                        print(f'{url} - {e}')
                        time.sleep(15)
                        continue
                    except Exception as e:
                        requests.get("https://proxy-onedash.ru/f1f8803370f027f6c51bb8f253757253/45a491")
                        print(e, "Wait 20 sec...reconnect")
                        time.sleep(20)
                    else:
                        break
        print(f'{file} - done!')


def mani():
    save_post()


if __name__ == '__main__':
    mani()
