import requests
import time
import pymysql
from datetime import datetime, timedelta
import csv
import os
import json
import sys

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
    "http": "http://247b11:e055ce@95.165.14.53:30935"
}

cookies = {
    'session-id': '130-6370399-0693343',
    'i18n-prefs': 'USD',
    'sp-cdn': '"L5Z9:RU"',
    'ubid-main': '130-2704916-6749568',
    'x-main': '"a0VHYdtYz0A542nfaSG6vnFzaDpfA5KUAHpeEh9Pg@78q5P3puneSmZ918B1jxsI"',
    'at-main': 'Atza|IwEBIK8CEDO2I72RX8-cqriokFe34RFHSS8gctxx3O9btsHtzirb_weL0vTRhkoGJ-NNSo9PwkunJnVYzm3mxS9OuxTgc5xHSL31y6Wq0flhfzEDyIdmgbLPnW57CwUg8SqhARP9YLX6Db0OZA8Oc3UG1Rsou4Ai1Yyj_RWnEQbgtgcJUDgNPBrWQd56fAHc0hn-oXjPnjCsN7ksegEYcxPW6yXsiRHeJ6GqnPjFliFpuxHpW9IgCdfjWMeY53nSjT9FZwA',
    'sess-at-main': '"yDpNqZCm7UR2rzwT1hESRODo0/cQEeJcGQvShzGuTV0="',
    'sst-main': 'Sst1|PQEBa5Og6LV5NNgCOOxLkf6zCZ2YgKDVco0Nf43w31qkXy-v7he5NFcHALVlI9Fjr9BZxPObp0fLjDSxGBY9wsbuakkGtZcROOBldV_VHEJAtJXTy4D4lplI-mJopO0pNqWh1NkiipKuFbb_LIhr7ni7-A8GVmeGVkDWMB-tC7V2iMSDZqhPxZVSHwD07d-JkhZc8tAgnymP9z9mJay-1hh5TGdroBHX7UZiOfJidnC5GsGZVo0tSf-eKTVmogJ2eotweA-wMf32WYrQ8Kg_Vg9aF4gM6nqjGO8hDzgMCgOWwDQ',
    's_fid': '614FC87C4897301A-028163E12CE94F26',
    's_cc': 'true',
    'skin': 'noskin',
    '_rails-root_session': 'cGdzY0VsdEtURFhaeGFtK2R1WTR3R0ZSZHBKWmhuVVl3aW40bTdGaVBhcVBxQU5JazVrR3JBMW9Ha3NYeUJzODdHdThSNldwZk9WVVNCc3JnUGpNV3EvV2U1aWVNMWdaaTdJOHhwVFV6M2hKQWh4K3M2REZtdUJUZm5pSGRJUVQrZEtnc25vaEQvSVJvRWlBaFduZHBnWnQxWkM5LzdJM1RQaWY3QklXS3VpNWdzK3NyQW5lOHVqeVFILytRNWtKLS0remlUMTltVE03eEFHRzROb1lYd0x3PT0%3D--4e3a6a3034e6d974143e46b0ce201bb52dadfb9a',
    's_sq': '%5B%5BB%5D%5D',
    'session-id-time': '2082787201l',
    'lc-main': 'en_US',
    'session-token': '"1S8infuIZo2KvpLfIpZyg955tGcPgY0uePINtJhR4TSFsQJvhE1egdLg23K3yQYuywudA6mTVNH5StiI4kVY4vopxus/ThPa/KVf4EruD/ljkBnQMz87u0t3sdpxW6KDEfVCX63bmPM6V/5A5Ys2hEtF2AeUwEP6Ei8oM8F+ZmkmexjG+KGkZzCqQltcUT2iT9W2Ur/OVtdYZQJdr0QUtgB9hBdTDL+m23IvyxjtsZD0tUK3/8BsP8kcdNpSuVsyZhMDV6W2qO4="',
    'csm-hit': 'tb:s-P5F39F8WM8KPCCQY7WTQ|1688145412574&t:1688145414769&adb:adblk_yes',
}

headers = {
    'authority': 'www.amazon.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en-US;q=0.9,en;q=0.8,de;q=0.7,zh;q=0.6,zh-TW;q=0.5,zh-CN;q=0.4',
    'cache-control': 'max-age=0',
    # 'cookie': 'session-id=130-6370399-0693343; i18n-prefs=USD; sp-cdn="L5Z9:RU"; ubid-main=130-2704916-6749568; x-main="a0VHYdtYz0A542nfaSG6vnFzaDpfA5KUAHpeEh9Pg@78q5P3puneSmZ918B1jxsI"; at-main=Atza|IwEBIK8CEDO2I72RX8-cqriokFe34RFHSS8gctxx3O9btsHtzirb_weL0vTRhkoGJ-NNSo9PwkunJnVYzm3mxS9OuxTgc5xHSL31y6Wq0flhfzEDyIdmgbLPnW57CwUg8SqhARP9YLX6Db0OZA8Oc3UG1Rsou4Ai1Yyj_RWnEQbgtgcJUDgNPBrWQd56fAHc0hn-oXjPnjCsN7ksegEYcxPW6yXsiRHeJ6GqnPjFliFpuxHpW9IgCdfjWMeY53nSjT9FZwA; sess-at-main="yDpNqZCm7UR2rzwT1hESRODo0/cQEeJcGQvShzGuTV0="; sst-main=Sst1|PQEBa5Og6LV5NNgCOOxLkf6zCZ2YgKDVco0Nf43w31qkXy-v7he5NFcHALVlI9Fjr9BZxPObp0fLjDSxGBY9wsbuakkGtZcROOBldV_VHEJAtJXTy4D4lplI-mJopO0pNqWh1NkiipKuFbb_LIhr7ni7-A8GVmeGVkDWMB-tC7V2iMSDZqhPxZVSHwD07d-JkhZc8tAgnymP9z9mJay-1hh5TGdroBHX7UZiOfJidnC5GsGZVo0tSf-eKTVmogJ2eotweA-wMf32WYrQ8Kg_Vg9aF4gM6nqjGO8hDzgMCgOWwDQ; s_fid=614FC87C4897301A-028163E12CE94F26; s_cc=true; skin=noskin; _rails-root_session=cGdzY0VsdEtURFhaeGFtK2R1WTR3R0ZSZHBKWmhuVVl3aW40bTdGaVBhcVBxQU5JazVrR3JBMW9Ha3NYeUJzODdHdThSNldwZk9WVVNCc3JnUGpNV3EvV2U1aWVNMWdaaTdJOHhwVFV6M2hKQWh4K3M2REZtdUJUZm5pSGRJUVQrZEtnc25vaEQvSVJvRWlBaFduZHBnWnQxWkM5LzdJM1RQaWY3QklXS3VpNWdzK3NyQW5lOHVqeVFILytRNWtKLS0remlUMTltVE03eEFHRzROb1lYd0x3PT0%3D--4e3a6a3034e6d974143e46b0ce201bb52dadfb9a; s_sq=%5B%5BB%5D%5D; session-id-time=2082787201l; lc-main=en_US; session-token="1S8infuIZo2KvpLfIpZyg955tGcPgY0uePINtJhR4TSFsQJvhE1egdLg23K3yQYuywudA6mTVNH5StiI4kVY4vopxus/ThPa/KVf4EruD/ljkBnQMz87u0t3sdpxW6KDEfVCX63bmPM6V/5A5Ys2hEtF2AeUwEP6Ei8oM8F+ZmkmexjG+KGkZzCqQltcUT2iT9W2Ur/OVtdYZQJdr0QUtgB9hBdTDL+m23IvyxjtsZD0tUK3/8BsP8kcdNpSuVsyZhMDV6W2qO4="; csm-hit=tb:s-P5F39F8WM8KPCCQY7WTQ|1688145412574&t:1688145414769&adb:adblk_yes',
    'device-memory': '8',
    'dnt': '1',
    'downlink': '9.6',
    'dpr': '1',
    'ect': '4g',
    'rtt': '150',
    'sec-ch-device-memory': '8',
    'sec-ch-dpr': '1',
    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-ch-viewport-width': '1219',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'viewport-width': '1219',
}

params = {
    'qid': '1688100700',
    's': 'books',
    'sr': '1-9',
}


category = id_category


def save_book(title, image, text, price, id_category_book, url_buy):
    title = title.strip()
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
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )
    cursor = connection.cursor()
    query = """
    INSERT INTO dle_post
    (autor, date, short_story, full_story, title, descr, category, alt_name, xfields, keywords, approve)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    data = [
        ('Book', date, short_story, full_story, title, '', id_category_book, alt_name, '', "e-book", 1)
    ]
    cursor.executemany(query, data)
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
    print("Done!")
    cursor.close()
    connection.close()


def get_book():
    chrome_options = Options()
    chrome_options.add_argument("--disable-javascript")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    )
    chrome_options.add_argument(
        "Accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    )
    chrome_options.add_argument(
        "Content-Security-Policy=child-src *.ya.ru *.yandex.ru ya.ru yandex.ru yastatic.net blob: mc.yandex.ru mc.yandex.md *.ya.ru *.yandex.ru ya.ru yandex.ru;style-src 'unsafe-inline' yastatic.net;report-uri https://csp.yandex.net/csp?project=morda&from=morda.big.ru&showid=1686995988339936-9664102628779455337-balancer-l7leveler-kubr-yp-sas-97-BAL-5200&h=stable-portal-mordago-74.sas.yp-c.yandex.net&yandexuid=3482926751678606372&uid=122620940&&version=2023-06-15-305&adb=1;connect-src *.strm.yandex.net mc.yandex.com ya.ru yabs.yandex.ru yastatic.net yastat.net yandex.ru wss://webasr.voicetech.yandex.net wss://uniproxy.alice.ya.ru mc.yandex.ru *.mc.yandex.ru adstat.yandex.ru mc.admetrica.ru;img-src *.verify.yandex.ru *.ya.ru *.yandex.ru ya.ru yabs.yandex.by yabs.yandex.kz yabs.yandex.ru yabs.yandex.uz yandex.ru 'self' yastatic.net data: avatars.mds.yandex.net favicon.yandex.net blob: mc.admetrica.ru mc.yandex.com *.mc.yandex.ru adstat.yandex.ru mc.yandex.ru;script-src 'nonce-q+e3Uhous97yDeP138m/7A==' mc.yandex.com yastatic.net yandex.ru ya.ru blob: mc.yandex.ru *.mc.yandex.ru adstat.yandex.ru;default-src yastatic.net yastat.net;font-src yastatic.net;media-src yastatic.net avatars.mds.yandex.net"
    )
    chrome_options.add_argument(
        "Accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    )
    chromedriver_path = "chromedriver"
    os.environ['PATH'] += os.pathsep + chromedriver_path
    driver = webdriver.Chrome(
        options=chrome_options,
        # seleniumwire_options=seleniumwire_options, # Подключает прокси-сервер
    )
    for file in os.listdir('books_url'):
        name_category = file[:file.find("_")]
        id_amazon = [k for k, v in id_category_amazon.items() if v == name_category]
        id_amazon = id_amazon[0]
        id_category_book = [k for k, v in id_category.items() if v == id_amazon]
        id_category_book = id_category_book[0]
        path = os.path.join('books_url', file)
        with open(path, 'r') as urls:
            urls = urls.readlines()
            for url in urls:
                url = url.rstrip('\n')
                id_link = url[url.find("dp/")+3:]
                id_link = id_link[:id_link.find("/")]
                url_buy = f"https://www.amazon.com/dp/{id_link}?&tag=ebooks0d4-20"
                driver.implicitly_wait(3)
                driver.get(url)
                html = driver.page_source
                with open(f'page.html', 'w', encoding='utf-8') as page:
                    page.write(html)
                # driver.quit()
                with open(f'page.html', 'r', encoding='utf-8') as page:
                    soup = BeautifulSoup(page, 'html.parser')
                    try:
                        title = soup.find("span", id="productTitle").text
                    except AttributeError:
                        continue
                    try:
                        image = soup.find("div", class_="image-wrapper").find("img")["src"]
                    except AttributeError:
                        try:
                            image = soup.find("div", id="img-canvas").find_all("img")[-1]["src"]
                        except AttributeError:
                            try:
                                image = soup.find("div", id="ebooks-img-canvas").find("img")["src"]
                            except AttributeError:
                                image = soup.find("ul", class_="maintain-height").find_all("li")[0]
                                image = image.find("img")["src"]
                    try:
                        price = soup.find("div", id="tmmSwatches").find_all("span", class_="a-button-inner")
                        price = price[0].text.replace("\n", "")  # взял первую цену из списка
                        p_text = soup.find("div", class_="a-expander-content")
                        product_details = soup.find("div", id="rich_product_information").find_all('li')
                    except IndexError:
                        continue
                    details = []
                    for li in product_details:
                        details.append(li.text)
                    details = details[:-1]
                    text = ""
                    for p in p_text:
                        text += p.text
                    text += '<p><strong>' + '<br>'.join(details) + '</strong></p>'
                    save_book(title, image, text, price, id_category_book, url_buy)

        print(f'{file} - done!')


def books_category(category_key, category_value):
    url = f"https://www.amazon.com/s?rh=n%3A{category_key}&fs=true"
    response = requests.get(
        url,
        headers=headers,
        proxies=proxies,
        cookies=cookies,
        params=params,
    ).text
    soup = BeautifulSoup(response, 'lxml')
    pagination = soup.find("span", class_="s-pagination-strip").find_all("span", "s-pagination-disabled")
    last_page = int(pagination[-1].text)
    for page in range(1, last_page + 1):
        time.sleep(0.05)
        url_page = f"https://www.amazon.com/s?i=stripbooks&rh=n%3A{category_key}&fs=true&page={page}"
        response_page = requests.get(
            url_page,
            headers=headers,
            proxies=proxies,
            cookies=cookies,
            params=params,
        ).text
        soup = BeautifulSoup(response_page, 'lxml')
        books = soup.find_all('div', class_='sg-row')
        links_to_books = []
        p = 1
        for book in books:
            print(p)
            while True:
                try:
                    href_book = book.find('a', class_='a-link-normal').get("href")
                    links_to_books.append("https://www.amazon.com" + href_book)
                except AttributeError:
                    print(href_book)
                    continue
                else:
                    break
            p += 1
        data = '\n'.join(str(element) for element in links_to_books)
        with open(f'{category_value}_links_books.txt', 'a', encoding='utf-8') as file:
            file.write(data)
    print(f'{category_value} done!')


def books_category_selenium(category_key, category_value):
    seleniumwire_options = {
        'proxy': {
            'http': f'http://95.165.14.53:30935',
            'https': f'https://95.165.14.53:30935',
            # 'no_proxy': '',
            'auth': ('a21f86', 'cfc487'),
        }
    }
    chrome_options = Options()
    # chrome_options.add_argument(
    #     "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    # )
    # chrome_options.add_argument(
    #     "Set-Cookie=_ym_d=; Path=/; Domain=ya.ru; Expires=Wed, 19 Jun 2013 09:59:48 GMT"
    # )
    # chrome_options.add_argument(
    #     'cookie: session-id=262-9244124-2095467; session-id-time=2082787201l; i18n-prefs=EUR; ubid-acbde=257-1558258-5223425; sp-cdn="L5Z9:RU"; session-token="T4TuGBlxKpFyb0myK9mRyBMMQM/MBOWJmBqdcRgvyr1xjOg539K0IXu4XbY9J5tBu6pYoO/Wa9rTqJClgL0W7AQYUXvoBl1lmkvZi9UF5m6PITd8I0tKpmhkyxBIVXrIb1t/xfiIOTZqIjxKBK0isIlKCRbGuaO66/LxmSx+rcMussc5dyyJioGX86esbo6AMKKEzy7mWzgajOntjiyKAiaoTsXQenvhJS/JnE4k18k="; csm-hit=tb:s-A9ABDZSRJ525MA9NWYV0|1688542856371&t:1688542857494&adb:adblk_yes')

    chromedriver_path = "chromedriver"
    os.environ['PATH'] += os.pathsep + chromedriver_path
    driver = webdriver.Chrome(
        options=chrome_options,
        # seleniumwire_options=seleniumwire_options, # Подключает прокси-сервер
    )
    for page in range(1, 76):
        driver.get(f"https://www.amazon.de/s?i=stripbooks&rh=n%3A{category_key}&fs=true&page={page}")
        html = driver.page_source
        print(html)
        soup = BeautifulSoup(html, 'html.parser')
        books = soup.find_all('div', class_='sg-row')
        links_to_books = set()
        for book in books:
            # driver.implicitly_wait(5)
            try:
                href_book = book.find('a', class_='a-link-normal').get("href")
                if str(href_book).find('/dp/') != -1:
                    links_to_books.add("https://www.amazon.de" + href_book)
            except AttributeError as e:
                print(f"{page} - {e}")
        time.sleep(0.2)
        data = '\n'.join(str(element) for element in links_to_books)
        with open(f'{category_value}_links_books.txt', 'a', encoding='utf-8') as file:
            file.write(data)
    print(f"{category_value}.........DONE!")


def get_id_category():
    for key, value in id_category_amazon.items():
        books_category_selenium(key, value)


def mani():
    get_id_category()


if __name__ == '__main__':
    mani()
