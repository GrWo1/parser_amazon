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
    "http": "http://a21f86:cfc487@95.165.14.53:30935"
}

cookies = {
    'session-id': '262-9244124-2095467',
    'session-id-time': '2082787201l',
    'i18n-prefs': 'EUR',
    'ubid-acbde': '257-1558258-5223425',
    'sp-cdn': '"L5Z9:RU"',
    'session-token': '"b1bJDTKb2s6HGp6jvuKh7MdAd+vopadTf66/1oe4BvkZbndzwyJxkmWUAtijL0Dh/mdE6HGv6Gx3EF2MyLtXZkYJCaBOVqhW2SaJKfw60w6WG2xJx6aOrI7KHnQVAdu4yDiwQLMjWsgAyMiQbsgDb0+j5i2/5WN+2sOBeQdyzf7qHjAIj2ewirV8TJpPgb2nS31qp2sEYBsrRSXOYGn2xQ1/wi0Pk5REJ0on8I3DH44="',
    'csm-hit': 'tb:C0DZGZX3RQ174YJBGAJ6+s-BWJ34TVZ15TRGF3MP5AS|1688549429947&t:1688549429947&adb:adblk_yes',
}

headers = {
    'authority': 'www.amazon.de',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en-US;q=0.9,en;q=0.8,de;q=0.7,zh;q=0.6,zh-TW;q=0.5,zh-CN;q=0.4',
    'cache-control': 'max-age=0',
    # 'cookie': 'session-id=262-9244124-2095467; session-id-time=2082787201l; i18n-prefs=EUR; ubid-acbde=257-1558258-5223425; sp-cdn="L5Z9:RU"; session-token="b1bJDTKb2s6HGp6jvuKh7MdAd+vopadTf66/1oe4BvkZbndzwyJxkmWUAtijL0Dh/mdE6HGv6Gx3EF2MyLtXZkYJCaBOVqhW2SaJKfw60w6WG2xJx6aOrI7KHnQVAdu4yDiwQLMjWsgAyMiQbsgDb0+j5i2/5WN+2sOBeQdyzf7qHjAIj2ewirV8TJpPgb2nS31qp2sEYBsrRSXOYGn2xQ1/wi0Pk5REJ0on8I3DH44="; csm-hit=tb:C0DZGZX3RQ174YJBGAJ6+s-BWJ34TVZ15TRGF3MP5AS|1688549429947&t:1688549429947&adb:adblk_yes',
    'device-memory': '8',
    'dnt': '1',
    'downlink': '10',
    'dpr': '2',
    'ect': '4g',
    'referer': 'https://www.amazon.de/s?i=stripbooks&rh=n%3A186606&dc&fs=true&ds=v1%3A5mGI1FtXF32BgHMErA5ZBeNXt%2BaiZHJQVEjTpQCBFXQ&qid=1688549204&ref=sr_ex_n_1',
    'rtt': '50',
    'sec-ch-device-memory': '8',
    'sec-ch-dpr': '2',
    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-ch-ua-platform-version': '"12.5.0"',
    'sec-ch-viewport-width': '663',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'viewport-width': '663',
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
                        title = soup.find("span", id="productTitle").text
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
                        except AttributeError:
                            price = "$0.00"
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
                        requests.get("https://proxy.onedash.net/f1f8803370f027f6c51bb8f253757253/45a491")
                        print(e, "Wait 20 sec...reconnect")
                        time.sleep(20)
                    else:
                        break
        print(f'{file} - done!')


def books_category(category_key, category_value):
    p = 1
    for page in range(1, 76):
        time.sleep(3)
        url_page = f"https://www.amazon.de/s?i=stripbooks&rh=n%3A{category_key}&fs=true&page={page}"
        response_page = requests.get(
            url_page,
            headers=headers,
            # proxies=proxies,
            cookies=cookies,
            # params=params,
        ).text
        soup = BeautifulSoup(response_page, 'html.parser')
        books = soup.find_all('div', class_='aok-relative')
        links_to_books = set()
        k = 1
        for book in books:
            try:
                href_book = book.find('a', class_='a-link-normal').get("href")
                links_to_books.add("https://www.amazon.de" + href_book)
            except AttributeError:
                continue
        data = '\n'.join(str(element) for element in links_to_books)
        with open(f'{category_value}_links_books.txt', 'a', encoding='utf-8') as file:
            file.write(data)
        p += 1
    print(f'{category_value}.........done!')
    time.sleep(1000)


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
        "Set-Cookie=_ym_d=; Path=/; Domain=ya.ru; Expires=Wed, 19 Jun 2013 09:59:48 GMT"
    )
    chrome_options.add_argument(
        "Cookie=is_gdpr=0; is_gdpr_b=CNCdfRDKrAEoAg==; i=2EhgG3FoFgbN2C0wGnW5Qke9GJO4OlzMrJaZxLgxpEW8eumhtDG2ju/eegT8ngP2lPzALHCMwRePyEST7dr17vagIcc=; yandexuid=3482926751678606372; gdpr=0; _ym_uid=1679125888882470962; yandex_login=serega-orig; L=AEt5W3QHV3FhSXhldnNbVgJSVnpVSAN0PwQnBDcWbiw6Jg4=.1679125897.15285.362044.399b5bd53e9dad649072d8ec72be5c90; my=YwA=; yandex_csyr=1685545806; KIykI=1; Session_id=3:1686133857.5.0.1679125897439:6H4dXg:27.1.2:1|122620940.0.2.3:1679125897|6:10182540.700429.FPPdj6LlwACNvsZlFlSYKQAPQs4; ys=c_chck.4071014247#udn.cDrQodC10YDQs9C10Lk%3D; mda2_beacon=1686133857658; _ym_isad=1; yandex_gid=213; bh=EkAiTm90LkEvQnJhbmQiO3Y9IjgiLCAiQ2hyb21pdW0iO3Y9IjExNCIsICJHb29nbGUgQ2hyb21lIjt2PSIxMTQiGgUiYXJtIiIQIjExNC4wLjU3MzUuMTMzIioCPzAyAiIiOgcibWFjT1MiQggiMTIuNS4wIkoEIjY0IlJcIk5vdC5BL0JyYW5kIjt2PSI4LjAuMC4wIiwgIkNocm9taXVtIjt2PSIxMTQuMC41NzM1LjEzMyIsICJHb29nbGUgQ2hyb21lIjt2PSIxMTQuMC41NzM1LjEzMyJaAj8w; _yasc=cSmdCDtLF6QgqWn8csieqcLzxla5LuZbyQZDxXcPgbsUADsK89At9XgLyqRJ3AsYygYQ; yp=1702763947.szm.2:1440x900:622x711#2001493857.udn.cDrQodC10YDQs9C10Lk%3D#2000906409.pcs.1#1688224231.hdrc.0#1689587929.ygu.1#4294967295.skin.s; _ym_d=1686995988"
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

    for page in range(1, 76):
        driver.implicitly_wait(0)
        driver.get(f"https://www.amazon.com/s?i=stripbooks&rh=n%3A{category_key}&fs=true&page={page}")
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        books = soup.find_all('div', class_='sg-row')
        links_to_books = set()
        for book in books:
            try:
                href_book = book.find('a', class_='a-link-normal').get("href")
                if str(href_book).find('/help/customer') == -1 and str(href_book).find('/books-category/') == -1:
                    links_to_books.add("https://www.amazon.com" + href_book)
            except AttributeError:
                print(0)
        time.sleep(0.5)
        data = '\n'.join(str(element) for element in links_to_books)
        with open(f'{category_value}_links_books.txt', 'a', encoding='utf-8') as file:
            file.write(data)


def get_id_category():
    for key, value in id_category_amazon.items():
        books_category(key, value)


def mani():
    get_id_category()


if __name__ == '__main__':
    mani()
