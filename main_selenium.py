import requests
import time
import pymysql
from datetime import datetime, timedelta
import csv
import os
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
    "http": "http://247b11:e055ce@95.165.14.53:30935"
}


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
        id_category_book = [k for k, v in id_category.items() if v == name_category][0]
        path = os.path.join('books_url', file)
        name_csv = f"{name_category}.csv"
        path_data = os.path.join('DATA', name_csv)
        fields_to_save = [
            "date",
            "short_story",
            "full_story",
            "title",
            "category",
            "alt_name",
        ]
        with open(path_data, 'w', ) as file_csv:
            writer = csv.DictWriter(file_csv, fieldnames=fields_to_save)
            writer.writeheader()

        with open(path, 'r') as urls:
            urls = urls.readlines()
            k = 1
            for url in urls:
                url = url.rstrip('\n')
                id_link = url[url.find("dp/") + 3:]
                id_link = id_link[:id_link.find("/")]
                url_buy = f"https://www.amazon.de/dp/{id_link}?&tag=buch0ed-21"
                while True:
                    try:
                        driver.implicitly_wait(0)
                        driver.get(url)
                        html = driver.page_source
                        soup = BeautifulSoup(html, 'html.parser')
                        title = soup.find("span", id="productTitle").text
                        title = title.strip()
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
                        image_link = f'<p> <center><a href="{url_buy}" target="_blank"><img src="{image}" "></a> </center></p>'
                        try:
                            price = soup.find("div", id="tmmSwatches").find_all("span", class_="a-button-inner")
                            price = price[0].text.replace("\n", "")  # взял первую цену из списка
                            price = price[price.find("€"):].strip()
                        except AttributeError:
                            price = "€0.00"
                        price = f'<p><br></p> <p>Preis: {price}</p>'
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

                        url_buy = f'<p> <center><a href="{url_buy}" target="_blank">{title}</a></center> </p>'
                        short_story = f'{image_link}{price}{text[:201]}...'
                        full_story = f'{image_link}{price}{text}{url_buy}'
                        date_now = datetime.now() - timedelta(days=1)
                        reg = re.compile('[^a-zA-Z0-9 ]')
                        alt_name = reg.sub('', title).lower().replace(" ", "-")
                        alt_name = alt_name[:189]
                        alt_name = alt_name[:alt_name.rfind(" ")]
                        alt_name = alt_name.replace("--", "-")
                        data_to_save = [
                            date_now,
                            short_story,
                            full_story,
                            title,
                            id_category_book,
                            alt_name,
                        ]
                        with open(path_data, 'a', newline="") as file_csv:
                            writer = csv.writer(file_csv)
                            writer.writerow(data_to_save)
                        print(k)
                        k += 1
                    except AttributeError as e:
                        print(f'{url} - {e}')
                        time.sleep(15)
                        continue
                    except Exception as e:
                        # requests.get("https://proxy.onedash.net/f1f8803370f027f6c51bb8f253757253/45a491")
                        print(e, "Wait 20 sec...reconnect")
                        time.sleep(20)
                    else:
                        break
        print(f'{file} - done!')


def mani():
    get_book()


if __name__ == '__main__':
    mani()
