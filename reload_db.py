import time
import pymysql
from datetime import datetime, timedelta
import os
import re

from dotenv import load_dotenv


load_dotenv()

HOST = os.getenv('HOST_DATABASE')
USER = os.getenv('USER_DATABASE')
PASSWORD = os.getenv('PASSWORD_DATABASE')
DATABASE = os.getenv('DATABASE')
USERPREFIX = os.getenv('USERPREFIX')


def get_book():
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )
    cursor = connection.cursor()

    # cursor.execute(
    #     '''
    #         DELETE e1 FROM dle_post as e1
    #         INNER JOIN dle_post as e2
    #         WHERE e1.id < e2.id AND e1.alt_name = e2.alt_name
    #         ;
    #     '''
    # )

    cursor.execute(
        f'''
            SELECT id, alt_name
            FROM dle_post
            WHERE alt_name LIKE '%---%'

            ;
        '''
    )

    # regexp '[^a-zA-Z0-9]'
    # WHERE CHAR_LENGTH(alt_name) > 150
    # WHERE alt_name LIKE '%>%'

    results = cursor.fetchall()
    # print(len(results))

    # s = k = len(results) // 100
    # # for i in results:
    # #     print(i)
    data = []
    for item in results:
        alt_name = item[1]
        alt_name = alt_name.replace("---", "-")
        # reg = re.compile('[^a-zA-Z0-9-]')
        # alt_name = reg.sub('', alt_name).lower()
        data.append((item[0], alt_name))

    for book in data:
        key = book[0]
        value = book[1]
        query = "UPDATE dle_post SET alt_name=%s WHERE id=%s"
        values = (value, key)
        cursor.execute(query, values)
        connection.commit()
    #     if t == 100:
    #         k -= 1
    #         t = 0
    #         print(f'[{"*"*(s-k)}{"." * k}]')

    cursor.close()
    connection.close()


def mani():
    get_book()


if __name__ == '__main__':
    mani()
