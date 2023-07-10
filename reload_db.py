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


def update_db():
    # Подключение к БД
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )
    cursor = connection.cursor()

    # Удалить дубликаты
    cursor.execute(
        '''
            DELETE e1 FROM dle_post as e1
            INNER JOIN dle_post as e2
            WHERE e1.id < e2.id AND e1.alt_name = e2.alt_name
            ;
        '''
    )
    connection.commit()

    # Замена значений в БД с помощью регулярных выражения
    cursor.execute(
        f'''
            SELECT id
            FROM dle_post
            ;
        '''
    )
    results = cursor.fetchall()
    print(len(results))


# Запись результата выборки из БД в переменную results
    # results = cursor.fetchall()

# Примеры регулярных выражений
    # regexp '[^a-zA-Z0-9]'
    # WHERE CHAR_LENGTH(alt_name) > 150
    # WHERE alt_name LIKE '%>%'


# Создане нового списка с данными на основе полученных из базы
    # data = []
    # for item in results:
    #     alt_name = item[1]
    #     alt_name = alt_name.replace("---", "-")
    #     # reg = re.compile('[^a-zA-Z0-9-]')
    #     # alt_name = reg.sub('', alt_name).lower()
    #     data.append((item[0], alt_name))

# Обновление базы
#     for book in data:
#         key = book[0]
#         value = book[1]
#         query = "UPDATE dle_post SET alt_name=%s WHERE id=%s"
#         values = (value, key)
#         cursor.execute(query, values)
#         connection.commit()

    cursor.close()
    connection.close()


def mani():
    update_db()


if __name__ == '__main__':
    mani()
