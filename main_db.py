import time
import pymysql
from datetime import datetime, timedelta
import csv
import os
import json
import sys
import re


from category import id_category, id_category_amazon
from dotenv import load_dotenv


load_dotenv()

HOST = os.getenv('HOST_DATABASE')
USER = os.getenv('USER_DATABASE')
PASSWORD = os.getenv('PASSWORD_DATABASE')
DATABASE = os.getenv('DATABASE')


def save_post() -> None:
    ''' Добавление записей в базу данных'''

    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )
    cursor = connection.cursor()
    for file in os.listdir('full_data'):
        path = os.path.join('full_data', file)
        with open(path, 'r') as table:
            reader = csv.DictReader(table)
            k = 1
            for row in reader:
                data_save = [
                    (
                        'Buch',
                        row['date'],
                        row['short_story'],
                        row['full_story'],
                        row['title'],
                        '',
                        row['category'],
                        row['alt_name'],
                        '',
                        'Buch, Bücher, Book, eBook',
                        '1',
                    )
                ]
                query = """
                INSERT INTO dle_post
                (autor, date, short_story, full_story, title, descr, category, alt_name, xfields, keywords, approve)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    (last_id, row['category'])
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


def mani():
    save_post()


if __name__ == '__main__':
    mani()
