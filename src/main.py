import bs4
import requests
import queue
import threading
import psycopg2
import json
from multiprocessing.dummy import Pool


seen = queue.Queue()
cores = 8
pool = Pool(cores)
limit = 15865


def insert_vids(cursor, data, table_chan):
    sql_insert_chann = 'INSERT INTO youtube.channels.channels (channel_id) VALUES (%s)'

    for datum in data:
        if datum not in table_chan:
            cursor.execute(sql_insert_chann, [datum])


def seen_daemon():
    conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
    table_chans = get_incumbent_chans(conn)

    while True:
        idx, channels = seen.get(block=True)
        print(channels)
        cursor = conn.cursor()
        insert_vids(cursor, channels, table_chans)
        conn.commit()
        cursor.close()


def page(idx):
    url = f'https://dbase.tube/chart/channels/subscribers/all?page={idx}&spf=navigate'
    return requests.get(url)


def soup_page(idx):
    req = page(idx)
    text = req.text
    json_data = json.loads(text)
    body = json_data['body']['spf_content']

    return bs4.BeautifulSoup(body, 'html.parser')


def vids(i):
    pg = soup_page(i)
    channels = []
    select = pg.select('a.aj.row')
    for a_href in select:
        path = a_href['href'].split('/')
        channels.append(path[2])

    seen.put((i, channels))


def get_incumbent_chans(conn):
    postgresql_select_query = 'SELECT channel_id FROM youtube.channels.channels'
    cursor = conn.cursor()
    cursor.execute(postgresql_select_query)
    records = cursor.fetchall()

    ignore = set()
    for i in records:
        ignore.add(i[0])

    print(len(ignore), 'channels from table')

    cursor.close()
    return ignore


def main():
    threading.Thread(target=seen_daemon, daemon=True).start()
    pool.map(vids, range(limit))
    print('done')


main()
