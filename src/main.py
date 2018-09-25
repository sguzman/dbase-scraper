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


def seen_daemon():
    while True:
        op, msg = seen.get(block=True)
        print(op, msg)


def page(idx):
    url = f'https://dbase.tube/chart/channels/subscribers/all?page={idx}&spf=navigate'
    return requests.get(url)


def soup_page(idx):
    req = page(idx)
    text = req.text
    json_data = json.loads(text)
    body = json_data['body']['spf_content']

    return bs4.BeautifulSoup(body)


def send(i):
    pg = soup_page(i)
    channels = []
    select = pg.select('a.aj.row')
    for a_href in select:
        path = a_href['href'].split('/')
        channels.append(path[2])

    seen.put((i, channels))


def main():
    threading.Thread(target=seen_daemon, daemon=True).start()
    for i in range(limit):
        send(i)
    print('done')


main()
