import bs4
import requests
import queue
import threading
import psycopg2
from multiprocessing.dummy import Pool


seen = queue.Queue()
cores = 8
pool = Pool(cores)
limit = 15865

is_all = []


def seen_daemon():
    while True:
        op, msg = seen.get(block=True)
        print(op, msg)
        is_all.append(msg == 200)


def page(idx):
    url = f'https://dbase.tube/chart/channels/subscribers/all?page={idx}&spf=navigate'
    return requests.get(url)


def send(i):
    seen.put((i, page(i).status_code))


def main():
    threading.Thread(target=seen_daemon, daemon=True).start()
    pool.map(send, range(limit))
    print('done')
    print(all(is_all))


main()
