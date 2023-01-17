import base64
import logging
import multiprocessing as mp
import os
import time
from logging.handlers import TimedRotatingFileHandler

import requests
from playwright.sync_api import sync_playwright

CANTONS = ['AG', 'AI', 'AR', 'BE', 'BL', 'BS',
           'FL', 'FR', 'GE', 'GL', 'GR', 'JU', 'LU',
           'NE', 'NW', 'OW', 'SG', 'SH', 'SO',
           'SZ', 'TG', 'TI', 'UR', 'VD', 'VS', 'ZG', 'ZH']

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - [%(filename)s.%(funcName)s] '
    '- %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[TimedRotatingFileHandler('logs/datapipe.log',
                                       when='d',
                                       interval=1,
                                       backupCount=5),
              logging.StreamHandler()
              ]
)


def crawl(urls):
    for url in urls:
        tries = 0
        while True:
            try:
                time.sleep(3)
                start = time.perf_counter()
                with sync_playwright() as p:
                    browser = p.chromium.launch()
                    page = browser.new_page()
                    response = page.goto(url)
                    logging.info(response.status)
                    if response.status != 200:
                        raise Exception('Failed to load website')
                    logging.info(f'waiting for URL {url}.')
                    page.wait_for_function(
                        '() => window.status === "ready_to_print"',
                        timeout=900000)
                    logging.info(
                        f'Succeeded for url {url} and took'
                        f' {time.perf_counter() - start}.')
                    browser.close()
            except Exception as e:
                logging.error(e)
                tries += 1
                logging.error(f'Failed {tries} times for url {url}.')
                if tries > 3:
                    logging.error(f'FAILED FINAL TIME FOR URL {url}.')
                    break
                continue
            break


def crawl_pages():
    base_url = 'http://ermd.ethz.ch'
    response_API = requests.get(f'{base_url}/riaws/v1/earthquakes')
    earthquakes = response_API.json()
    urls = []
    for e in earthquakes:
        originid = e['originid']
        originid = base64.b64encode(
            originid.encode('ascii')).decode('ascii')
        country_url = f'{base_url}/?originid={originid}'
        cantons_url = [country_url]
        for c in CANTONS:
            cantons_url.append(f'{country_url}&canton={c}')

        urls.append(cantons_url)

    with mp.Pool(8) as pool:
        pool.map(crawl, urls)


if __name__ == '__main__':
    crawl_pages()
