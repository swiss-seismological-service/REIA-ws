import base64
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import requests

CANTONS = ['CH', 'AG', 'AI', 'AR', 'BE', 'BL', 'BS',
           'FL', 'FR', 'GE', 'GL', 'GR', 'JU', 'LU',
           'NE', 'NW', 'OW', 'SG', 'SH', 'SO',
           'SZ', 'TG', 'TI', 'UR', 'VD', 'VS', 'ZG', 'ZH']

LANGUAGES = ['de', 'en', 'fr', 'it']

URL = 'http://ermd.ethz.ch'
PRINT = f'{URL}/pdf/?ready_status=ready_to_print&url='


def setup_logger():
    os.makedirs('logs', exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(process)d - [%(filename)s.%(funcName)s] '
        '- %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[TimedRotatingFileHandler('logs/download.log',
                                           when='d',
                                           interval=1,
                                           backupCount=5),
                  logging.StreamHandler()
                  ]
    )


TESTURL = 'http://ermd.ethz.ch/pdf/?ready_status=ready_to_print&url=aHR0cDovL2VybWQuZXRoei5jaC8/b3JpZ2luaWQ9YzIxcE9tTm9MbVYwYUhvdWMyVmtMM05qWlc1aGNtbHZMMDl5YVdkcGJpOUJhV2RzWlY5Tk5WODUmbG5nPWVu'  # noqa


def generate_urls(earthquakes):
    urls = []
    for e in earthquakes:
        eq_dict = {}
        originid = base64.b64encode(
            e['originid'].encode('ascii')).decode('ascii')
        event_url = f'{URL}/?originid={originid}'
        for c in CANTONS:
            eq_dict[c] = {}
            for lng in LANGUAGES:
                url = f'{event_url}?canton={c}&lng={lng}'
                eq_dict[c][lng] = base64.b64encode(
                    url.encode('ascii')).decode('ascii')
                break
            break
        d = {e['event_text']: eq_dict}
        urls.append(d)
        break

    return urls


def download_pdfs(urls, base_dir):
    for event_key, event in urls.items():
        event_dir = f'{base_dir}/{event_key}'
        os.makedirs(event_dir)
        for canton_key, canton in event.items():
            canton_dir = f'{event_dir}/{canton_key}'
            os.makedirs(canton_dir)
            for lang_key, encoded in canton.items():
                filename = Path(f'{canton_dir}/scenario_{lang_key}.pdf')
                res = requests.get(f'{PRINT}{encoded}', timeout=300)
                filename.write_bytes(res.content)


def download():
    setup_logger()
    base_dir = 'earthquake_scenarios'
    os.makedirs(base_dir, exist_ok=True)
    os.system('rm -rf earthquake_scenarios/*')

    earthquakes = requests.get(f'{URL}/riaws/v1/earthquakes').json()

    urls = generate_urls(earthquakes)

    for ev in urls:
        download_pdfs(ev, base_dir)


if __name__ == '__main__':
    download()
