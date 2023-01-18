import base64
import logging
import multiprocessing as mp
import os
import time
from itertools import repeat
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pandas as pd
import requests

COUNTRY = ['CH']
CANTONS = ['AG', 'AI', 'AR', 'BE', 'BL', 'BS',
           'FL', 'FR', 'GE', 'GL', 'GR', 'JU', 'LU',
           'NE', 'NW', 'OW', 'SG', 'SH', 'SO',
           'SZ', 'TG', 'TI', 'UR', 'VD', 'VS', 'ZG', 'ZH']

KANTON_FOLDERNAMES = {'de': 'Kanton',
                      'fr': 'Canton',
                      'it': 'Canton',
                      'en': 'Canton'}
LANGUAGES = ['de', 'en', 'fr', 'it']
LANGUAGE_NAMES = {'de': 'Deutsch', 'fr': 'Français',
                  'it': 'Italiano', 'en': 'English'}
HISTORISCHE_SZENARIEN = {'de': 'Historische Szenarien',
                         'fr': 'Scénarios historiques',
                         'it': 'Scenari storici',
                         'en': 'Historical scenarios'}
KANTONALE_SZENARIEN = {'de': 'Kantonale Szenarien',
                       'fr': 'Scénarios cantonaux',
                       'it': 'Scenari cantonali',
                       'en': 'Cantonal scenarios'}
NATIONALE_UEBERSICHT = {'de': 'Nationale Übersicht',
                        'fr': 'Aperçu national',
                        'it': 'Panoramica nazionale',
                        'en': 'National overview'}
KANTONALE_UEBERSICHTEN = {'de': 'Kantonale Übersichten',
                          'fr': 'Aperçus cantonaux',
                          'it': 'Panoramiche cantonali',
                          'en': 'Cantonal overviews'}
HISTORISCHE_SZENARIEN_JAHRE = {'Aigle': 1584,
                               'Altdorf': 1774,
                               'Ardon': 1524,
                               'Basel': 1356,
                               'Brig-Glis': 1755,
                               'Churwalden': 1295,
                               'Ftan': 1622,
                               'Sierre': 1946,
                               'Stalden-Visp': 1855,
                               'Unterwalden': 1601}

KATEGORIEN = {'de': {'fatalities': 'Todesopfer',
                     'injuries': 'Verletzte',
                     'displaced': 'Schutzsuchende',
                     'building-damage': 'Gebäudeschäden',
                     'building-costs': 'Gebäudekosten'},
              'fr': {'fatalities': 'victimes',
                     'injuries': 'blesses',
                     'displaced': 'personnes-recherchant-un-abri',
                     'building-damage': 'degats-aux-batiments',
                     'building-costs': 'couts-degats-aux-batiments'},
              'it': {'fatalities': 'vittime',
                     'injuries': 'feriti',
                     'displaced': 'sfollati',
                     'building-damage': 'danni-agli-edifici',
                     'building-costs': 'costi-danni-edifici'},
              'en': {'fatalities': 'fatalities',
                     'injuries': 'injuries',
                     'displaced': 'displaced',
                     'building-damage': 'building-damage',
                     'building-costs': 'building-costs'}
              }

URL = 'http://ermd.ethz.ch'
PRINT = f'{URL}/pdf/?ready_status=ready_to_print&url='
WS = f'{URL}/riaws/v1'


def setup_logger():
    os.makedirs('logs', exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG,
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


def generate_pdf_urls(earthquakes: list):
    urls = []
    for e in earthquakes:
        eq_dict = {'earthquake': {}, 'urls': {}}
        eq_dict['earthquake']['city'], _, \
            eq_dict['earthquake']['canton'] = e['event_text'].replace(
            '(', '').replace(')', '').strip().rpartition(' ')
        eq_dict['earthquake']['magnitude'] = e['magnitude_value']
        eq_dict['earthquake']['loss'] = next(
            e['_oid'] for e in e['calculation'] if e['_type'] == 'loss')
        eq_dict['earthquake']['damage'] = next(
            e['_oid'] for e in e['calculation'] if e['_type'] == 'damage')

        originid = base64.b64encode(
            e['originid'].encode('ascii')).decode('ascii')
        event_url = f'{URL}/?originid={originid}'

        for lng in LANGUAGES:
            eq_dict['urls'][lng] = {}
            for c in COUNTRY+CANTONS:
                url = f'{event_url}&canton={c}&lng={lng}'
                eq_dict['urls'][lng][c] = base64.b64encode(
                    url.encode('ascii')).decode('ascii')

        urls.append(eq_dict)

    return urls


def download_csv(url, filename, category):
    csv = pd.read_csv(url)
    csv = csv.drop('losscategory', axis=1)

    if category in [v['building-costs'] for k, v in KATEGORIEN.items()]:
        csv = csv.rename(
            columns={'mean': 'mean [M CHF]',
                     'quantile10': 'percentile10 [M CHF]',
                     'quantile90': 'percentile90 [M CHF]'})
    else:
        csv = csv.rename(columns={'quantile10': 'percentile10',
                                  'quantile90': 'percentile90'})

    csv.to_csv(filename, index=False)


def download_scenario_csv(loss, damage, national_dir, cantonal_dir, city, lng):
    # National
    # CH_fatalities
    cat = KATEGORIEN[lng]['fatalities']
    filename = os.path.join(
        national_dir, f"SZN-{city}_CH_{cat}.csv")
    url = f"{WS}/loss/{loss}/occupants/Country?format=csv"
    download_csv(url, filename, cat)

    # CH_displaced
    cat = KATEGORIEN[lng]['displaced']
    filename = os.path.join(
        national_dir, f"SZN-{city}_CH_{cat}.csv")
    url = f"{WS}/loss/{loss}/businessinterruption/Country?format=csv"
    download_csv(url, filename, cat)

    # CH_building-costs
    cat = KATEGORIEN[lng]['building-costs']
    filename = os.path.join(
        national_dir, f"SZN-{city}_CH_{cat}.csv")
    url = f"{WS}/loss/{loss}/structural/Country?format=csv"
    download_csv(url, filename, cat)

    # CH-KT_injuries
    cat = KATEGORIEN[lng]['injuries']
    filename = os.path.join(
        national_dir, f"SZN-{city}_CH-KT_{cat}.csv")
    url = f"{WS}/loss/{loss}/nonstructural/Canton?format=csv"
    download_csv(url, filename, cat)

    # CH-KT_building-damage
    cat = KATEGORIEN[lng]['building-damage']
    filename = os.path.join(
        national_dir, f"SZN-{city}_CH-KT_{cat}.csv")
    url = f"{WS}/damage/{damage}/structural/Canton?format=csv"
    download_csv(url, filename, cat)

    # Kantonal
    # KT_fatalities
    cat = KATEGORIEN[lng]['fatalities']
    filename = os.path.join(
        cantonal_dir, f"SZN-{city}_KT_{cat}.csv")
    url = f"{WS}/loss/{loss}/occupants/Canton?format=csv"
    download_csv(url, filename, cat)

    # KT_displaced
    cat = KATEGORIEN[lng]['displaced']
    filename = os.path.join(
        cantonal_dir, f"SZN-{city}_KT_{cat}.csv")
    url = f"{WS}/loss/{loss}/businessinterruption/Canton?format=csv"
    download_csv(url, filename, cat)

    # KT_building-costs
    cat = KATEGORIEN[lng]['building-costs']
    filename = os.path.join(
        cantonal_dir, f"SZN-{city}_KT_{cat}.csv")
    url = f"{WS}/loss/{loss}/structural/Canton?format=csv"
    download_csv(url, filename, cat)

    for c in CANTONS:
        # KT-XX_injuries
        cat = KATEGORIEN[lng]['injuries']
        filename = os.path.join(
            cantonal_dir, f"SZN-{city}_KT-{c}_{cat}.csv")
        url = f"{WS}/loss/{loss}/nonstructural/" \
            f"CantonGemeinde?format=csv&aggregation_tag={c}"
        download_csv(url, filename, cat)

        # KT-XX_building-damage
        cat = KATEGORIEN[lng]['building-damage']
        filename = os.path.join(
            cantonal_dir, f"SZN-{city}_KT-{c}_{cat}.csv")
        url = f"{WS}/damage/{damage}/structural/" \
            f"CantonGemeinde?format=csv&aggregation_tag={c}"
        download_csv(url, filename, cat)


def download_scenarios(urls, base_dir, historical):
    start = time.perf_counter()
    info = urls['earthquake']
    urls = urls['urls']
    logging.info(
        f"Start downloading scenario {info['city']} {info['magnitude']}")

    for lng in LANGUAGES:
        if historical:
            event_name = f"{info['city']} "
            f"{HISTORISCHE_SZENARIEN_JAHRE[info['city']]}"

            event_dir = os.path.join(
                base_dir, LANGUAGE_NAMES[lng],
                HISTORISCHE_SZENARIEN[lng], event_name)

        else:
            event_name = info['city']

            event_dir = os.path.join(
                base_dir,
                LANGUAGE_NAMES[lng],
                KANTONALE_SZENARIEN[lng],
                f"{KANTON_FOLDERNAMES[lng]} {info['canton']}",
                event_name)

        national_dir = os.path.join(event_dir, NATIONALE_UEBERSICHT[lng])
        cantonal_dir = os.path.join(event_dir, KANTONALE_UEBERSICHTEN[lng])

        os.makedirs(national_dir)
        os.makedirs(cantonal_dir)

        download_scenario_csv(
            info['loss'], info['damage'], national_dir, cantonal_dir,
            info['city'], lng)

        logging.debug(f'GET scenario of {event_name} for CH.')
        filename = Path(national_dir) / \
            Path(f"{lng.upper()}_SZN_{info['city']}_CH.pdf")
        res = requests.get(f"{PRINT}{urls[lng]['CH']}", timeout=300)
        filename.write_bytes(res.content)
        for c in CANTONS:
            logging.debug(f'GET scenario of {event_name} for {c}.')
            filename = Path(cantonal_dir) / \
                Path(
                    f"{lng.upper()}_SZN_{info['city']}_KT-{c}.pdf")
            res = requests.get(f"{PRINT}{urls[lng][c]}", timeout=300)
            filename.write_bytes(res.content)
    logging.info(
        f"Took {time.perf_counter()-start} to download "
        f"event {info['city']} {info['magnitude']}")

# print(base64.b64decode(
#     encoded.encode('ascii')).decode('ascii'))


def create_folder_structure(base_dir):
    for lng in LANGUAGES:
        lng_dir = os.path.join(base_dir, LANGUAGE_NAMES[lng])
        os.makedirs(lng_dir)
        historical_dir = os.path.join(lng_dir, HISTORISCHE_SZENARIEN[lng])
        os.makedirs(historical_dir)
        cantonal_dir = os.path.join(lng_dir, KANTONALE_SZENARIEN[lng])
        for c in CANTONS:
            canton_dir = os.path.join(
                cantonal_dir, f'{KANTON_FOLDERNAMES[lng]} {c}')
            os.makedirs(canton_dir)


def download():
    setup_logger()
    base_dir = 'earthquake_scenarios'
    os.makedirs(base_dir, exist_ok=True)
    os.system('rm -rf earthquake_scenarios/*')

    create_folder_structure(base_dir)

    earthquakes = requests.get(f'{URL}/riaws/v1/earthquakes').json()

    urls = generate_pdf_urls(earthquakes)

    cantonal_scenarios = [
        u for u in urls if float(u['earthquake']['magnitude']) == 6]

    historical_scenarios = [
        u for u in urls if float(u['earthquake']['magnitude']) != 6]

    with mp.Pool(2) as pool:
        pool.starmap(download_scenarios, zip(
            cantonal_scenarios, repeat(base_dir), repeat(False)))


if __name__ == '__main__':
    mp.freeze_support()
    download()
