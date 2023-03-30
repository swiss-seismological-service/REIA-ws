import base64
import logging
import os
import time
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
NATIONALE_UEBERSICHT = {'de': 'Nationale Übersicht',
                        'fr': 'Aperçu national',
                        'it': 'Panoramica nazionale',
                        'en': 'National overview'}
KANTONALE_UEBERSICHTEN = {'de': 'Kantonale Übersichten',
                          'fr': 'Aperçus cantonaux',
                          'it': 'Panoramiche cantonali',
                          'en': 'Cantonal overviews'}

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

URL = 'http://ermscenario.ethz.ch'
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


def generate_pdf_urls(earthquake: dict):

    eq_dict = {'earthquake': {}, 'urls': {}}
    eq_dict['earthquake']['city'], _, \
        eq_dict['earthquake']['canton'] = earthquake['event_text'].replace(
        '(', '').replace(')', '').strip().rpartition(' ')
    eq_dict['earthquake']['magnitude'] = earthquake['magnitude_value']
    eq_dict['earthquake']['loss'] = next(
        e['_oid'] for e in earthquake['calculation'] if
        e['_type'] == 'loss')
    eq_dict['earthquake']['damage'] = next(
        e['_oid'] for e in earthquake['calculation'] if
        e['_type'] == 'damage')

    originid = base64.b64encode(
        earthquake['originid'].encode('ascii')).decode('ascii')
    event_url = f'{URL}/?originid={originid}'

    for lng in LANGUAGES:
        eq_dict['urls'][lng] = {}
        for c in COUNTRY+CANTONS:
            url = f'{event_url}&canton={c}&lng={lng}'
            eq_dict['urls'][lng][c] = base64.b64encode(
                url.encode('ascii')).decode('ascii')

    return eq_dict


def download_csv(url, filename, category):
    csv = pd.read_csv(url)
    csv = csv.drop('losscategory', axis=1)

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


def download_scenarios(url, base_dir, languages):
    start = time.perf_counter()
    info = url['earthquake']
    url = url['urls']
    logging.info(
        f"Start downloading scenario {info['city']} {info['magnitude']}")

    for lng in languages:

        event_name = info['city']

        event_dir = os.path.join(
            base_dir,
            LANGUAGE_NAMES[lng])

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
        res = requests.get(f"{PRINT}{url[lng]['CH']}", timeout=300)
        filename.write_bytes(res.content)
        for c in CANTONS:
            logging.debug(f'GET scenario of {event_name} for {c}.')
            filename = Path(cantonal_dir) / \
                Path(
                    f"{lng.upper()}_SZN_{info['city']}_KT-{c}.pdf")
            res = requests.get(f"{PRINT}{url[lng][c]}", timeout=300)
            filename.write_bytes(res.content)
    logging.info(
        f"Took {time.perf_counter()-start} to download "
        f"event {info['city']} {info['magnitude']}")


def create_folder_structure(base_dir, languages):
    for lng in languages:
        lng_dir = os.path.join(base_dir, LANGUAGE_NAMES[lng])
        os.makedirs(lng_dir)


def download(oid=None, lngs=LANGUAGES):
    oid = oid or "smi:ch.ethz.sed/scenario/Origin/VaduzCantonal_M6_2"
    setup_logger()

    originid = base64.b64encode(
        oid.encode('ascii')).decode('ascii')
    earthquake = requests.get(f'{URL}/riaws/v1/earthquake/{originid}').json()

    scenario_dir = oid.split('/')[-1]
    os.makedirs(scenario_dir, exist_ok=True)
    os.system(f'rm -rf {scenario_dir}/*')

    create_folder_structure(scenario_dir, lngs)

    url = generate_pdf_urls(earthquake)

    download_scenarios(url, scenario_dir, lngs)


if __name__ == '__main__':
    download(
        oid="smi:ch.ethz.sed/scenario/Origin/Porrentruy_M4_0",
        lngs=['de', 'fr'])
