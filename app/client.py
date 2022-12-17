import base64
import enum
from datetime import datetime
from typing import Union

import pandas as pd
import requests


class LossCategory(str, enum.Enum):
    CONTENTS = 'contents'
    BUSINESS_INTERRUPTION = 'businessinterruption'
    NONSTRUCTURAL = 'nonstructural'
    OCCUPANTS = 'occupants'
    STRUCTURAL = 'structural'


class LossType(str, enum.Enum):
    LOSS = 'loss'
    DAMAGE = 'damage'


class AggregationTypes(str, enum.Enum):
    CANTONGEMEINDE = 'CantonGemeinde'
    CANTON = 'Canton'
    COUNTRY = 'Country'


CANTONS = ['AG', 'AI', 'AR', 'BE', 'BL', 'BS', 'FL',
           'FR', 'GE', 'GL', 'GR', 'JU', 'LU',
           'NE', 'NW', 'OW', 'SG', 'SH', 'SO', 'SZ',
           'TG', 'TI', 'UR', 'VD', 'VS', 'ZG', 'ZH']

WS_URL = 'http://ermd.ethz.ch/riaws/v1'


def get_earthquakes() -> list:
    response = requests.get(f'{WS_URL}/earthquakes')
    origin_ids = [r['originid'] for r in response.json()]
    return origin_ids


def get_calculation(origin_id: str, loss_type: LossType) -> dict:
    b64_id = base64.b64encode(origin_id.encode('utf-8')).decode('utf-8')
    response = requests.get(f'{WS_URL}/earthquake/{b64_id}')
    calc = [c for c in response.json()['calculation'] if c['status'] ==
            'complete' and c['_type'] == loss_type.value]
    calc.sort(key=lambda r: datetime.strptime(
        r['creationinfo']['creationtime'], '%Y-%m-%dT%H:%M:%S'), reverse=True)
    return calc[0]


def get_risk_values(origin_id: str,
                    loss_type: LossType,
                    loss_category: LossCategory,
                    aggregation_type: AggregationTypes,
                    canton: Union[str, None] = None) -> pd.DataFrame:

    if canton and aggregation_type is AggregationTypes.COUNTRY:
        raise ValueError(
            'It is not possible to use COUNTRY aggregation '
            'together with canton parameter.')
    if canton and canton not in CANTONS:
        raise ValueError(f'Canton "{canton}" doesn\'t exist')

    if aggregation_type == AggregationTypes.COUNTRY and \
            loss_type == LossType.DAMAGE:
        raise NotImplementedError(
            'Country wide damage statistics are '
            'not implemented yet. Only Canton '
            'or Municipality aggregation are available for damage.')

    print('loading...', end='\r')

    calc = get_calculation(origin_id, loss_type)

    url = \
        f'{WS_URL}/{loss_type}/{calc["_oid"]}/' \
        f'{loss_category}/{aggregation_type}'

    if canton:
        url = f'{url}?aggregation_tag={canton}'

    response = requests.get(url, timeout=300).json()
    response = [response] if isinstance(response, dict) else response
    return pd.DataFrame.from_dict(response, orient='columns')
