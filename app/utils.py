import pandas as pd
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from app.wquantile import weighted_quantile


def pandas_read_sql(stmt, db):
    """
    wrapper around pandas read_sql to use sqlalchemy engine
    and correctly close and dispose of the connections
    afterwards.
    """
    en = db.get_bind()
    with en.connect() as con:
        df = pd.read_sql(stmt, con)
    en.dispose()
    return df


def replace_path_param_type(app: FastAPI,
                            path_param: str,
                            new_type: type):

    for i, route in enumerate(app.router.routes):
        if f'{{{path_param}}}' in route.path:
            path = route.path
            route.endpoint.__annotations__[path_param] = new_type
            endpoint = route.endpoint
            methods = route.methods

            del app.router.routes[i]
            app.add_api_route(path, endpoint, methods=methods)


def csv_response(data: pd.DataFrame, filename: str) -> StreamingResponse:
    output = data.to_csv(index=False)
    return StreamingResponse(
        iter([output]),
        media_type='text/csv',
        headers={"Content-Disposition":
                 f"attachment;filename={filename}.csv"})


def aggregate_by_branch_and_event(
        data: pd.DataFrame, aggregation_type) -> pd.DataFrame:

    group = data.groupby(
        lambda x: data['branchid'].loc[x] *
        (10 ** 9) + data['eventid'].loc[x])

    value_column = [i for i in data.columns if 'value' in i]

    values = pd.DataFrame()
    values['weight'] = group.apply(
        lambda x: x['weight'].sum() / len(x))
    for name in value_column:
        values[name] = group.apply(
            lambda x: x[name].sum())
    values[aggregation_type] = False
    return values


def calculate_statistics(
        data: pd.DataFrame, aggregation_type: str) -> pd.DataFrame:
    # either loss_value or damage_value
    value_column = [i for i in data.columns if 'value' in i]

    statistics = pd.DataFrame()

    # calculate weighted loss
    for col in value_column:

        base_name = col.split('_')[0]

        data['weighted'] = data['weight'] * \
            data[col]

        # initialize with mean
        statistics[f'{base_name}_mean'] = data.groupby(
            aggregation_type)['weighted'].sum()

        # calculate quantiles
        statistics[f'{base_name}_pc10'], statistics[f'{base_name}_pc90'] = \
            zip(*data.groupby(aggregation_type).apply(
                lambda x: weighted_quantile(
                    x[col], (0.1, 0.9), x['weight'])))

        # drop intermediate column again form original df
        data.drop(columns=['weighted'])

    statistics = statistics.rename_axis(
        'tag').reset_index()

    statistics['tag'] = statistics['tag'].apply(lambda x: [x] if x else [])

    return statistics


def merge_statistics_to_buildings(statistics: pd.DataFrame,
                                  buildings: pd.DataFrame,
                                  aggregation_type: str) -> pd.DataFrame:
    statistics['merge_tag'] = statistics['tag'].apply(
        lambda x: ''.join(sorted(x)))
    buildings = pd.concat([
        buildings,
        pd.DataFrame([{'buildingcount': buildings['buildingcount'].sum(),
                       aggregation_type: ''}])
    ], ignore_index=True)

    statistics = statistics.merge(
        buildings.rename(columns={'buildingcount': 'buildings'}),
        how='inner',
        left_on='merge_tag',
        right_on=aggregation_type).fillna(0)

    return statistics
