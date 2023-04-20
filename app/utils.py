import pandas as pd
from fastapi.responses import StreamingResponse

from app.wquantile import weighted_quantile


def csv_response(data: pd.DataFrame, filename: str) -> StreamingResponse:
    output = data.to_csv(index=False)
    return StreamingResponse(
        iter([output]),
        media_type='text/csv',
        headers={"Content-Disposition":
                 f"attachment;filename={filename}.csv"})


def calculate_statistics(data: pd.DataFrame, aggregation_type: str):
    # either loss_value or damage_value
    value_column = next(i for i in data.columns if 'value' in i)

    # calculate weighted loss
    data['weighted'] = data['weight'] * \
        data[value_column]

    # initialize with mean
    statistics = pd.DataFrame({'mean': data.groupby(
        aggregation_type)['weighted'].sum()})

    # calculate quantiles
    statistics['percentile10'], statistics['percentile90'] = \
        zip(*data.groupby(aggregation_type).apply(
            lambda x: weighted_quantile(
                x[value_column], (0.1, 0.9), x['weight'])))

    # drop intermediate column again form original df
    data.drop(columns=['weighted'])

    statistics = statistics.rename_axis(
        'tag').reset_index()

    return statistics
