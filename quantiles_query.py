import time

import pandas as pd
from esloss.datamodel import ELossCategory, LossValue
from sqlalchemy import Float, cast, func, select
from sqlalchemy.orm import scoped_session

from app import crud
from app.database import SessionLocal
from app.wquantile import weighted_quantile


def queries():
    db = scoped_session(SessionLocal)
    calculation_id = 1
    aggregation_type = 'CantonGemeinde'
    losscategory = ELossCategory.CONTENTS
    aggregationtag = None  # 'AG'
    aggregationtag_like = 'AG%'

    # buildings
    # b = crud.read_total_buildings(db, 1, 'CantonGemeinde', 'AG')

    # now = time.perf_counter()
    # db_result = crud.read_aggregation_losses_df(db, calculation_id,
    #                                             aggregation_type,
    #                                             losscategory,
    #                                             aggregationtag)
    # print(db_result)
    # print(time.perf_counter()-now)
    now = time.perf_counter()
    # db_result = crud.read_mean_losses_df(
    #     db, calculation_id, aggregation_type, losscategory, aggregationtag)
    q01 = 0.1
    q09 = 0.9
    filter = crud.calculationid_filter(calculation_id)
    filter &= crud.losscategory_filter(losscategory)
    filter &= crud.tagname_filter(aggregationtag)
    filter &= crud.tagname_like_filter(aggregationtag_like)

    riskvalue = crud.statement_select_per_tag(
        aggregation_type, filter).subquery()

    db_result1 = pd.read_sql(riskvalue, db.get_bind())

    print(db_result1)

    # quantiles = select((riskvalue.c.loss_value).label('y'),
    #                    ((func.sum(riskvalue.c.weight)
    #                      .over(order_by=riskvalue.c.loss_value,
    #                            rows=(None, 0)) - 0.5 * riskvalue.c.weight) /
    #                     func.sum(riskvalue.c.weight).over())
    #                    .label('x')).subquery()

    # lower = select((quantiles.c.x).label('x'), (quantiles.c.y).label('y')).filter(quantiles.c.x <= q01).order_by(
    #     quantiles.c.x.desc()).limit(1).subquery()

    # db_result = pd.read_sql(lower, db.get_bind())

    # print(db_result)
    # return
    # lower = select((quantiles.c.x).label('x'), (quantiles.c.y).label('y')).filter(quantiles.c.x <= q01).order_by(
    #     quantiles.c.x.desc()).limit(1).subquery()
    # higher = select((quantiles.c.x).label('x'), (quantiles.c.y).label('y')).filter(quantiles.c.x > q01).order_by(
    #     quantiles.c.x.asc()).limit(1).subquery()
    # stmt = select(
    #     (
    #         (higher.c.x - q01) * lower.c.y
    #         + (q01 - lower.c.x) * higher.c.y
    #     ) / (higher.c.x - lower.c.x),
    #     (
    #         (higher.c.x - q09) * lower.c.y
    #         + (q09 - lower.c.x) * higher.c.y
    #     ) / (higher.c.x - lower.c.x)
    # )
    # stmt = select(higher.c.x, higher.c.y, lower.c.x, lower.c.y)
    # print(stmt)
    # db_result = pd.read_sql(stmt, db.get_bind())
    # print(db_result)
    # print(time.perf_counter()-now)
    # print(weighted_quantile(
    #     db_result1['loss_value'], (0.1, 0.9), db_result1['weight']))
    # print((db_result1['loss_value']*db_result1['weight']).sum())


if __name__ == '__main__':
    queries()
