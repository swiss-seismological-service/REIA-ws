
from datetime import datetime

import pandas as pd
from esloss.datamodel import (AggregationTag, Asset, Calculation,
                              EarthquakeInformation, ELossCategory,
                              ExposureModel, LossCalculation,
                              LossCalculationBranch, LossValue,
                              asset_aggregationtag, riskvalue_aggregationtag)
from sqlalchemy import func, select
from sqlalchemy.orm import Session, with_polymorphic


def losscategory_filter(
    f): return LossValue.losscategory == f if f else True


def tagname_filter(f): return LossValue.aggregationtags.any(
    AggregationTag.name == f) if f else True


def tagname_like_filter(f): return LossValue.aggregationtags.any(
    AggregationTag.name.like(f)) if f else True


def calculationid_filter(f):
    return LossValue._calculation_oid == f if f else True


def read_country_statistics(db: Session, calculation_id, loss_category):

    filter = calculationid_filter(calculation_id)
    filter &= losscategory_filter(loss_category)

    tag_sub = select(AggregationTag).where(
        AggregationTag.type == 'Canton').subquery()

    stmt = select(func.sum(LossValue.loss_value).label('loss_value'),
                  (func.sum(LossValue.weight) /
                   func.count(LossValue._oid)).label('weight')
                  ) \
        .select_from(LossValue) \
        .where(filter) \
        .join(riskvalue_aggregationtag) \
        .join(tag_sub) \
        .group_by(
            ((LossValue._losscalculationbranch_oid *
              (10 ** 6))+LossValue.eventid).label('event'))

    return pd.read_sql(stmt, db.get_bind())


def read_total_buildings(db: Session,
                         calculation_id: int,
                         aggregation_type: str,
                         canton: str) -> int:
    tag_sub = select(AggregationTag).where(
        AggregationTag.type == aggregation_type).subquery()

    exp_sub = select(ExposureModel._oid) \
        .join(LossCalculationBranch) \
        .join(Calculation) \
        .where(Calculation._oid == calculation_id).limit(1).subquery()

    stmt = select(func.sum(Asset.buildingcount), tag_sub.c.name)\
        .select_from(Asset)\
        .join(asset_aggregationtag) \
        .join(tag_sub) \
        .join(exp_sub, exp_sub.c._oid == Asset._exposuremodel_oid) \
        .where(tag_sub.c.name.like(f'{canton}%')) \
        .group_by(tag_sub.c.name)

    return pd.read_sql(stmt, db.get_bind())


def read_mean_losses(db: Session,
                     calculation_id,
                     aggregation_type,
                     loss_category,
                     filter_tag: str | None = None,
                     filter_like_tag: str | None = None):

    filter = calculationid_filter(calculation_id)
    filter &= losscategory_filter(loss_category)
    filter &= tagname_filter(filter_tag)
    filter &= tagname_like_filter(filter_like_tag)

    tag_sub = select(AggregationTag).where(
        AggregationTag.type == aggregation_type).subquery()

    stmt = select(func.sum(LossValue.loss_value*LossValue.weight),
                  tag_sub.c.name)\
        .select_from(LossValue)\
        .where(LossValue.losscategory == loss_category) \
        .where(LossValue._calculation_oid == calculation_id) \
        .join(riskvalue_aggregationtag) \
        .join(tag_sub) \
        .where(filter) \
        .group_by(tag_sub.c.name)
    print(stmt)
    return pd.read_sql(stmt, db.get_bind())


def read_aggregation_losses_df(db: Session,
                               calculation_id: int,
                               aggregation_type: str,
                               loss_category: ELossCategory,
                               filter_tag: str | None = None,
                               filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    filter = calculationid_filter(calculation_id)
    filter &= losscategory_filter(loss_category)
    filter &= tagname_filter(filter_tag)
    filter &= tagname_like_filter(filter_like_tag)

    tag_sub = select(AggregationTag).where(
        AggregationTag.type == aggregation_type).subquery()
    stmt = select(LossValue._oid,
                  LossValue.loss_value,
                  tag_sub.c.name.label(aggregation_type),
                  LossValue.weight) \
        .select_from(LossValue)\
        .where(filter) \
        .join(riskvalue_aggregationtag) \
        .join(tag_sub)
    return pd.read_sql(stmt, db.get_bind())


def read_earthquakes(db: Session, starttime: datetime | None,
                     endtime: datetime | None) -> list[EarthquakeInformation]:
    stmt = select(EarthquakeInformation)
    if starttime:
        stmt = stmt.filter(EarthquakeInformation.time >= starttime)
    if endtime:
        stmt = stmt.filter(EarthquakeInformation.time <= endtime)
    return db.execute(stmt).unique().scalars().all()


def read_calculations(db: Session, starttime: datetime | None,
                      endtime: datetime | None) -> list[Calculation]:
    all_calculations = with_polymorphic(Calculation, [LossCalculation])
    stmt = select(all_calculations)
    if starttime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime >= starttime)
    if endtime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime <= endtime)
    return db.execute(stmt).unique().scalars().all()


def read_calculation(db: Session, id: int) -> list[Calculation]:
    all_calculations = with_polymorphic(Calculation, [LossCalculation])
    stmt = select(all_calculations).where(Calculation._oid == id)
    return db.execute(stmt).unique().scalar()
