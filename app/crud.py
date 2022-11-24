
from datetime import datetime

import pandas as pd
from esloss.datamodel import (AggregationTag, Asset, Calculation,
                              EarthquakeInformation, ELossCategory,
                              ExposureModel, LossCalculation,
                              LossCalculationBranch, LossValue,
                              asset_aggregationtag, riskvalue_aggregationtag)
from sqlalchemy import func, select
from sqlalchemy.orm import Session, with_polymorphic
from sqlalchemy.sql import Select


def losscategory_filter(
    f): return LossValue.losscategory == f if f else True


def tagname_filter(f): return LossValue.aggregationtags.any(
    AggregationTag.name == f) if f else True


def tagname_like_filter(f): return LossValue.aggregationtags.any(
    AggregationTag.name.like(f)) if f else True


def calculationid_filter(f):
    return LossValue._calculation_oid == f if f else True


def read_total_buildings(db: Session,
                         calculation_id: int,
                         agg_type: str,
                         canton: str) -> int:
    tag_sub = select(AggregationTag).where(
        AggregationTag.type == agg_type).subquery()

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


def read_total_losses_v2(db: Session,
                         calculation_id,
                         agg_type,
                         loss_category,
                         canton):
    tag_sub = select(AggregationTag).where(
        AggregationTag.type == agg_type).subquery()

    stmt = select(func.sum(LossValue.loss_value*LossValue.weight),
                  tag_sub.c.name)\
        .select_from(LossValue)\
        .where(LossValue.losscategory == loss_category) \
        .where(LossValue._calculation_oid == calculation_id) \
        .join(riskvalue_aggregationtag) \
        .join(tag_sub) \
        .where(tag_sub.c.name.like(f'{canton}%')) \
        .group_by(tag_sub.c.name)
    print(stmt)
    return pd.read_sql(stmt, db.get_bind())


def statement_select_per_tag(agg_type: str,
                             filter: bool = True) -> Select:
    tag_sub = select(AggregationTag).where(
        AggregationTag.type == agg_type).subquery()
    stmt = select(LossValue._oid,
                  LossValue.loss_value,
                  tag_sub.c.name.label(agg_type),
                  LossValue.weight) \
        .select_from(LossValue)\
        .where(filter) \
        .join(riskvalue_aggregationtag) \
        .join(tag_sub)

    return stmt


def read_aggregation_losses_df(db: Session,
                               calculation_id: int,
                               agg_type: str,
                               loss_category: ELossCategory | None = None,
                               filter_tag: str | None = None,
                               filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    filter = calculationid_filter(calculation_id)
    filter &= losscategory_filter(loss_category)
    filter &= tagname_filter(filter_tag)
    filter &= tagname_like_filter(filter_like_tag)

    stmt = statement_select_per_tag(agg_type, filter)
    return pd.read_sql(stmt, db.get_bind())


def read_mean_losses_df(db: Session,
                        calculation_id: int,
                        aggregation_type: str,
                        loss_category: ELossCategory | None = None,
                        filter_tag: str | None = None,
                        filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    filter = calculationid_filter(calculation_id)
    filter &= losscategory_filter(loss_category)
    filter &= tagname_filter(filter_tag)
    filter &= tagname_like_filter(filter_like_tag)

    per_tag = statement_select_per_tag(aggregation_type, filter)
    stmt = select(
        (func.sum(per_tag.c.loss_value*per_tag.c.weight)).label('mean'),
        getattr(per_tag.c, aggregation_type)) \
        .group_by(getattr(per_tag.c, aggregation_type))
    print(stmt)
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
