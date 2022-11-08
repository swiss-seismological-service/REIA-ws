
from datetime import datetime

import pandas as pd
from esloss.datamodel import (AggregatedLoss, AggregationTag, Calculation,
                              EarthquakeInformation, ELossCategory,
                              RiskCalculation, aggregatedloss_aggregationtag)
from sqlalchemy import func, select
from sqlalchemy.orm import Session, with_polymorphic
from sqlalchemy.sql import Select


def losscategory_filter(
    f): return AggregatedLoss.losscategory == f if f else True


def tagname_filter(f): return AggregatedLoss.aggregationtags.any(
    AggregationTag.name == f) if f else True


def calculationid_filter(f):
    return AggregatedLoss._calculation_oid == f if f else True


def statement_select_per_tag(agg_type: str,
                             filter: bool = True) -> Select:
    tag_sub = select(AggregationTag).where(
        AggregationTag.type == agg_type).subquery()
    stmt = select(AggregatedLoss.loss_value,
                  AggregatedLoss.loss_uncertainty,
                  AggregatedLoss.eventid,
                  tag_sub.c.name.label(agg_type)) \
        .select_from(AggregatedLoss).join(aggregatedloss_aggregationtag) \
        .join(tag_sub) \
        .where(filter) \
        .order_by(tag_sub.c.name, AggregatedLoss.eventid)

    return stmt


def read_aggregation_losses_df(db: Session,
                               calculation_id: int,
                               agg_type: str,
                               loss_category: ELossCategory | None = None,
                               filter_tag: str | None = None) \
        -> pd.DataFrame:

    filter = calculationid_filter(calculation_id)
    filter &= losscategory_filter(loss_category)
    filter &= tagname_filter(filter_tag)

    stmt = statement_select_per_tag(agg_type, filter)
    return pd.read_sql(stmt, db.get_bind())


def read_mean_losses_df(db: Session,
                        calculation_id: int,
                        aggregation_type: str,
                        loss_category: ELossCategory | None = None,
                        filter_tag: str | None = None) \
        -> pd.DataFrame:

    filter = calculationid_filter(calculation_id)
    filter &= losscategory_filter(loss_category)
    filter &= tagname_filter(filter_tag)

    per_tag = statement_select_per_tag(aggregation_type, filter)
    stmt = select(
        (func.sum(per_tag.c.loss_value)/1250).label('mean'),
        (func.sum(per_tag.c.loss_uncertainty)/(1250 ^ 2)).label('variance'),
        getattr(per_tag.c, aggregation_type)) \
        .group_by(getattr(per_tag.c, aggregation_type))
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
    all_calculations = with_polymorphic(Calculation, [RiskCalculation])
    stmt = select(all_calculations)
    if starttime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime >= starttime)
    if endtime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime <= endtime)
    return db.execute(stmt).unique().scalars().all()


def read_calculation(db: Session, id: int) -> list[Calculation]:
    all_calculations = with_polymorphic(Calculation, [RiskCalculation])
    stmt = select(all_calculations).where(Calculation._oid == id)
    return db.execute(stmt).unique().scalar()
