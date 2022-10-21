
from datetime import datetime

import pandas as pd
from esloss.datamodel import (AggregatedLoss, AggregationTag,
                              EarthquakeInformation, LossCalculation,
                              aggregatedloss_aggregationtag)
from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import label


def read_losses_test(db: Session, losscalculation_id: int) \
        -> list[AggregatedLoss]:

    stmt2 = select(AggregationTag).where(
        AggregationTag.type == 'CantonGemeinde').subquery()
    stmt = select(
        func.avg(AggregatedLoss.loss_value),
        func.sum(AggregatedLoss.loss_uncertainty) /
        func.pow(func.count(AggregatedLoss.loss_value), 2),
        func.count(AggregatedLoss.loss_value),
        stmt2.c.name).select_from(
        AggregatedLoss).join(
        aggregatedloss_aggregationtag).join(
        stmt2).where(
        AggregatedLoss._losscalculation_oid == losscalculation_id).where(
        AggregatedLoss.aggregationtags.any(AggregationTag.name == 'VS')
    ).group_by(stmt2.c.name)

    return db.execute(stmt).unique().scalars().all()


def read_losses(db: Session, losscalculation_id: int) \
        -> list[AggregatedLoss]:

    stmt = select(AggregatedLoss).where(
        AggregatedLoss._losscalculation_oid == losscalculation_id)
    print(stmt)
    return db.execute(stmt).unique().scalars().all()


def read_tag_losses(db: Session, losscalculation_id: int, tag: str) \
        -> list[AggregatedLoss]:

    stmt = select(AggregatedLoss).where(
        AggregatedLoss._losscalculation_oid == losscalculation_id).where(
        AggregatedLoss.aggregationtags.any(AggregationTag.name == tag)
    )
    return db.execute(stmt).unique().scalars().all()


def read_tag_losses_df(db: Session, losscalculation_id: int, tag: str) \
        -> pd.DataFrame:
    stmt = select(AggregatedLoss).where(
        AggregatedLoss._losscalculation_oid == losscalculation_id).where(
        AggregatedLoss.aggregationtags.any(AggregationTag.name == tag)
    )
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
                      endtime: datetime | None) -> list[LossCalculation]:
    stmt = select(LossCalculation)
    if starttime:
        stmt = stmt.filter(
            LossCalculation.creationinfo_creationtime >= starttime)
    if endtime:
        stmt = stmt.filter(
            LossCalculation.creationinfo_creationtime <= endtime)
    return db.execute(stmt).unique().scalars().all()


def read_calculation(db: Session, id: int) -> list[LossCalculation]:
    stmt = select(LossCalculation).where(LossCalculation._oid == id)
    return db.execute(stmt).unique().scalar()
