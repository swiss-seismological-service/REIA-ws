
from datetime import datetime

from esloss.datamodel import (AggregatedLoss, EarthquakeInformation,
                              LossCalculation)
from sqlalchemy import select
from sqlalchemy.orm import Session


def read_losses(db: Session, losscalculation_id: int) \
        -> list[AggregatedLoss]:
    stmt = select(AggregatedLoss).where(
        AggregatedLoss._losscalculation_oid == losscalculation_id)
    return db.execute(stmt).unique().scalars().all()


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
