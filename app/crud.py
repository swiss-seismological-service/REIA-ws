
from esloss.datamodel import (AggregatedLoss, EarthquakeInformation,
                              LossCalculation)
from sqlalchemy import select
from sqlalchemy.orm import Session


def read_losses(db: Session, losscalculation_id: int) \
        -> list[AggregatedLoss]:
    stmt = select(AggregatedLoss).where(
        AggregatedLoss._losscalculation_oid == losscalculation_id)
    return db.execute(stmt).unique().scalars().all()


def read_earthquakes(db: Session) -> list[EarthquakeInformation]:
    stmt = select(EarthquakeInformation)
    return db.execute(stmt).unique().scalars().all()


def read_calculations(db: Session) -> list[LossCalculation]:
    stmt = select(LossCalculation)
    return db.execute(stmt).unique().scalars().all()
