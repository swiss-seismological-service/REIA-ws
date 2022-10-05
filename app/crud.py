from typing import List

from esloss.datamodel import AggregatedLoss
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.schemas import AggregatedLossSchema


def get_losses(db: Session, losscalculation_id: int) \
        -> List[AggregatedLossSchema]:
    stmt = select(AggregatedLoss).where(
        AggregatedLoss._losscalculation_oid == losscalculation_id)
    return db.execute(stmt).unique().scalars().all()
