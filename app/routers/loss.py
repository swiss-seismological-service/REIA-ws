from esloss.datamodel import EEarthquakeType, ELossCategory
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import (AggregatedLossSchema, AggregationTagSchema,
                         ELossStatistics, LossStatisticsSchema)

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{losscalculation_id}", response_model=list[AggregatedLossSchema],
            response_model_exclude_none=True)
async def get_losses(losscalculation_id: int, db: Session = Depends(get_db)):
    """
    Returns a list of all realizations of loss for a calculation.
    """
    db_result = crud.read_losses(db, losscalculation_id)
    if not db_result:
        raise HTTPException(status_code=404, detail="No loss found.")
    return db_result


@router.get("/{losscalculation_id}/{statistic}",
            response_model=list[LossStatisticsSchema],
            response_model_exclude_none=True)
async def get_loss_statistics(losscalculation_id: int,
                              statistic: str,
                              aggregationtypes: list[str] | None = None,
                              db: Session = Depends(get_db)):
    """
    Returns a list of statistical measures for the available aggregations.
    """

    db_result = crud.read_losses(db, losscalculation_id)
    if not db_result:
        raise HTTPException(status_code=404, detail="No loss found.")

    db_calculation = crud.read_calculation(db, losscalculation_id)
    print(db_calculation.aggregateby)
    # if not set(aggregationtypes.issubset([a for a in db_calculation.aggregateby]))
    return [LossStatisticsSchema(loss={'value': 0},
                                 losscategory=ELossCategory.STRUCTURAL,
                                 aggregationtags=[AggregationTagSchema(
                                     type='Canton', name='Valais')],
                                 statisticstype=ELossStatistics.MEAN)]
