import time
from typing import List

from esloss.datamodel import EEarthquakeType, ELossCategory
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import (AggregatedLossSchema, AggregationTagSchema,
                         ELossStatistics, LossStatisticsSchema)

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{losscalculation_id}/aggregated/{aggregation_type}",
            # response_model=list[AggregatedLossSchema],
            response_model_exclude_none=True)
async def get_losses(losscalculation_id: int,
                     aggregation_type: str,
                     losscategory: ELossCategory | None = None,
                     aggregationtag: str | None = None,
                     db: Session = Depends(get_db)):
    """
    Returns a list of all realizations of loss for a calculation.
    """
    now = time.perf_counter()
    db_result = crud.read_tag_losses_df(db, losscalculation_id, 'd')

    if db_result.empty:
        raise HTTPException(status_code=404, detail="No loss found.")

    print(time.perf_counter()-now)
    print(db_result)

    now = time.perf_counter()
    db_result = crud.read_aggregation_losses_df(db, losscalculation_id,
                                                aggregation_type,
                                                losscategory,
                                                aggregationtag)
    if db_result.empty:
        raise HTTPException(status_code=404, detail="No loss found.")
    print(time.perf_counter()-now)
    print(db_result)
    return None


@router.get("/{losscalculation_id}/aggregated/{aggregation_type}/mean",
            response_model=list[LossStatisticsSchema],
            response_model_exclude_none=True)
async def get_mean_losses(losscalculation_id: int,
                          aggregation_type: str,
                          losscategory: ELossCategory | None = None,
                          # aggregationtags: List[str] = Query(default=[]),
                          aggregationtag: str | None = None,
                          db: Session = Depends(get_db)):
    """
    Returns a list of statistical measures for the available aggregations.
    """
    now = time.perf_counter()
    db_result = crud.read_mean_losses_df(
        db, losscalculation_id, aggregation_type, losscategory, aggregationtag)

    if db_result.empty:
        raise HTTPException(status_code=404, detail="No loss found.")

    print(time.perf_counter()-now)
    print(db_result)

    return [LossStatisticsSchema(loss={'value': 0},
                                 losscategory=ELossCategory.STRUCTURAL,
                                 aggregationtags=[AggregationTagSchema(
                                     type='Canton', name='Valais')],
                                 statisticstype=ELossStatistics.MEAN)]
