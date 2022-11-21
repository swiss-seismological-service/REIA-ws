from esloss.datamodel import ELossCategory
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db

# from app.schemas import AggregationTagSchema

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{calculation_id}/aggregated/{aggregation_type}",
            # response_model=list[AggregatedLossSchema],
            response_model_exclude_none=True)
async def get_losses(calculation_id: int,
                     aggregation_type: str,
                     losscategory: ELossCategory | None = None,
                     aggregationtag: str | None = None,
                     db: Session = Depends(get_db)):
    """
    Returns a list of all realizations of loss for a calculation.
    """

    db_result = crud.read_aggregation_losses_df(db, calculation_id,
                                                aggregation_type,
                                                losscategory,
                                                aggregationtag)
    if db_result.empty:
        raise HTTPException(status_code=404, detail="No loss found.")
    print(db_result)
    return None


@router.get("/{calculation_id}/aggregated/{aggregation_type}/mean",
            # response_model=list[LossStatisticsSchema],
            response_model_exclude_none=True)
async def get_mean_losses(calculation_id: int,
                          aggregation_type: str,
                          losscategory: ELossCategory | None = None,
                          # aggregationtags: List[str] = Query(default=[]),
                          aggregationtag: str | None = None,
                          db: Session = Depends(get_db)):
    """
    Returns a list of statistical measures for the available aggregations.
    """
    db_result = crud.read_mean_losses_df(
        db, calculation_id, aggregation_type, losscategory, aggregationtag)
    print(db_result)
    if db_result.empty:
        raise HTTPException(status_code=404, detail="No loss found.")
    return None
    # return [LossStatisticsSchema(loss={'value': 0},
    #                              losscategory=ELossCategory.STRUCTURAL,
    #                              aggregationtags=[AggregationTagSchema(
    #                                  type='Canton', name='Valais')],
    #                              statisticstype=ELossStatistics.MEAN)]
