from esloss.datamodel import ELossCategory
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import RiskValueStatisticsSchema
from app.wquantile import weighted_quantile

router = APIRouter(prefix='/damage', tags=['damage'])


@router.get("/{calculation_id}/{loss_category}/{aggregation_type}",
            response_model=list[RiskValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_damage(calculation_id: int,
                     aggregation_type: str,
                     loss_category: ELossCategory,
                     aggregation_tag: str | None = None,
                     db: Session = Depends(get_db)):
    """
    Returns a list of all realizations of loss for a calculation.
    """
    return None
    like_tag = f'%{aggregation_tag}%' if aggregation_tag else None

    db_result = \
        crud.read_aggregation_losses_df(db,
                                        calculation_id,
                                        aggregation_type,
                                        loss_category,
                                        filter_like_tag=like_tag)

    if db_result.empty:
        raise HTTPException(status_code=404, detail="No loss found.")

    result = []

    for tag, group in db_result.groupby(aggregation_type):
        mean = (group['weight']*group['loss_value']).sum()
        q10, q90 = weighted_quantile(
            group['loss_value'], (0.1, 0.9), group['weight'])

        result.append(
            RiskValueStatisticsSchema(
                mean=mean,
                quantile10=q10,
                quantile90=q90,
                losscategory=loss_category,
                tag=tag)
        )

    return result
