from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from reia.datamodel import ELossCategory
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import LossValueStatisticsSchema
from app.utils import (aggregate_by_branch_and_event, calculate_statistics,
                       csv_response)

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{calculation_id}/{loss_category}/{aggregation_type}",
            response_model=list[LossValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_losses(calculation_id: int,
                     aggregation_type: str,
                     loss_category: ELossCategory,
                     filter_tag_like: str | None = None,
                     format: Literal['json', 'csv'] = 'json',
                     sum: bool = False,
                     db: Session = Depends(get_db)):
    """
    Returns a list of the loss for a specific category and aggregated
    by a specific aggregation type.
    """
    like_tag = f'%{filter_tag_like}%' if filter_tag_like else None

    tags = crud.read_aggregationtags(db, aggregation_type, like_tag)

    if tags.empty:
        raise HTTPException(
            status_code=404, detail="No aggregationtags found.")

    db_result = \
        crud.read_aggregated_loss(db,
                                  calculation_id,
                                  aggregation_type,
                                  loss_category,
                                  filter_like_tag=like_tag)

    if db_result.empty:
        if not crud.read_calculation(db, calculation_id):
            raise HTTPException(
                status_code=404, detail="Loss calculation not found.")

    db_result = db_result.merge(
        tags, how='outer', on=aggregation_type).fillna(0)

    if sum:
        db_result = aggregate_by_branch_and_event(db_result, aggregation_type)

    statistics = calculate_statistics(db_result, aggregation_type)

    statistics['losscategory'] = loss_category.value

    if format == 'csv':
        return csv_response(statistics, 'loss')

    return [LossValueStatisticsSchema.parse_obj(x)
            for x in statistics.to_dict('records')]
