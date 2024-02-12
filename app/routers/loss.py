import asyncio

from fastapi import APIRouter, HTTPException, Request

from app import crud
from app.database import DBSessionDep
from app.schemas import LossValueStatisticsSchema, ReturnFormats
from app.utils import (aggregate_by_branch_and_event, calculate_statistics,
                       csv_response)
from config import Settings

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{calculation_id}/{loss_category}/{aggregation_type}",
            response_model=list[LossValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_losses(calculation_id: int,
                     aggregation_type: str,
                     request: Request,
                     loss_category: Settings.RiskCategory,
                     db: DBSessionDep,
                     filter_tag_like: str | None = None,
                     format: ReturnFormats = ReturnFormats.JSON,
                     sum: bool = False):
    """
    Returns a list of the loss for a specific category and aggregated
    by a specific aggregation type.
    """

    like_tag = f'%{filter_tag_like}%' if filter_tag_like else None

    tags = crud.read_aggregationtags(db, aggregation_type, like_tag)

    db_result = crud.read_aggregated_loss(db,
                                          calculation_id,
                                          aggregation_type,
                                          loss_category,
                                          filter_like_tag=like_tag)

    [tags, db_result] = await asyncio.gather(tags, db_result)

    if tags.empty:
        raise HTTPException(
            status_code=404, detail="No aggregationtags found.")

    if db_result.empty:
        if not await crud.read_calculation(db, calculation_id):
            raise HTTPException(
                status_code=404, detail="Loss calculation not found.")

    db_result = db_result.merge(
        tags, how='outer', on=aggregation_type) \
        .infer_objects(copy=False).fillna(0)

    if sum:
        db_result = aggregate_by_branch_and_event(db_result, aggregation_type)

    statistics = calculate_statistics(db_result, aggregation_type)

    statistics['category'] = loss_category

    if format == ReturnFormats.CSV:
        return csv_response('loss', locals())

    return [LossValueStatisticsSchema.model_validate(x)
            for x in statistics.to_dict('records')]
