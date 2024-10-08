from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from pandas import DataFrame

from app import crud
from app.database import DBSessionDep
from app.schemas import DamageValueStatisticsSchema, ReturnFormats
from app.utils import (aggregate_by_branch_and_event, calculate_statistics,
                       csv_response, merge_statistics_to_buildings)
from config import Settings

router = APIRouter(prefix='/damage', tags=['damage'])


async def calculate_damages(calculation_id: int,
                            aggregation_type: str,
                            damage_category: Settings.RiskCategory,
                            db: DBSessionDep,
                            filter_tag_like: str | None = None,
                            sum: bool = False):

    like_tag = f'%{filter_tag_like}%' if filter_tag_like else None

    tags = await crud.read_aggregationtags(
        db, aggregation_type, calculation_id, like_tag)

    db_result = await crud.read_aggregated_damage(
        db, calculation_id,
        aggregation_type,
        damage_category,
        filter_like_tag=like_tag)

    db_buildings = await crud.read_total_buildings(
        db, calculation_id,
        aggregation_type,
        filter_like_tag=like_tag)

    if tags.empty:
        raise HTTPException(
            status_code=404, detail="Aggregationtag not found.")

    if db_result.empty or db_buildings.empty:
        if not await crud.read_calculation(db, calculation_id):
            raise HTTPException(
                status_code=404, detail="Damage calculation not found.")

    # merge with aggregationtags to add missing (no damage) aggregationtags
    db_result = db_result.merge(
        tags, how='outer', on=aggregation_type) \
        .infer_objects(copy=False).fillna(0)

    if sum:
        db_result = aggregate_by_branch_and_event(db_result, aggregation_type)

    statistics = calculate_statistics(db_result, aggregation_type)

    statistics['category'] = damage_category

    statistics = merge_statistics_to_buildings(
        statistics, db_buildings, aggregation_type)

    return statistics


@router.get("/{calculation_id}/{damage_category}/{aggregation_type}",
            response_model=list[DamageValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_damage(
        calculation_id: int,
        damage_category: Settings.RiskCategory,
        aggregation_type: str,
        request: Request,
        statistics: Annotated[DataFrame, Depends(calculate_damages)],
        filter_tag_like: str | None = None,
        sum: bool = False,
        format: ReturnFormats = ReturnFormats.JSON,):

    if format == ReturnFormats.CSV:
        return csv_response('damage', locals())

    return [DamageValueStatisticsSchema.model_validate(x)
            for x in statistics.to_dict('records')]
