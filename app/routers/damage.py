from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException
from pandas import DataFrame
from reia.datamodel import ELossCategory
from sqlalchemy.orm import Session

from app import crud
from app.database import get_db
from app.schemas import (DamageValueStatisticsReportSchema,
                         DamageValueStatisticsSchema)
from app.utils import (aggregate_by_branch_and_event, calculate_statistics,
                       csv_response, merge_statistics_to_buildings)
from config import Settings, get_settings

router = APIRouter(prefix='/damage', tags=['damage'])


def calculate_damages(calculation_id: int,
                      aggregation_type: str,
                      damage_category: ELossCategory,
                      settings: Annotated[Settings, Depends(get_settings)],
                      filter_tag_like: str | None = None,
                      format: Literal['json', 'csv'] = 'json',
                      sum: bool = False,
                      db: Session = Depends(get_db)):

    like_tag = f'%{filter_tag_like}%' if filter_tag_like else None

    tags = crud.read_aggregationtags(db, aggregation_type, like_tag)

    if tags.empty:
        raise HTTPException(
            status_code=404, detail="Aggregationtag not found.")

    db_result = crud.read_aggregated_damage(
        db, calculation_id,
        aggregation_type,
        damage_category,
        filter_like_tag=like_tag)

    db_result['damage_value'] = \
        db_result['dg2_value'] + db_result['dg3_value'] + \
        db_result['dg4_value'] + db_result['dg5_value']

    db_buildings = crud.read_total_buildings(
        db, calculation_id,
        aggregation_type,
        filter_like_tag=like_tag)

    if db_result.empty or db_buildings.empty:
        if not crud.read_calculation(db, calculation_id):
            raise HTTPException(
                status_code=404, detail="Damage calculation not found.")

    db_result = db_result.merge(
        tags, how='outer', on=aggregation_type).fillna(0)

    if sum:
        db_result = aggregate_by_branch_and_event(db_result, aggregation_type)

    statistics = calculate_statistics(db_result, aggregation_type)

    statistics['losscategory'] = damage_category.value

    statistics = merge_statistics_to_buildings(
        statistics, db_buildings, aggregation_type)

    statistics['damage_percentage'] = statistics['damage_mean'] / \
        statistics['buildings'] * 100
    return statistics


@router.get("/{calculation_id}/{damage_category}/{aggregation_type}/report",
            include_in_schema=False,
            response_model=list[DamageValueStatisticsReportSchema],
            response_model_exclude_none=True)
async def get_damage_report(
        statistics: Annotated[DataFrame, Depends(calculate_damages)]):

    if format == 'csv':
        return csv_response(statistics, 'damage')

    return [DamageValueStatisticsReportSchema.parse_obj(x)
            for x in statistics.to_dict('records')]


@router.get("/{calculation_id}/{damage_category}/{aggregation_type}",
            response_model=list[DamageValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_damage(
        statistics: Annotated[DataFrame, Depends(calculate_damages)]):

    if format == 'csv':
        return csv_response(statistics, 'damage')

    return [DamageValueStatisticsSchema.parse_obj(x)
            for x in statistics.to_dict('records')]
