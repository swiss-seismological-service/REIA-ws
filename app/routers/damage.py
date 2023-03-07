import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from reia.datamodel import ELossCategory
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import DamageValueStatisticsSchema
from app.utils import calculate_statistics, csv_response

router = APIRouter(prefix='/damage', tags=['damage'])


@router.get("/{calculation_id}/{damage_category}/Country",
            response_model=DamageValueStatisticsSchema,
            response_model_exclude_none=True)
async def get_country_damage(calculation_id: int,
                             damage_category: ELossCategory,
                             format: str = 'json',
                             db: Session = Depends(get_db)):
    """
    Returns a list of all realizations of loss for a calculation.
    """
    db_buildings = crud.read_total_buildings_country(db, calculation_id)

    db_result = crud.read_country_damage(db, calculation_id, damage_category)

    if db_result.empty or not db_buildings:
        if not crud.read_calculation(db, calculation_id):
            raise HTTPException(status_code=404, detail="No damage found.")
        else:
            return DamageValueStatisticsSchema(mean=0,
                                               quantile10=0,
                                               quantile90=0,
                                               percentage=0,
                                               losscategory=damage_category,
                                               tag='CH')
    db_result['Country'] = 'CH'

    statistics = calculate_statistics(db_result, 'Country')
    statistics['losscategory'] = damage_category.value
    statistics['percentage'] = statistics['mean'] / db_buildings * 100

    if format == 'csv':
        return csv_response(statistics, 'loss')

    return [DamageValueStatisticsSchema(**x)
            for x in statistics.to_dict('records')][0]


@router.get("/{calculation_id}/{damage_category}/{aggregation_type}",
            response_model=list[DamageValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_damage(calculation_id: int,
                     aggregation_type: str,
                     damage_category: ELossCategory,
                     aggregation_tag: str | None = None,
                     format: str = 'json',
                     db: Session = Depends(get_db)):
    """
    Returns a list of all realizations of loss for a calculation.
    """

    like_tag = f'%{aggregation_tag}%' if aggregation_tag else None

    tags = crud.read_aggregationtags(db, aggregation_type, like_tag)

    if tags.empty:
        raise HTTPException(
            status_code=404, detail="No aggregationtags found.")

    db_result = crud.read_aggregated_damage(
        db, calculation_id,
        aggregation_type,
        damage_category,
        filter_like_tag=like_tag)

    db_buildings = crud.read_total_buildings(
        db, calculation_id,
        aggregation_type,
        filter_like_tag=like_tag)

    if db_result.empty or db_buildings.empty:
        if not crud.read_calculation(db, calculation_id):
            raise HTTPException(status_code=404, detail="No damage found.")

    db_result = db_result.merge(
        tags, how='outer', on=aggregation_type).fillna(0)

    statistics = calculate_statistics(db_result, aggregation_type)
    statistics['losscategory'] = damage_category.value

    percentages = pd.DataFrame(
        {'percentage': statistics.set_index('tag')['mean'] /
         db_buildings.set_index(aggregation_type)['buildingcount'] * 100})

    statistics = statistics.merge(
        percentages, how='outer', left_on='tag', right_index=True).fillna(0)

    if format == 'csv':
        return csv_response(statistics, 'loss')

    return [DamageValueStatisticsSchema(**x)
            for x in statistics.to_dict('records')]
