from fastapi import APIRouter, Depends, HTTPException
from reia.datamodel import ELossCategory
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import RiskValueStatisticsSchema
from app.utils import calculate_statistics, csv_response

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{calculation_id}/{loss_category}/Country",
            response_model=RiskValueStatisticsSchema,
            response_model_exclude_none=True)
async def get_country_losses(calculation_id: int,
                             loss_category: ELossCategory,
                             format: str = 'json',
                             db: Session = Depends(get_db)):
    """
    Returns a list of all realizations of loss for a calculation.
    """

    db_result = crud.read_country_loss(db, calculation_id, loss_category)

    if db_result.empty:
        if not crud.read_calculation(db, calculation_id):
            raise HTTPException(status_code=404, detail="No loss found.")
        else:
            return RiskValueStatisticsSchema(mean=0,
                                             percentile10=0,
                                             percentile90=0,
                                             losscategory=loss_category,
                                             tag='CH')

    db_result['Country'] = 'CH'

    statistics = calculate_statistics(db_result, 'Country')

    statistics['losscategory'] = loss_category.value

    if format == 'csv':
        return csv_response(statistics, 'loss')

    return [RiskValueStatisticsSchema(**x)
            for x in statistics.to_dict('records')][0]


@router.get("/{calculation_id}/{loss_category}/{aggregation_type}",
            response_model=list[RiskValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_losses(calculation_id: int,
                     aggregation_type: str,
                     loss_category: ELossCategory,
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

    db_result = \
        crud.read_aggregated_loss(db,
                                  calculation_id,
                                  aggregation_type,
                                  loss_category,
                                  filter_like_tag=like_tag)

    if db_result.empty:
        if not crud.read_calculation(db, calculation_id):
            raise HTTPException(status_code=404, detail="No loss found.")

    db_result = db_result.merge(
        tags, how='outer', on=aggregation_type).fillna(0)

    statistics = calculate_statistics(db_result, aggregation_type)

    statistics['losscategory'] = loss_category.value

    if format == 'csv':
        return csv_response(statistics, 'loss')

    return [RiskValueStatisticsSchema(**x)
            for x in statistics.to_dict('records')]
