from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request

from app import crud
from app.database import DBSessionDep, paginate
from app.schemas import (DamageCalculationSchema, LossCalculationSchema,
                         PaginatedResponse)

router = APIRouter(prefix='/calculation', tags=['calculations'])


@router.get('',
            response_model=PaginatedResponse[LossCalculationSchema |
                                             DamageCalculationSchema],
            response_model_exclude_none=True)
async def read_calculations(request: Request,
                            db: DBSessionDep,
                            starttime: datetime | None = None,
                            endtime: datetime | None = None,
                            limit: int = Query(50, ge=0),
                            offset: int = Query(0, ge=0)):
    '''
    Returns a list of calculations.
    '''
    query = crud.read_calculations(starttime, endtime)
    return await paginate(db, query, limit, offset)


@router.get('/{oid}',
            response_model=LossCalculationSchema | DamageCalculationSchema,
            response_model_exclude_none=True)
async def read_calculation(oid: int,
                           request: Request,
                           db: DBSessionDep):
    '''
    Returns the requested calculation.
    '''
    db_result = await crud.read_calculation(db, oid)

    if not db_result:
        raise HTTPException(status_code=404, detail='No calculation found.')

    return db_result
