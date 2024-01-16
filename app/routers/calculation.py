from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app import crud
from app.database import get_db, paginate
from app.schemas import (DamageCalculationSchema, LossCalculationSchema,
                         PaginatedResponse)

router = APIRouter(prefix='/calculation', tags=['calculations'])


@router.get('',
            response_model=PaginatedResponse[LossCalculationSchema |
                                             DamageCalculationSchema],
            response_model_exclude_none=True)
def read_calculations(request: Request,
                      starttime: datetime | None = None,
                      endtime: datetime | None = None,
                      limit: int = Query(50, ge=0),
                      offset: int = Query(0, ge=0),
                      db: Session = Depends(get_db)):
    '''
    Returns a list of calculations.
    '''
    db_result = crud.read_calculations(db, starttime, endtime)
    return paginate(db, db_result, limit, offset)


@router.get('/{oid}',
            response_model=LossCalculationSchema | DamageCalculationSchema,
            response_model_exclude_none=True)
def read_calculation(oid: int,
                     request: Request,
                     db: Session = Depends(get_db)):
    '''
    Returns the requested calculation.
    '''
    db_result = crud.read_calculation(db, oid)

    if not db_result:
        raise HTTPException(status_code=404, detail='No calculation found.')

    return db_result
