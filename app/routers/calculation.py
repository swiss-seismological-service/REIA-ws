from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.database import get_db
from app.schemas import DamageCalculationSchema, LossCalculationSchema

router = APIRouter(prefix='/calculation', tags=['calculations'])


@router.get('',
            response_model=list[LossCalculationSchema |
                                DamageCalculationSchema],
            response_model_exclude_none=True)
async def read_calculations(starttime: datetime | None = None,
                            endtime: datetime | None = None,
                            db: Session = Depends(get_db)):
    '''
    Returns a list of calculations.
    '''
    db_result = crud.read_calculations(db, starttime, endtime)
    return db_result or []


@router.get('/{oid}',
            response_model=LossCalculationSchema | DamageCalculationSchema,
            response_model_exclude_none=True)
async def read_calculation(oid: int,
                           db: Session = Depends(get_db)):
    '''
    Returns the requested calculation.
    '''
    db_result = crud.read_calculation(db, oid)

    if not db_result:
        raise HTTPException(status_code=404, detail='No calculation found.')

    return db_result
