from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import DamageCalculationSchema, LossCalculationSchema

router = APIRouter(prefix='/calculation', tags=['calculations', 'earthquakes'])


@router.get('',
            response_model=list[LossCalculationSchema |
                                DamageCalculationSchema],
            response_model_exclude_none=True)
async def read_calculations(starttime: datetime | None = None,
                            endtime: datetime | None = None,
                            db: Session = Depends(get_db)):
    '''
    Returns a list of Calculations.
    '''
    db_result = crud.read_calculations(db, starttime, endtime)
    if not db_result:
        raise HTTPException(status_code=404, detail='No calculations found.')
    return db_result


@router.get('/{oid}',
            response_model=LossCalculationSchema | DamageCalculationSchema,
            response_model_exclude_none=True)
async def read_calculation(oid: int,
                           db: Session = Depends(get_db)):
    '''
    Returns a the requested Calculation.
    '''
    db_result = crud.read_calculation(db, oid)
    if not db_result:
        raise HTTPException(status_code=404, detail='No calculation found.')
    return db_result
