import base64
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import (DamageCalculationSchema, LossCalculationSchema,
                         RiskAssessmentSchema)

router = APIRouter(tags=['calculations', 'earthquakes'])


@router.get('/calculation',
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


@router.get('/calculation/{oid}',
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


@router.get('/riskassessment', response_model=list[RiskAssessmentSchema],
            response_model_exclude_none=True)
async def read_risk_assessments(starttime: datetime | None = None,
                                endtime: datetime | None = None,
                                originid: str | None = None,
                                published: bool | None = None,
                                preferred: bool | None = None,
                                db: Session = Depends(get_db)):
    '''
    Returns a list of Risk Assessments.
    '''
    if originid:
        originid = base64.b64decode(originid).decode('utf-8')

    db_result = crud.read_risk_assessments(db, originid, starttime, endtime,
                                           published, preferred)

    if not db_result:
        raise HTTPException(
            status_code=404, detail='No risk assessments found.')

    return db_result


@router.get('/riskassessment/{oid}',
            response_model=RiskAssessmentSchema,
            response_model_exclude_none=True)
async def read_risk_assessment(oid: int, db: Session = Depends(get_db)):
    '''
    Returns the requested Risk Assessment.
    '''
    db_result = crud.read_risk_assessment(db, oid)

    if not db_result:
        raise HTTPException(
            status_code=404, detail='No risk assessments found.')

    return db_result
