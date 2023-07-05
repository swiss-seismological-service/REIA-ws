import base64
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app import crud
from app.database import get_db, paginate
from app.schemas import PaginatedResponse, RiskAssessmentSchema

router = APIRouter(prefix='/riskassessment', tags=['riskassessments'])


@router.get('', response_model=PaginatedResponse[RiskAssessmentSchema],
            response_model_exclude_none=True)
async def read_risk_assessments(originid: str | None = None,
                                starttime: datetime | None = None,
                                endtime: datetime | None = None,
                                published: bool | None = None,
                                preferred: bool | None = None,
                                limit: int = Query(50, ge=0),
                                offset: int = Query(0, ge=0),
                                db: Session = Depends(get_db)):
    '''
    Returns a list of RiskAssessments.
    '''
    if originid:
        originid = base64.b64decode(originid).decode('utf-8')

    db_result = crud.read_risk_assessments(db, originid, starttime, endtime,
                                           published, preferred)

    return paginate(db, db_result, limit, offset)


@router.get('/{oid}',
            response_model=RiskAssessmentSchema,
            response_model_exclude_none=True)
async def read_risk_assessment(oid: int, db: Session = Depends(get_db)):
    '''
    Returns the requested RiskAssessment.
    '''
    db_result = crud.read_risk_assessment(db, oid)

    if not db_result:
        raise HTTPException(status_code=404, detail='No riskassessment found.')

    return db_result
