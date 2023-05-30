import base64
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import RiskAssessmentSchema

router = APIRouter(prefix='/riskassessment', tags=['riskassessments'])


@router.get('', response_model=list[RiskAssessmentSchema],
            response_model_exclude_none=True)
async def read_risk_assessments(originid: str | None = None,
                                starttime: datetime | None = None,
                                endtime: datetime | None = None,
                                published: bool | None = None,
                                preferred: bool | None = None,
                                db: Session = Depends(get_db)):
    '''
    Returns a list of RiskAssessments.
    '''
    if originid:
        originid = base64.b64decode(originid).decode('utf-8')

    db_result = crud.read_risk_assessments(db, originid, starttime, endtime,
                                           published, preferred)

    if not db_result:
        raise HTTPException(status_code=404, detail='No earthquakes found.')

    result = []

    shakemap_db_infos = crud.read_earthquakes_information(
        tuple(str(e.originid) for e in db_result))

    for ra in db_result:
        info = next((i for i in shakemap_db_infos if
                    i['origin_publicid'] == ra.originid), {})

        for k, v in info.items():
            setattr(ra, k, v)

        earthquake_schema = \
            RiskAssessmentSchema.from_orm(ra)

        result.append(earthquake_schema)

    return result


@router.get('/{oid}',
            response_model=RiskAssessmentSchema,
            response_model_exclude_none=True)
async def read_risk_assessment(oid: int, db: Session = Depends(get_db)):
    '''
    Returns the requested RiskAssessment.
    '''
    db_result = crud.read_risk_assessment(db, oid)

    if not db_result:
        raise HTTPException(status_code=404, detail='No earthquakes found.')

    shakemap_db_infos = crud.read_earthquakes_information(
        (str(db_result.originid),))

    if len(shakemap_db_infos) > 0:
        for k, v in shakemap_db_infos[0].items():
            setattr(db_result, k, v)

    db_result = RiskAssessmentSchema.from_orm(db_result)

    return db_result
