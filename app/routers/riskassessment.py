import base64
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import RiskAssessmentSchema

router = APIRouter(prefix='/riskassessment/info', tags=['riskassessments'])


@router.get('', response_model=list[RiskAssessmentSchema],
            response_model_exclude_none=True)
async def read_risk_assessments(originid: str | None = None,
                                starttime: datetime | None = None,
                                endtime: datetime | None = None,
                                published: bool | None = None,
                                preferred: bool | None = None,
                                db: Session = Depends(get_db)):
    '''
    Returns a list of Earthquakes
    '''
    if originid:
        originid = base64.b64decode(originid).decode('utf-8')

    db_result = crud.read_risk_assessments(db, originid, starttime, endtime,
                                           published, preferred)

    if not db_result:
        raise HTTPException(status_code=404, detail='No earthquakes found.')

    result = []

    shakemap_db_infos = crud.read_earthquakes_information(
        tuple(e.originid for e in db_result))

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
    Returns the requested earthquake
    '''
    db_result = crud.read_risk_assessment(db, oid)

    if not db_result:
        raise HTTPException(status_code=404, detail='No earthquakes found.')

    shakemap_db_infos = crud.read_earthquakes_information(
        (db_result.originid,))

    if len(shakemap_db_infos) > 0:
        for k, v in shakemap_db_infos[0].items():
            setattr(db_result, k, v)

    db_result = RiskAssessmentSchema.from_orm(db_result)

    return db_result


@router.get('/{originid}/dangerlevel')
async def read_danger_level(originid: str, db: Session = Depends(get_db)):
    originid = base64.b64decode(originid).decode('utf-8')
    db_result = crud.read_danger_levels((originid,))
    return db_result


@router.get('/{originid}/info')
async def read_info(originid: str, db: Session = Depends(get_db)):
    originid = base64.b64decode(originid).decode('utf-8')
    db_result = crud.read_ria_parameters((originid,))
    if not db_result:
        raise HTTPException(
            status_code=404, detail='No info found for originid.')
    return db_result[0]


@router.get('/{originid}/region')
async def read_region_name(originid: str, db: Session = Depends(get_db)):
    originid = base64.b64decode(originid).decode('utf-8')
    db_result = crud.read_region_name([originid])
    if db_result:
        return {'region_name': db_result[0]['region_name']}
    return {'region_name': None}


@router.get('/{originid}/description/{lang}')
async def read_description(originid: str,
                           lang: str,
                           db: Session = Depends(get_db)):
    originid = base64.b64decode(originid).decode('utf-8')
    db_result = crud.read_ria_text(originid, lang)
    if db_result:
        return {'description': db_result['ria_text']}
    return {'description': None}
