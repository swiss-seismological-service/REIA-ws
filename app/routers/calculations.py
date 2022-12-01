from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import (DamageCalculationSchema, EarthquakeInformationSchema,
                         LossCalculationSchema)

router = APIRouter(tags=['calculations', 'earthquakes'])


@router.get('/earthquakes', response_model=list[EarthquakeInformationSchema],
            response_model_exclude_none=True)
async def read_earthquakes(starttime: datetime | None = None,
                           endtime: datetime | None = None,
                           db: Session = Depends(get_db)):
    '''
    Returns a list of Earthquakes
    '''
    db_result = crud.read_earthquakes(db, starttime, endtime)

    if not db_result:
        raise HTTPException(status_code=404, detail='No earthquakes found.')

    result = []

    shakemap_db_infos = crud.read_earthquake_information(
        tuple(e.originid for e in db_result))

    for earthquake_info in db_result:
        info = next(i for i in shakemap_db_infos if
                    i['origin_publicid'] == earthquake_info.originid)

        for k, v in info.items():
            setattr(earthquake_info, k, v)

        earthquake_schema = \
            EarthquakeInformationSchema.from_orm(earthquake_info)

        result.append(earthquake_schema)

    return result


@router.get('/calculations',
            response_model=list[LossCalculationSchema |
                                DamageCalculationSchema],
            response_model_exclude_none=True)
async def read_calculations(starttime: datetime | None = None,
                            endtime: datetime | None = None,
                            db: Session = Depends(get_db)):
    '''
    Returns a list of Calculations
    '''
    db_result = crud.read_calculations(db, starttime, endtime)
    if not db_result:
        raise HTTPException(status_code=404, detail='No calculations found.')
    return db_result


@router.get('/calculation/{id}',
            response_model=LossCalculationSchema | DamageCalculationSchema,
            response_model_exclude_none=True)
async def read_calculation(id: int,
                           db: Session = Depends(get_db)):
    '''
    Returns a list of Calculations
    '''
    db_result = crud.read_calculation(db, id)
    if not db_result:
        raise HTTPException(status_code=404, detail='No calculation found.')
    return db_result
