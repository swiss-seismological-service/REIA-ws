from datetime import datetime

import psycopg2
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

    # conn = psycopg2.connect(
    #     'dbname=test user=postgres host=localhost password=password port=5432')
    # cursor = conn.cursor()
    # cursor.execute("CREATE TABLE test (id serial PRIMARY KEY, "
    #                "num integer, data varchar);")
    # cursor.execute(
    #     "INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
    # conn.commit()
    # cursor.execute("SELECT * FROM test;")
    # db_earthquake = cursor.fetchone()
    # cursor.close()
    # conn.close()
    # print(db_earthquake)

    return db_result


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
