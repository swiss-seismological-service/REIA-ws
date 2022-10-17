from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import EarthquakeInformationSchema, LossCalculationSchema

router = APIRouter(tags=['calculations', 'earthquakes'])


@router.get("/earthquakes", response_model=list[EarthquakeInformationSchema],
            response_model_exclude_none=True)
async def read_earthquakes(starttime: datetime | None = None,
                           endtime: datetime | None = None,
                           db: Session = Depends(get_db)):
    """
    Returns a list of Forecasts
    """
    db_result = crud.read_earthquakes(db, starttime, endtime)
    if not db_result:
        raise HTTPException(status_code=404, detail="No earthquakes found.")
    return db_result


@router.get("/calculations", response_model=list[LossCalculationSchema],
            response_model_exclude_none=True)
async def read_calculations(starttime: datetime | None = None,
                            endtime: datetime | None = None,
                            db: Session = Depends(get_db)):
    """
    Returns a list of Forecasts
    """
    db_result = crud.read_calculations(db, starttime, endtime)
    if not db_result:
        raise HTTPException(status_code=404, detail="No calculations found.")
    return db_result
