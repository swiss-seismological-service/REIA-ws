from app import crud
from app.dependencies import get_db
from app.schemas import AggregatedLossSchema, EarthquakeInformationSchema
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

router = APIRouter(tags=['calculations', 'earthquakes'])


@router.get("/earthquakes", response_model=list[EarthquakeInformationSchema],
            response_model_exclude_none=True)
async def read_earthquakes(db: Session = Depends(get_db)):
    """
    Returns a list of Forecasts
    """
    db_result = crud.read_earthquakes(db)
    if not db_result:
        raise HTTPException(status_code=404, detail="No earthquakes found.")
    return db_result


@router.get("/calculations", response_model=list[AggregatedLossSchema],
            response_model_exclude_none=True)
async def read_calculations(db: Session = Depends(get_db)):
    """
    Returns a list of Forecasts
    """
    # db_result = crud.get_losses(db, losscalculation_id)
    # if not db_result:
    # raise HTTPException(status_code=404, detail="No earthquakes found.")
    return 1
