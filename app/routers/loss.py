from app.dependencies import get_db
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/", response_model=dict,
            response_model_exclude_none=True)
async def read_forecasts(db: Session = Depends(get_db)):
    """
    Returns a list of Forecasts
    """
    db_result = 1
    if not db_result:
        raise HTTPException(status_code=404, detail="No loss found.")
    return db_result
