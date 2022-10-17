from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import AggregatedLossSchema

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{losscalculation_id}", response_model=list[AggregatedLossSchema],
            response_model_exclude_none=True)
async def get_losses(losscalculation_id: int, db: Session = Depends(get_db)):
    """
    Returns a list of Forecasts
    """
    db_result = crud.read_losses(db, losscalculation_id)
    if not db_result:
        raise HTTPException(status_code=404, detail="No loss found.")
    return db_result
