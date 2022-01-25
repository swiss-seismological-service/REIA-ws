from sqlalchemy.orm import Session

from . import models, schemas


def get_earthquake(db: Session, earthquake_id: int):
    return db.query(models.Earthquake).filter(
        models.Earthquake.id == earthquake_id).first()


def get_earthquakes(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Earthquake).offset(skip).limit(limit).all()


def create_earthquake(db: Session, earthquake: schemas.EarthquakeBase):

    db_earthquake = models.Earthquake(**earthquake.dict())
    db.add(db_earthquake)
    db.commit()
    db.refresh(db_earthquake)
    return db_earthquake


def create_cantonal_losses(
        db: Session, losses: schemas.LossesBase, earthquake_id: int):

    db_loss = models.Losses(
        **losses.dict(), earthquake_id=earthquake_id)
    db.add(db_loss)
    db.commit()
    db.refresh(db_loss)
    return db_loss
