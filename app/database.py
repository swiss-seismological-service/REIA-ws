import enum

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.crud import get_aggregation_types
from app.schemas import Aggregation
from config import get_settings

engine = create_engine(get_settings().SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False,
                            autoflush=False,
                            bind=engine)


db = SessionLocal()
Aggregation.types = enum.Enum(
    'AggregationTypes', get_aggregation_types(db))
db.close()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
