from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.config import get_settings


def postgresql_url():
    settings = get_settings()
    SQLALCHEMY_DATABASE_URL = (
        f"postgresql://{settings.DB_USER}:"
        f"{settings.DB_PASSWORD}@"
        f"{settings.POSTGRES_HOST}:"
        f"{settings.POSTGRES_PORT}/{settings.DB_NAME}")
    return SQLALCHEMY_DATABASE_URL


SQLALCHEMY_DATABASE_URL = postgresql_url()
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False,
                            autoflush=False,
                            bind=engine)
