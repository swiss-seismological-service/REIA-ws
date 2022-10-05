from config.config import get_settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def postgresql_url():
    settings = get_settings()
    SQLALCHEMY_DATABASE_URL = (
        f"postgresql://{settings.POSTGRES_USER}:"
        f"{settings.POSTGRES_PASSWORD}@"
        f"{settings.POSTGRES_SERVER}:"
        f"{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
    return SQLALCHEMY_DATABASE_URL


SQLALCHEMY_DATABASE_URL = postgresql_url()
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False,
                            autoflush=False,
                            bind=engine)
