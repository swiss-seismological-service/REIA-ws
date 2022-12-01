from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_SERVER: str
    POSTGRES_PORT: str
    POSTGRES_DB: str
    SHAKEMAP_SERVER: str
    SHAKEMAP_PORT: str
    SHAKEMAP_USER: str
    SHAKEMAP_PASSWORD: str
    SHAKEMAP_DB: str

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    Settings.SHAKEMAP_STRING = f'dbname={Settings().SHAKEMAP_DB} ' \
        f'user={Settings().SHAKEMAP_USER} ' \
        f'host={Settings().SHAKEMAP_SERVER} ' \
        f'password={Settings().SHAKEMAP_PASSWORD} ' \
        f'port={Settings().SHAKEMAP_PORT}'
    return Settings()
