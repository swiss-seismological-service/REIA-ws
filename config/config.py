from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    DB_USER: str
    DB_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: str
    DB_NAME: str
    SHAKEMAP_SERVER: str
    SHAKEMAP_PORT: str
    SHAKEMAP_USER: str
    SHAKEMAP_PASSWORD: str
    SHAKEMAP_DB: str
    ROOT_PATH: str

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
