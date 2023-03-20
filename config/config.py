from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    DB_USER: str
    DB_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: str
    DB_NAME: str
    ROOT_PATH: str

    SCENARIO_INFO_SERVER: str
    SCENARIO_INFO_PORT: str
    SCENARIO_INFO_USER: str
    SCENARIO_INFO_PASSWORD: str
    SCENARIO_INFO_DB: str

    EARTHQUAKE_INFO_SERVER: str
    EARTHQUAKE_INFO_PORT: str
    EARTHQUAKE_INFO_USER: str
    EARTHQUAKE_INFO_PASSWORD: str
    EARTHQUAKE_INFO_DB: str

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    Settings.SCENARIO_INFO = f'dbname={Settings().SCENARIO_INFO_DB} ' \
        f'user={Settings().SCENARIO_INFO_USER} ' \
        f'host={Settings().SCENARIO_INFO_SERVER} ' \
        f'password={Settings().SCENARIO_INFO_PASSWORD} ' \
        f'port={Settings().SCENARIO_INFO_PORT}'
    Settings.EARTHQUAKE_INFO = f'dbname={Settings().EARTHQUAKE_INFO_DB} ' \
        f'user={Settings().EARTHQUAKE_INFO_USER} ' \
        f'host={Settings().EARTHQUAKE_INFO_SERVER} ' \
        f'password={Settings().EARTHQUAKE_INFO_PASSWORD} ' \
        f'port={Settings().EARTHQUAKE_INFO_PORT}'
    return Settings()
