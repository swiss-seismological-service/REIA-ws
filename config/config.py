from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_SERVER: str
    POSTGRES_PORT: str
    POSTGRES_DB: str

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    return Settings()
