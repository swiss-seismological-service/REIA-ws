import enum
from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    POSTGRES_HOST: str
    POSTGRES_PORT: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    ROOT_PATH: str

    ALLOW_ORIGINS: list
    ALLOW_ORIGIN_REGEX: str

    class RiskCategory(str, enum.Enum):
        CONTENTS = 'contents'
        BUSINESS_INTERRUPTION = 'displaced'
        NONSTRUCTURAL = 'injured'
        OCCUPANTS = 'fatalities'
        STRUCTURAL = 'structural'

    @property
    def EARTHQUAKE_INFO(self) -> str:
        return f'dbname={self.EARTHQUAKE_INFO_DB} ' \
            f'user={self.EARTHQUAKE_INFO_USER} ' \
            f'host={self.EARTHQUAKE_INFO_SERVER} ' \
            f'password={self.EARTHQUAKE_INFO_PASSWORD} ' \
            f'port={self.EARTHQUAKE_INFO_PORT}'

    @property
    def SCENARIO_INFO(self) -> str:
        return f'dbname={self.SCENARIO_INFO_DB} ' \
            f'user={self.SCENARIO_INFO_USER} ' \
            f'host={self.SCENARIO_INFO_SERVER} ' \
            f'password={self.SCENARIO_INFO_PASSWORD} ' \
            f'port={self.SCENARIO_INFO_PORT}'

    @property
    def SQLALCHEMY_DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:" \
            f"{self.POSTGRES_PASSWORD}@" \
            f"{self.POSTGRES_HOST}:" \
            f"{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    return Settings()
