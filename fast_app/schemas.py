from typing import List, Optional
from enum import Enum
from pydantic import BaseModel
from datetime import datetime


class Cantons(str, Enum):
    Aargau = 'AG'
    Basel = 'BS'
    Bern = 'BE'
    Fribourg = 'FR'
    Genf = 'GE'
    Glarus = 'GL'
    Graubunden = 'GR'
    Jura = 'JU'
    Luzern = 'LU'
    Neuenburg = 'NE'
    Schaffhausen = 'SH'
    Schwyz = 'SZ'
    Solothurn = 'SO'
    Wallis = 'VS'
    Zurich = 'ZH'


class LossesBase(BaseModel):
    canton: Cantons
    loss_value: Optional[float]


class EarthquakeBase(BaseModel):
    danger_level: int
    city: str
    canton: Cantons
    magnitude: float
    datetime: datetime
    depth: float
    intensity: str
    meta: str
    longitude: float
    latitude: float
    human_losses: Optional[float]


class Losses(LossesBase):
    id: int
    earthquake_id: int

    class Config:
        orm_mode = True


class Earthquake(EarthquakeBase):
    id: int
    cantonal_losses: List[Losses] = []

    class Config:
        orm_mode = True
