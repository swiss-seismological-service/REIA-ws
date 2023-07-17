import enum
from datetime import datetime
from typing import Any, Generic, List, Optional, Type, TypeVar

from pydantic import BaseConfig, BaseModel, Field, create_model
from pydantic.generics import GenericModel
from pydantic.utils import GetterDict
from reia.datamodel import EEarthquakeType, EStatus
from sqlalchemy.inspection import inspect

from config import Settings

BaseConfig.arbitrary_types_allowed = True
BaseConfig.orm_mode = True

M = TypeVar('M')


class PaginatedResponse(GenericModel, Generic[M]):
    count: int = Field(description='Number of items returned in the response')
    items: List[M] = Field(
        description='List of items returned in the '
        'response following given criteria')


class ValueGetter(GetterDict):
    def get(self, key: str, default: Any) -> Any:
        # if the key-col mapping is 1:1 just return the value
        if hasattr(self._obj, key):
            return getattr(self._obj, key, default)

        # get this SQLAlchemy objects' column names.
        inspected = inspect(type(self._obj))
        cols = [c.name for c in inspected.columns]
        cols += inspected.relationships.keys()

        # else it's probably a sub value
        # get all column names which are present for this key
        elem = [k for k in cols if k.startswith(f'{key}_')]
        if elem:
            # create a dict for the sub value
            return_dict = {}
            for k in elem:
                return_dict[k.partition(
                    '_')[-1]] = getattr(self._obj, k, default)
            return return_dict
        else:
            return default


def creationinfo_factory() -> Type[BaseModel]:
    _func_map = dict([
        ('author', (Optional[str], None)),
        ('agencyid', (Optional[str], None)),
        ('creationtime', (Optional[datetime], None)),
        ('version', (Optional[str], None)),
        ('copyrightowner', (Optional[str], None)),
        ('licence', (Optional[str], None)),
    ])
    retval = create_model(
        'CreationInfo',
        __config__=BaseConfig,
        **_func_map)
    return retval


class ReturnFormats(str, enum.Enum):
    JSON = 'json'
    CSV = 'csv'


class CreationInfo(creationinfo_factory()):
    pass


class AggregationTagSchema(BaseModel):
    type: str
    name: str


class CalculationBranchSchema(BaseModel):
    weight: float
    config: dict
    status: EStatus
    type: str = Field(..., alias='_type')


class LossCalculationBranchSchema(CalculationBranchSchema):
    _calculation_oid: int


class DamageCalculationBranchSchema(CalculationBranchSchema):
    _calculation_oid: int


class CalculationSchema(BaseModel):
    oid: int = Field(..., alias='_oid')
    aggregateby: list[str]
    creationinfo: CreationInfo
    status: EStatus
    description: Optional[str]
    type: str = Field(..., alias='_type')

    class Config:
        getter_dict = ValueGetter


class LossCalculationSchema(CalculationSchema):
    losscalculationbranches: list[LossCalculationBranchSchema]


class DamageCalculationSchema(CalculationSchema):
    damagecalculationbranches: list[DamageCalculationBranchSchema]


class RiskAssessmentSchema(BaseModel):
    oid: int = Field(..., alias='_oid')
    originid: str

    type: EEarthquakeType

    losscalculation: CalculationSchema | None
    damagecalculation: CalculationSchema | None

    creationinfo: CreationInfo
    preferred: bool
    published: bool

    class Config:
        getter_dict = ValueGetter


class RiskValue(BaseModel):
    category: Settings.RiskCategory
    tag: list[str]


class LossValueStatisticsSchema(RiskValue):
    loss_mean: float
    loss_pc10: float
    loss_pc90: float


class DamageValueStatisticsReportSchema(RiskValue):
    damage_mean: float
    damage_pc10: float
    damage_pc90: float
    damage_percentage: float


class DamageValueStatisticsSchema(RiskValue):
    dg1_mean: float
    dg1_pc10: float
    dg1_pc90: float

    dg2_mean: float
    dg2_pc10: float
    dg2_pc90: float

    dg3_mean: float
    dg3_pc10: float
    dg3_pc90: float

    dg4_mean: float
    dg4_pc10: float
    dg4_pc90: float

    dg5_mean: float
    dg5_pc10: float
    dg5_pc90: float

    buildings: float
