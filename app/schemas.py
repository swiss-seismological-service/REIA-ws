# import enum
from datetime import datetime
from typing import Any, Optional, Type

from pydantic import BaseConfig, BaseModel, Field, create_model
from pydantic.utils import GetterDict
from reia.datamodel import EEarthquakeType, ELossCategory, EStatus
from sqlalchemy.inspection import inspect

BaseConfig.arbitrary_types_allowed = True
BaseConfig.orm_mode = True


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


class RiskAssessmentInfoSchema(RiskAssessmentSchema):
    event_text: str | None
    depth_value: float | None
    time_value: datetime | None
    magnitude_value: str | None
    latitude_value: float | None
    longitude_value: float | None
    evaluationmode: str | None
    dangerlevel: int | None


class RiskAssessmentDescriptionSchema(BaseModel):
    description: str | None = None


class RiskValueStatisticsSchema(BaseModel):
    mean: float
    percentile10: float
    percentile90: float
    losscategory: ELossCategory
    tag: str


class DamageValueStatisticsSchema(RiskValueStatisticsSchema):
    percentage: float
