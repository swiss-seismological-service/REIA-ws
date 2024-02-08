import enum
from datetime import datetime
from typing import Generic, List, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, computed_field
from reia.datamodel import EEarthquakeType, EStatus

from config import Settings


class Model(BaseModel):
    model_config = ConfigDict(extra='allow',
                              arbitrary_types_allowed=True,
                              from_attribute=True)


M = TypeVar('M')


class PaginatedResponse(Model, Generic[M]):
    count: int = Field(description='Number of items returned in the response')
    items: List[M] = Field(
        description='List of items returned in the '
        'response following given criteria')


class CreationInfoSchema(Model):
    author: str | None = None
    agencyid: str | None = None
    creationtime: datetime | None = None
    version: str | None = None
    copyrightowner: str | None = None
    licence: str | None = None


def creationinfo_factory(obj: Model) -> CreationInfoSchema:
    return CreationInfoSchema(
        author=obj.creationinfo_author,
        agencyid=obj.creationinfo_agencyid,
        creationtime=obj.creationinfo_creationtime,
        version=obj.creationinfo_version,
        copyrightowner=obj.creationinfo_copyrightowner,
        licence=obj.creationinfo_licence)


class CreationInfoMixin(Model):
    creationinfo_author: str | None = Field(default=None, exclude=True)
    creationinfo_agencyid: str | None = Field(default=None, exclude=True)
    creationinfo_creationtime: datetime = Field(default=None, exclude=True)
    creationinfo_version: str | None = Field(default=None, exclude=True)
    creationinfo_copyrightowner: str | None = Field(default=None, exclude=True)
    creationinfo_licence: str | None = Field(default=None, exclude=True)

    @computed_field
    @property
    def creationinfo(self) -> CreationInfoSchema:
        return creationinfo_factory(self)


class ReturnFormats(str, enum.Enum):
    JSON = 'json'
    CSV = 'csv'


class AggregationTagSchema(Model):
    type: str
    name: str


class CalculationBranchSchema(Model):
    weight: float
    config: dict
    status: EStatus
    type: str = Field(..., alias='_type')


class LossCalculationBranchSchema(CalculationBranchSchema):
    _calculation_oid: int


class DamageCalculationBranchSchema(CalculationBranchSchema):
    _calculation_oid: int


class CalculationSchema(CreationInfoMixin):
    oid: int = Field(..., alias='_oid')
    aggregateby: list[str]
    status: EStatus
    description: Optional[str]
    type: str = Field(..., alias='_type')


class LossCalculationSchema(CalculationSchema):
    losscalculationbranches: list[LossCalculationBranchSchema]


class DamageCalculationSchema(CalculationSchema):
    damagecalculationbranches: list[DamageCalculationBranchSchema]


class RiskAssessmentSchema(CreationInfoMixin):
    oid: UUID = Field(..., alias='_oid')
    originid: str

    type: EEarthquakeType

    losscalculation: CalculationSchema | None
    damagecalculation: CalculationSchema | None

    preferred: bool
    published: bool

    # @root_validator(pre=True)
    # @model_validator(mode='before')
    # def parse_obj(cls, values):
    #     print(values.__pydantic_extra__)
    # print(cls.__pydantic_extra__)
    # print(values.__pydantic_extra__)
    #     def get(self, key: str, default: Any) -> Any:
    # if the key-col mapping is 1:1 just return the value
    # if hasattr(cls._obj, key):
    #     return getattr(self._obj, key, default)

    # # get this SQLAlchemy objects' column names.
    # inspected = inspect(type(self._obj))
    # cols = [c.name for c in inspected.columns]
    # cols += inspected.relationships.keys()

    # # else it's probably a sub value
    # # get all column names which are present for this key
    # elem = [k for k in cols if k.startswith(f'{key}_')]
    # if elem:
    #     # create a dict for the sub value
    #     return_dict = {}
    #     for k in elem:
    #         return_dict[k.partition(
    #             '_')[-1]] = getattr(self._obj, k, default)
    #     return return_dict
    # else:
    #     return default
    # return values


class RiskValue(Model):
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
