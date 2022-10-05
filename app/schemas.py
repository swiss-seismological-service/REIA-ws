from typing import Any, List, Optional, Type

from esloss.datamodel.lossvalues import ELossCategory
from pydantic import BaseConfig, BaseModel, create_model
from pydantic.utils import GetterDict
from sqlalchemy.inspection import inspect

BaseConfig.arbitrary_types_allowed = True
BaseConfig.orm_mode = True


def real_value_factory(quantity_type: type) -> Type[BaseModel]:
    _func_map = dict([
        ('value', (Optional[quantity_type], None)),
        ('uncertainty', (Optional[float], None)),
        ('loweruncertainty', (Optional[float], None)),
        ('upperuncertainty', (Optional[float], None)),
        ('confidencelevel', (Optional[float], None)),
        ('pdfvariable', (Optional[List[float]], None)),
        ('pdfprobability', (Optional[List[float]], None)),
        ('pdfbinedges', (Optional[List[float]], None))
    ])

    retval = create_model(
        f'Real{quantity_type.__name__}',
        __config__=BaseConfig,
        **_func_map)
    return retval


class ValueGetter(GetterDict):
    def get(self, key: str, default: Any) -> Any:
        # get SQLAlchemy's column names.
        cols = self._obj.__table__.columns.keys()
        cols += inspect(type(self._obj)).relationships.keys()

        # if the key-col mapping is 1:1 just return the value
        if key in cols:
            return getattr(self._obj, key, default)

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


class RealFloatValue(real_value_factory(float)):
    pass


class AggregationTagSchema(BaseModel):
    type: str
    name: str
    _assetcollection_oid: int


class AggregatedLossSchema(BaseModel):
    loss: RealFloatValue
    eventid: int
    losscategory: ELossCategory
    _losscalculation_oid: int
    _type: str
    aggregationtags: list[AggregationTagSchema]

    class Config:
        getter_dict = ValueGetter
