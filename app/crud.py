
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from reia.datamodel import (AggregationTag, Asset, Calculation,  # noqa
                            DamageCalculationBranch, DamageValue,
                            EarthquakeInformation, ELossCategory,
                            ExposureModel, LossCalculation, LossValue,
                            asset_aggregationtag, riskvalue_aggregationtag)
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, with_polymorphic


def losscategory_filter(f, model):
    return model.losscategory == f if f else True


def tagname_filter(f, model): return model.aggregationtags.any(
    AggregationTag.name == f) if f else True


def tagname_like_filter(f, model): return model.aggregationtags.any(
    AggregationTag.name.like(f)) if f else True


def tagtype_filter(t, model): return model.aggregationtags.any(
    AggregationTag.type == t) if t else True


def calculationid_filter(f, model):
    return model._calculation_oid == f if f else True


def aggregation_type_subquery(aggregation_type):
    return select(AggregationTag).where(
        AggregationTag.type == aggregation_type).subquery()


def read_country_loss(db: Session, calculation_id, loss_category):

    filter = calculationid_filter(calculation_id, LossValue)
    filter &= losscategory_filter(loss_category, LossValue)

    type_sub = aggregation_type_subquery('Canton')

    stmt = select(func.sum(LossValue.loss_value).label('loss_value'),
                  (func.sum(LossValue.weight) /
                   func.count(LossValue._oid)).label('weight')
                  ) \
        .select_from(LossValue) \
        .where(filter) \
        .join(riskvalue_aggregationtag) \
        .join(type_sub) \
        .group_by(
            ((LossValue._losscalculationbranch_oid *
              (10 ** 6))+LossValue.eventid).label('event'))

    return pd.read_sql(stmt, db.get_bind())


def read_total_buildings(db: Session,
                         calculation_id: int,
                         aggregation_type: str,
                         filter_tag: str | None = None,
                         filter_like_tag: str | None = None) -> int:

    type_sub = aggregation_type_subquery(aggregation_type)

    exp_sub = select(ExposureModel._oid) \
        .join(DamageCalculationBranch) \
        .join(Calculation) \
        .where(Calculation._oid == calculation_id) \
        .limit(1).subquery()

    filter = tagname_like_filter(filter_like_tag, Asset)
    filter &= tagname_filter(filter_tag, Asset)

    stmt = select(func.sum(Asset.buildingcount).label('buildingcount'),
                  type_sub.c.name.label(aggregation_type))\
        .select_from(Asset)\
        .join(asset_aggregationtag) \
        .join(type_sub) \
        .join(exp_sub, exp_sub.c._oid == Asset._exposuremodel_oid) \
        .where(filter) \
        .group_by(type_sub.c.name)

    return pd.read_sql(stmt, db.get_bind())


def read_aggregated_loss(db: Session,
                         calculation_id: int,
                         aggregation_type: str,
                         loss_category: ELossCategory,
                         filter_tag: str | None = None,
                         filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    stmt = f"SELECT lr.loss_value, tags_of_type.name AS \"{aggregation_type}\", lr.weight FROM  (SELECT * FROM loss_riskvalue as lr  WHERE lr._calculation_oid = {calculation_id} AND lr.losscategory = '{loss_category.value.upper()}'  AND lr._type = 'LOSS' ) AS lr  JOIN loss_assoc_riskvalue_aggregationtag AS assoc  ON lr._oid = assoc.riskvalue  AND lr._calculation_oid = assoc._calculation_oid  AND lr.losscategory = assoc.losscategory JOIN ( SELECT  loss_aggregationtag._oid AS _oid,  loss_aggregationtag.type AS type,  loss_aggregationtag.name AS name  FROM  loss_aggregationtag  WHERE  loss_aggregationtag.type = '{aggregation_type}' AND loss_aggregationtag.name LIKE '{filter_like_tag}%') AS tags_of_type  ON tags_of_type._oid = assoc.aggregationtag  WHERE  tags_of_type.name LIKE '{filter_like_tag}%' AND lr.losscategory = '{loss_category.value.upper()}'  AND lr._calculation_oid = {calculation_id} AND lr._type = 'LOSS'"  # noqa

    return pd.read_sql(text(stmt), db.get_bind())


def read_aggregated_damage(db: Session,
                           calculation_id: int,
                           aggregation_type: str,
                           loss_category: ELossCategory,
                           filter_tag: str | None = None,
                           filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    # filter = calculationid_filter(calculation_id, DamageValue)
    # filter &= losscategory_filter(loss_category, DamageValue)
    # filter &= tagname_filter(filter_tag, DamageValue)
    # filter &= tagname_like_filter(
    #     filter_like_tag, DamageValue)

    # type_sub = aggregation_type_subquery(aggregation_type)

    # damagevalues = DamageValue.dg2_value + \
    #     DamageValue.dg3_value + \
    #     DamageValue.dg4_value + \
    #     DamageValue.dg5_value \

    # stmt = select(damagevalues.label('damage_value'),
    #               type_sub.c.name.label(aggregation_type),
    #               DamageValue.weight) \
    #     .select_from(DamageValue)\
    #     .where(filter) \
    #     .join(riskvalue_aggregationtag) \
    #     .join(type_sub)

    stmt = f"SELECT (lr.dg2_value + lr.dg3_value + lr.dg4_value + lr.dg5_value) AS damage_value, lr.weight, tags_of_type.ag_name AS \"{aggregation_type}\" FROM  (SELECT * FROM loss_riskvalue as lr  WHERE lr._calculation_oid = {calculation_id} AND lr.losscategory = '{loss_category.value.upper()}'  AND lr._type = 'DAMAGE' ) AS lr  JOIN loss_assoc_riskvalue_aggregationtag AS assoc  ON (lr._oid = assoc.riskvalue  AND lr._calculation_oid = assoc._calculation_oid  AND lr.losscategory = assoc.losscategory) JOIN ( SELECT  loss_aggregationtag._oid AS _oid,  loss_aggregationtag.type AS type,  loss_aggregationtag.name AS ag_name  FROM  loss_aggregationtag  WHERE  loss_aggregationtag.type = '{aggregation_type}' AND loss_aggregationtag.name LIKE '{filter_like_tag}%' ) AS tags_of_type  ON tags_of_type._oid = assoc.aggregationtag  WHERE  tags_of_type.ag_name LIKE '{filter_like_tag}%' AND lr.losscategory = '{loss_category.value.upper()}'  AND lr._calculation_oid = {calculation_id} AND lr._type = 'DAMAGE'"  # noqa

    return pd.read_sql(text(stmt), db.get_bind())


def read_earthquakes(db: Session, starttime: datetime | None,
                     endtime: datetime | None) -> list[EarthquakeInformation]:
    stmt = select(EarthquakeInformation)
    if starttime:
        stmt = stmt.filter(EarthquakeInformation.time >= starttime)
    if endtime:
        stmt = stmt.filter(EarthquakeInformation.time <= endtime)
    return db.execute(stmt).unique().scalars().all()


def read_earthquake(db: Session, originid: str) -> list[EarthquakeInformation]:
    stmt = select(EarthquakeInformation).where(
        EarthquakeInformation.originid == originid)
    return db.execute(stmt).unique().scalar()


def read_calculations(db: Session, starttime: datetime | None,
                      endtime: datetime | None) -> list[Calculation]:
    all_calculations = with_polymorphic(Calculation, [LossCalculation])
    stmt = select(all_calculations)
    if starttime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime >= starttime)
    if endtime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime <= endtime)
    return db.execute(stmt).unique().scalars().all()


def read_calculation(db: Session, id: int) -> list[Calculation]:
    all_calculations = with_polymorphic(Calculation, [LossCalculation])
    stmt = select(all_calculations).where(Calculation._oid == id)
    return db.execute(stmt).unique().scalar()


def read_mean_losses(db: Session,
                     calculation_id,
                     aggregation_type,
                     loss_category,
                     filter_tag: str | None = None,
                     filter_like_tag: str | None = None):

    filter = calculationid_filter(calculation_id, LossValue)
    filter &= losscategory_filter(loss_category, LossValue)
    filter &= tagname_filter(filter_tag, LossValue)
    filter &= tagname_like_filter(filter_like_tag, LossValue)

    type_sub = aggregation_type_subquery(aggregation_type)

    stmt = select(func.sum(LossValue.loss_value*LossValue.weight),
                  type_sub.c.name.label(aggregation_type))\
        .select_from(LossValue)\
        .join(riskvalue_aggregationtag) \
        .join(type_sub) \
        .where(filter) \
        .group_by(type_sub.c.name)

    return pd.read_sql(stmt, db.get_bind())


def read_earthquakes_information(originids: tuple[str]) -> list[dict]:

    from config.config import get_settings
    settings = get_settings()
    conn = psycopg2.connect(settings.SHAKEMAP_STRING)

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT"
                   " origin_publicid,"
                   " event_text,"
                   " description_de,"
                   " description_en,"
                   " description_fr,"
                   " description_it,"
                   " magnitude_value,"
                   " time_value,"
                   " depth_value,"
                   " latitude_value, longitude_value"
                   " FROM public.sm_origin"
                   f" WHERE origin_publicid IN {originids};")

    db_earthquakes = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{k: v for k, v in d.items()} for d in db_earthquakes]


def read_earthquake_information(originid: str) -> dict:
    from config.config import get_settings
    settings = get_settings()
    conn = psycopg2.connect(settings.SHAKEMAP_STRING)

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT"
                   " origin_publicid,"
                   " event_text,"
                   " description_de,"
                   " description_en,"
                   " description_fr,"
                   " description_it,"
                   " magnitude_value,"
                   " time_value,"
                   " depth_value,"
                   " latitude_value, longitude_value"
                   " FROM public.sm_origin"
                   f" WHERE origin_publicid = '{originid}';")

    db_earthquake = cursor.fetchone()
    cursor.close()
    conn.close()
    return {k: v for k, v in db_earthquake.items()}
