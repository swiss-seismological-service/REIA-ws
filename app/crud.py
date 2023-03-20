
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from reia.datamodel import (AggregationTag, Asset, Calculation,  # noqa
                            DamageCalculationBranch, DamageValue,
                            ECalculationType, ELossCategory, ExposureModel,
                            LossCalculation, LossValue, RiskAssessment,
                            asset_aggregationtag, riskvalue_aggregationtag)
from sqlalchemy import and_, func, select
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

    risk_sub = select(LossValue).where(and_(
        LossValue._calculation_oid == calculation_id,
        LossValue.losscategory == loss_category,
        LossValue._type == ECalculationType.LOSS
    )).subquery()

    agg_sub = select(AggregationTag).where(
        AggregationTag.type == 'Canton'
    ).subquery()

    stmt = select(func.sum(risk_sub.c.loss_value).label('loss_value'),
                  (func.sum(risk_sub.c.weight) /
                   func.count(risk_sub.c._oid)).label('weight')
                  ) \
        .select_from(risk_sub) \
        .join(riskvalue_aggregationtag, and_(
            riskvalue_aggregationtag.c.riskvalue == risk_sub.c._oid,
            riskvalue_aggregationtag.c.losscategory == risk_sub.c.losscategory,
            riskvalue_aggregationtag.c._calculation_oid ==
            risk_sub.c._calculation_oid
        )) \
        .join(agg_sub, and_(
            agg_sub.c._oid == riskvalue_aggregationtag.c.aggregationtag,
            agg_sub.c.type == riskvalue_aggregationtag.c.aggregationtype
        )) \
        .where(and_(
            risk_sub.c.losscategory == loss_category,
            risk_sub.c._calculation_oid == calculation_id,
            risk_sub.c._type == ECalculationType.LOSS
        )) \
        .group_by(
            ((risk_sub.c._losscalculationbranch_oid *
              (10 ** 9))+risk_sub.c.eventid).label('event'))

    return pd.read_sql(stmt, db.get_bind())


def read_country_damage(db: Session, calculation_id, loss_category):

    damage_sub = select(DamageValue).where(and_(
        DamageValue._calculation_oid == calculation_id,
        DamageValue.losscategory == loss_category,
        DamageValue._type == ECalculationType.DAMAGE
    )).subquery()

    agg_sub = select(AggregationTag).where(
        AggregationTag.type == 'Canton'
    ).subquery()
    stmt = select(func.sum(damage_sub.c.dg2_value +
                           damage_sub.c.dg3_value +
                           damage_sub.c.dg4_value +
                           damage_sub.c.dg5_value).label('damage_value'),
                  (func.sum(damage_sub.c.weight) /
                   func.count(damage_sub.c._oid)).label('weight')
                  ) \
        .select_from(damage_sub) \
        .join(riskvalue_aggregationtag, and_(
            riskvalue_aggregationtag.c.riskvalue == damage_sub.c._oid,
            riskvalue_aggregationtag.c.losscategory ==
            damage_sub.c.losscategory,
            riskvalue_aggregationtag.c._calculation_oid ==
            damage_sub.c._calculation_oid
        )) \
        .join(agg_sub, and_(
            agg_sub.c._oid == riskvalue_aggregationtag.c.aggregationtag,
            agg_sub.c.type == riskvalue_aggregationtag.c.aggregationtype
        )) \
        .where(and_(
            damage_sub.c.losscategory == loss_category,
            damage_sub.c._calculation_oid == calculation_id,
            damage_sub.c._type == ECalculationType.DAMAGE
        )) \
        .group_by(
            ((damage_sub.c._damagecalculationbranch_oid *
              (10 ** 9))+damage_sub.c.eventid).label('event'))

    return pd.read_sql(stmt, db.get_bind())


def read_total_buildings_country(db: Session, calculation_id: int) -> int:
    exp_sub = select(ExposureModel._oid) \
        .join(DamageCalculationBranch) \
        .join(Calculation) \
        .where(Calculation._oid == calculation_id) \
        .limit(1).scalar_subquery()

    stmt = select(func.sum(Asset.buildingcount).label('buildingcount')) \
        .select_from(Asset) \
        .where(Asset._exposuremodel_oid == exp_sub)

    return db.execute(stmt).scalar()


def read_total_buildings(db: Session,
                         calculation_id: int,
                         aggregation_type: str,
                         filter_tag: str | None = None,
                         filter_like_tag: str | None = None) -> pd.DataFrame:

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

    risk_sub = select(LossValue).where(and_(
        LossValue._calculation_oid == calculation_id,
        LossValue.losscategory == loss_category,
        LossValue._type == ECalculationType.LOSS
    )).subquery()

    agg_sub = select(AggregationTag).where(and_(
        AggregationTag.type == aggregation_type,
        AggregationTag.name.like(filter_like_tag) if filter_like_tag else True,
        (AggregationTag.name == filter_tag) if filter_tag else True
    )).subquery()

    stmt = select(risk_sub.c.loss_value,
                  risk_sub.c.weight,
                  agg_sub.c.name.label(aggregation_type)) \
        .select_from(risk_sub) \
        .join(riskvalue_aggregationtag, and_(
            riskvalue_aggregationtag.c.riskvalue == risk_sub.c._oid,
            riskvalue_aggregationtag.c.losscategory == risk_sub.c.losscategory,
            riskvalue_aggregationtag.c._calculation_oid ==
            risk_sub.c._calculation_oid
        )) \
        .join(agg_sub, and_(
            agg_sub.c._oid == riskvalue_aggregationtag.c.aggregationtag,
            agg_sub.c.type == riskvalue_aggregationtag.c.aggregationtype
        )) \
        .where(and_(
            agg_sub.c.name.like(filter_like_tag) if filter_like_tag else True,
            (AggregationTag.name == filter_tag) if filter_tag else True,
            risk_sub.c.losscategory == loss_category,
            risk_sub.c._calculation_oid == calculation_id
        ))
    return pd.read_sql(stmt, db.get_bind())


def read_aggregationtags(db: Session, aggregation_type: str,
                         tag_like: str | None):
    stmt = select(AggregationTag.name.label(aggregation_type)).where(and_(
        AggregationTag.type == aggregation_type,
        AggregationTag.name.like(tag_like) if tag_like else True
    ))

    return pd.read_sql(stmt, db.get_bind())


def read_aggregated_damage(db: Session,
                           calculation_id: int,
                           aggregation_type: str,
                           loss_category: ELossCategory,
                           filter_tag: str | None = None,
                           filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    damage_sub = select(
        DamageValue.dg2_value,
        DamageValue.dg3_value,
        DamageValue.dg4_value,
        DamageValue.dg5_value,
        DamageValue.weight,
        DamageValue._calculation_oid,
        DamageValue.losscategory,
        DamageValue._oid,
        DamageValue._type
    ).where(and_(
        DamageValue._calculation_oid == calculation_id,
        DamageValue.losscategory == loss_category,
        DamageValue._type == ECalculationType.DAMAGE
    )).subquery()

    agg_sub = select(
        AggregationTag.name,
        AggregationTag.type,
        AggregationTag._oid
    ).where(and_(
            AggregationTag.type == aggregation_type,
            AggregationTag.name.like(
                filter_like_tag) if filter_like_tag else True,
            (AggregationTag.name == filter_tag) if filter_tag else True
            )).subquery()

    stmt = select((damage_sub.c.dg2_value +
                   damage_sub.c.dg3_value +
                   damage_sub.c.dg4_value +
                   damage_sub.c.dg5_value).label('damage_value'),
                  damage_sub.c.weight,
                  agg_sub.c.name.label(aggregation_type)) \
        .select_from(damage_sub) \
        .join(riskvalue_aggregationtag, and_(
            riskvalue_aggregationtag.c.riskvalue == damage_sub.c._oid,
            riskvalue_aggregationtag.c.losscategory ==
            damage_sub.c.losscategory,
            riskvalue_aggregationtag.c._calculation_oid ==
            damage_sub.c._calculation_oid
        )) \
        .join(agg_sub, and_(
            agg_sub.c._oid == riskvalue_aggregationtag.c.aggregationtag,
            agg_sub.c.type == riskvalue_aggregationtag.c.aggregationtype
        )) \
        .where(and_(
            agg_sub.c.name.like(filter_like_tag) if filter_like_tag else True,
            (AggregationTag.name == filter_tag) if filter_tag else True,
            damage_sub.c.losscategory == loss_category,
            damage_sub.c._calculation_oid == calculation_id,
            damage_sub.c._type == ECalculationType.DAMAGE
        ))

    return pd.read_sql(stmt, db.get_bind())


def read_risk_assessments(
    db: Session, originid: str | None,
        starttime: datetime | None = None,
        endtime: datetime | None = None,
        published: bool | None = None,
        preferred: bool | None = None) -> list[RiskAssessment]:

    stmt = select(RiskAssessment)
    if starttime:
        stmt = stmt.filter(
            RiskAssessment.creationinfo_creationtime >= starttime)
    if endtime:
        stmt = stmt.filter(RiskAssessment.creationinfo_creationtime <= endtime)
    if originid:
        stmt = stmt.filter(RiskAssessment.originid == originid)
    if published:
        stmt = stmt.filter(RiskAssessment.published == published)
    if preferred:
        stmt = stmt.filter(RiskAssessment.preferred == preferred)
    return db.execute(stmt).unique().scalars().all()


def read_risk_assessment(db: Session, oid: int) -> RiskAssessment:
    stmt = select(RiskAssessment).where(
        RiskAssessment._oid == oid)
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


def read_calculation(db: Session, id: int) -> Calculation:
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


def read_region_name(originids: tuple[str]) -> str:
    from config.config import get_settings
    settings = get_settings()
    conn = psycopg2.connect(settings.EARTHQUAKE_INFO)

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "select originreference.m_originid, "
        "eventdescription.m_text as region_name "
        "from eventdescription "
        "inner join event on eventdescription._parent_oid = event._oid "
        "inner join originreference on "
        "originreference._parent_oid = event._oid "
        "where eventdescription.m_type = 'region name' "
        "and originreference.m_originid in  ({});".format(
            ','.join(['%s'] * len(originids))), originids)
    db_earthquakes = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{k: v for k, v in d.items()} for d in db_earthquakes]


def read_earthquakes_information(originids: tuple[str]) -> list[dict]:

    from config.config import get_settings
    settings = get_settings()
    conn = psycopg2.connect(settings.SCENARIO_INFO)

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
                   " WHERE origin_publicid IN ({});".format(
                       ','.join(['%s'] * len(originids))), originids)

    db_earthquakes = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{k: v for k, v in d.items()} for d in db_earthquakes]


def read_danger_level(originid: str) -> int:
    from config.config import get_settings
    settings = get_settings()
    conn = psycopg2.connect(settings.EARTHQUAKE_INFO)

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "select ("
        "xpath("
        "'//mydefns:alert/mydefns:info/mydefns:parameter/mydefns:value/text()'"
        ", encode(m_data, 'escape')::xml, ARRAY[ARRAY['mydefns', "
        "'urn:oasis:names:tc:emergency:cap:1.2']] ) )[1] as alarmlevel"
        "    from product"
        "    where m_producttype = 'cap-with-zones-text-message' "
        "        and m_referredpublicobject in "
        "            (select anyorigin.m_originid "
        "            from event inner join originreference as anyorigin "
        "                on anyorigin._parent_oid = event._oid"
        "                inner join originreference as myorigin "
        "                on myorigin._parent_oid = event._oid"
        "                    where anyorigin.m_originid = "
        "                    product.m_referredpublicobject "
        "                        and myorigin.m_originid = '{}'"
        "             order by product._oid desc limit 1)"
        "        order by product._oid desc limit 1".format(originid)
    )

    danger_level = cursor.fetchone()
    cursor.close()
    conn.close()
    if danger_level and 'alarmlevel' in danger_level:
        return int(danger_level['alarmlevel'])
    else:
        return None


def read_earthquake_information(originid: str) -> dict:
    from config.config import get_settings
    settings = get_settings()
    conn = psycopg2.connect(settings.SCENARIO_INFO)

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

    db_earthquake = cursor.fetchone() or {}
    cursor.close()
    conn.close()
    return {k: v for k, v in db_earthquake.items()}


def read_ria_text(originid: str, language: str) -> str:
    from config.config import get_settings
    settings = get_settings()
    conn = psycopg2.connect(settings.EARTHQUAKE_INFO)

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "select ria_text from ( "
        "    select * from ( "
        "        select convert_from(m_data,'UTF8') as ria_text, 1 as prio, m_producttype, m_language "
        "        from product  "
        "        where m_referredpublicobject = '{0}' "
        "        order by product._oid desc "
        "    ) as ria_text_for_this_originid "
        "    UNION "
        "    select * from ( "
        "        select convert_from(m_data,'UTF8'), 2 as prio, m_producttype, m_language "
        "        from product inner join originreference as referredoriginreference  "
        "            on m_referredpublicobject = referredoriginreference.m_originid "
        "        inner join event on referredoriginreference._parent_oid = event._oid "
        "        where event.m_preferredoriginid = '{0}' "
        "        order by product._oid desc "
        "    ) as ria_text_for_preferred_originid "
        "    union  "
        "    select * from ( "
        "        select convert_from(m_data,'UTF8'), 3 as prio, m_producttype, m_language "
        "        from product inner join originreference as referredoriginreference  "
        "            on m_referredpublicobject = referredoriginreference.m_originid "
        "        inner join originreference as otheroriginreference  "
        "            on referredoriginreference._oid <> otheroriginreference._oid "
        "                    and referredoriginreference._parent_oid = otheroriginreference._parent_oid "
        "        where otheroriginreference.m_originid = '{0}' "
        "        order by product._oid desc "
        "    ) as latest_ria_text_for_any_originid "
        "    ) as allriatexts "
        "    where m_producttype = 'ria-text-message' "
        "    and m_language = '{1}' -- english, german, french, italian "
        "    order by prio LIMIT 1 ".format(originid, language))
    db_earthquake = cursor.fetchone() or {}
    cursor.close()
    conn.close()
    return {k: v for k, v in db_earthquake.items()}
