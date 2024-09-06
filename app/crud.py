from datetime import datetime
from uuid import UUID

import pandas as pd
from reia.datamodel import (AggregationTag, Asset, Calculation,  # noqa
                            CalculationBranch, DamageCalculation,
                            DamageCalculationBranch, DamageValue,
                            ECalculationType, ELossCategory, ExposureModel,
                            LossCalculation, LossValue, RiskAssessment,
                            asset_aggregationtag, riskvalue_aggregationtag)
from sqlalchemy import Select, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectin_polymorphic, selectinload

from app.utils import pandas_read_sql
from config import Settings


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


async def get_aggregation_types(db: AsyncSession):
    stmt = select(AggregationTag.type).distinct()
    results = await db.execute(stmt)
    types = results.scalars().all()
    edict = {}
    for t in types:
        edict[t.upper()] = t
    return edict


async def read_total_buildings_country(
        db: AsyncSession, calculation_id: int) -> int | None:
    exp_sub = select(ExposureModel._oid) \
        .join(DamageCalculationBranch) \
        .join(Calculation) \
        .where(Calculation._oid == calculation_id) \
        .limit(1).scalar_subquery()

    stmt = select(func.sum(Asset.buildingcount).label('buildingcount')) \
        .select_from(Asset) \
        .where(Asset._exposuremodel_oid == exp_sub)
    result = await db.execute(stmt)
    return result.scalar()


async def read_total_buildings(db: AsyncSession,
                               calculation_id: int,
                               aggregation_type: str,
                               filter_tag: str | None = None,
                               filter_like_tag: str | None = None) \
        -> pd.DataFrame:

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

    return await pandas_read_sql(stmt, db)


async def read_aggregated_loss(db: AsyncSession,
                               calculation_id: int,
                               aggregation_type: str,
                               loss_category: Settings.RiskCategory,
                               filter_tag: str | None = None,
                               filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    loss_category = ELossCategory[loss_category.name]

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
                  risk_sub.c._calculationbranch_oid.label('branchid'),
                  risk_sub.c.eventid,
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
            risk_sub.c._calculation_oid == calculation_id,
            risk_sub.c._type == ECalculationType.LOSS
        ))
    return await pandas_read_sql(stmt, db)


async def read_aggregationtags(db: AsyncSession, aggregation_type: str,
                               calculation_id: int, tag_like: str | None):
    stmt = select(ExposureModel._oid) \
        .join(CalculationBranch) \
        .join(Calculation) \
        .where(Calculation._oid == calculation_id)

    exposuremodel_oids = await db.execute(stmt)
    exposuremodel_oids = exposuremodel_oids.unique().scalars().all()

    stmt = select(AggregationTag.name.label(aggregation_type)).where(and_(
        AggregationTag.type == aggregation_type,
        AggregationTag.name.like(tag_like) if tag_like else True,
        AggregationTag._exposuremodel_oid.in_(exposuremodel_oids)
    ))

    df = await pandas_read_sql(stmt, db)
    df.drop_duplicates(inplace=True)
    return df


async def read_aggregated_damage(db: AsyncSession,
                                 calculation_id: int,
                                 aggregation_type: str,
                                 loss_category: Settings.RiskCategory,
                                 filter_tag: str | None = None,
                                 filter_like_tag: str | None = None) \
        -> pd.DataFrame:

    loss_category = ELossCategory[loss_category.name]

    damage_sub = select(DamageValue).where(and_(
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

    stmt = select(damage_sub.c.dg1_value.label('dg1_value'),
                  damage_sub.c.dg2_value.label('dg2_value'),
                  damage_sub.c.dg3_value.label('dg3_value'),
                  damage_sub.c.dg4_value.label('dg4_value'),
                  damage_sub.c.dg5_value.label('dg5_value'),
                  damage_sub.c.weight,
                  damage_sub.c._calculationbranch_oid.label('branchid'),
                  damage_sub.c.eventid,
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

    return await pandas_read_sql(stmt, db)


def read_risk_assessments(
        originid: str | None,
        starttime: datetime | None = None,
        endtime: datetime | None = None,
        published: bool | None = None,
        preferred: bool | None = None) -> Select:

    stmt = select(RiskAssessment) \
        .options(selectinload(RiskAssessment.losscalculation),
                 selectinload(RiskAssessment.damagecalculation))
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

    stmt = stmt.order_by(RiskAssessment.creationinfo_creationtime.desc())

    # using pagination and only returning query statement
    return stmt


async def read_risk_assessment(db: AsyncSession, oid: UUID) -> RiskAssessment:
    stmt = select(RiskAssessment) \
        .where(RiskAssessment._oid == oid) \
        .options(selectinload(RiskAssessment.losscalculation),
                 selectinload(RiskAssessment.damagecalculation))
    result = await db.execute(stmt)
    return result.unique().scalar()


def read_calculations(starttime: datetime | None,
                      endtime: datetime | None) -> Select:

    stmt = select(Calculation).options(
        selectin_polymorphic(
            Calculation, [LossCalculation, DamageCalculation]),
        selectinload(LossCalculation.losscalculationbranches),
        selectinload(DamageCalculation.damagecalculationbranches)
    )
    if starttime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime >= starttime)
    if endtime:
        stmt = stmt.filter(
            Calculation.creationinfo_creationtime <= endtime)

    stmt = stmt.order_by(Calculation.creationinfo_creationtime.desc())

    # using pagination and only returning query statement
    return stmt


async def read_calculation(db: AsyncSession, id: int) -> Calculation:
    stmt = select(Calculation).options(
        selectin_polymorphic(
            Calculation, [LossCalculation, DamageCalculation]),
        selectinload(LossCalculation.losscalculationbranches),
        selectinload(DamageCalculation.damagecalculationbranches)
    ).where(Calculation._oid == id)
    results = await db.execute(stmt)
    return results.unique().scalar()


async def read_mean_losses(db: AsyncSession,
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

    return await pandas_read_sql(stmt, db)
