import base64
from typing import Literal

from fastapi import APIRouter, HTTPException

from app import crud
from app.schemas import (RiskAssessmentDescriptionSchema,
                         RiskAssessmentInfoSchema)

router = APIRouter(prefix='/origin', tags=['origin'])


@router.get('/{originid}', response_model=RiskAssessmentInfoSchema)
async def read_info(originid: str):
    originid = base64.b64decode(originid).decode('utf-8')
    db_result = crud.read_ria_parameters((originid,))
    if not db_result:
        raise HTTPException(
            status_code=404, detail='No info found for originid.')
    return db_result[0]


@router.get('/{originid}/dangerlevel')
async def read_danger_level(originid: str):
    """
    Returns the danger level for the given originid.
    """
    originid = base64.b64decode(originid).decode('utf-8')
    db_result = crud.read_danger_levels((originid,))
    return db_result


@router.get('/{originid}/description/{lang}',
            response_model=RiskAssessmentDescriptionSchema,)
async def read_description(originid: str,
                           lang: Literal['de', 'en', 'fr', 'it']):
    """
    Returns the description of the event for the given originid and language.
    """
    language_mapping = {"de": "german", "en": "english",
                        "fr": "french", "it": "italian"}

    originid = base64.b64decode(originid).decode('utf-8')
    db_result = crud.read_ria_text(originid, language_mapping[lang])

    if db_result:
        return {'description': db_result['ria_text']}

    return {'description': None}
