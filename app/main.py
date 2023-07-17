from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import calculation, damage, loss, riskassessment
from config import get_settings

app = FastAPI(root_path=get_settings().ROOT_PATH,
              title="Rapid Earthquake Impact Assessment Switzerland")

app.include_router(loss.router, prefix='/v1')
app.include_router(damage.router, prefix='/v1')
app.include_router(riskassessment.router, prefix='/v1')
app.include_router(calculation.router, prefix='/v1')


origins = get_settings().ALLOW_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=get_settings().ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
