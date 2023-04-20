from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from reia.datamodel.base import ORMBase

from app.database import engine
from app.routers import calculation, damage, loss, origin, riskassessment
from config.config import get_settings

ORMBase.metadata.create_all(bind=engine)

app = FastAPI(root_path=get_settings().ROOT_PATH)
app.include_router(loss.router, prefix='/v1')
app.include_router(damage.router, prefix='/v1')
app.include_router(riskassessment.router, prefix='/v1')
app.include_router(calculation.router, prefix='/v1')
app.include_router(origin.router, prefix='/v1')


origins = [
    "http://localhost",
    "http://localhost:5000",
    "http://localhost:8000",
    "http://localhost:8001",
    "http://127.0.0.1",
    "http://127.0.0.1:5000",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8001"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex='http.*://.*\\.ethz\\.ch.*',
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
