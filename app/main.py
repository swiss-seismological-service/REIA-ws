from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from reia.datamodel.base import ORMBase

from app.database import engine
from app.routers import calculations, damage, loss

ORMBase.metadata.create_all(bind=engine)

app = FastAPI(root_path='/riaws')
app.include_router(loss.router, prefix='/v1')
app.include_router(damage.router, prefix='/v1')
app.include_router(calculations.router, prefix='/v1')


origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://127.0.0.1:5000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex='http.*://.*\\.ethz\\.ch',
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
