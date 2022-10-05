from esloss.datamodel.base import ORMBase
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import engine
from app.routers import loss

ORMBase.metadata.create_all(bind=engine)

app = FastAPI()
app.include_router(loss.router, prefix='/v1')


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
