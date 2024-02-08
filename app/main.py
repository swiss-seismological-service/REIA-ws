from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from app.database import sessionmanager
from app.routers import calculation, damage, loss, riskassessment
from config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Function that handles startup and shutdown events.
    To understand more, read https://fastapi.tiangolo.com/advanced/events/
    """
    yield
    if sessionmanager._engine is not None:
        # Close the DB connection
        await sessionmanager.close()

app = FastAPI(lifespan=lifespan, root_path=get_settings().ROOT_PATH,
              title="Rapid Earthquake Impact Assessment Switzerland")

app.include_router(loss.router, prefix='/v1')
app.include_router(damage.router, prefix='/v1')
app.include_router(riskassessment.router, prefix='/v1')
app.include_router(calculation.router, prefix='/v1')

app = CORSMiddleware(
    app=app,
    allow_origins=get_settings().ALLOW_ORIGINS,
    allow_origin_regex=get_settings().ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)
