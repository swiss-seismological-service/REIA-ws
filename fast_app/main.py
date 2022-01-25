import time
from pyppeteer import launch
from typing import List

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from . import crud, models, schemas
from .database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/earthquakes/", response_model=schemas.Earthquake)
def create_earthquake(
        earthquake: schemas.EarthquakeBase, db: Session = Depends(get_db)):
    return crud.create_earthquake(db=db, earthquake=earthquake)


@app.get("/earthquakes/", response_model=List[schemas.Earthquake])
def read_earthquakes(
        skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    earthquakes = crud.get_earthquakes(db, skip=skip, limit=limit)
    return earthquakes


@app.get("/earthquakes/{earthquake_id}", response_model=schemas.Earthquake)
def read_earthquake(earthquake_id: int, db: Session = Depends(get_db)):
    db_earthquake = crud.get_earthquake(db, earthquake_id=earthquake_id)
    if db_earthquake is None:
        raise HTTPException(status_code=404, detail="Earthquake not found")
    time.sleep(2)
    return db_earthquake


@app.post(
    "/earthquakes/{earthquake_id}/losses/", response_model=schemas.Losses)
def create_losses_for_earthquake(
        earthquake_id: int,
        loss: schemas.LossesBase, db: Session = Depends(get_db)):
    return crud.create_cantonal_losses(
        db=db, losses=loss, earthquake_id=earthquake_id)


@app.get("/pdf", response_class=FileResponse)
async def main():
    some_file_path = "fast_app/pdfs/schaden.pdf"

    browser = await launch()
    page = await browser.newPage()
    await page.emulateMedia("print")
    await page.goto('http://localhost:5000')

    watchDog = page.waitForFunction('window.status === "ready_to_print"')
    await watchDog
    await page.pdf({'path': 'fast_app/pdfs/schaden.pdf',
                    'preferCSSPageSize': True, 'printBackground': True})
    await browser.close()

    return some_file_path


@app.get("/pdf_ocms", response_class=FileResponse)
async def pdf_ocms():
    some_file_path = "fast_app/pdfs/schaden.pdf"

    browser = await launch()
    page = await browser.newPage()
    await page.emulateMedia("print")
    await page.goto('http://localhost/mercury-json/demo-schaden/')

    watchDog = page.waitForFunction('window.status === "ready_to_print"')
    await watchDog
    await page.pdf({'path': 'fast_app/pdfs/schaden.pdf',
                    'preferCSSPageSize': True, 'printBackground': True})
    await browser.close()

    return some_file_path
