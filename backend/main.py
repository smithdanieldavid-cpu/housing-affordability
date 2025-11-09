from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base
from models import HousingAffordability, GovernmentGPHI
from pydantic import BaseModel
import os


# Create DB tables (if not exist)
Base.metadata.create_all(bind=engine)


app = FastAPI(title="Housing Affordability API")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
def get_db():
    """Provides a database session for a request and ensures it is closed."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/api/years")
def get_years(db: Session = next(get_db())):
    """Returns a list of all unique years available in the HousingAffordability data."""
    rows = db.query(HousingAffordability.year).order_by(HousingAffordability.year).all()
    return [r[0] for r in rows]


@app.get("/api/data")
def get_all_data(db: Session = next(get_db())):
    """Returns all records from the HousingAffordability table."""
    rows = db.query(HousingAffordability).order_by(HousingAffordability.year).all()
    return [r.as_dict() for r in rows]


@app.get("/api/data/{year}")
def get_year(year: int, db: Session = next(get_db())):
    """Returns the HousingAffordability record for a specific year."""
    row = db.query(HousingAffordability).filter(HousingAffordability.year == year).first()
    if not row:
        raise HTTPException(status_code=404, detail="Year not found")
    return row.as_dict()


@app.get("/api/government")
def get_governments(db: Session = next(get_db())):
    """Returns all records from the GovernmentGPHI table, ordered by descending GPHI score."""
    rows = db.query(GovernmentGPHI).order_by(GovernmentGPHI.gphi_score.desc()).all()
    return [r.as_dict() for r in rows]


@app.get("/health")
def health():
    """Simple health check endpoint."""
    return {"status": "ok"}