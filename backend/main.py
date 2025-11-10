import os
import json
import logging
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict

# --- SQLAlchemy Imports (Modern 2.0 Style) ---
from sqlalchemy import create_engine, Integer, Float, String
from sqlalchemy.orm import sessionmaker, Mapped, mapped_column, DeclarativeBase
from sqlalchemy.sql.expression import select # Added for modern querying (though not used below, it's good practice)

# --- Configuration & Setup ---

# Define the Declarative Base (SQLAlchemy 2.0)
class Base(DeclarativeBase):
    pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up database engine (using SQLite for simplicity and portability)
DATABASE_URL = "sqlite:///./affordability.db"
engine = create_engine(DATABASE_URL)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Database Model (SQLAlchemy 2.0 Mapped) ---
class HousingAffordability(Base):
    """Database model for a single year's metrics."""
    __tablename__ = "housing_metrics"
    
    # Using Mapped[] and mapped_column() for 2.0 compliance
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    year: Mapped[int] = mapped_column(Integer, unique=True, index=True, nullable=False)
    median_price: Mapped[Optional[float]] = mapped_column(Float)
    median_income: Mapped[Optional[float]] = mapped_column(Float)
    affordability_index: Mapped[Optional[float]] = mapped_column(Float)
    interest_rate: Mapped[Optional[float]] = mapped_column(Float)
    government_party: Mapped[Optional[str]] = mapped_column(String)
    gphi_score: Mapped[Optional[float]] = mapped_column(Float)

# --- Pydantic Schemas (Pydantic v2) ---

class AffordabilityMetric(BaseModel):
    """Schema for annual metric data point."""
    year: int
    median_price: Optional[float]
    median_income: Optional[float]
    affordability_index: Optional[float]
    interest_rate: Optional[float]
    government_party: Optional[str]
    gphi_score: Optional[float]
    
    # Use model_config for Pydantic v2 from_attributes
    model_config = ConfigDict(from_attributes=True)

class GovernmentTermSummary(BaseModel):
    """Schema for the aggregated government term expected by the frontend."""
    party: str
    start_year: int
    end_year: int
    duration_years: int
    average_gphi_score: float
    annual_metrics: List[AffordabilityMetric]

# --- FastAPI Setup ---
app = FastAPI(title="Housing Affordability API")

# Add CORS middleware
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency to get a DB session
def get_db():
    """Provides a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Utility Functions for Aggregation ---

# (The utility functions 'calculate_government_terms' and 'finalize_term' remain the same
# as they were already logically sound and only relied on standard Python List/Dict operations.)

def calculate_government_terms(raw_data: List[HousingAffordability]) -> List[GovernmentTermSummary]:
    """
    Processes the raw time-series data to calculate aggregate metrics for each
    continuous period of government, as expected by the React frontend.
    """
    if not raw_data:
        return []

    # Sort data by year to ensure continuity
    sorted_data = sorted(raw_data, key=lambda x: x.year)

    terms: List[Dict[str, Any]] = []
    current_term: Optional[Dict[str, Any]] = None

    for row in sorted_data:
        # We only process years where the government party is known
        if row.government_party:
            party = row.government_party

            if not current_term or current_term['party'] != party:
                # Finalize the previous term if it exists
                if current_term and len(current_term['annual_metrics']) > 1:
                    terms.append(finalize_term(current_term))

                # Start a new term
                current_term = {
                    'party': party,
                    'start_year': row.year,
                    'annual_metrics': [],
                    'total_gphi_score': 0.0,
                }
            
            # Continue the current term
            current_term['annual_metrics'].append(row)
            current_term['total_gphi_score'] += row.gphi_score if row.gphi_score is not None else 0.0
        else:
            # If a row has no government_party, it breaks the current term continuity
            if current_term and len(current_term['annual_metrics']) > 1:
                terms.append(finalize_term(current_term))
                current_term = None # Reset current term

    # Finalize the last term
    if current_term and len(current_term['annual_metrics']) > 1:
        terms.append(finalize_term(current_term))
        
    # Convert dictionaries to Pydantic models
    return [GovernmentTermSummary(**term) for term in terms]

def finalize_term(term: Dict[str, Any]) -> Dict[str, Any]:
    """Calculates summary stats for a finalized term."""
    term_data = term['annual_metrics']
    count = len(term_data)

    # Convert SQLAlchemy objects to Pydantic models for the nested list
    # The .from_orm() method is now .model_validate() in Pydantic v2 for non-BaseModel instances, 
    # but since data models are configured with from_attributes=True, it will still work, 
    # though model_validate is the modern v2 way. Using .model_validate(d.dict()) or similar is safer
    # but .from_orm is fine for now if you are using Pydantic compatibility mode.
    annual_metrics_pydantic = [AffordabilityMetric.model_validate(d) for d in term_data]


    # Calculate average GPHI score
    avg_gphi = term['total_gphi_score'] / count

    return {
        'party': term['party'],
        'start_year': term['start_year'],
        'end_year': term_data[-1].year,
        'duration_years': term_data[-1].year - term['start_year'] + 1,
        'average_gphi_score': round(avg_gphi, 2),
        'annual_metrics': annual_metrics_pydantic,
    }


# --- API Endpoints ---

@app.get("/health", response_model=Dict[str, str])
def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"}

##@app.get("/api/data", response_model=List[AffordabilityMetric])
##def get_all_data(db: SessionLocal = Depends(get_db)): # Use Depends to ensure a session is provided
    """
    Retrieves all raw annual housing metric data.
    """
    try:
        # Use db.scalars() and select() for modern SQLAlchemy 2.0 querying
        # If your environment is still on SQLAlchemy 1.x syntax, the old db.query().all() will work.
        # We will keep the old syntax for compatibility, but note the 2.0 method is cleaner:
        # data = db.scalars(select(HousingAffordability).order_by(HousingAffordability.year)).all()
        
        data = db.query(HousingAffordability).order_by(HousingAffordability.year).all()
        
        if not data:
            raise HTTPException(status_code=404, detail="No data found in the database.")
        
        # FastAPI/Pydantic automatically convert the SQLAlchemy model objects to the response_model
        return data
    except Exception as e:
        logger.error(f"Database query error in /api/data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error accessing data.")

##@app.get("/api/government_terms", response_model=List[GovernmentTermSummary])
##def get_government_terms(db: SessionLocal = Depends(get_db)): # Use Depends for session
    """
    Retrieves aggregated housing metric data grouped by government party terms.
    """
    try:
        # Get all raw data
        raw_data = db.query(HousingAffordability).order_by(HousingAffordability.year).all()
        if not raw_data:
            raise HTTPException(status_code=404, detail="No raw data available to calculate terms.")
        
        # Calculate and return aggregated terms
        terms_summary = calculate_government_terms(raw_data)
        
        if not terms_summary:
             raise HTTPException(status_code=404, detail="Data was found but could not be grouped into political terms.")

        # Sort the final output by average GPHI score (highest first) for the frontend
        terms_summary.sort(key=lambda x: x.average_gphi_score, reverse=True)
        
        return terms_summary

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing government terms: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during data aggregation.")