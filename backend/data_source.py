from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Use a SQLite database file for simplicity.
# This will create a file named 'housing.db' in your backend directory.
SQLALCHEMY_DATABASE_URL = "sqlite:///./housing.db"

# Setting connect_args is necessary for SQLite connections to allow multiple threads
# to access the same connection, typical for FastAPI/SessionLocal usage.
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

# SessionLocal is the actual database session class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class used by SQLAlchemy models (e.g., HousingAffordability)
Base = declarative_base()

def get_db():
    """Provides a database session for a request and ensures it is closed."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()