"""
Simple starter ETL: this script is intentionally minimal. It shows how to:
- read a CSV (or download from ABS),
- compute affordability metrics, and
- upsert rows into the housing_affordability table.

You will replace the "load_sample_data()" function with real ABS API calls or local CSV reads.
"""
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import HousingAffordability, Base
import pandas as pd


# Ensure the table is created before proceeding (if not already done by main.py)
Base.metadata.create_all(bind=engine)


def load_sample_data() -> pd.DataFrame:
    """
    Loads minimal sample data for demonstration.

    This function should be replaced with real ABS API calls or local CSV reads.
    """
    data = [
        {"year": 2020, "avg_house_price_capitals_aud": 720000, "median_household_income_aud": 85000, "cpi_index_1980_100": 155},
        {"year": 2021, "avg_house_price_capitals_aud": 780000, "median_household_income_aud": 86000, "cpi_index_1980_100": 160},
    ]
    return pd.DataFrame(data)


def compute_and_upsert(df: pd.DataFrame):
    """
    Computes derived metrics (Price-to-Income and Affordability Index)
    and performs an upsert operation into the HousingAffordability table.
    """
    db: Session = SessionLocal()
    try:
        # T: Transform
        df['price_to_income_ratio'] = df['avg_house_price_capitals_aud'] / df['median_household_income_aud']
        df['affordability_index'] = 100 * (df['median_household_income_aud'] / df['avg_house_price_capitals_aud'])

        # L: Load/Upsert
        for _, row in df.iterrows():
            # Check if record exists (Upsert logic)
            obj = db.query(HousingAffordability).filter(HousingAffordability.year == int(row['year'])).first()
            if not obj:
                obj = HousingAffordability(year=int(row['year']))

            # Update/set attributes
            for col in [
                'avg_house_price_capitals_aud',
                'median_household_income_aud',
                'cpi_index_1980_100',
                'price_to_income_ratio',
                'affordability_index',
            ]:
                setattr(obj, col, row.get(col))

            obj.data_quality = 'derived'
            db.add(obj)

        db.commit()
    finally:
        db.close()


if __name__ == '__main__':
    # E: Extract (from sample data)
    df = load_sample_data()
    
    # T & L
    compute_and_upsert(df)
    
    print('Sample data loaded.')