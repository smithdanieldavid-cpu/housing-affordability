"""
Historical Housing Affordability ETL (1980-2023)

This script loads a mock historical dataset, calculates derived metrics 
like the Price-to-Income Ratio and the Government Performance Housing Index (GPHI Score),
and upserts the results into the database.

NOTE: You will need to update your 'models.py' to include the new columns:
- government_party (String)
- interest_rate (Float)
- gphi_score (Float)
"""
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import HousingAffordability, Base
import pandas as pd
import numpy as np

# Ensure the table is created before proceeding (if not already done by main.py)
Base.metadata.create_all(bind=engine)


def load_sample_data() -> pd.DataFrame:
    """
    Generates a mock historical dataset spanning 1980 to 2023 (44 years).
    
    This function simulates realistic, complex data needed for the dashboard, 
    including government terms and varying interest rates.
    """
    years = range(1980, 2024)
    n = len(years)

    # 1. Base Metrics Simulation
    # Starting values (1980): House Price ~ 70k, Income ~ 15k
    house_price = (np.arange(n) * 15000) + 70000 + np.random.normal(0, 15000, n).cumsum()
    household_income = (np.arange(n) * 2000) + 15000 + np.random.normal(0, 500, n).cumsum()
    
    # Simulate realistic interest rates (high in 80s, low in 2010s, rising late)
    interest_rate = np.clip(
        10 - np.sin(np.linspace(0, 2 * np.pi, n) * 4) * 4 + np.arange(n) * 0.05, 
        2.5, 17
    ) + np.random.normal(0, 0.5, n)

    # 2. Government Party Simulation (simplified alternating terms)
    # Labor (1980-1983), Coalition (1984-1995), Labor (1996-2007), Coalition (2008-2019), Labor (2020-2023)
    party_map = {
        'Labor': list(range(1980, 1984)) + list(range(1996, 2008)) + list(range(2020, 2024)),
        'Coalition': list(range(1984, 1996)) + list(range(2008, 2020))
    }
    
    party = []
    for year in years:
        if year in party_map['Labor']:
            party.append('Labor')
        elif year in party_map['Coalition']:
            party.append('Coalition')
        else:
            party.append('N/A')

    # Create the DataFrame
    df = pd.DataFrame({
        "year": years,
        "avg_house_price_capitals_aud": house_price.round(0),
        "median_household_income_aud": household_income.round(0),
        "interest_rate": interest_rate.round(1),
        "government_party": party
    })
    
    return df


def compute_and_upsert(df: pd.DataFrame):
    """
    Computes derived metrics and performs an upsert operation.
    """
    db: Session = SessionLocal()
    try:
        # T1: Compute basic affordability metrics
        df['price_to_income_ratio'] = df['avg_house_price_capitals_aud'] / df['median_household_income_aud']
        # Affordability Index (Higher is better)
        df['affordability_index'] = 100 * (df['median_household_income_aud'] / df['avg_house_price_capitals_aud'])

        # T2: Calculate Annual Price Change for GPHI calculation
        df['annual_price_change_pct'] = df['avg_house_price_capitals_aud'].pct_change() * 100
        df['annual_price_change_pct'] = df['annual_price_change_pct'].fillna(0) # First year has 0 change

        # T3: Calculate GPHI Score (Government Performance Housing Index)
        # GPHI is a composite score where higher is BETTER.
        # It weights affordability positively and rising prices/high rates negatively.
        # Formula: (Affordability Index * 0.5) - (Interest Rate * 2) - (Annual Price Change %)
        df['gphi_score'] = (
            (df['affordability_index'] * 0.5) - 
            (df['interest_rate'] * 2) - 
            (df['annual_price_change_pct'] * 1.5)
        )

        # L: Load/Upsert
        for _, row in df.iterrows():
            year = int(row['year'])
            
            # Check if record exists (Upsert logic)
            obj = db.query(HousingAffordability).filter(HousingAffordability.year == year).first()
            if not obj:
                obj = HousingAffordability(year=year)

            # Define columns to upsert (must match models.py structure)
            cols_to_upsert = [
                'avg_house_price_capitals_aud',
                'median_household_income_aud',
                'interest_rate',
                'government_party',
                'price_to_income_ratio',
                'affordability_index',
                'gphi_score'
            ]
            
            for col in cols_to_upsert:
                # Use .item() to safely extract Python values from numpy/pandas types
                value = row.get(col)
                if isinstance(value, np.floating):
                    value = value.item()
                elif isinstance(value, np.integer):
                    value = int(value)

                setattr(obj, col, value)

            obj.data_quality = 'derived_historical_mock'
            db.add(obj)

        db.commit()
    finally:
        db.close()


if __name__ == '__main__':
    print('Starting ETL: Generating 1980-2023 historical data...')
    # E: Extract (from mock data generation)
    df = load_sample_data()
    
    # T & L
    compute_and_upsert(df)
    
    print('Historical data generated and loaded successfully (44 years).')