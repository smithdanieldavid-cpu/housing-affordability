from sqlalchemy import Column, Integer, String, Numeric, Text
from sqlalchemy.orm import declarative_base

# Define the base class for declarative class definitions
Base = declarative_base()


class HousingAffordability(Base):
    """
    Represents historical data points related to housing affordability and
    key economic and demographic indicators.

    UPDATED COLUMNS:
    - government_party: Added for political context.
    - interest_rate: Replaces the old mortgage_rate_pct.
    - gphi_score: The calculated Government Performance Housing Index.
    """
    __tablename__ = 'housing_affordability'
    year = Column(Integer, primary_key=True)
    avg_house_price_capitals_aud = Column(Numeric)
    yoy_growth_pct = Column(Numeric)
    cpi_index_1980_100 = Column(Numeric)
    real_house_price_index = Column(Numeric)
    avg_mortgage_size_aud = Column(Numeric)
    median_household_income_aud = Column(Numeric)
    price_to_income_ratio = Column(Numeric)
    affordability_index = Column(Numeric)

    # --- NEW / UPDATED COLUMNS FOR DASHBOARD ---
    # Replaces mortgage_rate_pct (used by new ETL)
    interest_rate = Column(Numeric) 
    # New columns required by the ETL and dashboard
    government_party = Column(String(64))
    gphi_score = Column(Numeric)
    # --- END NEW COLUMNS ---

    mortgage_rate_pct = Column(Numeric) # Keeping old name for compatibility/documentation
    mortgage_service_ratio = Column(Numeric)
    avg_household_size = Column(Numeric)
    crude_birth_rate_per_1000 = Column(Numeric)
    net_migration = Column(Integer)
    foreign_ownership_pct = Column(Numeric)
    investor_ownership_pct = Column(Numeric)
    negative_gearing_cost_aud_bn = Column(Numeric)
    data_quality = Column(String(32))
    notes = Column(Text)

    def as_dict(self):
        """Returns the model instance data as a dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# NOTE: The GovernmentGPHI table is no longer required as the dashboard 
# now aggregates the time-series data from HousingAffordability on the fly.
# class GovernmentGPHI(Base):
#     """
#     Represents the Government Performance Housing Index (GPHI) scores,
#     calculated for various government terms.
#     """
#     __tablename__ = 'gphi_government'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     government = Column(String(128))
#     party = Column(String(64))
#     start_year = Column(Integer)
#     end_year = Column(Integer)
#     avg_ai = Column(Numeric)
#     delta_ai = Column(Numeric)
#     delta_pir = Column(Numeric)
#     avg_real_price_growth = Column(Numeric)
#     delta_msr = Column(Numeric)
#     gphi_score = Column(Numeric)
#     grade = Column(String(4))
#     notes = Column(Text)

#     def as_dict(self):
#         """Returns the model instance data as a dictionary."""
#         return {c.name: getattr(self, c.name) for c in self.__table__.columns}