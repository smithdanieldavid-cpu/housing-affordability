import json
import logging
import requests
import time
from typing import List, Dict, Any, Optional
# The import below assumes score_calculator.py is in the same directory.
# If you are running this from a directory above, you might need relative import adjustments.
from score_calculator import calculate_gphi_score, calculate_government_terms, _get_government_party

# --- 1. Configuration and Global Constants ---

# Configure logging to display INFO level messages
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# API Configuration
ABS_API_BASE_URL = "https://data.api.abs.gov.au/rest/data"
MAX_RETRIES = 3
CACHE_TIMEOUT_SECONDS = 3600  # 1 hour cache duration

# SDMX Query Parameters
DATAFLOWS = {
    "RPPI": {"id": "ABS,RPPI,1.0.0", "key": "1.2.10.100.Q"}, 
    "CPI": {"id": "ABS,CPI,1.1.0", "key": "1.1.10000.10.50.Q"},
}

# --- 2. FastAPI Setup and Caching ---

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# This creates the 'app' variable uvicorn needs
app = FastAPI(title="Housing Affordability API", version="1.0.0")

# Global variables for in-memory caching
CACHED_TERMS: List[Dict[str, Any]] = []
LAST_FETCH_TIME: float = 0.0

# --- START: CRITICAL CORS FIX ---
# We must explicitly list the domains that are allowed to make requests (your frontend).
# This is secure and reliable.
origins = [
    # 1. Your custom domain
    "https://affordable-housing.com.au", 
    # 2. Your direct GitHub Pages domain
    "https://smithdanieldavid-cpu.github.io", 
    # 3. Common local development ports (for testing locally)
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:5500", 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # Use the explicit list of safe domains
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)
# --- END: CRITICAL CORS FIX ---

# --- 3. Data Processing and Transformation Functions (Modified/Integrated) ---

def _transform_abs_data(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parses the complex SDMX-JSON structure, aggregates quarterly data into 
    annual averages, and calls the external scoring function.
    """
    logger.info("Starting SDMX-JSON data transformation.")
    
    # Check for core data structure elements
    series_data = raw_data.get('dataSets', [{}])[0].get('series', {})
    if not series_data:
        logger.error("SDMX response is missing 'dataSets' or 'series'. Cannot process.")
        return []

    # Identify the index of the TIME_PERIOD dimension for lookup
    time_dimension_index = -1
    for i, dim in enumerate(raw_data.get('structure', {}).get('dimensions', {}).get('observation', [])):
        if dim.get('id') == 'TIME_PERIOD':
            time_dimension_index = i
            break
            
    if time_dimension_index == -1:
        logger.error("Could not find TIME_PERIOD dimension in SDMX structure.")
        return []

    # 1. Aggregate quarterly data across different series types (RPPI/CPI)
    quarterly_data: Dict[str, Dict[str, float]] = {}
    
    for series_key, series_value in series_data.items():
        # Determine the metric name (rppi_index or cpi_index)
        metric_name = series_value.get('id_prefix') 
        if not metric_name:
            continue
            
        metric_field = 'rppi_index' if metric_name == 'RPPI' else 'cpi_index'
            
        observations = series_value.get('observations', {})
        for time_key, obs_data in observations.items():
            value = float(obs_data[0]) 
            time_period = raw_data['structure']['dimensions']['observation'][time_dimension_index]['values'][int(time_key)]['id']

            if time_period not in quarterly_data:
                quarterly_data[time_period] = {}

            quarterly_data[time_period][metric_field] = value

    # 2. Convert Quarterly Data to Annual Averages
    annual_data: Dict[int, Dict[str, Any]] = {}

    for time_period, metrics in quarterly_data.items():
        try:
            year = int(time_period.split('-')[0])
            
            if year not in annual_data:
                annual_data[year] = {
                    'year': year,
                    'rppi_total': 0.0,
                    'cpi_total': 0.0,
                    'rppi_count': 0,
                    'cpi_count': 0,
                    'government_party': _get_government_party(year) 
                }
            
            if 'rppi_index' in metrics:
                annual_data[year]['rppi_total'] += metrics['rppi_index']
                annual_data[year]['rppi_count'] += 1
                
            if 'cpi_index' in metrics:
                annual_data[year]['cpi_total'] += metrics['cpi_index']
                annual_data[year]['cpi_count'] += 1
                
        except (ValueError, IndexError) as e:
            logger.warning(f"Skipping malformed time period '{time_period}': {e}")
            continue

    # 3. Finalize Annual Data and Calculate GPHI
    final_records: List[Dict[str, Any]] = []

    for year, data in annual_data.items():
        if data['rppi_count'] >= 2 and data['cpi_count'] >= 2:
            avg_rppi = data['rppi_total'] / data['rppi_count']
            avg_cpi = data['cpi_total'] / data['cpi_count']
            
            gphi_score = calculate_gphi_score(avg_rppi, avg_cpi)
            
            final_records.append({
                'year': year,
                'avg_rppi_index': round(avg_rppi, 2),
                'avg_cpi_index': round(avg_cpi, 2),
                'gphi_score': gphi_score,
                'government_party': data['government_party']
            })
        else:
            logger.info(f"Skipping year {year}: Insufficient quarterly data ({data['rppi_count']} RPPI, {data['cpi_count']} CPI).")


    logger.info(f"Transformation complete. Generated {len(final_records)} annual records.")
    return final_records


def fetch_housing_data() -> Optional[Dict[str, Any]]:
    """
    Fetches raw housing and economic data using the ABS Data API. 
    Returns the combined raw SDMX data structure or None on critical failure.
    """
    combined_raw_data: Dict[str, Any] = {}
    
    for metric_name, config in DATAFLOWS.items():
        dataflow_id = config['id']
        data_key = config['key']
        api_url = f"{ABS_API_BASE_URL}/{dataflow_id}/{data_key}?startPeriod=2000&format=jsondata&detail=full"
        
        logger.info(f"Attempting to fetch {metric_name} data from ABS API at: {dataflow_id}")

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status() 

                raw_api_data = response.json()
                logger.info(f"Successfully received {metric_name} data on attempt {attempt + 1}.")
                
                # Merge logic for SDMX structure
                if not combined_raw_data:
                    combined_raw_data = raw_api_data
                else:
                    new_series = raw_api_data.get('dataSets', [{}])[0].get('series', {})
                    if 'dataSets' in combined_raw_data and len(combined_raw_data['dataSets']) > 0:
                        combined_raw_data['dataSets'][0]['series'].update(new_series)
                
                # Add an identifier to the series keys for the parser
                for key, series_data in raw_api_data.get('dataSets', [{}])[0].get('series', {}).items():
                     if 'id_prefix' not in series_data:
                        series_data['id_prefix'] = metric_name 

                for key, series_data in combined_raw_data.get('dataSets', [{}])[0].get('series', {}).items():
                     if 'id_prefix' not in series_data:
                         if 'RPPI' in dataflow_id: series_data['id_prefix'] = 'RPPI'
                         elif 'CPI' in dataflow_id: series_data['id_prefix'] = 'CPI'
                break 

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP Error on attempt {attempt + 1} for {metric_name}: {e.response.status_code} - {e.response.reason}")
            except requests.exceptions.ConnectionError:
                logger.error(f"Connection Error on attempt {attempt + 1} for {metric_name}.")
            except requests.exceptions.Timeout:
                logger.error(f"Timeout Error on attempt {attempt + 1} for {metric_name}.")
            except Exception as e:
                logger.error(f"An unexpected error occurred on attempt {attempt + 1} for {metric_name}: {e}")

            if attempt < MAX_RETRIES - 1:
                delay = 2 ** attempt
                logger.info(f"Retrying {metric_name} in {delay} seconds...")
                time.sleep(delay)
        else:
            logger.error(f"Failed to fetch {metric_name} data after {MAX_RETRIES} attempts. Skipping metric.")

    if not combined_raw_data:
        logger.error("Failed to fetch data for all required metrics.")
        return None

    return combined_raw_data


# --- 4. Main Data Pipeline and Caching Logic ---

def load_and_cache_data() -> List[Dict[str, Any]]:
    """
    Checks cache validity, runs the full data pipeline if the cache is stale, 
    and updates the global cache variables.
    """
    global CACHED_TERMS
    global LAST_FETCH_TIME

    # Check if the cache is still valid
    if (time.time() - LAST_FETCH_TIME) < CACHE_TIMEOUT_SECONDS and CACHED_TERMS:
        logger.info(f"Serving data from cache. Next update in {(CACHE_TIMEOUT_SECONDS - (time.time() - LAST_FETCH_TIME)):.0f} seconds.")
        return CACHED_TERMS

    logger.info("Cache is stale or empty. Running full data pipeline to fetch new data...")
    
    try:
        raw_data = fetch_housing_data()
        
        if not raw_data:
            # If fetching fails, we try to fall back to the old cache
            raise RuntimeError("Data fetching failed for all required metrics.")

        annual_records = _transform_abs_data(raw_data)
        
        if not annual_records:
            raise RuntimeError("Data transformation failed or resulted in zero records.")

        logger.info(f"Successfully generated {len(annual_records)} transformed annual data points.")

        terms_summary = calculate_government_terms(annual_records)

        # Sort the final output by average GPHI score (highest first = better performance)
        terms_summary.sort(key=lambda x: x.get('average_gphi_score', 0) if x.get('average_gphi_score') is not None else -float('inf'), reverse=True)

        # Update cache and timestamp
        CACHED_TERMS = terms_summary
        LAST_FETCH_TIME = time.time()
        logger.info(f"Cache updated successfully with {len(CACHED_TERMS)} government terms.")

        return CACHED_TERMS

    except Exception as e:
        logger.error(f"Critical error during data pipeline execution: {e}")
        
        # If the pipeline fails, return the old cached data if it exists, otherwise raise
        if CACHED_TERMS:
            logger.warning("Pipeline failed but returning stale data from cache to maintain service availability.")
            return CACHED_TERMS
        
        # If no cached data is available, raise an HTTP 500 error
        raise HTTPException(
            status_code=500, 
            detail=f"Data pipeline failed to initialize or fetch fresh data: {e}. Check server logs."
        )


# --- 5. FastAPI Endpoint ---

@app.get("/api/government_term", response_model=List[Dict[str, Any]])
def get_government_terms():
    """
    API endpoint to retrieve the calculated government performance in housing terms.
    Data is served from an in-memory cache, refreshing every 1 hour (3600 seconds).
    """
    return load_and_cache_data()