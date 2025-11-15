import json
import logging
import requests
import time
from typing import List, Dict, Any

# Import the scoring and aggregation functions from the new file
from score_calculator import calculate_gphi_score, calculate_government_terms, _get_government_party

# --- 1. Configuration and Global Constants ---

# Configure logging to display INFO level messages
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Output file path for the aggregated and scored data
OUTPUT_FILENAME = "processed_terms.json"

# API Configuration
# ABS Data API (SDMX) endpoint for data retrieval
ABS_API_BASE_URL = "https://data.api.abs.gov.au/rest/data"
MAX_RETRIES = 3

# --- SDMX Query Parameters ---
# Defined Dataflows and their specific SDMX keys for the desired time series.
DATAFLOWS = {
    # RPPI: Residential Property Price Index, Weighted Average of 8 Capital Cities
    "RPPI": {"id": "ABS,RPPI,1.0.0", "key": "1.2.10.100.Q"}, 
    
    # CPI: All Groups Consumer Price Index, Weighted Average of 8 Capital Cities
    "CPI": {"id": "ABS,CPI,1.1.0", "key": "1.1.10000.10.50.Q"},
}


# --- 2. Data Processing and Transformation Functions ---

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
        metric_name = series_value.get('id_prefix') # Added in fetch_housing_data
        if not metric_name:
            continue
            
        metric_field = 'rppi_index' if metric_name == 'RPPI' else 'cpi_index'
            
        observations = series_value.get('observations', {})
        for time_key, obs_data in observations.items():
            # The value is at index 0 of the observation array
            value = float(obs_data[0]) 

            # Look up the actual time period (e.g., '2022-Q1') using the time_key index
            time_period = raw_data['structure']['dimensions']['observation'][time_dimension_index]['values'][int(time_key)]['id']

            if time_period not in quarterly_data:
                quarterly_data[time_period] = {}

            quarterly_data[time_period][metric_field] = value

    # 2. Convert Quarterly Data to Annual Averages
    annual_data: Dict[int, Dict[str, Any]] = {}

    for time_period, metrics in quarterly_data.items():
        try:
            # Extract year from format YYYY-QX
            year = int(time_period.split('-')[0])
            
            if year not in annual_data:
                # Initialize annual record
                annual_data[year] = {
                    'year': year,
                    'rppi_total': 0.0,
                    'cpi_total': 0.0,
                    'rppi_count': 0,
                    'cpi_count': 0,
                    # Get government party from the imported function
                    'government_party': _get_government_party(year) 
                }
            
            # Sum up the quarterly values
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
        # Require a minimum of 2 quarters of data for a reliable annual average
        if data['rppi_count'] >= 2 and data['cpi_count'] >= 2:
            avg_rppi = data['rppi_total'] / data['rppi_count']
            avg_cpi = data['cpi_total'] / data['cpi_count']
            
            # --- Call the external scoring function ---
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


def fetch_housing_data() -> List[Dict[str, Any]]:
    """
    Fetches raw housing and economic data using the ABS Data API. 
    It makes sequential calls for RPPI and CPI and merges the results.
    """
    combined_raw_data: Dict[str, Any] = {}
    
    for metric_name, config in DATAFLOWS.items():
        dataflow_id = config['id']
        data_key = config['key']
        
        # Request data from 2000 to present, full detail, JSON format.
        api_url = f"{ABS_API_BASE_URL}/{dataflow_id}/{data_key}?startPeriod=2000&format=jsondata&detail=full"
        
        logger.info(f"Attempting to fetch {metric_name} data from ABS API at: {dataflow_id}")

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status() 

                raw_api_data = response.json()
                logger.info(f"Successfully received {metric_name} data on attempt {attempt + 1}.")
                
                # Merge logic: SDMX format is tricky; we combine the series into one dictionary.
                if not combined_raw_data:
                    # First metric: store the whole structure
                    combined_raw_data = raw_api_data
                else:
                    # Subsequent metrics: merge the new series into the existing dataSets[0].series
                    new_series = raw_api_data.get('dataSets', [{}])[0].get('series', {})
                    combined_raw_data['dataSets'][0]['series'].update(new_series)
                
                # Add an identifier to the series keys to help the parser distinguish RPPI vs CPI
                for key, series_data in combined_raw_data['dataSets'][0]['series'].items():
                    if 'id_prefix' not in series_data:
                        if 'RPPI' in dataflow_id:
                             series_data['id_prefix'] = 'RPPI'
                        elif 'CPI' in dataflow_id:
                             series_data['id_prefix'] = 'CPI'
                
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
            logger.error(f"Failed to fetch {metric_name} data after {MAX_RETRIES} attempts. Cannot proceed.")
            return [] 

    # Transform the combined raw data
    return _transform_abs_data(combined_raw_data)


# --- 3. Main Workflow ---

def main():
    """The main execution flow: fetch, process, sort, and save the data."""
    try:
        # 1. Fetch raw data from ABS and transform it
        raw_data = fetch_housing_data()
        logger.info(f"Successfully loaded {len(raw_data)} transformed annual data points.")

        # 2. Score and calculate aggregated terms (using imported function)
        terms_summary = calculate_government_terms(raw_data)

        if not terms_summary:
            logger.warning("No complete government terms were generated from the data.")
        
        # 3. Sort the final output by average GPHI score (highest first = better performance)
        terms_summary.sort(key=lambda x: x.get('average_gphi_score', 0), reverse=True)

        # 4. Write to JSON file
        with open(OUTPUT_FILENAME, 'w') as f:
            json.dump(terms_summary, f, indent=4)
        
        logger.info(f"Successfully processed and saved {len(terms_summary)} government terms to {OUTPUT_FILENAME}")

    except Exception as e:
        logger.error(f"A critical error occurred during the data pipeline execution: {e}")


# --- 4. Execution Entry Point ---

if __name__ == "__main__":
    main()