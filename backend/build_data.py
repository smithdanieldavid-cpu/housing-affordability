import json
import logging
import httpx as requests
import time
import os
from typing import List, Dict, Any, Optional

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# CRITICAL FIX: Set to the official, current ABS API base URL.
ABS_API_BASE = "https://data.api.abs.gov.au/rest/data"
MAX_RETRIES = 3

# --- UPDATED: Dataflow IDs changed to official ABS Publication Numbers ---
DATAFLOWS = {
    "RPPI": {
        # REPLACED: Residential Property Price Indexes (6416.0) was discontinued in Dec Qtr 2021.
        # Now using the replacement: Total Value of Dwellings (same publication number 6416.0).
        "id": "6416.0",
        # Key for: Mean Price of Residential Dwellings, Australia, Quarterly.
        # This replaces the RPPI index with a direct measure of average price.
        "key": "M1.AUS.Q", 
    },
    "CPI": {
        # Consumer Price Index, Australia (Publication 6401.0)
        "id": "6401.0",
        "key": "1.AUS.Q", # Key for: All Groups CPI, Australia, Quarterly
    },
}

GOVERNMENT_HISTORY = [
    {'start_year': 2022, 'party': 'Labor'},
    {'start_year': 2013, 'party': 'Liberal/National'},
    {'start_year': 2007, 'party': 'Labor'},
    {'start_year': 1996, 'party': 'Liberal/National'},
]

# ---- Import scoring and term logic from score_calculator.py ----
from score_calculator import calculate_gphi_score, calculate_government_terms

# -----------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------

def _get_government_party(year: int) -> str:
    """Determines the governing party for a given year."""
    for g in reversed(GOVERNMENT_HISTORY):
        if year >= g['start_year']:
            return g['party']
    return "Unknown"

# -----------------------------------------------------------
# 1. Fetch ABS Data
# -----------------------------------------------------------

def fetch_abs_data() -> Optional[Dict[str, Any]]:
    """Fetch RPPI (now Mean Dwelling Price) + CPI from ABS API using the required endpoint structure."""

    CORRECT_BASE_URL = ABS_API_BASE

    combined = {"data": {}}

    for metric, cfg in DATAFLOWS.items():

        # The URL now uses the publication ID as the dataflow ID
        url = f"{CORRECT_BASE_URL}/{cfg['id']}/{cfg['key']}?startPeriod=2000&format=jsondata"
        logger.info(f"Fetching {metric} → {url}")

        success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Use httpx.get with follow_redirects=True for robust network access
                r = requests.get(url, timeout=20, follow_redirects=True)
                r.raise_for_status()
                payload = r.json()

                combined["data"][metric] = payload
                success = True
                break

            except Exception as e:
                logger.error(f"{metric} fetch failed (attempt {attempt}): {e} for url: {url}")
                if attempt < MAX_RETRIES:
                    delay = 2 ** attempt
                    logger.info(f"Retrying in {delay}s...")
                    time.sleep(delay)

        if not success:
            logger.error(f"FAILED to fetch {metric} after {MAX_RETRIES} attempts")
            return None

    return combined

# -----------------------------------------------------------
# 2. Transform → Annual Averages
# -----------------------------------------------------------

def transform_abs(abs_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Converts RPPI + CPI quarterly → annual averages and computes GPHI score."""
    if not abs_data or "data" not in abs_data:
        return []

    # Extract series structure info from the SDMX JSON response
    try:
        rppi = abs_data["data"]["RPPI"]["data"]["observations"]
        rppi_time = abs_data["data"]["RPPI"]["data"]["structure"]["dimensions"]["observation"][0]["values"]
        cpi = abs_data["data"]["CPI"]["data"]["observations"]
        cpi_time = abs_data["data"]["CPI"]["data"]["structure"]["dimensions"]["observation"][0]["values"]
    except KeyError as e:
        logger.error(f"ABS data structure error: Missing key {e}")
        return []

    quarterly: Dict[str, Dict[str, float]] = {}

    # Aggregate quarterly data points
    for obs_key, obs_val in rppi.items():
        date = rppi_time[int(obs_key)]["id"]
        # IMPORTANT: The data returned from the API is a string, so we must convert it to a float.
        # This is a common requirement when working with ABS JSON API data.
        quarterly.setdefault(date, {})["rppi"] = float(obs_val[0])

    for obs_key, obs_val in cpi.items():
        date = cpi_time[int(obs_key)]["id"]
        quarterly.setdefault(date, {})["cpi"] = float(obs_val[0])

    # Convert quarterly → annual totals
    annual: Dict[int, Dict[str, Any]] = {}

    for period, vals in quarterly.items():
        year = int(period.split("-")[0])
        bucket = annual.setdefault(year, {
            "year": year, "rppi_total": 0, "cpi_total": 0,
            "rppi_count": 0, "cpi_count": 0,
        })

        if "rppi" in vals:
            bucket["rppi_total"] += vals["rppi"]
            bucket["rppi_count"] += 1
        if "cpi" in vals:
            bucket["cpi_total"] += vals["cpi"]
            bucket["cpi_count"] += 1

    # Calculate final annual averages and GPHI scores
    final = []
    for year, a in annual.items():
        # Require at least two quarters of data to calculate a meaningful annual average
        if a["rppi_count"] < 2 or a["cpi_count"] < 2:
            continue

        avg_rppi = a["rppi_total"] / a["rppi_count"]
        avg_cpi = a["cpi_total"] / a["cpi_count"]

        final.append({
            "year": year,
            "avg_rppi": round(avg_rppi, 2),
            "avg_cpi": round(avg_cpi, 2),
            "gphi_score": calculate_gphi_score(avg_rppi, avg_cpi),
            "government_party": _get_government_party(year),
        })

    final.sort(key=lambda x: x["year"])
    return final


# -----------------------------------------------------------
# 3. Build JSON file
# -----------------------------------------------------------
def build_json():
    """Fetches, processes, and writes the final JSON file for the frontend."""
    logger.info("Starting data build process...")
    # NOTE: Assuming 'frontend/data' structure exists or is created by the environment/caller script
    # os.makedirs("frontend/data", exist_ok=True) # Commented out as per instructions, assuming caller handles file paths

    raw = fetch_abs_data()
    if not raw:
        logger.error("Build failed: Could not fetch raw data from ABS.")
        # Removed the 'raise RuntimeError' as per general guidelines to avoid crashing the whole build process.
        # However, for this specific critical function, I'll keep the raise to signal a hard failure.
        raise RuntimeError("Data build failed due to API error.")

    annual = transform_abs(raw)
    logger.info("Aggregating government terms…")
    terms = calculate_government_terms(annual)

    terms.sort(key=lambda t: t.get("average_gphi_score", 0), reverse=True)

    # Use a relative path assuming the script is run from a root directory
    output_path = "frontend/data/government_term.json"
    
    # Ensure directory exists before writing
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(terms, f, indent=2)

    logger.info(f"Data written → {output_path}")


if __name__ == "__main__":
    build_json()