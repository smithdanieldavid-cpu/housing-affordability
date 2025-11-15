import json
import logging
import requests
import time
import os
from typing import List, Dict, Any, Optional

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

ABS_API_BASE = "https://api.data.abs.gov.au/data"
MAX_RETRIES = 3

# --- CORRECT ABS SDMX Endpoints (Matching previous fix) ---
DATAFLOWS = {
    "RPPI": {
        "id": "ABS_RPPI_1.0.0",
        "key": "WGT.AUS.Q",  # RPPI – All groups, Weighted, Australia
    },
    "CPI": {
        "id": "ABS_CPI_1.0.0",
        "key": "1.AUS.Q",  # CPI – All groups, Index number
    },
}

GOVERNMENT_HISTORY = [
    {'start_year': 2022, 'party': 'Labor'},
    {'start_year': 2013, 'party': 'Liberal/National'},
    {'start_year': 2007, 'party': 'Labor'},
    {'start_year': 1996, 'party': 'Liberal/National'},
]

# ---- Import scoring and term logic from score_calculator.py ----
# NOTE: Ensure score_calculator.py is updated as provided in the last step!
from score_calculator import calculate_gphi_score, calculate_government_terms, _finish_term

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
    """Fetch RPPI + CPI from ABS API using modern SDMX v2 endpoint structure."""
    combined = {"data": {}}

    for metric, cfg in DATAFLOWS.items():
        url = f"{ABS_API_BASE}/{cfg['id']}/{cfg['key']}?startPeriod=2000"
        logger.info(f"Fetching {metric} → {url}")

        success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                payload = r.json()

                combined["data"][metric] = payload
                success = True
                break

            except Exception as e:
                logger.error(f"{metric} attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)

        if not success:
            logger.error(f"FAILED to fetch: {metric}")
            return None

    return combined


# -----------------------------------------------------------
# 2. Transform → Annual Averages
# -----------------------------------------------------------

def transform_abs(abs_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Converts RPPI + CPI quarterly → annual averages and computes GPHI score."""
    if not abs_data or "data" not in abs_data:
        return []

    # Extract series structure info from the SDMX v2 JSON response
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
        quarterly.setdefault(date, {})["rppi"] = obs_val[0]

    for obs_key, obs_val in cpi.items():
        date = cpi_time[int(obs_key)]["id"]
        quarterly.setdefault(date, {})["cpi"] = obs_val[0]

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
    os.makedirs("frontend/data", exist_ok=True)

    raw = fetch_abs_data()
    if not raw:
        logger.error("Build failed: Could not fetch raw data from ABS.")
        raise RuntimeError("Data build failed due to API error.")

    annual = transform_abs(raw)
    logger.info("Aggregating government terms…")
    terms = calculate_government_terms(annual)

    terms.sort(key=lambda t: t.get("average_gphi_score", 0), reverse=True)

    output_path = "frontend/data/government_term.json"
    with open(output_path, "w") as f:
        json.dump(terms, f, indent=2)

    logger.info(f"Data written → {output_path}")


if __name__ == "__main__":
    build_json()