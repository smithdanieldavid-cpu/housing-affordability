from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import requests
import time
import os
from typing import List, Dict, Any, Optional

# -----------------------------------------------------------
# Logging
# -----------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------
# Constants
# -----------------------------------------------------------

ABS_API_BASE = "https://api.data.abs.gov.au/data"
MAX_RETRIES = 3
CACHE_TTL = 3600  # 1 hour

# --- Correct ABS Endpoints (UPDATED KEYS) ---
DATAFLOWS = {
    "RPPI": {
        "id": "ABS_RPPI_1.0.0",
        "key": "WGT.AUS.Q", # ✅ Corrected key for RPPI - All groups, Weighted, Australia
    },
    "CPI": {
        "id": "ABS_CPI_1.0.0",
        "key": "1.AUS.Q", # ✅ Corrected key for CPI - All groups, Index number
    },
}

# --- Government history ---
GOVERNMENT_HISTORY = [
    {'start_year': 2022, 'party': 'Labor'},
    {'start_year': 2013, 'party': 'Liberal/National'},
    {'start_year': 2007, 'party': 'Labor'},
    {'start_year': 1996, 'party': 'Liberal/National'},
]

# -----------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------

def _get_government_party(year: int) -> str:
    for g in reversed(GOVERNMENT_HISTORY):
        if year >= g['start_year']:
            return g['party']
    return "Unknown"


def calculate_gphi_score(avg_rppi: float, avg_cpi: float) -> float:
    raw = avg_rppi / avg_cpi
    return round(100 - (raw * 0.4), 2)


# -----------------------------------------------------------
# Fetch ABS Data
# -----------------------------------------------------------

def fetch_abs_data() -> Optional[Dict[str, Any]]:
    """
    Fetch RPPI + CPI from ABS API using modern SDMX endpoint structure.
    """
    combined = {"data": {}}

    for metric, cfg in DATAFLOWS.items():
        # URL format: BASE/ID/KEY?startPeriod=2000
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
# Transform → Annual Averages
# -----------------------------------------------------------

def transform_abs(abs_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts RPPI + CPI quarterly → annual averages.
    """

    if not abs_data or "data" not in abs_data:
        return []

    # Extract series structure info from the SDMX v2 JSON response
    try:
        rppi = abs_data["data"]["RPPI"]["data"]["observations"]
        rppi_time = abs_data["data"]["RPPI"]["data"]["structure"]["dimensions"]["observation"][0]["values"]

        cpi = abs_data["data"]["CPI"]["data"]["observations"]
        cpi_time = abs_data["data"]["CPI"]["data"]["structure"]["dimensions"]["observation"][0]["values"]
    except KeyError as e:
        logger.error(f"ABS data structure error during transform: Missing key {e}")
        return []

    quarterly: Dict[str, Dict[str, float]] = {}

    # RPPI
    for obs_key, obs_val in rppi.items():
        date = rppi_time[int(obs_key)]["id"]  # YYYY-QX
        value = obs_val[0]
        quarterly.setdefault(date, {})
        quarterly[date]["rppi"] = value

    # CPI
    for obs_key, obs_val in cpi.items():
        date = cpi_time[int(obs_key)]["id"]
        value = obs_val[0]
        quarterly.setdefault(date, {})
        quarterly[date]["cpi"] = value

    annual: Dict[int, Dict[str, Any]] = {}

    for period, vals in quarterly.items():
        year = int(period.split("-")[0])
        bucket = annual.setdefault(year, {
            "year": year,
            "rppi_total": 0,
            "cpi_total": 0,
            "rppi_count": 0,
            "cpi_count": 0,
        })

        if "rppi" in vals:
            bucket["rppi_total"] += vals["rppi"]
            bucket["rppi_count"] += 1

        if "cpi" in vals:
            bucket["cpi_total"] += vals["cpi"]
            bucket["cpi_count"] += 1

    final = []

    for year, a in annual.items():
        if a["rppi_count"] < 2 or a["cpi_count"] < 2:
            continue

        avg_rppi = a["rppi_total"] / a["rppi_count"]
        avg_cpi = a["cpi_total"] / a["cpi_count"]
        gphi = calculate_gphi_score(avg_rppi, avg_cpi)

        final.append({
            "year": year,
            "avg_rppi": round(avg_rppi, 2),
            "avg_cpi": round(avg_cpi, 2),
            "gphi_score": gphi,
            "government_party": _get_government_party(year),
        })

    final.sort(key=lambda x: x["year"])
    return final


# -----------------------------------------------------------
# Aggregate → Government Terms
# -----------------------------------------------------------

def calculate_government_terms(annual: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not annual:
        return []

    terms = []
    current = None

    for row in annual:
        party = row["government_party"]
        score = row["gphi_score"]

        if party == "Unknown":
            continue

        if not current or current["party"] != party:
            if current:
                terms.append(finish_term(current))
            current = {
                "party": party,
                "start_year": row["year"],
                "years": [],
                "total_gphi": 0,
            }

        current["years"].append(row)
        current["total_gphi"] += score

    if current:
        terms.append(finish_term(current))

    return terms


def finish_term(term: Dict[str, Any]) -> Dict[str, Any]:
    years = term["years"]
    avg = term["total_gphi"] / len(years)
    return {
        "government_name": f"{term['party']} ({years[0]['year']}-{years[-1]['year']})",
        "government_party": term["party"],
        "start_year": years[0]["year"],
        "end_year": years[-1]["year"],
        "duration_years": len(years),
        "average_gphi_score": round(avg, 2),
        "annual_metrics": years,
    }

# -----------------------------------------------------------
# Caching Layer
# -----------------------------------------------------------

CACHE = None
LAST_FETCH = 0

def load_data():
    global CACHE, LAST_FETCH

    if CACHE and (time.time() - LAST_FETCH) < CACHE_TTL:
        return CACHE

    raw = fetch_abs_data()
    if not raw:
        # Note: The original error handling was correct, using HTTPException for API
        raise HTTPException(status_code=500, detail="Failed to fetch ABS data")

    annual = transform_abs(raw)
    terms = calculate_government_terms(annual)

    terms.sort(key=lambda t: t["average_gphi_score"], reverse=True)

    CACHE = terms
    LAST_FETCH = time.time()

    return terms

# -----------------------------------------------------------
# FastAPI Setup
# -----------------------------------------------------------

app = FastAPI(title="Housing Affordability API", version="3.1.0") # Updated version number

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/government_term")
def api_government_term():
    return load_data()


# -----------------------------------------------------------
# CLI Build Support (GitHub Actions)
# -----------------------------------------------------------

def build_json():
    """Writes the JSON file for your frontend GitHub Pages dashboard."""
    os.makedirs("frontend/data", exist_ok=True)

    # Use a try/except for CLI calls that aren't inside the FastAPI context
    try:
        output = load_data()
    except HTTPException as e:
        logger.error(f"Failed to load data for JSON build: {e.detail}")
        raise RuntimeError("Data build failed due to API error.") from e

    path = "frontend/data/government_term.json"

    with open(path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✔ Exported → {path}")


if __name__ == "__main__":
    build_json()