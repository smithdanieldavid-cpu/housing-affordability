from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import requests
import time
from typing import List, Dict, Any, Optional

# --- Scoring & Government Term Logic ---
from score_calculator import (
    calculate_gphi_score,
    calculate_government_terms,
    _get_government_party,
)

# -----------------------------------------------------------
# 1. Logging Configuration
# -----------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------
# 2. Constants
# -----------------------------------------------------------

ABS_API_BASE = "https://data.api.abs.gov.au/rest/data"
MAX_RETRIES = 3
CACHE_TTL = 3600  # 1 hour cache timeout

DATAFLOWS = {
    "RPPI": {
        "id": "ABS,RPPI,1.0.0",
        "key": "1.2.10.100.Q",
    },
    "CPI": {
        "id": "ABS,CPI,1.1.0",
        "key": "1.1.10000.10.50.Q",
    },
}

# -----------------------------------------------------------
# 3. FastAPI Setup & CORS
# -----------------------------------------------------------

app = FastAPI(title="Housing Affordability API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://affordable-housing.com.au",
        "https://smithdanieldavid-cpu.github.io",
        "http://localhost",
        "http://localhost:8000",
        "http://localhost:5500",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# -----------------------------------------------------------
# 4. Cache
# -----------------------------------------------------------

CACHE: List[Dict[str, Any]] = []
LAST_FETCH = 0.0

# -----------------------------------------------------------
# 5. Fetch Data from ABS
# -----------------------------------------------------------

def fetch_abs_data() -> Optional[Dict[str, Any]]:
    """Fetches and merges SDMX-JSON for RPPI and CPI."""
    combined = None

    for metric, cfg in DATAFLOWS.items():
        url = (
            f"{ABS_API_BASE}/{cfg['id']}/{cfg['key']}"
            f"?startPeriod=2000&format=jsondata&detail=full"
        )
        logger.info(f"Fetching {metric} from ABS…")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                data = r.json()

                # Add id_prefix to each series
                for _, series in data.get("dataSets", [{}])[0].get("series", {}).items():
                    series["id_prefix"] = metric

                # Merge structures
                if combined is None:
                    combined = data
                else:
                    combined_series = combined["dataSets"][0]["series"]
                    new_series = data["dataSets"][0]["series"]
                    combined_series.update(new_series)

                break  # success → break retry loop

            except Exception as e:
                logger.error(f"{metric} attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {metric} after {MAX_RETRIES} attempts")

    return combined

# -----------------------------------------------------------
# 6. Transform SDMX → Annual Data
# -----------------------------------------------------------

def transform_abs(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts quarterly SDMX data and produces annual averages."""
    if not data:
        logger.error("No ABS data provided.")
        return []

    series = data["dataSets"][0].get("series", {})
    dimensions = data["structure"]["dimensions"]["observation"]

    # Locate TIME_PERIOD index
    time_index = next(
        (i for i, d in enumerate(dimensions) if d["id"] == "TIME_PERIOD"),
        None,
    )
    if time_index is None:
        logger.error("TIME_PERIOD missing in SDMX structure.")
        return []

    quarterly: Dict[str, Dict[str, float]] = {}

    # Loop all RPPI + CPI series
    for _, s in series.items():
        metric = s.get("id_prefix")
        if metric not in ("RPPI", "CPI"):
            continue

        metric_key = "rppi_index" if metric == "RPPI" else "cpi_index"

        for obs_key, obs_val in s.get("observations", {}).items():
            period_id = dimensions[time_index]["values"][int(obs_key)]["id"]
            value = float(obs_val[0])

            if period_id not in quarterly:
                quarterly[period_id] = {}

            quarterly[period_id][metric_key] = value

    # Aggregate quarterly → annual
    annual: Dict[int, Dict[str, Any]] = {}

    for period, metrics in quarterly.items():
        year = int(period.split("-")[0])

        year_bucket = annual.setdefault(
            year,
            {
                "year": year,
                "rppi_total": 0,
                "cpi_total": 0,
                "rppi_count": 0,
                "cpi_count": 0,
                "government_party": _get_government_party(year),
            },
        )

        if "rppi_index" in metrics:
            year_bucket["rppi_total"] += metrics["rppi_index"]
            year_bucket["rppi_count"] += 1

        if "cpi_index" in metrics:
            year_bucket["cpi_total"] += metrics["cpi_index"]
            year_bucket["cpi_count"] += 1

    # Final annual output
    final = []
    for year, d in annual.items():
        if d["rppi_count"] < 2 or d["cpi_count"] < 2:
            continue  # skip incomplete years

        avg_rppi = d["rppi_total"] / d["rppi_count"]
        avg_cpi = d["cpi_total"] / d["cpi_count"]
        gphi = calculate_gphi_score(avg_rppi, avg_cpi)

        final.append(
            {
                "year": year,
                "avg_rppi_index": round(avg_rppi, 2),
                "avg_cpi_index": round(avg_cpi, 2),
                "gphi_score": gphi,
                "government_party": d["government_party"],
            }
        )

    final.sort(key=lambda x: x["year"])
    logger.info(f"Generated {len(final)} annual records.")
    return final

# -----------------------------------------------------------
# 7. Cache Wrapper
# -----------------------------------------------------------

def load_data() -> List[Dict[str, Any]]:
    global CACHE, LAST_FETCH

    if CACHE and (time.time() - LAST_FETCH) < CACHE_TTL:
        return CACHE

    raw = fetch_abs_data()
    if not raw:
        raise HTTPException(500, "Failed to fetch ABS data")

    annual = transform_abs(raw)
    if not annual:
        raise HTTPException(500, "ABS transformation failed")

    terms = calculate_government_terms(annual)
    terms.sort(key=lambda t: t.get("average_gphi_score", 0), reverse=True)

    CACHE = terms
    LAST_FETCH = time.time()
    return terms

# -----------------------------------------------------------
# 8. API Endpoint
# -----------------------------------------------------------

@app.get("/api/government_term")
def get_terms():
    return load_data()
