from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import requests
import time
from typing import List, Dict, Any, Optional

from score_calculator import (
    calculate_gphi_score,
    calculate_government_terms,
    _get_government_party,
)

# ============================================================
# 1. Logging Configuration
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================
# 2. Constants / Config
# ============================================================
ABS_API_BASE_URL = "https://data.api.abs.gov.au/rest/data"
MAX_RETRIES = 3
CACHE_TIMEOUT_SECONDS = 3600  # 1 hour

DATAFLOWS = {
    "RPPI": {"id": "ABS,RPPI,1.0.0", "key": "1.2.10.100.Q"},
    "CPI": {"id": "ABS,CPI,1.1.0", "key": "1.1.10000.10.50.Q"},
}

# ============================================================
# 3. FastAPI + CORS
# ============================================================
app = FastAPI(
    title="Housing Affordability API",
    version="1.0.0",
)

ALLOWED_ORIGINS = [
    "https://affordable-housing.com.au",
    "https://smithdanieldavid-cpu.github.io",
    "http://localhost",
    "http://localhost:5500",
    "http://localhost:8000",
    "http://127.0.0.1:5500",
    "http://127.0.0.1:8000",
    "*",  # dev only
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ============================================================
# 4. Cache
# ============================================================
CACHED_TERMS: List[Dict[str, Any]] = []
LAST_FETCH_TIME: float = 0.0

# ============================================================
# 5. SDMX Parsing Logic
# ============================================================
def _transform_abs_data(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts the complex SDMX-JSON response into annual aggregates and GPHI scores.
    """
    logger.info("Beginning SDMX transformation...")

    datasets = raw_data.get("dataSets", [])
    if not datasets:
        logger.error("Missing 'dataSets' key in SDMX response.")
        return []

    series_data = datasets[0].get("series", {})
    if not series_data:
        logger.error("SDMX response contains no 'series' data.")
        return []

    # Identify TIME_PERIOD index
    time_dim_index = next(
        (i for i, dim in enumerate(raw_data.get("structure", {})
                                              .get("dimensions", {})
                                              .get("observation", [])
                                  ) if dim.get("id") == "TIME_PERIOD"),
        -1
    )

    if time_dim_index == -1:
        logger.error("TIME_PERIOD dimension not found in SDMX structure.")
        return []

    quarterly_data: Dict[str, Dict[str, float]] = {}

    # Parse every series → quarter → observation
    for series_key, series_value in series_data.items():
        metric = series_value.get("id_prefix")
        if not metric:
            continue

        metric_field = "rppi_index" if metric == "RPPI" else "cpi_index"

        for obs_key, obs_values in series_value.get("observations", {}).items():
            try:
                value = float(obs_values[0])
                period = raw_data["structure"]["dimensions"]["observation"][time_dim_index]["values"][int(obs_key)]["id"]

                quarterly_data.setdefault(period, {})[metric_field] = value

            except Exception as e:
                logger.warning(f"Skipping malformed observation ({obs_key}): {e}")

    # Aggregate quarterly → annual
    annual: Dict[int, Dict[str, Any]] = {}

    for period, metrics in quarterly_data.items():
        try:
            year = int(period.split("-")[0])
        except ValueError:
            continue

        annual.setdefault(year, {
            "year": year,
            "rppi_total": 0.0, "rppi_count": 0,
            "cpi_total": 0.0, "cpi_count": 0,
            "government_party": _get_government_party(year)
        })

        if "rppi_index" in metrics:
            annual[year]["rppi_total"] += metrics["rppi_index"]
            annual[year]["rppi_count"] += 1
        if "cpi_index" in metrics:
            annual[year]["cpi_total"] += metrics["cpi_index"]
            annual[year]["cpi_count"] += 1

    # Finalize + compute GPHI
    final_records: List[Dict[str, Any]] = []

    for year, info in annual.items():
        if info["rppi_count"] < 2 or info["cpi_count"] < 2:
            logger.info(f"Skipping year {year}: insufficient quarterly data.")
            continue

        avg_rppi = info["rppi_total"] / info["rppi_count"]
        avg_cpi = info["cpi_total"] / info["cpi_count"]

        gphi = calculate_gphi_score(avg_rppi, avg_cpi)

        final_records.append({
            "year": year,
            "avg_rppi_index": round(avg_rppi, 2),
            "avg_cpi_index": round(avg_cpi, 2),
            "gphi_score": gphi,
            "government_party": info["government_party"],
        })

    logger.info(f"SDMX transformation complete: {len(final_records)} records generated.")
    return final_records

# ============================================================
# 6. ABS Data Fetching (Retry + Merge)
# ============================================================
def fetch_housing_data() -> Optional[Dict[str, Any]]:
    combined: Dict[str, Any] = {}

    for metric, config in DATAFLOWS.items():
        api_url = f"{ABS_API_BASE_URL}/{config['id']}/{config['key']}?startPeriod=2000&format=jsondata&detail=full"
        logger.info(f"Fetching {metric} data from ABS...")

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()

                data = response.json()
                logger.info(f"{metric} received (attempt {attempt+1})")

                # Add prefix tag so parser knows which metric it belongs to
                for key, series in data.get("dataSets", [{}])[0].get("series", {}).items():
                    series["id_prefix"] = metric

                # First dataset becomes base
                if not combined:
                    combined = data
                else:
                    # merge series
                    combined["dataSets"][0]["series"].update(
                        data["dataSets"][0]["series"]
                    )

                break  # exit retry loop

            except Exception as e:
                logger.error(f"{metric} fetch failed (attempt {attempt+1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    delay = 2 ** attempt
                    logger.info(f"Retrying in {delay}s...")
                    time.sleep(delay)

        else:
            logger.error(f"FAILED to fetch {metric} after {MAX_RETRIES} attempts")

    if not combined:
        logger.error("Could not fetch any ABS metrics.")
        return None

    return combined

# ============================================================
# 7. Main Pipeline + Cache
# ============================================================
def load_and_cache_data() -> List[Dict[str, Any]]:
    global CACHED_TERMS, LAST_FETCH_TIME

    # Serve from cache if fresh
    if CACHED_TERMS and (time.time() - LAST_FETCH_TIME) < CACHE_TIMEOUT_SECONDS:
        remaining = CACHE_TIMEOUT_SECONDS - (time.time() - LAST_FETCH_TIME)
        logger.info(f"Serving cached data. Next refresh in {remaining:.0f}s.")
        return CACHED_TERMS

    logger.info("Cache expired or empty — refreshing...")

    try:
        raw = fetch_housing_data()
        if not raw:
            raise RuntimeError("ABS fetch produced no data.")

        annual_records = _transform_abs_data(raw)
        if not annual_records:
            raise RuntimeError("Data transformation produced zero records.")

        terms = calculate_government_terms(annual_records)

        # Sort by score descending (better = higher)
        terms.sort(
            key=lambda row: row.get("average_gphi_score", float("-inf")),
            reverse=True
        )

        CACHED_TERMS = terms
        LAST_FETCH_TIME = time.time()

        logger.info(f"Cache updated with {len(terms)} terms.")
        return terms

    except Exception as e:
        logger.error(f"Pipeline failure: {e}")

        if CACHED_TERMS:
            logger.warning("Returning stale cached data.")
            return CACHED_TERMS

        raise HTTPException(
            status_code=500,
            detail=f"Critical failure: could not load data. {e}"
        )

# ============================================================
# 8. API Endpoint
# ============================================================
@app.get("/api/government_term")
def get_government_terms():
    return load_and_cache_data()
