import json
import logging
import requests
import time
from typing import List, Dict, Any, Optional

# ---- Import your scoring + term logic ----
from score_calculator import (
    calculate_gphi_score,
    calculate_government_terms,
    _get_government_party,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

ABS_API_BASE = "https://data.api.abs.gov.au/rest/data"
MAX_RETRIES = 3

DATAFLOWS = {
    "RPPI": {"id": "ABS,RPPI,1.0.0", "key": "1.2.10.100.Q"},
    "CPI": {"id": "ABS,CPI,1.1.0", "key": "1.1.10000.10.50.Q"},
}


# -----------------------------------------------------------
# 1. Fetch ABS data
# -----------------------------------------------------------
def fetch_abs_data() -> Optional[Dict[str, Any]]:
    combined = None

    for metric, cfg in DATAFLOWS.items():
        url = (
            f"{ABS_API_BASE}/{cfg['id']}/{cfg['key']}"
            f"?startPeriod=2000&format=jsondata&detail=full"
        )
        logger.info(f"Fetching {metric}…")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                data = r.json()

                # Annotate series
                for _, series in data.get("dataSets", [{}])[0].get("series", {}).items():
                    series["id_prefix"] = metric

                if combined is None:
                    combined = data
                else:
                    combined["dataSets"][0]["series"].update(
                        data["dataSets"][0]["series"]
                    )
                break

            except Exception as e:
                logger.error(f"{metric} attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)

    return combined


# -----------------------------------------------------------
# 2. Transform into annual metrics
# -----------------------------------------------------------
def transform_abs(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not data:
        return []

    series = data["dataSets"][0].get("series", {})
    dimensions = data["structure"]["dimensions"]["observation"]

    # TIME_PERIOD index
    time_index = next(
        (i for i, d in enumerate(dimensions) if d["id"] == "TIME_PERIOD"), None
    )
    if time_index is None:
        return []

    quarterly: Dict[str, Dict[str, float]] = {}

    for _, s in series.items():
        metric = s.get("id_prefix")
        if metric not in ("RPPI", "CPI"):
            continue

        key = "rppi_index" if metric == "RPPI" else "cpi_index"

        for obs_key, obs_val in s.get("observations", {}).items():
            period = dimensions[time_index]["values"][int(obs_key)]["id"]
            value = float(obs_val[0])

            quarterly.setdefault(period, {})[key] = value

    # Convert quarterly → annual
    annual: Dict[int, Dict[str, Any]] = {}

    for period, values in quarterly.items():
        year = int(period.split("-")[0])

        bucket = annual.setdefault(
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

        if "rppi_index" in values:
            bucket["rppi_total"] += values["rppi_index"]
            bucket["rppi_count"] += 1

        if "cpi_index" in values:
            bucket["cpi_total"] += values["cpi_index"]
            bucket["cpi_count"] += 1

    final = []
    for year, d in annual.items():
        if d["rppi_count"] < 2 or d["cpi_count"] < 2:
            continue

        avg_rppi = d["rppi_total"] / d["rppi_count"]
        avg_cpi = d["cpi_total"] / d["cpi_count"]

        final.append(
            {
                "year": year,
                "avg_rppi_index": round(avg_rppi, 2),
                "avg_cpi_index": round(avg_cpi, 2),
                "gphi_score": calculate_gphi_score(avg_rppi, avg_cpi),
                "government_party": d["government_party"],
            }
        )

    final.sort(key=lambda x: x["year"])
    return final


# -----------------------------------------------------------
# 3. Build JSON file
# -----------------------------------------------------------
def build_json():
    logger.info("Fetching ABS data…")
    raw = fetch_abs_data()
    annual = transform_abs(raw)

    logger.info("Aggregating government terms…")
    terms = calculate_government_terms(annual)

    # Sort terms by score descending
    terms.sort(key=lambda t: t.get("average_gphi_score", 0), reverse=True)

    # Save JSON
    output_path = "data/government_term.json"
    with open(output_path, "w") as f:
        json.dump(terms, f, indent=2)

    logger.info(f"Data written → {output_path}")


if __name__ == "__main__":
    build_json()
