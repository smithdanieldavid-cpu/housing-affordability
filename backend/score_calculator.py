from typing import List, Dict, Any

# --- Government History for Scoring ---
GOVERNMENT_HISTORY = [
    {'start_year': 2022, 'party': 'Labor'},
    {'start_year': 2013, 'party': 'Liberal/National'},
    {'start_year': 2007, 'party': 'Labor'},
    {'start_year': 1996, 'party': 'Liberal/National'},
]

# -----------------------------------------------------------
# 1. Government Party Lookup
# -----------------------------------------------------------

def _get_government_party(year: int) -> str:
    """Return the party governing in a given year based on history."""
    for entry in GOVERNMENT_HISTORY:
        if year >= entry["start_year"]:
            return entry["party"]
    return "Unknown"


# -----------------------------------------------------------
# 2. GPHi Score Calculation
# -----------------------------------------------------------

def calculate_gphi_score(avg_rppi: float, avg_cpi: float) -> float:
    """
    Calculates the Government Housing Performance Index (GPHI) score.
    Formula: 100 - (Ratio * 0.4).
    """
    if avg_cpi <= 0:
        return 0.0

    raw = avg_rppi / avg_cpi
    return round(100 - (raw * 0.4), 2)


# -----------------------------------------------------------
# 3. Group into Government Terms
# -----------------------------------------------------------

def calculate_government_terms(annual: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Groups rows by government party → computes averages for each term.
    """
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
                terms.append(_finish_term(current))
            current = {
                "party": party,
                "start_year": row["year"],
                "years": [],
                "total_gphi": 0,
            }

        current["years"].append(row)
        current["total_gphi"] += score

    if current:
        terms.append(_finish_term(current))

    return terms


def _finish_term(term: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to finalize a term dictionary."""
    years = term["years"]

    if not years:
        return {}

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