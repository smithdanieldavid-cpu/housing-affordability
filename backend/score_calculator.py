from typing import List, Optional, Dict, Any

# --- Government History for Scoring ---
# This defines the political terms used for aggregating the data.
# The data is tracked by the party that held government at the beginning of the year.
GOVERNMENT_HISTORY = [
    # Starts are based on the year the government first took office in a new term.
    {'start_year': 2022, 'party': 'Labor'}, 
    {'start_year': 2013, 'party': 'Liberal/National'}, 
    {'start_year': 2007, 'party': 'Labor'},
    {'start_year': 1996, 'party': 'Liberal/National'}, # Covers the 2000s
]


def _get_government_party(year: int) -> str:
    """
    Finds the party in power for a given year based on the predefined start years.
    This function is imported and used by the parsing logic in main.py.
    """
    for i in range(len(GOVERNMENT_HISTORY) - 1, -1, -1):
        if year >= GOVERNMENT_HISTORY[i]['start_year']:
            return GOVERNMENT_HISTORY[i]['party']
    return 'Unknown' 


def calculate_gphi_score(avg_rppi: float, avg_cpi: float) -> float:
    """
    Calculates the Good-Government Price-to-Income Index (GPHI) score.
    Higher score indicates better affordability.
    
    The score is inverted (100 - X) so that a lower raw ratio (RPPI / CPI) 
    results in a higher final score.
    """
    # Calculate the raw ratio: Property Index change relative to Income Proxy change (CPI)
    raw_ratio = avg_rppi / avg_cpi
    
    # Scaling Factor (0.4) is used to map the ratio to a 0-100 score range.
    gphi_score = round(100 - (raw_ratio * 0.4), 2)
    
    return gphi_score


def finalize_term(term: Dict[str, Any]) -> Dict[str, Any]:
    """Calculates final summary statistics (average GPHI) for a completed political term."""
    term_data = term['annual_metrics']
    count = len(term_data)

    if count == 0:
        return {}

    avg_gphi = term['total_gphi_score'] / count

    return {
        'party': term['party'],
        'start_year': term['start_year'],
        # Find the max year in the annual metrics for the end year
        'end_year': max(a['year'] for a in term_data),
        'duration_years': len(term_data),
        'average_gphi_score': round(avg_gphi, 2),
        'annual_metrics': term_data,
    }

def calculate_government_terms(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregates the raw annual time-series data into continuous government terms."""
    if not raw_data:
        return []

    sorted_data = sorted(raw_data, key=lambda x: x['year'])

    terms: List[Dict[str, Any]] = []
    current_term: Optional[Dict[str, Any]] = None

    for row in sorted_data:
        party = row.get('government_party')
        gphi_score = row.get('gphi_score', 0.0)

        if party and gphi_score > 0:
            if not current_term or current_term['party'] != party:
                # Finalize the previous term
                if current_term and len(current_term['annual_metrics']) > 0:
                    terms.append(finalize_term(current_term))

                # Start a new term
                current_term = {
                    'party': party,
                    'start_year': row['year'],
                    'annual_metrics': [],
                    'total_gphi_score': 0.0,
                }
            
            # Add the current year's data
            current_term['annual_metrics'].append(row)
            current_term['total_gphi_score'] += gphi_score
        else:
            # End the term if the party or score is missing/unknown
            if current_term and len(current_term['annual_metrics']) > 0:
                terms.append(finalize_term(current_term))
                current_term = None 

    # Finalize the last term
    if current_term and len(current_term['annual_metrics']) > 0:
        terms.append(finalize_term(current_term))
        
    # Return only non-empty, finalized terms
    return [term for term in terms if term]