from typing import Optional

import requests
from courses.config import API_HEADERS, API_URL, QUERY_ROWS


def fetch_course_data(start: int = 0, max_rows: int = QUERY_ROWS) -> Optional[dict]:
    """Fetches course data from the API."""
    params = {
        "query": f"rows={max_rows}&facet=true&facet.mincount=1&json.nl=map&start={start}"
    }
    try:
        response = requests.get(API_URL, headers=API_HEADERS, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
