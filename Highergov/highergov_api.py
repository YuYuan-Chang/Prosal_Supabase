# Regular imports
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import unicodedata

# Library imports
from itertools import chain

import pandas as pd
import json

# ---------------- Global Constants ----------------
DEFAULT_PAGE_SIZE = 25
DEFAULT_MAX_PAGE_NUMBER = 1

HIGHERGOV_KEY = "ddc5a2a906de4dc2980c5c727a3ecf37"

BASE_URL = "https://www.highergov.com"


def call_endpoint(endpoint: str, params: dict) -> dict:
    """
    Helper function to call the API endpoint.

    Args:
        endpoint: API endpoint path (e.g. "/api-external/agency/")
        params: Dictionary of query parameters.

    Returns:
        Parsed JSON response.

    Raises:
        requests.HTTPError if the response status is not 200.
    """
    url = BASE_URL + endpoint
    # Remove any parameters that are None.
    clean_params = {k: v for k, v in params.items() if v is not None}
    response = requests.get(url, params=clean_params)
    response.raise_for_status()
    return response.json()

def get_opportunities(
    api_key: str,
    agency_key: int = None,
    captured_date: str = None,
    opp_key: str = None,
    ordering: str = None,
    page_number: int = None,
    page_size: int = 10,
    posted_date: str = None,
    search_id: str = None,
    source_id: str = None,
    source_type: str = None,
    version_key: str = None
) -> dict:
    """
    Retrieve opportunities (federal contracts, grants, etc.).

    Update Frequency: Every 30 minutes.

    Endpoint: /api-external/opportunity/
    """
    params = {
        "api_key": api_key,
        "agency_key": agency_key,
        "captured_date": captured_date,
        "opp_key": opp_key,
        "ordering": ordering,
        "page_number": page_number,
        "page_size": page_size,
        "posted_date": posted_date,
        "search_id": search_id,
        "source_id": source_id,
        "source_type": source_type,
        "version_key": version_key
    }
    return call_endpoint("/api-external/opportunity/", params)

def get_all_opportunities_for_searchid(
    api_key: str,
    search_id: str,
    max_page_number: int = DEFAULT_MAX_PAGE_NUMBER,
    page_size: int = DEFAULT_PAGE_SIZE
) -> list:
    """
    Retrieve all opportunities for a given search_id from the 
    /api-external/opportunity/ endpoint by paginating through all pages.

    Args:
        api_key (str): Your HigherGov API Key.
        search_id (str): The HigherGov search id to filter opportunities.
        max_page_number (int): Maximum number of pages to retrieve; defaults to DEFAULT_MAX_PAGE_NUMBER.
        page_size (int): Number of records per page; defaults to DEFAULT_PAGE_SIZE.

    Returns:
        list: A list of opportunity records.
    """
    page_number = 1
    all_opportunities = []
    
    while True:
        resp = get_opportunities(
            api_key=api_key,
            search_id=search_id,
            page_number=page_number,
            page_size=page_size
        )
        results = resp.get("results", [])
        if not results:
            break
        
        all_opportunities.extend(results)
        
        # Check for a next page
        next_link = resp.get("links", {}).get("next")
        if not next_link or page_number >= max_page_number:
            break
        
        page_number += 1
        
    return all_opportunities

all_data = get_all_opportunities_for_searchid(api_key=HIGHERGOV_KEY, search_id="I1sN-gdKpKyZgXqIqATxh", max_page_number=100, page_size=10)
print(json.dumps(all_data, indent=4))
with open("highergov_api_full_data.json", "w") as file:
    json.dump(all_data, file, indent=4)