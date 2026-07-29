import csv
import json
import logging
import os
import time
import requests
from app.config import CSV_PATH, API_BACKUP_PATH, MOCK_API_URL, MOCK_API_HEALTH_URL

logger = logging.getLogger(__name__)

def fetch_events_from_api(url=MOCK_API_URL, max_retries=3, timeout=5) -> list:
    """
    Fetches paginated event records from the mock REST API.
    Handles pagination (looping page by page) and flaky 503 responses.
    If the API server is down, falls back to loading local events_api.json backup.
    """
    events = []
    page = 1
    has_more = True

    # Check if mock API is active first
    try:
        health_check = requests.get(MOCK_API_HEALTH_URL, timeout=2)
        if health_check.status_code != 200:
            logger.warning(f"Mock API health endpoint returned status {health_check.status_code}. Using local backup file.")
            return load_api_backup()
    except requests.RequestException as e:
        logger.warning(f"Failed to connect to Mock API ({e}). Falling back to local backup file.")
        return load_api_backup()

    while has_more:
        retries = 0
        success = False
        while retries < max_retries:
            try:
                params = {"page": page, "page_size": 50}
                response = requests.get(url, params=params, timeout=timeout)

                if response.status_code == 200:
                    try:
                        data = response.json()
                    except (json.JSONDecodeError, TypeError, ValueError) as json_err:
                        logger.error(f"Page {page} returned malformed JSON: {json_err}")
                        # This handles simulate=malformed. We return the backup or partial results.
                        # For robustness, we will fall back to local backup for API data.
                        raise ValueError("Malformed JSON response from API")

                    page_results = data.get("results", [])
                    events.extend(page_results)
                    has_more = data.get("has_more", False)
                    page += 1
                    success = True
                    break
                elif response.status_code == 503:
                    # Flaky mode: wait and retry
                    retry_after = int(response.headers.get("Retry-After", 1))
                    retries += 1
                    logger.warning(f"503 Service Unavailable on page {page}. Retrying in {retry_after}s... (Attempt {retries}/{max_retries})")
                    time.sleep(retry_after)
                else:
                    logger.error(f"API returned status code {response.status_code} for page {page}.")
                    retries += 1
                    time.sleep(0.5)
            except Exception as e:
                logger.error(f"Exception on page {page}: {e}")
                retries += 1
                time.sleep(0.5)

        if not success:
            logger.error(f"Failed to fetch page {page} after {max_retries} attempts. Aborting API fetch and falling back.")
            return load_api_backup()

    return events

def load_api_backup() -> list:
    """Loads events from the local backup json file."""
    if os.path.exists(API_BACKUP_PATH):
        logger.info(f"Loading API backup from {API_BACKUP_PATH}")
        with open(API_BACKUP_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception as e:
                logger.error(f"Failed to parse API backup file: {e}")
                return []
    logger.error(f"Backup API file not found at {API_BACKUP_PATH}")
    return []

def read_events_from_csv(path=CSV_PATH) -> list:
    """Reads events from events.csv file."""
    events = []
    if not os.path.exists(path):
        logger.error(f"CSV file not found at {path}")
        return events

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            events.append(dict(row))
    return events

def collect_all_events() -> tuple[list, int]:
    """
    Collects events from both the API and CSV.
    Returns a tuple (merged_list, total_received_count).
    """
    api_events = fetch_events_from_api()
    csv_events = read_events_from_csv()

    received_count = len(api_events) + len(csv_events)
    merged_events = []
    
    # Merge and assign source if missing
    for e in api_events:
        item = dict(e)
        if not item.get("source"):
            item["source"] = "API"
        merged_events.append(item)

    for e in csv_events:
        item = dict(e)
        if not item.get("source"):
            item["source"] = "CSV"
        merged_events.append(item)

    return merged_events, received_count
