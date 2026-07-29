import json
import logging
import os
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel

from app.config import (
    CLEANED_JSON_PATH,
    REJECTED_JSON_PATH,
    COMPANY_SCORES_JSON_PATH,
    REFERENCE_DATE
)
from app.ingestion.collector import collect_all_events
from app.ingestion.cleaner import clean_and_validate_events, deduplicate_events
from app.processing.spark_processor import process_risk_scores
from app.utils.analytics import generate_analytics_and_chart

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SignalWatch Company Risk Intelligence API",
    description="API for processing messy public events and assessing company risk.",
    version="1.0.0"
)

# Global caches for query endpoints
_company_scores_cache = []
_enriched_events_cache = []

def load_data_from_disk():
    """Loads processed data from disk into caches if available."""
    global _company_scores_cache, _enriched_events_cache
    
    # Load company scores
    if os.path.exists(COMPANY_SCORES_JSON_PATH):
        try:
            with open(COMPANY_SCORES_JSON_PATH, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content.startswith("["):
                    _company_scores_cache = json.loads(content)
                else:
                    _company_scores_cache = [json.loads(line) for line in content.splitlines() if line]
            logger.info(f"Loaded {len(_company_scores_cache)} company scores from disk.")
        except Exception as e:
            logger.error(f"Error loading company scores from disk: {e}")

    # Load enriched events
    enriched_path = os.path.join(os.path.dirname(COMPANY_SCORES_JSON_PATH), "enriched_events.json")
    if os.path.exists(enriched_path):
        try:
            with open(enriched_path, "r", encoding="utf-8") as f:
                _enriched_events_cache = json.load(f)
            logger.info(f"Loaded {len(_enriched_events_cache)} enriched events from disk.")
        except Exception as e:
            logger.error(f"Error loading enriched events from disk: {e}")

@app.on_event("startup")
async def startup_event():
    load_data_from_disk()

class IngestResponse(BaseModel):
    status: str
    received_records: int
    valid_records: int
    rejected_records: int
    duplicate_records: int
    companies_processed: int

@app.get("/health", status_code=status.HTTP_200_OK)
def health():
    return {"status": "healthy"}

@app.post("/ingest", response_model=IngestResponse)
def ingest(reference_date: Optional[str] = REFERENCE_DATE):
    """
    Runs the full data pipeline:
    1. Collects events from mock API & CSV.
    2. Validates & Normalizes.
    3. Deduplicates.
    4. Computes Event & Company Risk Scores via PySpark.
    5. Computes NumPy statistics and generates the Matplotlib chart.
    """
    global _company_scores_cache, _enriched_events_cache
    logger.info("Starting ingestion pipeline...")

    try:
        # Step 1: Collect
        raw_events, received_count = collect_all_events()
        logger.info(f"Collected {received_count} raw events.")

        # Step 2 & 3: Clean, Validate & Deduplicate
        valid_events, rejected_events = clean_and_validate_events(raw_events)
        unique_events, duplicate_count = deduplicate_events(valid_events)
        
        valid_count = len(unique_events)
        rejected_count = len(rejected_events)
        
        logger.info(f"Validation: {len(valid_events)} valid, {rejected_count} rejected.")
        logger.info(f"Deduplication: {valid_count} unique, {duplicate_count} duplicates.")

        # Write cleaned and rejected lists to disk
        os.makedirs(os.path.dirname(CLEANED_JSON_PATH), exist_ok=True)
        with open(CLEANED_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(unique_events, f, indent=2)

        os.makedirs(os.path.dirname(REJECTED_JSON_PATH), exist_ok=True)
        with open(REJECTED_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(rejected_events, f, indent=2)

        # Step 4: Spark Processing
        enriched_events, company_scores = process_risk_scores(reference_date=reference_date)
        if enriched_events is None or company_scores is None:
            raise HTTPException(
                status_code=500,
                detail="Spark processing failed."
            )

        # Save enriched events for later retrieval
        enriched_path = os.path.join(os.path.dirname(COMPANY_SCORES_JSON_PATH), "enriched_events.json")
        with open(enriched_path, "w", encoding="utf-8") as f:
            json.dump(enriched_events, f, indent=2)

        # Update cache
        _company_scores_cache = company_scores
        _enriched_events_cache = enriched_events

        # Step 5: Analytics & Chart
        stats = generate_analytics_and_chart(company_scores)
        logger.info(f"Generated stats: {stats}")

        return IngestResponse(
            status="completed",
            received_records=received_count,
            valid_records=valid_count,
            rejected_records=rejected_count,
            duplicate_records=duplicate_count,
            companies_processed=len(company_scores)
        )

    except Exception as e:
        logger.exception("Pipeline ingestion failed")
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline ingestion failed: {str(e)}"
        )

@app.get("/companies")
def get_companies(
    risk_level: Optional[str] = None,
    country: Optional[str] = None,
    minimum_score: Optional[float] = None,
    limit: Optional[int] = 100
):
    """
    Filters company risk scores.
    """
    if not _company_scores_cache:
        load_data_from_disk()
        if not _company_scores_cache:
            return []

    results = _company_scores_cache.copy()

    # Apply filters
    if risk_level:
        results = [c for c in results if c.get("risk_level", "").upper() == risk_level.upper()]
    if minimum_score is not None:
        results = [c for c in results if c.get("risk_score", 0.0) >= minimum_score]
    if country:
        company_countries = {}
        for ev in _enriched_events_cache:
            comp = ev.get("company_name")
            c_ctry = ev.get("country", "")
            if comp:
                if comp not in company_countries:
                    company_countries[comp] = set()
                if c_ctry:
                    company_countries[comp].add(c_ctry.lower())
        
        target_country = country.strip().lower()
        results = [
            c for c in results 
            if c.get("company_name") in company_countries 
            and target_country in company_countries[c.get("company_name")]
        ]

    # Apply limit
    if limit is not None and limit > 0:
        results = results[:limit]

    return results

@app.get("/companies/{company_name}")
def get_company_by_name(company_name: str):
    """
    Retrieves details for a single company:
    name, risk_score, risk_level, event_count, top_categories.
    """
    if not _company_scores_cache:
        load_data_from_disk()

    from app.ingestion.cleaner import normalize_company_name
    search_name = normalize_company_name(company_name).lower()

    for c in _company_scores_cache:
        if normalize_company_name(c.get("company_name", "")).lower() == search_name:
            return {
                "name": c.get("company_name"),
                "risk_score": c.get("risk_score"),
                "risk_level": c.get("risk_level"),
                "event_count": c.get("event_count"),
                "top_categories": c.get("top_categories")
            }

    raise HTTPException(
        status_code=404,
        detail=f"Company '{company_name}' not found."
    )

@app.get("/events")
def get_events(
    company: Optional[str] = None,
    category: Optional[str] = None,
    min_score: Optional[float] = None,
    country: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    # Count active query filters
    active_filters = 0
    if company is not None:
        active_filters += 1
    if category is not None:
        active_filters += 1
    if min_score is not None:
        active_filters += 1
    if country is not None:
        active_filters += 1
    if start_date is not None or end_date is not None:
        active_filters += 1

    if active_filters < 2:
        raise HTTPException(
            status_code=400,
            detail="At least two filter parameters must be provided. Available filters: "
                   "company, category, min_score, country, date range (start_date/end_date)."
        )

    if not _enriched_events_cache:
        load_data_from_disk()
        if not _enriched_events_cache:
            return []

    results = _enriched_events_cache.copy()

    # Apply filters
    if company:
        from app.ingestion.cleaner import normalize_company_name
        search_company = normalize_company_name(company).lower()
        results = [e for e in results if normalize_company_name(e.get("company_name", "")).lower() == search_company]

    if category:
        from app.ingestion.cleaner import clean_category
        try:
            search_cat = clean_category(category)
            results = [e for e in results if e.get("category") == search_cat]
        except ValueError:
            return []

    if min_score is not None:
        results = [e for e in results if e.get("event_risk_score", 0.0) >= min_score]

    if country:
        from app.ingestion.cleaner import clean_country
        search_country = clean_country(country).lower()
        results = [e for e in results if e.get("country", "").lower() == search_country]

    if start_date:
        try:
            s_date = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
            results = [e for e in results if datetime.fromisoformat(e.get("published_at").replace("Z", "+00:00")) >= s_date]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid start_date format. Use ISO format (e.g. YYYY-MM-DD): {e}")

    if end_date:
        try:
            e_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            results = [e for e in results if datetime.fromisoformat(e.get("published_at").replace("Z", "+00:00")) <= e_date]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid end_date format. Use ISO format: {e}")

    return results
