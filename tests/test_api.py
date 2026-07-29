import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from app.main import app, _company_scores_cache, _enriched_events_cache

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_events_endpoint_validation():
    # Less than 2 params -> 400 Bad Request
    response = client.get("/events?company=Quantum Payments")
    assert response.status_code == 400
    assert "At least two filter parameters" in response.json()["detail"]

    # Seed the cache
    _enriched_events_cache.clear()
    _enriched_events_cache.append({
        "event_id": "EVT-101",
        "company_name": "Quantum Payments",
        "category": "cybersecurity",
        "severity": 3,
        "confidence": 0.8,
        "published_at": "2026-07-20T10:30:00Z",
        "country": "United Arab Emirates",
        "source": "Public Monitor",
        "description": "Denial of service",
        "event_risk_score": 48.0
    })

    # Two params -> 200 OK
    response = client.get("/events?company=Quantum Payments&category=cybersecurity")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["event_id"] == "EVT-101"

def test_companies_endpoint():
    # Seed cache
    _company_scores_cache.clear()
    _company_scores_cache.extend([
        {
            "company_name": "Quantum Payments",
            "risk_score": 48.0,
            "risk_level": "MEDIUM",
            "event_count": 1,
            "top_categories": ["cybersecurity"]
        },
        {
            "company_name": "Bluepeak Media",
            "risk_score": 75.0,
            "risk_level": "HIGH",
            "event_count": 3,
            "top_categories": ["cybersecurity", "financial"]
        }
    ])

    response = client.get("/companies")
    assert response.status_code == 200
    assert len(response.json()) == 2

    # Filter by risk level
    response = client.get("/companies?risk_level=HIGH")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["company_name"] == "Bluepeak Media"

    # Filter by min score
    response = client.get("/companies?minimum_score=50")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["company_name"] == "Bluepeak Media"

    # GET specific company detail
    response = client.get("/companies/Quantum Payments")
    assert response.status_code == 200
    assert response.json()["name"] == "Quantum Payments"
    assert response.json()["risk_level"] == "MEDIUM"

    # GET 404 for unknown company
    response = client.get("/companies/Unknown Company")
    assert response.status_code == 404
