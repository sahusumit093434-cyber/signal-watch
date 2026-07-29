import pytest
from app.ingestion.cleaner import (
    normalize_company_name,
    clean_category,
    parse_date,
    parse_severity,
    parse_confidence,
    clean_and_validate_events,
    deduplicate_events
)

def test_normalize_company_name():
    assert normalize_company_name("  Quantum Payments  ") == "Quantum Payments"
    assert normalize_company_name("NORTHSTAR LOGISTICS Pvt Ltd") == "Northstar Logistics"
    assert normalize_company_name("Cobalt Financial Group, Ltd.") == "Cobalt Financial Group"
    assert normalize_company_name("Obsidian  Mining") == "Obsidian Mining"
    assert normalize_company_name("kestrel airlines") == "Kestrel Airlines"

def test_clean_category():
    assert clean_category("Cyber Security") == "cybersecurity"
    assert clean_category("CYBER_SECURITY") == "cybersecurity"
    assert clean_category("cyber") == "cybersecurity"
    assert clean_category("SUPPLY-CHAIN") == "supply_chain"
    assert clean_category("FINANCIAL_DISTRESS") == "financial"
    assert clean_category("management") == "leadership"
    
    with pytest.raises(ValueError):
        clean_category("unknown_category")

def test_parse_date():
    # ISO 8601 with Z
    assert parse_date("2026-07-20T10:30:00Z") == "2026-07-20T10:30:00Z"
    # space separated
    assert parse_date("2026-07-20 10:30:00") == "2026-07-20T10:30:00Z"
    # slash separated
    assert parse_date("2026/07/20 10:30") == "2026-07-20T10:30:00Z"
    # day-month-year
    assert parse_date("20-07-2026") == "2026-07-20T00:00:00Z"
    # long month name
    assert parse_date("July 3, 2026") == "2026-07-03T00:00:00Z"
    # short month name
    assert parse_date("03 Jul 2026") == "2026-07-03T00:00:00Z"

    with pytest.raises(ValueError):
        parse_date("not-a-date")

def test_parse_severity():
    assert parse_severity(4) == 4
    assert parse_severity("3") == 3
    assert parse_severity("5.0") == 5
    
    with pytest.raises(ValueError):
        parse_severity(0)
    with pytest.raises(ValueError):
        parse_severity(7)
    with pytest.raises(ValueError):
        parse_severity("high")

def test_parse_confidence():
    assert parse_confidence(0.85) == 0.85
    assert parse_confidence("0.5") == 0.5
    
    with pytest.raises(ValueError):
        parse_confidence(1.4)
    with pytest.raises(ValueError):
        parse_confidence(-0.2)
    with pytest.raises(ValueError):
        parse_confidence("invalid")

def test_clean_and_validate_events():
    events = [
        # Valid record
        {
            "event_id": "EVT-1",
            "company_name": "Test Company",
            "category": "cybersecurity",
            "severity": 3,
            "confidence": 0.8,
            "published_at": "2026-07-20T10:30:00Z"
        },
        # Invalid: blank company name
        {
            "event_id": "EVT-2",
            "company_name": "",
            "category": "cybersecurity",
            "severity": 3,
            "confidence": 0.8,
            "published_at": "2026-07-20T10:30:00Z"
        },
        # Invalid: confidence out of bounds
        {
            "event_id": "EVT-3",
            "company_name": "Test Company",
            "category": "cybersecurity",
            "severity": 3,
            "confidence": 1.5,
            "published_at": "2026-07-20T10:30:00Z"
        }
    ]
    
    valid, rejected = clean_and_validate_events(events)
    assert len(valid) == 1
    assert len(rejected) == 2
    assert valid[0]["company_name"] == "Test Company"
    assert rejected[0]["event_id"] == "EVT-2"
    assert "company name" in rejected[0]["rejection_reason"].lower()
    assert rejected[1]["event_id"] == "EVT-3"
    assert "confidence" in rejected[1]["rejection_reason"].lower()

def test_deduplicate_events():
    events = [
        {
            "event_id": "EVT-1",
            "company_name": "Test Company",
            "category": "cybersecurity",
            "severity": 3,
            "confidence": 0.8,
            "published_at": "2026-07-20T10:30:00Z",
            "description": "Short outage"
        },
        {
            "event_id": "EVT-2",
            "company_name": "Test Company Ltd.",
            "category": "cybersecurity",
            "severity": 3,
            "confidence": 0.9,  # Higher confidence
            "published_at": "2026-07-20T10:30:00Z",
            "description": "Short outage"
        }
    ]
    
    unique, count = deduplicate_events(events)
    assert count == 1
    assert len(unique) == 1
    assert unique[0]["event_id"] == "EVT-2"
    assert unique[0]["confidence"] == 0.9
