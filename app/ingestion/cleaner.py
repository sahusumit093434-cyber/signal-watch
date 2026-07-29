import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# Canonical Categories
CANONICAL_CATEGORIES = {
    "cybersecurity",
    "legal_regulatory",
    "financial",
    "supply_chain",
    "leadership",
    "fraud_reputation"
}

CATEGORY_MAPPING = {
    "cybersecurity": "cybersecurity",
    "cyber security": "cybersecurity",
    "cyber-security": "cybersecurity",
    "cyber_security": "cybersecurity",
    "cyber": "cybersecurity",
    
    "supply_chain": "supply_chain",
    "supply-chain": "supply_chain",
    "supply chain": "supply_chain",
    
    "financial": "financial",
    "financial_distress": "financial",
    "financial distress": "financial",
    "finance": "financial",
    
    "leadership": "leadership",
    "leadership_change": "leadership",
    "leadership change": "leadership",
    "management": "leadership",
    
    "fraud_reputation": "fraud_reputation",
    "fraud": "fraud_reputation",
    "reputation": "fraud_reputation",
    "fraud/reputation": "fraud_reputation",
    
    "legal_regulatory": "legal_regulatory",
    "regulatory": "legal_regulatory",
    "legal": "legal_regulatory",
    "legal/regulatory": "legal_regulatory",
}

COUNTRY_MAPPING = {
    "usa": "United States",
    "u.s.a.": "United States",
    "united states": "United States",
    "uk": "United Kingdom",
    "united kingdom": "United Kingdom",
    "uae": "United Arab Emirates",
    "united arab emirates": "United Arab Emirates",
    "india": "India",
    "aus": "Australia",
    "australia": "Australia",
    "singapore": "Singapore",
    "germany": "Germany",
    "japan": "Japan",
}

DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%d-%m-%Y",
    "%B %d, %Y",
    "%d %b %Y",
    "%b %d, %Y",
    "%Y-%m-%d"
]

def normalize_company_name(name: str) -> str:
    """
    Cleans company name: trims whitespace, replaces double spaces,
    and strips corporate suffixes for standard display name.
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Trim and collapse whitespace
    cleaned = " ".join(name.split())
    
    # Suffix matching
    suffix_pat = re.compile(
        r'\b(pvt\.?\s*ltd\.?|ltd\.?|inc\.?|corp\.?|corporation|co\.?|plc\.?|private\s+limited|limited)\b',
        re.IGNORECASE
    )
    
    # Strip trailing punctuation, then suffix, then trailing punctuation again
    cleaned = cleaned.rstrip(',. ')
    core_name = suffix_pat.sub('', cleaned).strip().rstrip(',. ')
    
    # Handle casing (Title Case but keep short acronyms like ABC uppercase)
    words = core_name.split()
    capitalized = []
    for w in words:
        if w.isupper() and len(w) <= 4:
            capitalized.append(w)
        else:
            capitalized.append(w.capitalize())
            
    return " ".join(capitalized)

def normalize_description(desc: str) -> str:
    """Normalizes description by removing non-alphanumeric chars and multiple spaces."""
    if not desc or not isinstance(desc, str):
        return ""
    cleaned = desc.strip().lower()
    cleaned = re.sub(r'[^a-z0-9\s]', ' ', cleaned)
    return " ".join(cleaned.split())

def parse_date(date_str: str) -> str:
    """Parses various date formats to ISO 8601 UTC string: YYYY-MM-DDTHH:MM:SSZ."""
    if not date_str or not isinstance(date_str, str):
        raise ValueError("Missing or invalid date type")
    
    # Clean spacing
    date_str_clean = " ".join(date_str.strip().split())
    
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str_clean, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
            
    raise ValueError(f"Unsupported date format: '{date_str}'")

def parse_severity(sev) -> int:
    """Validates and parses severity, ensuring it is an integer 1-5."""
    if sev is None:
        raise ValueError("Severity is missing")
    try:
        # Cast to float first in case it's e.g. "4.0", then to int
        val = int(float(str(sev).strip()))
        if val < 1 or val > 5:
            raise ValueError(f"Severity out of range (1-5): {sev}")
        return val
    except (ValueError, TypeError):
        raise ValueError(f"Invalid severity value: '{sev}'")

def parse_confidence(conf) -> float:
    """Validates and parses confidence, ensuring it is a float 0-1."""
    if conf is None:
        raise ValueError("Confidence is missing")
    try:
        val = float(str(conf).strip())
        if val < 0.0 or val > 1.0:
            raise ValueError(f"Confidence out of range (0-1): {conf}")
        return val
    except (ValueError, TypeError):
        raise ValueError(f"Invalid confidence value: '{conf}'")

def clean_category(cat: str) -> str:
    """Maps various category spelling variants to the canonical 6 values."""
    if not cat or not isinstance(cat, str):
        raise ValueError("Category is missing or invalid type")
    
    cleaned = cat.strip().lower()
    cleaned = cleaned.replace("_", " ").replace("-", " ")
    cleaned = " ".join(cleaned.split())
    
    if cleaned in CATEGORY_MAPPING:
        return CATEGORY_MAPPING[cleaned]
        
    for key, val in CATEGORY_MAPPING.items():
        if key in cleaned or cleaned in key:
            return val
            
    raise ValueError(f"Unknown category variant: '{cat}'")

def clean_country(country: str) -> str:
    """Normalizes country names to a standardized string."""
    if not country or not isinstance(country, str):
        return ""
    
    cleaned = " ".join(country.strip().split()).lower()
    if cleaned in COUNTRY_MAPPING:
        return COUNTRY_MAPPING[cleaned]
    return country.strip().title()

def clean_and_validate_events(events: list) -> tuple[list, list]:
    """
    Cleans and validates list of raw events.
    Returns (valid_events, rejected_events).
    Each rejected event will contain the original data and a "rejection_reason".
    """
    valid_events = []
    rejected_events = []
    
    for idx, e in enumerate(events):
        orig_record = dict(e)
        cleaned_record = dict(e)
        
        try:
            # 1. Company Name (Required)
            comp_name = cleaned_record.get("company_name")
            if not comp_name or str(comp_name).strip() == "":
                raise ValueError("Company name is required and cannot be blank")
            cleaned_record["company_name"] = normalize_company_name(str(comp_name))
            
            # 2. Category (Required)
            category = cleaned_record.get("category")
            cleaned_record["category"] = clean_category(category)
            
            # 3. Severity (Required)
            severity = cleaned_record.get("severity")
            cleaned_record["severity"] = parse_severity(severity)
            
            # 4. Confidence (Required)
            confidence = cleaned_record.get("confidence")
            cleaned_record["confidence"] = parse_confidence(confidence)
            
            # 5. Published At (Required)
            pub_at = cleaned_record.get("published_at")
            cleaned_record["published_at"] = parse_date(pub_at)
            
            # 6. Country (Optional)
            country = cleaned_record.get("country")
            cleaned_record["country"] = clean_country(country) if country else ""
            
            # 7. Source & Description (Optional)
            cleaned_record["source"] = str(cleaned_record.get("source") or "").strip()
            cleaned_record["description"] = str(cleaned_record.get("description") or "").strip()
            cleaned_record["event_id"] = str(cleaned_record.get("event_id") or "").strip()
            
            valid_events.append(cleaned_record)
            
        except Exception as err:
            rejection_reason = str(err)
            orig_record["rejection_reason"] = rejection_reason
            rejected_events.append(orig_record)
            logger.warning(f"Record #{idx} rejected: {rejection_reason}. Data: {orig_record}")
            
    return valid_events, rejected_events

def deduplicate_events(events: list) -> tuple[list, int]:
    """
    Deduplicates events based on company_name + category + description normalization.
    Deduplication strategy:
    If duplicates are found, prioritize:
    1. Higher confidence
    2. Higher severity
    3. Latest published_at date
    Returns (unique_events, duplicate_count).
    """
    groups = {}
    
    for e in events:
        norm_comp = normalize_company_name(e["company_name"]).lower()
        norm_cat = e["category"]
        norm_desc = normalize_description(e["description"])
        
        dup_key = (norm_comp, norm_cat, norm_desc)
        
        if dup_key not in groups:
            groups[dup_key] = []
        groups[dup_key].append(e)
        
    unique_events = []
    duplicate_count = 0
    
    for dup_key, group in groups.items():
        if len(group) > 1:
            duplicate_count += (len(group) - 1)
            
            # Sorting logic:
            # 1. Confidence descending
            # 2. Severity descending
            # 3. Date descending
            def sort_key(x):
                try:
                    dt = datetime.strptime(x["published_at"], "%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    dt = datetime.min
                return (x["confidence"], x["severity"], dt)
            
            sorted_group = sorted(group, key=sort_key, reverse=True)
            unique_events.append(sorted_group[0])
        else:
            unique_events.append(group[0])
            
    return unique_events, duplicate_count
