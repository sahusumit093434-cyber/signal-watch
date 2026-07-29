import os
import sys

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IS_VERCEL = os.environ.get("VERCEL") == "1" or os.environ.get("VERCEL") is not None

# Writable root: on Vercel, it must be /tmp
WRITABLE_ROOT = "/tmp" if IS_VERCEL else BASE_DIR

DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")

# Writable subdirectories
CLEANED_DATA_DIR = os.path.join(WRITABLE_ROOT, "data", "cleaned")
REJECTED_DATA_DIR = os.path.join(WRITABLE_ROOT, "data", "rejected")
PROCESSED_DATA_DIR = os.path.join(WRITABLE_ROOT, "data", "processed")
OUTPUTS_DIR = os.path.join(WRITABLE_ROOT, "outputs")

# File Paths
CSV_PATH = os.path.join(RAW_DATA_DIR, "events.csv")
API_BACKUP_PATH = os.path.join(RAW_DATA_DIR, "events_api.json")
CLEANED_JSON_PATH = os.path.join(CLEANED_DATA_DIR, "cleaned_events.json")
REJECTED_JSON_PATH = os.path.join(REJECTED_DATA_DIR, "rejected_events.json")
COMPANY_SCORES_JSON_PATH = os.path.join(PROCESSED_DATA_DIR, "company_scores.json")
COMPANY_SCORES_CSV_PATH = os.path.join(PROCESSED_DATA_DIR, "company_scores.csv")
CHART_PATH = os.path.join(OUTPUTS_DIR, "top_company_risks.png")

# API Configuration
MOCK_API_URL = "http://127.0.0.1:9000/api/v1/events"
MOCK_API_HEALTH_URL = "http://127.0.0.1:9000/health"

# Reference date for recency calculations (if None, defaults to current local time or dataset max)
# For reproducibility, we default it to '2026-07-24' (the date mentioned in data dictionary)
REFERENCE_DATE = "2026-07-24"

# Set JDK environment programmatically
JAVA_HOME_PATH = os.path.join(BASE_DIR, "jdk", "jdk-17.0.19+10")
if os.path.exists(JAVA_HOME_PATH):
    os.environ["JAVA_HOME"] = JAVA_HOME_PATH
    java_bin = os.path.join(JAVA_HOME_PATH, "bin")
    if java_bin not in os.environ.get("PATH", "").split(os.pathsep):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ["PATH"]

# Set PySpark python worker to the virtual environment python.exe
python_exe = os.path.join(BASE_DIR, ".venv", "Scripts", "python.exe")
if os.path.exists(python_exe):
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
