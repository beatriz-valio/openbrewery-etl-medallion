import os

# Default parameters for Open Brewery API requests
OPENBREWERY_BASE_URL = str(
    os.getenv("OPENBREWERY_BASE_URL", "https://api.openbrewerydb.org/v1/breweries")
)
OPENBREWERY_METADATA_URL = f"{OPENBREWERY_BASE_URL}/meta"
OPENBREWERY_PER_PAGE = int(os.getenv("OPENBREWERY_PER_PAGE", "200"))
OPENBREWERY_TIMEOUT_SEC = int(os.getenv("OPENBREWERY_TIMEOUT_SEC", "20"))
