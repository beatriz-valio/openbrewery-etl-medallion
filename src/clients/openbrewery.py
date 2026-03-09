import logging
from typing import Dict, Iterator, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .constants import (
    OPENBREWERY_BASE_URL,
    OPENBREWERY_METADATA_URL,
    OPENBREWERY_PER_PAGE,
    OPENBREWERY_TIMEOUT_SEC,
)

logger = logging.getLogger(__name__)


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=0.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET"),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_all_breweries(
    per_page: int = OPENBREWERY_PER_PAGE, timeout_sec: int = OPENBREWERY_TIMEOUT_SEC
) -> Iterator[List[Dict]]:
    session = _build_session()
    logger.info(
        "Fetching all breweries data from openbrewery with per_page=%s", per_page
    )
    try:
        page = 1
        while True:
            response = session.get(
                OPENBREWERY_BASE_URL,
                params={"page": page, "per_page": per_page},
                timeout=timeout_sec,
            )
            response.raise_for_status()
            data = response.json()
            if not data:
                break

            logger.info(
                "Fetched breweries data from openbrewery: page=%s records=%s",
                page,
                len(data),
            )
            yield data
            page += 1
    except requests.RequestException as e:
        logger.error("Error requesting openbrewery: %s", e)
        raise
    finally:
        logger.info("Finished fetching breweries data from openbrewery")
        session.close()


def fetch_breweries_metadata(
    per_page: int = OPENBREWERY_PER_PAGE, timeout_sec: int = OPENBREWERY_TIMEOUT_SEC
) -> Dict:
    session = _build_session()
    logger.info("Fetching breweries metadata from openbrewery")
    try:
        response = session.get(
            OPENBREWERY_METADATA_URL,
            params={"page": 1, "per_page": per_page},
            timeout=timeout_sec,
        )
        response.raise_for_status()

        payload = response.json()
        total = int(payload.get("total", 0))
        page = int(payload.get("page", 1))
        per_page_resp = int(payload.get("per_page", per_page))

        logger.info(
            "Metadata fetched from openbrewery: total=%s per_page=%s",
            total,
            per_page_resp,
        )

        return {
            "total": total,
            "page": page,
            "per_page": per_page_resp,
        }
    except requests.RequestException as e:
        logger.error("Error fetching metadata from openbrewery: %s", e)
        raise
    finally:
        logger.info("Finished fetching breweries metadata from openbrewery")
        session.close()
