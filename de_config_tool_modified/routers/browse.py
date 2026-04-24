"""File browser router - unified endpoint for all source types."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any
import logging
from services.file_browsers.registry import get_browser

logger = logging.getLogger(__name__)

router = APIRouter()


class BrowseRequest(BaseModel):
    """Request to list files/tables/objects from a source."""
    source_type: str
    connection: dict[str, Any]
    max_files: int = 100


class BrowseResponse(BaseModel):
    """Response with browsed files/tables/objects."""
    source_type: str
    label: str
    files: list[str]
    count: int
    truncated: bool
    browser_mode: str


# Map source types to display labels and browser modes
SOURCE_CONFIG = {
    "s3": {"label": "S3 Bucket", "browser_mode": "files"},
    "adls": {"label": "Azure Data Lake", "browser_mode": "files"},
    "postgresql": {"label": "PostgreSQL", "browser_mode": "tables"},
    "mysql": {"label": "MySQL", "browser_mode": "tables"},
    "azure_sql": {"label": "Azure SQL", "browser_mode": "tables"},
    "eventhub": {"label": "Event Hub", "browser_mode": "events"},
    "kafka": {"label": "Kafka", "browser_mode": "topics"},
    "mongodb": {"label": "MongoDB", "browser_mode": "collections"},
    "snowflake": {"label": "Snowflake", "browser_mode": "tables"},
    "salesforce": {"label": "Salesforce", "browser_mode": "objects"},
    "rest_api": {"label": "REST API", "browser_mode": "endpoints"},
}


@router.post("/list-files", response_model=BrowseResponse)
async def list_files(req: BrowseRequest) -> BrowseResponse:
    """
    List files/tables/objects from any registered source type.
    
    Single endpoint handles all 11 source types via plugin registry.
    
    Args:
        req: BrowseRequest with source_type, connection details, and max_files
    
    Returns:
        BrowseResponse with files list, count, truncation flag, and browser_mode
    
    Raises:
        HTTPException 400: If source_type not registered or connection fails
    """
    logger.info(f"Browse request: source_type={req.source_type}, max_files={req.max_files}")
    logger.info(f"Browse connection fields: {list(req.connection.keys())}")
    if "access_key_id" in req.connection:
        logger.info(f"  - access_key_id present: {bool(req.connection['access_key_id'])}")
    if "secret_access_key" in req.connection:
        logger.info(f"  - secret_access_key present: {bool(req.connection['secret_access_key'])}")
    if "authentication" in req.connection:
        logger.info(f"  - authentication: {req.connection['authentication']}")
    
    # Validate source type is registered
    try:
        browser_fn = get_browser(req.source_type)
    except ValueError as e:
        logger.error(f"Invalid source type: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    # Get source config for display labels
    source_config = SOURCE_CONFIG.get(req.source_type, {})
    label = source_config.get("label", req.source_type)
    browser_mode = source_config.get("browser_mode", "files")
    
    # Call the browser function
    try:
        files = await browser_fn(req.connection, req.max_files)
    except Exception as e:
        # Surface error message from SDK without exposing internals
        detail = str(e)
        if "connection failed" in detail.lower():
            detail = f"{req.source_type} connection failed: check credentials and connection details"
        raise HTTPException(status_code=400, detail=detail)
    
    # Build response
    return BrowseResponse(
        source_type=req.source_type,
        label=label,
        files=files,
        count=len(files),
        truncated=len(files) >= req.max_files,
        browser_mode=browser_mode
    )
