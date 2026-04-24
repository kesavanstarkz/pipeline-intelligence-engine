"""REST API endpoint browser - lists endpoints from OpenAPI or direct API call."""

import asyncio
from typing import Any
from .registry import register

@register("rest_api")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List endpoints/fields from a REST API.
    
    connection fields:
      - base_url: Base URL for API
      - endpoint: Endpoint path (optional)
      - authentication: "None", "Bearer Token", "API Key", or "Basic Auth"
      - bearer_token: Bearer token value
      - api_key: API key value
      - basic_username: Username for Basic Auth
      - basic_password: Password for Basic Auth
    
    Returns: List of endpoint paths or field names
    """
    try:
        import httpx
    except ImportError:
        raise ImportError("httpx not installed. Run: pip install httpx")
    
    async def _list_rest_api_endpoints() -> list[str]:
        base_url = connection.get("base_url", "")
        endpoint = connection.get("endpoint", "")
        auth = connection.get("authentication", "None")
        
        # Build headers
        headers = {}
        if auth == "Bearer Token":
            token = connection.get("bearer_token", "")
            headers["Authorization"] = f"Bearer {token}"
        elif auth == "API Key":
            api_key = connection.get("api_key", "")
            headers["X-API-Key"] = api_key
        elif auth == "Basic Auth":
            import base64
            username = connection.get("basic_username", "")
            password = connection.get("basic_password", "")
            creds = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {creds}"
        
        # Try OpenAPI first
        try:
            async with httpx.AsyncClient() as client:
                # Try common OpenAPI endpoints
                for openapi_path in ["/openapi.json", "/swagger.json", "/api/openapi.json"]:
                    try:
                        url = base_url + openapi_path
                        resp = await client.get(url, headers=headers, timeout=5.0)
                        if resp.status_code == 200:
                            spec = resp.json()
                            paths = spec.get("paths", {})
                            endpoint_list = list(paths.keys())[:max_files]
                            return endpoint_list
                    except:
                        continue
                
                # Fallback: call the endpoint directly
                url = base_url + (endpoint or "/")
                resp = await client.get(url, headers=headers, timeout=5.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        # Return keys from first element
                        first_item = data[0]
                        if isinstance(first_item, dict):
                            return list(first_item.keys())[:max_files]
                    elif isinstance(data, dict):
                        return list(data.keys())[:max_files]
                
                return []
        except Exception as e:
            raise Exception(f"REST API call failed: {str(e)}")
    
    return await _list_rest_api_endpoints()
