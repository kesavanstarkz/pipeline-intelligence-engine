"""Salesforce object browser - lists queryable objects."""

import asyncio
from typing import Any
from .registry import register

@register("salesforce")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List queryable objects from Salesforce.
    
    connection fields:
      - instance_url: Salesforce instance URL
      - username: Username
      - password: Password
      - security_token: Security token
      - authentication: "OAuth2 Password" or "OAuth2 JWT"
    
    Returns: List of Salesforce object names (Account, Opportunity, etc.)
    """
    try:
        from simple_salesforce import Salesforce
    except ImportError:
        raise ImportError("simple-salesforce not installed. Run: pip install simple-salesforce")
    
    def _list_salesforce_objects() -> list[str]:
        instance_url = connection.get("instance_url", "")
        username = connection.get("username", "")
        password = connection.get("password", "")
        security_token = connection.get("security_token", "")
        
        try:
            sf = Salesforce(
                instance=instance_url.split("//")[1].split(".")[0],
                username=username,
                password=password,
                security_token=security_token
            )
        except Exception as e:
            raise Exception(f"Salesforce authentication failed: {str(e)}")
        
        try:
            describe = sf.describe()
            objects = describe.get("sobjects", [])
            
            # Filter to queryable objects only
            queryable_objects = [
                obj["name"] for obj in objects
                if obj.get("queryable", False)
            ]
            
            return sorted(queryable_objects)[:max_files]
        except Exception as e:
            raise Exception(f"Salesforce describe failed: {str(e)}")
    
    return await asyncio.to_thread(_list_salesforce_objects)
