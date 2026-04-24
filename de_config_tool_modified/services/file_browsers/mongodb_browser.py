"""MongoDB collection browser - lists collections in a database."""

import asyncio
from typing import Any
from .registry import register

@register("mongodb")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List collections from MongoDB database.
    
    connection fields:
      - host: Hostname
      - port: Port (default: 27017)
      - database: Database name
      - username: Username (optional)
      - password: Password (optional)
      - collection: Collection name (optional, for sample count)
    
    Returns: List of collection names with optional document count metadata
    """
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError:
        raise ImportError("motor not installed. Run: pip install motor")
    
    async def _list_mongodb_collections() -> list[str]:
        host = connection.get("host", "localhost")
        port = connection.get("port", 27017)
        database = connection.get("database", "")
        username = connection.get("username", "")
        password = connection.get("password", "")
        
        # Build URI
        if username and password:
            uri = f"mongodb://{username}:{password}@{host}:{port}/{database}"
        else:
            uri = f"mongodb://{host}:{port}/{database}"
        
        try:
            client = AsyncIOMotorClient(uri)
            db = client[database]
            collections = await db.list_collection_names()
            
            # Optionally add document counts if collection specified
            result = []
            for coll in collections[:max_files]:
                coll_obj = db[coll]
                try:
                    count = await coll_obj.estimated_document_count()
                    result.append(f"{coll} (≈ {count:,} docs)")
                except:
                    result.append(coll)
            
            return result
        except Exception as e:
            raise Exception(f"MongoDB connection failed: {str(e)}")
    
    return await _list_mongodb_collections()
