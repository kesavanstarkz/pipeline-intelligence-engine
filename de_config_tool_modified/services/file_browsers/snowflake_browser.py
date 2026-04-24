"""Snowflake table browser - lists tables in schema."""

import asyncio
from typing import Any
from .registry import register

@register("snowflake")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List tables from Snowflake schema.
    
    connection fields:
      - account: Snowflake account identifier
      - warehouse: Warehouse name
      - database: Database name
      - schema: Schema name
      - username: Username
      - password: Password
      - authentication: "Password" or "Key Pair"
      - private_key: PEM private key (for Key Pair auth)
    
    Returns: List of table names
    """
    try:
        import snowflake.connector
    except ImportError:
        raise ImportError("snowflake-connector-python not installed. Run: pip install snowflake-connector-python")
    
    def _list_snowflake_tables() -> list[str]:
        account = connection.get("account", "")
        warehouse = connection.get("warehouse", "")
        database = connection.get("database", "")
        schema = connection.get("schema", "")
        username = connection.get("username", "")
        password = connection.get("password", "")
        auth_type = connection.get("authentication", "Password")
        
        try:
            kwargs = {
                "account": account,
                "warehouse": warehouse,
                "database": database,
                "schema": schema,
                "user": username,
            }
            
            if auth_type == "Key Pair":
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.backends import default_backend
                
                pem_key = connection.get("private_key", "").encode()
                p_key = serialization.load_pem_private_key(
                    pem_key,
                    password=None,
                    backend=default_backend()
                )
                kwargs["private_key_content"] = p_key
            else:  # Password
                kwargs["password"] = password
            
            conn = snowflake.connector.connect(**kwargs)
        except Exception as e:
            raise Exception(f"Snowflake connection failed: {str(e)}")
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
            tables = [row[1] for row in cursor.fetchall()[:max_files]]
            return tables
        finally:
            conn.close()
    
    return await asyncio.to_thread(_list_snowflake_tables)
