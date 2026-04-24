"""Azure SQL (SQL Server) table browser - lists tables."""

import asyncio
from typing import Any
from .registry import register

@register("azure_sql")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List tables from Azure SQL database.
    
    connection fields:
      - server: Server hostname
      - database: Database name
      - username: Username (for SQL Auth)
      - password: Password (for SQL Auth)
      - authentication: "SQL Auth" or "Managed Identity"
    
    Returns: List of table names
    """
    try:
        import aioodbc
    except ImportError:
        raise ImportError("aioodbc not installed. Run: pip install aioodbc")
    
    async def _list_azure_sql_tables() -> list[str]:
        server = connection.get("server", "")
        database = connection.get("database", "")
        username = connection.get("username", "")
        password = connection.get("password", "")
        auth_type = connection.get("authentication", "SQL Auth")
        
        # Build ODBC connection string
        conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={server};Database={database};"
        
        if auth_type == "SQL Auth":
            conn_str += f"UID={username};PWD={password};"
        elif auth_type == "Managed Identity":
            conn_str += "Authentication=ActiveDirectoryMsi;"
        
        try:
            conn = await aioodbc.connect(dsn=conn_str)
        except Exception as e:
            raise Exception(f"Azure SQL connection failed: {str(e)}")
        
        try:
            async with conn.cursor() as cursor:
                query = f"""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """
                await cursor.execute(query)
                rows = await cursor.fetchall()
                tables = [row[0] for row in rows]
                return tables[:max_files]
        finally:
            await conn.close()
    
    return await _list_azure_sql_tables()
