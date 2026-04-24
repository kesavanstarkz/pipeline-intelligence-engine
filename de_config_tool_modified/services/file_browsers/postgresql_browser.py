"""PostgreSQL table browser - lists tables in a schema."""

import asyncio
from typing import Any
from .registry import register

@register("postgresql")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List tables from PostgreSQL database.
    
    connection fields:
      - host: Hostname
      - port: Port number (default: 5432)
      - database: Database name
      - username: Username
      - password: Password
      - schema: Schema name (default: "public")
    
    Returns: List of table names
    """
    try:
        import asyncpg
    except ImportError:
        raise ImportError("asyncpg not installed. Run: pip install asyncpg")
    
    async def _list_postgresql_tables() -> list[str]:
        host = connection.get("host", "localhost")
        port = connection.get("port", 5432)
        database = connection.get("database", "")
        username = connection.get("username", "")
        password = connection.get("password", "")
        schema = connection.get("schema", "public")
        
        try:
            conn = await asyncpg.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
        except Exception as e:
            raise Exception(f"PostgreSQL connection failed: {str(e)}")
        
        try:
            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = $1
                ORDER BY table_name
                LIMIT $2
            """
            tables = await conn.fetch(query, schema, max_files)
            return [row["table_name"] for row in tables]
        finally:
            await conn.close()
    
    return await _list_postgresql_tables()
