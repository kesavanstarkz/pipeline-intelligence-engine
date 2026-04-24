"""MySQL table browser - lists tables in a database."""

import asyncio
from typing import Any
from .registry import register

@register("mysql")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List tables from MySQL database.
    
    connection fields:
      - host: Hostname
      - port: Port number (default: 3306)
      - database: Database name
      - username: Username
      - password: Password
    
    Returns: List of table names
    """
    try:
        import aiomysql
    except ImportError:
        raise ImportError("aiomysql not installed. Run: pip install aiomysql")
    
    async def _list_mysql_tables() -> list[str]:
        host = connection.get("host", "localhost")
        port = connection.get("port", 3306)
        database = connection.get("database", "")
        username = connection.get("username", "")
        password = connection.get("password", "")
        
        try:
            conn = await aiomysql.connect(
                host=host,
                port=port,
                db=database,
                user=username,
                password=password
            )
        except Exception as e:
            raise Exception(f"MySQL connection failed: {str(e)}")
        
        try:
            async with conn.cursor() as cursor:
                query = f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                    LIMIT {max_files}
                """
                await cursor.execute(query)
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
        finally:
            conn.close()
    
    return await _list_mysql_tables()
