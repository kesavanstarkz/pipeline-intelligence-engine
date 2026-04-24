"""Azure Data Lake Storage (ADLS) file browser - lists container blobs."""

import asyncio
from typing import Any
from .registry import register

@register("adls")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List files from ADLS Gen2 container.
    
    connection fields:
      - account_name: Storage account name
      - container: Container name
      - path: Optional path prefix
      - authentication: "Managed Identity", "SAS Token", or "Access Key"
      - account_key: For Access Key auth
      - sas_token: For SAS Token auth
    
    Returns: List of terminal file names (stripped of prefix, files only)
    """
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        from azure.identity import DefaultAzureCredential
    except ImportError:
        raise ImportError(
            "Azure SDK not installed. Run: pip install azure-storage-file-datalake azure-identity"
        )
    
    def _list_adls_files() -> list[str]:
        account_name = connection.get("account_name", "")
        container = connection.get("container", "")
        path = connection.get("path", "")
        auth = connection.get("authentication", "Managed Identity")
        
        # Build client with auth
        account_url = f"https://{account_name}.dfs.core.windows.net"
        
        if auth == "Managed Identity":
            credential = DefaultAzureCredential()
            client = DataLakeServiceClient(account_url=account_url, credential=credential)
        elif auth == "SAS Token":
            sas_token = connection.get("sas_token", "")
            client = DataLakeServiceClient(account_url=f"{account_url}?{sas_token}")
        elif auth == "Access Key":
            account_key = connection.get("account_key", "")
            client = DataLakeServiceClient(account_url=account_url, credential=account_key)
        else:
            raise ValueError(f"Unknown auth type: {auth}")
        
        try:
            fs_client = client.get_file_system_client(container)
            paths = fs_client.get_paths(path=path, max_results=max_files)
        except Exception as e:
            raise Exception(f"ADLS list_paths failed: {str(e)}")
        
        files = []
        for p in paths:
            if not p.is_directory:
                # Get terminal filename
                name = p.name.split("/")[-1]
                files.append(name)
        
        return files[:max_files]
    
    return await asyncio.to_thread(_list_adls_files)
