"""Azure Event Hub browser - lists event hubs and partitions."""

import asyncio
from typing import Any
from .registry import register

@register("eventhub")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List Event Hub names and partition IDs.
    
    connection fields:
      - namespace: Event Hub namespace
      - eventhub_name: Event Hub name
      - authentication: "SAS Token" or "Managed Identity"
      - sas_key_name: SAS key name
      - sas_key_value: SAS key value
    
    Returns: List of pseudo-"files" (hub name + partition IDs)
    """
    try:
        from azure.eventhub.aio import EventHubConsumerClient
        from azure.identity import DefaultAzureCredential
    except ImportError:
        raise ImportError(
            "Azure Event Hub SDK not installed. Run: pip install azure-eventhub azure-identity"
        )
    
    async def _list_eventhub_partitions() -> list[str]:
        namespace = connection.get("namespace", "")
        eventhub_name = connection.get("eventhub_name", "")
        auth_type = connection.get("authentication", "SAS Token")
        
        fully_qualified_namespace = f"{namespace}.servicebus.windows.net"
        
        try:
            if auth_type == "Managed Identity":
                credential = DefaultAzureCredential()
                client = EventHubConsumerClient.from_connection_string(
                    f"Endpoint=sb://{fully_qualified_namespace}/;SharedAccessKeyName=;SharedAccessKey=;",
                    eventhub_name=eventhub_name,
                    credential=credential
                )
            else:  # SAS Token
                sas_key_name = connection.get("sas_key_name", "")
                sas_key_value = connection.get("sas_key_value", "")
                conn_str = (
                    f"Endpoint=sb://{fully_qualified_namespace}/;"
                    f"SharedAccessKeyName={sas_key_name};"
                    f"SharedAccessKey={sas_key_value};"
                )
                client = EventHubConsumerClient.from_connection_string(
                    conn_str,
                    eventhub_name=eventhub_name
                )
            
            # Get partition IDs to verify connectivity
            partition_ids = await client.get_partition_ids()
            hub_props = await client.get_eventhub_properties()
            
            # Return hub name + partition IDs
            files = [f"eventhub: {eventhub_name}"]
            for pid in partition_ids:
                files.append(f"partition: {pid}")
            
            return files[:max_files]
        except Exception as e:
            raise Exception(f"Event Hub connection failed: {str(e)}")
    
    return await _list_eventhub_partitions()
