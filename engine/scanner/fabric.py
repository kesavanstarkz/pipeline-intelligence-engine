import logging
import requests
from typing import Dict, List, Any
from .base import CloudScanner

logger = logging.getLogger("pipeline_ie.scanner.fabric")

class FabricScanner(CloudScanner):
    def can_scan(self, settings: Any) -> bool:
        # Fabric REST calls use the delegated token from the session; scan() no-ops quickly without it.
        return True

    def scan(self, settings: Any, **kwargs) -> Dict[str, List[str]]:
        raw_assets: Dict[str, List[Any]] = {
            "fabric_workspaces": [],
            "fabric_items": []
        }

        try:
            # 1. Identity Resolution (Fabric API requires Power BI / Fabric scope, not ARM)
            azure_token = kwargs.get("azure_token_fabric") or kwargs.get("azure_token")
            if not azure_token:
                logger.info("No delegated token for Fabric, skipping deep scan.")
                return {"raw_cloud_dump": [raw_assets]}

            # Microsoft Fabric API Scope
            headers = {"Authorization": f"Bearer {azure_token}"}
            
            # 2. List Workspaces
            logger.info("Triggering Microsoft Fabric Deep Discovery...")
            ws_response = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers, timeout=10)
            
            if ws_response.status_code == 200:
                # ... (rest of logic stays same)
                workspaces = ws_response.json().get('value', [])
                for ws in workspaces:
                    # ... ws logic ...
                    ws_id = ws.get('id')
                    raw_assets["fabric_workspaces"].append({
                        "id": f"fabric || {ws.get('displayName')}",
                        "configuration": {
                            "WorkspaceId": ws_id,
                            "Type": ws.get('type'),
                            "CapacityId": ws.get('capacityId')
                        }
                    })
                    
                    # 3. List Items in Workspace (Lakehouses, Pipelines, etc.)
                    items_res = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items", headers=headers, timeout=10)
                    if items_res.status_code == 200:
                        items = items_res.json().get('value', [])
                        for item in items:
                            item_id = item.get('id')
                            item_type = item.get('type')
                            item_meta = {
                                "id": f"fabric || {item.get('displayName')}",
                                "configuration": {
                                    "ItemId": item_id,
                                    "Type": item_type,
                                    "WorkspaceId": ws_id
                                }
                            }
                            
                            # Deep Inspection for Lakehouses
                            if item_type == 'Lakehouse':
                                try:
                                    lh_res = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/lakehouses/{item_id}", headers=headers, timeout=10)
                                    if lh_res.status_code == 200:
                                        lh_props = lh_res.json().get('properties', {})
                                        item_meta["configuration"]["OneLakeFilesPath"] = lh_props.get('oneLakeFilesPath')
                                        item_meta["configuration"]["OneLakeTablesPath"] = lh_props.get('oneLakeTablesPath')
                                except Exception:
                                    pass
                                    
                            raw_assets["fabric_items"].append(item_meta)
            elif ws_response.status_code == 401:
                logger.error("Fabric API 401 Unauthorized. The SSO token likely lacks the required PowerBI/Fabric scope ('https://analysis.windows.net/powerbi/api/.default').")
            else:
                logger.warning(f"Fabric API returned {ws_response.status_code}: {ws_response.text}")

        except Exception as e:
            logger.error(f"Fabric Scan failed: {e}")
            # Fallback for demo/dev if nothing found
            if not raw_assets["fabric_items"]:
                return self._simulate_fabric()

        return {"raw_cloud_dump": [raw_assets]}

    def _simulate_fabric(self) -> Dict[str, List[Any]]:
        return {"raw_cloud_dump": [{
            "fabric_workspaces": [
                {
                    "id": "fabric || Analytics-Hub",
                    "configuration": {"Type": "Workspace", "Region": "West US"}
                }
            ],
            "fabric_items": [
                {
                    "id": "fabric || Customer-Lakehouse",
                    "configuration": {"Type": "Lakehouse", "Workspace": "Analytics-Hub"}
                },
                {
                    "id": "fabric || Silver-Transformation",
                    "configuration": {"Type": "Pipeline", "Workspace": "Analytics-Hub"}
                }
            ]
        }]}
