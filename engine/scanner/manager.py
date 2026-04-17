import logging
from typing import Dict, List, Any

from .aws import AWSScanner

logger = logging.getLogger("pipeline_ie.scanner.manager")

class ScannerManager:
    def __init__(self):
        # Register available scanners
        from .azure import AzureScanner
        from .gcp import GCPScanner
        
        self.scanners = [
            AWSScanner(),
            AzureScanner(),
            GCPScanner()
        ]

    def scan_all(self, settings: Any) -> Dict[str, List[str]]:
        aggregated = {
            "framework": [],
            "source": [],
            "ingestion": [],
            "dq_rules": []
        }
        
        for scanner in self.scanners:
            if scanner.can_scan(settings):
                name = scanner.__class__.__name__
                logger.info(f"Triggering {name}...")
                try:
                    res = scanner.scan(settings)
                    for k, v in res.items():
                        if k not in aggregated:
                            aggregated[k] = []
                        
                        for item in v:
                            if item not in aggregated[k]:
                                aggregated[k].append(item)
                except Exception as e:
                    logger.error(f"{name} failed: {e}")
                    
        return aggregated

def _make_scanner_manager() -> ScannerManager:
    return ScannerManager()


# Lazy singleton — created on first access so imports never block
_scanner_manager_instance: ScannerManager | None = None


def get_scanner_manager() -> ScannerManager:
    global _scanner_manager_instance
    if _scanner_manager_instance is None:
        _scanner_manager_instance = _make_scanner_manager()
    return _scanner_manager_instance


# Backwards-compatible alias (used by api/main.py)
scanner_manager = get_scanner_manager()
