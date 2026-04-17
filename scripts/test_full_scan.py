from engine.pipeline_engine import PipelineIntelligenceEngine
from engine.scanner.manager import ScannerManager

engine = PipelineIntelligenceEngine()
manager = ScannerManager()

print("Starting AWS Scan...")
live_data = manager.scan_all()
cloud_dump = live_data.get("raw_cloud_dump", [])
print(f"Scan found: {len(cloud_dump[0].keys())} services mapped.")

print("Starting LLM analysis...")
try:
    result = engine.analyze(
        metadata={"source": "Local Scan Test"},
        config={"test": True},
        raw_json=cloud_dump,
        use_llm=True
    )
    print("LLM finished!")
    if result.nodes:
        print("Nodes found:", len(result.nodes))
    else:
        print("No nodes found, LLM returned:", result.llm_inference)
except Exception as e:
    print("Error:", e)
