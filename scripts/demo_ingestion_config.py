import sys
import os
import json

# Ensure we can import from the root pipeline-intelligence-engine directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.pipeline_engine import PipelineIntelligenceEngine

def run_ingestion_demo():
    print("=== Pipeline Intelligence: Deep Config Extraction Demo ===")
    
    # 1. CREATE A PIPELINE BY OURSELVES (A "Simpler" One)
    # We define the raw JSON of a simple transactional event pipeline.
    simple_pipeline_payload = {
        "pipeline_name": "ecommerce_clickstream_pipeline",
        "description": "Real-time user click event ingestion into warehouse.",
        "architecture_topology": {
            "source_system": {
                "id": "kafka-topic-clickstream",
                "type": "Apache Kafka",
                "format": "Unstructured JSON",
                "details": "Streaming events from mobile clients."
            },
            "ingestion_compute": {
                "id": "spark-streaming-job-01",
                "type": "PySpark (Databricks)",
                "framework": "Python 3.9",
                "type_of_ingestion": "Streaming / Micro-Batch", 
                "supported": True
            },
            "storage_layer": {
                "id": "snowflake-dw-raw",
                "type": "Relational Database (RDBMS)",
                "details": "Raw storage tier."
            }
        }
    }

    print("\n➜ 1. Created Pipeline Blueprint:")
    print(json.dumps(simple_pipeline_payload, indent=2))
    
    # 2. EXTRACT INGESTION CONFIG USING THE ENGINE
    engine = PipelineIntelligenceEngine()
    print("\n➜ 2. Handing payload to Pipeline Intelligence Engine (with LLM Deep Extraction)...")
    print("  (Waiting for local AI to parse and synthesize config arrays...)")
    
    # We pass the payload into the engine just like the API does.
    result = engine.analyze(
        metadata={"source": "Local Demo Script"},
        config=simple_pipeline_payload,
        raw_json=simple_pipeline_payload,
        use_llm=True
    )
    
    print("\n=== ✨ EXTRACTED INGESTION & SOURCE CONFIGURATIONS ✨ ===")
    if result.nodes and len(result.nodes) > 0:
        for node in result.nodes:
            print(f"\n⚙️  NODE: {node.get('title')} [{node.get('role').upper()}]")
            
            # This is the exact block of configs we just programmed the LLM to extract!
            inferred_config = node.get('config', {})
            print(json.dumps(inferred_config, indent=2))
    else:
        print("\n⚠️ Extraction failed. Make sure your local Ollama (DeepSeek) server is running!")
        
    print("\n(Note: The UI will automatically render these same configs inside the Interactive Graph!)")

if __name__ == "__main__":
    run_ingestion_demo()
