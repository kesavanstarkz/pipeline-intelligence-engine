#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

try:
    from services.lambda_extractor import extract_mock
    print("✅ Import successful")
    
    # Test the dynamic-s3-config template
    result = extract_mock("dynamic-config-etl")
    print("✅ Mock extraction successful")
    print(f"Framework type: {result['framework_type']}")
    print(f"Source config: {result['source_config']}")
    print(f"Config source: {result['config_source']}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()