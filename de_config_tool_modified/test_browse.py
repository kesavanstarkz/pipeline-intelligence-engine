import requests
import json

# Test 1: Simple string connection data
req = {
    'source_type': 's3',
    'connection': {'bucket': 'my-bucket', 'region': 'us-east-1', 'auth_type': 'iam'},
    'max_files': 5
}

res = requests.post('http://localhost:8000/api/browse/list-files', json=req)
print(f'Test 1 (S3): Status {res.status_code}')
if res.status_code != 200:
    print(f'  Response: {res.text[:200]}')

# Test 2: With all string values (like form would send)
req2 = {
    'source_type': 'postgresql',
    'connection': {
        'host': 'localhost',
        'port': '5432',
        'database': 'test',
        'user': 'postgres',
        'password': 'pass',
        'authentication': 'Password',
        'ssl_mode': 'disable'
    },
    'max_files': 10
}

res2 = requests.post('http://localhost:8000/api/browse/list-files', json=req2)
print(f'Test 2 (PostgreSQL with all strings): Status {res2.status_code}')
if res2.status_code != 200:
    print(f'  Response: {res2.text[:200]}')
