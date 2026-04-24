FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps (core only — cloud SDKs are optional)
COPY requirements.txt .
RUN pip install --no-cache-dir \
    fastapi>=0.111.0 \
    uvicorn[standard]>=0.29.0 \
    python-multipart>=0.0.9 \
    jinja2>=3.1.4 \
    httpx>=0.27.0 \
    pydantic>=2.0.0 \
    boto3>=1.34.0 \
    python-dotenv>=1.0.0

# Optional: install full requirements (uncomment for production)
# RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create empty static dir if needed
RUN mkdir -p static

EXPOSE 8000

# Load .env if present, then start
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2"]
