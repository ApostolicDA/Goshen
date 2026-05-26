# ─────────────────────────────────────────────
#  Goshen Analytics Pipeline — Docker Image
#  Python 3.11 + dbt-bigquery
# ─────────────────────────────────────────────
FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files first (layer caching)
COPY requirements.txt .

# Install Python + dbt dependencies
RUN pip install --no-cache-dir --timeout 300 -r requirements.txt

# Copy the entire project
COPY . .

# Create logs directory
RUN mkdir -p /app/logs

# Default command — overridden by docker-compose
CMD ["python", "run_ingestion.py"]
