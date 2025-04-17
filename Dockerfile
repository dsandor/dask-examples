# Use Python 3.9 slim as base image
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python packages
RUN pip install --no-cache-dir \
    duckdb \
    boto3 \
    pandas \
    fastapi \
    uvicorn \
    pydantic

# Copy the query script
COPY query_s3.py /app/

# Make the script executable
RUN chmod +x /app/query_s3.py

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a non-root user
RUN useradd -m -u 1000 appuser

# Create cache directory and set permissions
RUN mkdir -p /data/cache && chown -R appuser:appuser /data

# Switch to non-root user
USER appuser

# Expose the port
EXPOSE 8000

# Set the entrypoint
ENTRYPOINT ["python", "/app/query_s3.py"] 