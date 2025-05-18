#!/bin/bash

# Create data directory if it doesn't exist
mkdir -p ./data

# Start PostgreSQL container using docker-compose
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to start..."
sleep 5

# Check if PostgreSQL is running
docker-compose ps

echo "PostgreSQL is now running on localhost:5432"
echo "Username: postgres"
echo "Password: Password123"
echo "Database: csvdata"
