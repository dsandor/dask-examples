#!/usr/bin/env python3
import argparse
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import duckdb
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    results: list
    columns: list
    execution_time_ms: float
    timestamp: str
    source_tables: List[str]

class TableConfig(BaseModel):
    url: str
    port: int
    table_name: str

class Config(BaseModel):
    tables: Dict[str, TableConfig]

class DistributedQueryServer:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.conn = duckdb.connect(database=':memory:')
        self.client = httpx.AsyncClient(timeout=30.0)  # 30 second timeout
        
    def _load_config(self, config_path: str) -> Config:
        """Load configuration from JSON file."""
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        return Config(**config_data)
    
    def _parse_query(self, query: str) -> Set[str]:
        """Extract table names from the query."""
        # Simple regex to find table names in FROM and JOIN clauses
        table_pattern = r'(?i)(?:FROM|JOIN)\s+(\w+)'
        tables = set(re.findall(table_pattern, query))
        return tables
    
    def _validate_tables(self, tables: Set[str]) -> None:
        """Validate that all tables in the query exist in the config."""
        missing_tables = tables - set(self.config.tables.keys())
        if missing_tables:
            raise HTTPException(
                status_code=400,
                detail=f"Tables not found in configuration: {', '.join(missing_tables)}"
            )
    
    def _parse_join_conditions(self, query: str) -> List[Tuple[str, str, str, str]]:
        """Parse join conditions from the query.
        Returns a list of tuples (left_table, left_column, right_table, right_column).
        """
        # Match JOIN clauses with ON conditions
        join_pattern = r'(?i)JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        joins = re.findall(join_pattern, query)
        
        # Also check for WHERE conditions that might be join conditions
        where_pattern = r'(?i)WHERE\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        where_joins = re.findall(where_pattern, query)
        
        # Combine both types of joins
        all_joins = []
        for join in joins:
            # Format: (joined_table, left_column, right_column)
            all_joins.append((join[0], join[1], join[2], join[3], join[4]))
        
        for join in where_joins:
            # Format: (left_table, left_column, right_table, right_column)
            all_joins.append((join[0], join[1], join[2], join[3]))
        
        return all_joins

    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT clause from the query."""
        limit_pattern = r'(?i)LIMIT\s+(\d+)'
        match = re.search(limit_pattern, query)
        if match:
            return int(match.group(1))
        return None

    def _remove_limit(self, query: str) -> str:
        """Remove LIMIT clause from the query."""
        return re.sub(r'(?i)\s+LIMIT\s+\d+', '', query)
    
    async def _get_table_metadata(self, table: str) -> Dict:
        """Get metadata about a table from its data container."""
        table_config = self.config.tables[table]
        
        # Ensure URL has http:// prefix
        base_url = table_config.url
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"
        
        url = f"{base_url}:{table_config.port}/metadata"
        logger.info(f"Getting metadata from {url}")
        
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting metadata from {url}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error getting metadata from {url}: {str(e)}"
            )
    
    async def _execute_remote_query(self, table: str, query: str) -> pd.DataFrame:
        """Execute a query on a remote data container."""
        table_config = self.config.tables[table]
        
        # Ensure URL has http:// prefix
        base_url = table_config.url
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"
        
        url = f"{base_url}:{table_config.port}/query"
        logger.info(f"Executing remote query on {url}: {query}")
        
        try:
            response = await self.client.post(
                url,
                json={"query": query}
            )
            response.raise_for_status()
            result = response.json()
            
            # Convert the results to a pandas DataFrame
            df = pd.DataFrame(result["results"])
            if not df.empty:
                df.columns = result["columns"]
            return df
        except httpx.HTTPError as e:
            logger.error(f"HTTP error when querying {url}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error connecting to data container at {url}: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Error when querying {url}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error when querying {url}: {str(e)}"
            )
    
    async def _optimize_join_query(self, query: str) -> Tuple[str, List[pd.DataFrame]]:
        """Optimize a join query by first querying the smaller table and then using those IDs
        to query only the necessary rows from the larger table."""
        
        # Parse the query to get involved tables
        tables = self._parse_query(query)
        self._validate_tables(tables)
        
        # Extract LIMIT if present
        limit = self._extract_limit(query)
        query_without_limit = self._remove_limit(query)
        
        # If there's only one table, no need for optimization
        if len(tables) == 1:
            table = list(tables)[0]
            table_config = self.config.tables[table]
            
            # Extract the columns needed for this table
            table_pattern = rf'(?i)SELECT\s+(.*?)\s+FROM\s+{table}'
            match = re.search(table_pattern, query_without_limit)
            if match:
                columns = match.group(1)
            else:
                columns = "*"
            
            # Create and execute the subquery
            subquery = f"SELECT {columns} FROM {table_config.table_name}"
            if limit:
                subquery += f" LIMIT {limit}"
            logger.info(f"Executing single table query: {subquery}")
            df = await self._execute_remote_query(table, subquery)
            
            # Create a temporary table in DuckDB
            temp_table = f"temp_{table}"
            self.conn.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM df")
            
            # Return the modified query and the list of dataframes
            modified_query = query_without_limit.replace(table, temp_table)
            if limit:
                modified_query += f" LIMIT {limit}"
            return modified_query, [(table, pd.DataFrame())]  # Empty DataFrame as placeholder
        
        # Get join conditions
        join_conditions = self._parse_join_conditions(query_without_limit)
        
        # If we can't parse join conditions, fall back to the original approach
        if not join_conditions:
            logger.warning("Could not parse join conditions, falling back to original approach")
            return await self._execute_distributed_query_original(query)
        
        # Get metadata for all tables to determine which one is smaller
        table_metadata = {}
        for table in tables:
            table_metadata[table] = await self._get_table_metadata(table)
        
        # Determine the smallest table to start with
        smallest_table = min(tables, key=lambda t: table_metadata[t]["row_count"])
        logger.info(f"Smallest table is {smallest_table} with {table_metadata[smallest_table]['row_count']} rows")
        
        # Extract the columns needed for the smallest table
        table_pattern = rf'(?i)SELECT\s+(.*?)\s+FROM\s+{smallest_table}'
        match = re.search(table_pattern, query_without_limit)
        if match:
            columns = match.group(1)
        else:
            columns = "*"
        
        # Create and execute the subquery for the smallest table
        smallest_table_config = self.config.tables[smallest_table]
        subquery = f"SELECT {columns} FROM {smallest_table_config.table_name}"
        if limit:
            subquery += f" LIMIT {limit}"
        logger.info(f"Executing subquery for smallest table {smallest_table}: {subquery}")
        smallest_df = await self._execute_remote_query(smallest_table, subquery)
        
        # Create a temporary table for the smallest table
        temp_smallest_table = f"temp_{smallest_table}"
        self.conn.execute(f"CREATE TABLE {temp_smallest_table} AS SELECT * FROM smallest_df")
        
        # For each join condition, extract the IDs from the smallest table and use them to query the other table
        dfs = [(smallest_table, smallest_df)]
        modified_query = query_without_limit.replace(smallest_table, temp_smallest_table)
        
        for join in join_conditions:
            # Determine which table is the other table in this join
            if join[0] == smallest_table:
                other_table = join[3]
                other_column = join[4]
                smallest_column = join[1]
            else:
                other_table = join[0]
                other_column = join[1]
                smallest_column = join[3]
            
            # Skip if we've already processed this table
            if any(table == other_table for table, _ in dfs):
                continue
            
            # Extract the IDs from the smallest table
            id_query = f"SELECT DISTINCT {smallest_column} FROM {temp_smallest_table}"
            id_df = self.conn.execute(id_query).fetchdf()
            
            if id_df.empty:
                logger.warning(f"No IDs found in {smallest_table}.{smallest_column}")
                continue
            
            # Convert IDs to a list for the IN clause
            ids = id_df[smallest_column].tolist()
            id_list = ", ".join([f"'{id}'" if isinstance(id, str) else str(id) for id in ids])
            
            # Limit the number of IDs to avoid overly long queries
            if len(ids) > 1000:
                logger.warning(f"Too many IDs ({len(ids)}), limiting to 1000")
                id_list = ", ".join([f"'{id}'" if isinstance(id, str) else str(id) for id in ids[:1000]])
            
            # Create and execute the subquery for the other table
            other_table_config = self.config.tables[other_table]
            other_subquery = f"SELECT * FROM {other_table_config.table_name} WHERE {other_column} IN ({id_list})"
            # Note: We don't apply LIMIT here because we want all matching rows from the second table
            logger.info(f"Executing subquery for {other_table} with {len(ids)} IDs")
            other_df = await self._execute_remote_query(other_table, other_subquery)
            
            # Create a temporary table for the other table
            temp_other_table = f"temp_{other_table}"
            self.conn.execute(f"CREATE TABLE {temp_other_table} AS SELECT * FROM other_df")
            
            # Update the modified query
            modified_query = modified_query.replace(other_table, temp_other_table)
            
            # Add to the list of dataframes
            dfs.append((other_table, other_df))
        
        # Add back the LIMIT clause if it was present
        if limit:
            modified_query += f" LIMIT {limit}"
        
        return modified_query, dfs
    
    async def _execute_distributed_query_original(self, query: str) -> Tuple[str, List[pd.DataFrame]]:
        """Original implementation of distributed query execution."""
        # Parse the query to get involved tables
        tables = self._parse_query(query)
        self._validate_tables(tables)
        
        # Extract LIMIT if present
        limit = self._extract_limit(query)
        query_without_limit = self._remove_limit(query)
        
        # For each table, create a subquery to get the required data
        dfs = []
        for table in tables:
            table_config = self.config.tables[table]
            
            # Extract the columns needed for this table
            # This is a simplified version - in practice, you'd need a proper SQL parser
            table_pattern = rf'(?i)SELECT\s+(.*?)\s+FROM\s+{table}'
            match = re.search(table_pattern, query_without_limit)
            if match:
                columns = match.group(1)
            else:
                columns = "*"
            
            # Create and execute the subquery
            subquery = f"SELECT {columns} FROM {table_config.table_name}"
            logger.info(f"Executing subquery for table {table}: {subquery}")
            df = await self._execute_remote_query(table, subquery)
            
            # Create a temporary table in DuckDB
            temp_table = f"temp_{table}"
            self.conn.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM df")
            dfs.append((table, df))
        
        # Execute the join in DuckDB
        # Replace table names in the original query with temp table names
        modified_query = query_without_limit
        for table, _ in dfs:
            temp_table = f"temp_{table}"
            modified_query = modified_query.replace(table, temp_table)
        
        # Add back the LIMIT clause if it was present
        if limit:
            modified_query += f" LIMIT {limit}"
        
        return modified_query, dfs
    
    async def _execute_distributed_query(self, query: str) -> pd.DataFrame:
        """Execute a query across multiple data containers."""
        try:
            # Try to optimize the join query
            modified_query, dfs = await self._optimize_join_query(query)
            
            # Execute the modified query
            logger.info(f"Executing optimized query: {modified_query}")
            result = self.conn.execute(modified_query).fetchdf()
            
            # Clean up temporary tables
            for table, _ in dfs:
                temp_table = f"temp_{table}"
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            
            return result
        except Exception as e:
            logger.error(f"Error in optimized query execution: {str(e)}")
            logger.info("Falling back to original approach")
            
            # Fall back to the original approach
            modified_query, dfs = await self._execute_distributed_query_original(query)
            
            # Execute the modified query
            logger.info(f"Executing original query: {modified_query}")
            result = self.conn.execute(modified_query).fetchdf()
            
            # Clean up temporary tables
            for table, _ in dfs:
                temp_table = f"temp_{table}"
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            
            return result
    
    async def execute_query(self, query_request: QueryRequest) -> QueryResponse:
        """Execute a distributed query and return the results."""
        start_time = time.time()
        
        try:
            # Execute the distributed query
            result = await self._execute_distributed_query(query_request.query)
            
            # Convert results to list of dictionaries
            results = result.to_dict('records')
            columns = list(result.columns)
            
            execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            # Get the source tables from the query
            source_tables = list(self._parse_query(query_request.query))
            
            return QueryResponse(
                results=results,
                columns=columns,
                execution_time_ms=execution_time,
                timestamp=datetime.now().isoformat(),
                source_tables=source_tables
            )
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

def main():
    parser = argparse.ArgumentParser(description='Start a distributed query server')
    parser.add_argument('--config', default='config.json', help='Path to configuration file')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind the server to')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create FastAPI app
    app = FastAPI(
        title="Distributed DuckDB Query API",
        description="API for querying data across multiple DuckDB containers",
        version="1.0.0"
    )
    
    # Create distributed query server
    server = DistributedQueryServer(args.config)
    
    @app.get("/")
    async def root():
        return {
            "message": "Distributed DuckDB Query API is running",
            "docs_url": "/docs",
            "redoc_url": "/redoc",
            "available_tables": list(server.config.tables.keys())
        }
    
    @app.post("/query", response_model=QueryResponse)
    async def query(query_request: QueryRequest):
        return await server.execute_query(query_request)
    
    # Start the server
    logger.info(f"Starting distributed query server on {args.host}:{args.port}")
    logger.info(f"Available tables: {', '.join(server.config.tables.keys())}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main() 