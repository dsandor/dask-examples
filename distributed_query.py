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
    
    def _parse_join_conditions(self, query: str) -> Tuple[List[str], List[str], List[str]]:
        """Parse join conditions from SQL query to determine table relationships."""
        # Extract table names from FROM and JOIN clauses
        tables = []
        join_conditions = []
        where_conditions = []
        
        # Split query into parts
        parts = query.lower().split()
        current_table = None
        in_where = False
        
        for i, word in enumerate(parts):
            if word == 'from':
                current_table = parts[i + 1].strip(';')
                tables.append(current_table)
            elif word == 'join':
                current_table = parts[i + 1].strip(';')
                tables.append(current_table)
            elif word == 'on':
                # Extract join condition
                condition = ' '.join(parts[i + 1:]).split('where')[0].strip(';')
                join_conditions.append(condition)
            elif word == 'where':
                in_where = True
                # Extract where conditions
                where_clause = ' '.join(parts[i + 1:]).strip(';')
                where_conditions.append(where_clause)
                break
        
        return tables, join_conditions, where_conditions

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
    
    async def _optimize_join_query(self, query: str) -> str:
        """Optimize join query by querying smaller table first."""
        tables, join_conditions, where_conditions = self._parse_join_conditions(query)
        
        if len(tables) < 2:
            return query
        
        # Get metadata for all tables
        table_metadata = {}
        for table in tables:
            try:
                table_metadata[table] = await self._get_table_metadata(table)
            except Exception as e:
                logger.error(f"Error getting metadata for {table}: {str(e)}")
                return query
        
        # Find the smallest table
        smallest_table = min(table_metadata.items(), key=lambda x: x[1]['row_count'])
        
        # Create temporary tables for each data source
        temp_tables = []
        try:
            for table in tables:
                container = self.config.tables[table]
                temp_table = f"temp_{table}"
                temp_tables.append(temp_table)
                
                # Drop existing temporary table if it exists
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                # Create temporary table
                self.conn.execute(f"""
                    CREATE TABLE {temp_table} AS 
                    SELECT * FROM '{container.url}:{container.port}/{container.table_name}'
                """)
            
            # Build optimized query
            optimized_query = f"""
                WITH {', '.join(f'{temp} AS (SELECT * FROM {temp})' for temp in temp_tables)}
                SELECT * FROM {temp_tables[0]}
                {' '.join(f'JOIN {temp} ON {cond}' for temp, cond in zip(temp_tables[1:], join_conditions))}
                {'WHERE ' + ' AND '.join(where_conditions) if where_conditions else ''}
            """
            
            return optimized_query
        except Exception as e:
            logger.error(f"Error in query optimization: {str(e)}")
            return query
        finally:
            # Clean up temporary tables
            for temp_table in temp_tables:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                except Exception as e:
                    logger.error(f"Error dropping temporary table {temp_table}: {str(e)}")
    
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
            modified_query = await self._optimize_join_query(query)
            
            # Execute the modified query
            logger.info(f"Executing optimized query: {modified_query}")
            result = self.conn.execute(modified_query).fetchdf()
            
            return result
        except Exception as e:
            logger.error(f"Error in optimized query execution: {str(e)}")
            logger.info("Falling back to original approach")
            
            # Fall back to the original approach
            modified_query, dfs = await self._execute_distributed_query_original(query)
            
            # Execute the modified query
            logger.info(f"Executing original query: {modified_query}")
            result = self.conn.execute(modified_query).fetchdf()
            
            return result
    
    def _parse_query_components(self, query: str) -> Dict:
        """Parse a SQL query into its components (SELECT, FROM, WHERE, LIMIT)."""
        # Initialize components
        components = {
            'select': '*',
            'from': None,
            'where': None,
            'limit': None
        }
        
        # Convert to lowercase for easier parsing
        query_lower = query.lower()
        
        # Extract SELECT clause
        select_match = re.search(r'select\s+(.*?)\s+from', query_lower)
        if select_match:
            components['select'] = select_match.group(1)
        
        # Extract FROM clause
        from_match = re.search(r'from\s+(\w+)', query_lower)
        if from_match:
            components['from'] = from_match.group(1)
        
        # Extract WHERE clause
        where_match = re.search(r'where\s+(.*?)(?:\s+limit\s+\d+)?$', query_lower)
        if where_match:
            components['where'] = where_match.group(1)
        
        # Extract LIMIT clause
        limit_match = re.search(r'limit\s+(\d+)', query_lower)
        if limit_match:
            components['limit'] = limit_match.group(1)
        
        return components

    def _build_container_query(self, components: Dict) -> str:
        """Build a query for a data container from parsed components."""
        query_parts = []
        
        # Add SELECT clause
        query_parts.append(f"SELECT {components['select']}")
        
        # Add FROM clause
        if components['from']:
            query_parts.append(f"FROM {components['from']}")
        
        # Add WHERE clause
        if components['where']:
            query_parts.append(f"WHERE {components['where']}")
        
        # Add LIMIT clause
        if components['limit']:
            query_parts.append(f"LIMIT {components['limit']}")
        
        return ' '.join(query_parts)

    async def execute_query(self, query_request: QueryRequest) -> QueryResponse:
        """Execute a distributed query and return the results."""
        try:
            start_time = time.time()
            print(f"\nExecuting distributed query at {datetime.now().isoformat()}")
            print(f"Query: {query_request.query}")
            
            # Parse the query to understand what tables are involved
            tables, join_conditions, where_conditions = self._parse_join_conditions(query_request.query)
            
            if len(tables) == 1:
                # Single table query - forward to appropriate container
                table = tables[0]
                container = self.config.tables.get(table)
                if not container:
                    raise ValueError(f"No data container found for table '{table}'")
                
                # Parse the query components
                components = self._parse_query_components(query_request.query)
                
                # Build the container query
                container_query = self._build_container_query(components)
                print(f"Forwarding query to container: {container_query}")
                
                # Forward the query to the data container
                async with httpx.AsyncClient() as client:
                    container_url = f"{container.url}:{container.port}"
                    print(f"Connecting to container at: {container_url}")
                    response = await client.post(
                        f"{container_url}/query",
                        json={"query": container_query}
                    )
                    response.raise_for_status()
                    response_data = response.json()
                    # Add source_tables to the response
                    response_data['source_tables'] = [table]
                    return response_data
            else:
                # Multi-table query - use optimization
                optimized_query = await self._optimize_join_query(query_request.query)
                result = self.conn.execute(optimized_query).fetchdf()
                
                # Convert results to list of dictionaries
                results = result.to_dict('records')
                columns = list(result.columns)
                
                execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
                print(f"\nQuery Execution Stats:")
                print(f"  Execution time: {execution_time:.2f}ms")
                print(f"  Rows returned: {len(results):,}")
                print(f"  Columns: {', '.join(columns)}")
                
                return QueryResponse(
                    results=results,
                    columns=columns,
                    execution_time_ms=execution_time,
                    timestamp=datetime.now().isoformat(),
                    source_tables=tables
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
    async def execute_query(query_request: QueryRequest):
        return await server.execute_query(query_request)
    
    # Start the server
    logger.info(f"Starting distributed query server on {args.host}:{args.port}")
    logger.info(f"Available tables: {', '.join(server.config.tables.keys())}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main() 