from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os
from typing import List, Dict, Any

app = FastAPI()

# Database connection
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

engine = create_engine(DATABASE_URL)

@app.get("/")
async def root():
    return {"message": "Hello World API"}

@app.get("/tables")
async def list_tables() -> List[str]:
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in result]
            return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/table/{table_name}")
async def get_table_data(table_name: str) -> List[Dict[Any, Any]]:
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name} LIMIT 100"))
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result]
            return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 