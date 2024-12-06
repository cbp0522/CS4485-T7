from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import sqlite3
import re

app = FastAPI()

# Add CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

DATABASE_FILE = "t5.db"

class ImportData(BaseModel):
    data: List[Dict[str, Any]]
    table_name: str

def get_db_connection():
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn

@app.post("/kpi_management/api/import_kpis")
async def import_kpis(payload: ImportData):
    data = payload.data
    table_name = payload.table_name

    if not data or not isinstance(data, list) or len(data) == 0:
        raise HTTPException(status_code=400, detail="No data provided or data is not an array.")

    if not table_name or not isinstance(table_name, str):
        raise HTTPException(status_code=400, detail="Invalid or missing table_name.")

    # Infer columns from the first row
    first_row = data[0]
    columns = list(first_row.keys())
    if not columns:
        raise HTTPException(status_code=400, detail="No columns found in the data.")

    # Sanitize table name: only allow alphanumeric and underscores
    sanitized_table_name = re.sub(r'[^a-zA-Z0-9_]', '', table_name)
    if not sanitized_table_name:
        raise HTTPException(status_code=400, detail="Invalid table name after sanitization.")

    # All columns as TEXT for simplicity, can be adjusted based on your needs
    column_definitions = ", ".join([f'"{col}" TEXT' for col in columns])
    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{sanitized_table_name}" ({column_definitions})'

    conn = get_db_connection()
    try:
        conn.execute(create_table_sql)
        conn.commit()

        # Insert rows
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f'INSERT INTO "{sanitized_table_name}" ({", ".join([f"{c}" for c in columns])}) VALUES ({placeholders})'
        values_list = []
        for row in data:
            # Convert all values to strings (if needed)
            values = [str(row.get(col, "")) for col in columns]
            values_list.append(values)

        conn.executemany(insert_sql, values_list)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error importing data: {e}")
    finally:
        conn.close()

    return {"message": f"Data successfully imported into table '{sanitized_table_name}'."}


# Example endpoint to retrieve available tables (matches your frontend usage):
@app.get("/kpi_management/api/tables")
async def get_tables():
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row["name"] for row in cur.fetchall() if row["name"] != "sqlite_sequence"]
    finally:
        conn.close()
    return tables

@app.get("/")
async def test():
    return "hello world"

# Example endpoint to retrieve data from a specific table:
@app.get("/kpi_management/api/kpis")
async def get_kpis(table: str):
    # Sanitize table name
    sanitized_table_name = re.sub(r'[^a-zA-Z0-9_]', '', table)
    if not sanitized_table_name:
        raise HTTPException(status_code=400, detail="Invalid table name.")

    conn = get_db_connection()
    try:
        cur = conn.execute(f'SELECT * FROM "{sanitized_table_name}"')
        rows = [dict(row) for row in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {e}")
    finally:
        conn.close()

    return rows
