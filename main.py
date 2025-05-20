from fastapi import FastAPI, HTTPException
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from db import SessionLocal
from groq_client import get_sql_from_prompt
import sqlparse
import pandas as pd
from sqlalchemy import create_engine, text,inspect
from pymongo import MongoClient
from qdrant_client import QdrantClient
from io import StringIO, BytesIO
from sqlalchemy.orm import sessionmaker

app = FastAPI()


from typing import Optional
from pydantic import BaseModel


def get_db_schema(engine) -> str:
    inspector = inspect(engine)
    schema_info = []

    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        column_defs = ", ".join([f"{col['name']} ({col['type']})" for col in columns])
        schema_info.append(f"Table: {table_name} → Columns: {column_defs}")

    return "\n".join(schema_info)

    
PS_DB_CONFIG = {}

@app.post("/configure-db")
def configure_db(
    host: str = Form(...),
    port: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    database: str = Form(...)
):
    PS_DB_CONFIG.update({
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
    })
    return {"status": "success", "message": "Database configured successfully"}

@app.post("/query")
async def query_handler(
    prompt: str = Form(...),
    file: UploadFile | None = File(None)
):
    if not PS_DB_CONFIG:
        raise HTTPException(status_code=400, detail="Database not configured. Use /configure-db first.")

    db_url = (
        f"postgresql://{PS_DB_CONFIG['username']}:{PS_DB_CONFIG['password']}"
        f"@{PS_DB_CONFIG['host']}:{PS_DB_CONFIG['port']}/{PS_DB_CONFIG['database']}"
    )
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    
    file_data = None
    if file:
        if file.content_type not in ("text/csv", "application/vnd.ms-excel", 
                                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            raise HTTPException(status_code=400, detail="Only CSV or Excel files are accepted")
        contents = await file.read()
        try:
            if file.content_type == "text/csv":
                df = pd.read_csv(StringIO(contents.decode('utf-8')))
            else:
                df = pd.read_excel(BytesIO(contents))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")
        file_data = df.to_dict(orient="records")

    final_prompt = prompt + "\n\n" + str(file_data) if file_data else prompt
    schema = get_db_schema(engine)

    sql = get_sql_from_prompt(final_prompt, schema,"postgres")

    parsed = sqlparse.parse(sql)
    if any(token.ttype is None and token.value.lower().startswith("drop") for token in parsed[0].tokens):
        raise HTTPException(status_code=400, detail="Dangerous query blocked")

    db = SessionLocal()
    try:
        result = db.execute(text(sql))
        if result.returns_rows:
            rows = result.mappings().all()
            return {"status": "success", "data": rows}
        else:
            db.commit()
            return {"status": "success", "message": "Query executed successfully"}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        db.close()



MYSQL_DB_CONFIG = {}

@app.post("/configure-db-mysql")
def configure_mysql_db(
    host: str = Form(...),
    port: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    database: str = Form(...)
):
    MYSQL_DB_CONFIG.update({
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
    })
    
    try:
        test_url = (
            f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        )
        engine = create_engine(test_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MySQL connection failed: {str(e)}")

    return {"status": "success", "message": "MySQL database configured successfully"}


@app.post("/query-mysql")
async def query_mysql_handler(
    prompt: str = Form(...),
    file: UploadFile | None = File(None)
):
    if not MYSQL_DB_CONFIG:
        raise HTTPException(status_code=400, detail="MySQL database not configured. Use /configure-db-mysql first.")

    db_url = (
        f"mysql+pymysql://{MYSQL_DB_CONFIG['username']}:{MYSQL_DB_CONFIG['password']}"
        f"@{MYSQL_DB_CONFIG['host']}:{MYSQL_DB_CONFIG['port']}/{MYSQL_DB_CONFIG['database']}"
    )
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    file_data = None
    if file:
        if file.content_type not in ("text/csv", "application/vnd.ms-excel", 
                                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            raise HTTPException(status_code=400, detail="Only CSV or Excel files are accepted")
        contents = await file.read()
        try:
            if file.content_type == "text/csv":
                df = pd.read_csv(StringIO(contents.decode('utf-8')))
            else:
                df = pd.read_excel(BytesIO(contents))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")
        file_data = df.to_dict(orient="records")

    final_prompt = prompt + "\n\n" + str(file_data) if file_data else prompt
    schema = get_db_schema(engine)
    print(schema)
    sql = get_sql_from_prompt(final_prompt, schema, "mysql")

    parsed = sqlparse.parse(sql)
    if any(token.ttype is None and token.value.lower().startswith("drop") for token in parsed[0].tokens):
        raise HTTPException(status_code=400, detail="Dangerous query blocked")

    db = SessionLocal()
    try:
        result = db.execute(text(sql))
        if result.returns_rows:
            rows = result.mappings().all()
            return {"status": "success", "data": rows}
        else:
            db.commit()
            return {"status": "success", "message": "Query executed successfully"}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        db.close()



from fastapi import Form, HTTPException
from pymongo import MongoClient

MONGO_CONFIG = {}

@app.post("/configure-db-mongo")
def configure_mongo(connection_string: str = Form(...), database: str = Form(...)):
    try:
        client = MongoClient(connection_string)
        client[database].command("ping")
        MONGO_CONFIG.update({
            "client": client,
            "database": database
        })
        return {"status": "success", "message": "MongoDB connected"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MongoDB connection failed: {str(e)}")


def get_mongo_schema(db):
    schema_text = ""
    for coll_name in db.list_collection_names():
        sample = db[coll_name].find_one()
        if sample:
            sample.pop("_id", None)
            schema_text += f"Collection: {coll_name} → Sample document: {sample}\n"
    return schema_text


from bson import ObjectId

def serialize_mongo_result(data):
    if isinstance(data, list):
        return [serialize_mongo_result(doc) for doc in data]
    elif isinstance(data, dict):
        return {
            k: str(v) if isinstance(v, ObjectId) else serialize_mongo_result(v)
            for k, v in data.items()
        }
    else:
        return data

@app.post("/query-mongo")
async def query_mongo_handler(prompt: str = Form(...)):
    if not MONGO_CONFIG:
        raise HTTPException(status_code=400, detail="MongoDB not configured")

    client = MONGO_CONFIG["client"]
    db = client[MONGO_CONFIG["database"]]

    schema = get_mongo_schema(db)
    print(schema)
    

    sql = get_sql_from_prompt(prompt, schema, "mongodb")
    print("Generated Mongo code:\n", sql)
    print("Type of code:", type(sql))
    try:
        local_vars = {"db": db, "results": []}
        exec(sql, {}, local_vars)
        raw_results = local_vars.get("results", [])
        serialized = serialize_mongo_result(raw_results)
        return {"status": "success", "data":serialized}
    except Exception as e:
        return {"status": "error", "message": f"Query execution failed: {str(e)}"}

