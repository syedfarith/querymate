import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from pymongo import MongoClient
from bson import ObjectId
import sqlparse
from io import StringIO, BytesIO

from groq_client import get_sql_from_prompt
def get_sqlalchemy_schema(engine):
    inspector = inspect(engine)
    schema_info = []
    for table in inspector.get_table_names():
        columns = inspector.get_columns(table)
        schema_info.append(f"Table: {table}")
    return "\n".join(schema_info)

def get_mongo_schema(db):
    schema_text = ""
    for coll_name in db.list_collection_names():
        sample = db[coll_name].find_one()
        if sample:
            sample.pop("_id", None)
            schema_text += f"Collection: {coll_name} → Sample: {sample}\n"
    return schema_text

def parse_file(file):
    try:
        if file.content_type == "text/csv":
            df = pd.read_csv(StringIO(file.getvalue().decode("utf-8")))
        else:
            df = pd.read_excel(BytesIO(file.getvalue()))
        return df.to_dict(orient="records")
    except Exception as e:
        st.error(f"Failed to parse uploaded file: {e}")
        return None

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

# UI Starts
st.set_page_config(page_title="Natural Language to SQL/Mongo Queries", page_icon="🧠")
st.title("QueryMate")
st.title("🧠 Natural Language to SQL/Mongo Queries")

db_type = st.sidebar.selectbox("Database Type", ["PostgreSQL", "MySQL", "MongoDB"])

# Sidebar DB config
st.sidebar.subheader("DB Configuration")
if db_type == "MongoDB":
    connection_string = st.sidebar.text_input("MongoDB Connection String (e.g., mongodb://user:pass@host:port/)")
    database = st.sidebar.text_input("Database Name")
else:
    host = st.sidebar.text_input("Host", value="localhost")
    port = st.sidebar.text_input("Port", value="5432" if db_type == "PostgreSQL" else "6449")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    database = st.sidebar.text_input("Database Name")
file = st.file_uploader("Optional: Upload CSV/Excel", type=["csv", "xlsx"])
prompt = st.text_area("Enter your natural language request", height=100)

if "engine" not in st.session_state:
    st.session_state["engine"] = None
if "mongo_db" not in st.session_state:
    st.session_state["mongo_db"] = None
if "schema" not in st.session_state:
    st.session_state["schema"] = ""

# Connect button
if st.sidebar.button("Connect"):
    try:
        if db_type in ["PostgreSQL", "MySQL"]:
            db_url = (
                f"postgresql://{username}:{password}@{host}:{port}/{database}"
                if db_type == "PostgreSQL"
                else f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
            )
            engine = create_engine(db_url)
            engine.connect().close()
            st.session_state["engine"] = engine
            schema = get_sqlalchemy_schema(engine)
            st.session_state["schema"] = schema
            st.success(f"{db_type} connected successfully!")
        elif db_type == "MongoDB":
            client = MongoClient(connection_string)
            db = client[database]
            db.command("ping")  # test the connection
            st.session_state["mongo_db"] = db
            schema = get_mongo_schema(db)
            st.session_state["schema"] = schema
            st.success("MongoDB connected successfully!")
    except Exception as e:
        st.error(f"Connection failed: {e}")


from bson import ObjectId, Decimal128
import datetime

def sanitize_bson(doc):
    for k, v in doc.items():
        if isinstance(v, (ObjectId, Decimal128)):
            doc[k] = str(v)
        elif isinstance(v, datetime.datetime):
            doc[k] = v.isoformat()
    return doc

def mongo_cursor_to_df(cursor):
    docs = [sanitize_bson(doc) for doc in cursor]
    return pd.DataFrame(docs)

# Run query
if st.button("Run Query"):
    try:
        if not prompt.strip():
            st.warning("Please enter a query prompt.")
            st.stop()

        file_data = parse_file(file) if file else None
        final_prompt = prompt + "\n\n" + str(file_data) if file_data else prompt

        sql = get_sql_from_prompt(final_prompt, st.session_state["schema"], db_type.lower())

        if db_type in ["PostgreSQL", "MySQL"]:
            engine = st.session_state["engine"]
            parsed = sqlparse.parse(sql)
            if any(token.ttype is None and token.value.lower().startswith("drop") for token in parsed[0].tokens):
                st.error("DROP queries are not allowed.")
                st.stop()
            with engine.connect() as conn:
                result = conn.execute(text(sql))
                if result.returns_rows:
                    df = pd.DataFrame(result.mappings().all())
                    st.dataframe(df)
                else:
                    st.success("Query executed successfully (no rows returned).")
        else:
            db = st.session_state["mongo_db"]
            local_vars = {"db": db, "results": []}
            exec(sql, {}, local_vars)
            result = local_vars.get("results", [])
            df = mongo_cursor_to_df(result)
            st.dataframe(df)

    except Exception as e:
        st.error(f"Query failed: {e}")
