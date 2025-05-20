import streamlit as st
import requests
import pandas as pd

API_BASE = "http://localhost:8000"  # Change if FastAPI is hosted elsewhere

st.set_page_config(layout="wide")
st.title("QueryMate")
st.title("🧠 Natural Language to Database Query Interface")

db_type = st.selectbox("Select Database Type", ["PostgreSQL", "MySQL", "MongoDB"])

with st.sidebar:
    st.header("Configure Database")

    if db_type in ["PostgreSQL", "MySQL"]:
        host = st.text_input("Host", "localhost")
        port = st.text_input("Port", "5432" if db_type == "PostgreSQL" else "3306")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database Name")

        if st.button("Connect"):
            payload = {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "database": database,
            }
            endpoint = "/configure-db" if db_type == "PostgreSQL" else "/configure-db-mysql"
            try:
                res = requests.post(API_BASE + endpoint, data=payload)
                st.success(res.json().get("message"))
            except Exception as e:
                st.error(f"Connection failed: {e}")

    elif db_type == "MongoDB":
        mongo_conn = st.text_input("Mongo URI", "mongodb://localhost:27017/")
        mongo_db = st.text_input("Database Name")

        if st.button("Connect"):
            payload = {"connection_string": mongo_conn, "database": mongo_db}
            try:
                res = requests.post(API_BASE + "/configure-db-mongo", data=payload)
                st.success(res.json().get("message"))
            except Exception as e:
                st.error(f"Connection failed: {e}")

st.subheader("Ask a Question")
prompt = st.text_area("Describe your query in natural language")

uploaded_file = st.file_uploader("Upload CSV/Excel (optional)", type=["csv", "xlsx"])

if st.button("Run Query"):
    if not prompt.strip():
        st.warning("Please enter a prompt")
    else:
        files = {"file": uploaded_file.getvalue()} if uploaded_file else None
        data = {"prompt": prompt}
        endpoint = {
            "PostgreSQL": "/query",
            "MySQL": "/query-mysql",
            "MongoDB": "/query-mongo"
        }[db_type]

        try:
            if files:
                files = {
                    "file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)
                }
                res = requests.post(API_BASE + endpoint, data=data, files=files)
            else:
                res = requests.post(API_BASE + endpoint, data=data)

            response = res.json()

            if response["status"] == "success":
                if "data" in response:
                    df = pd.DataFrame(response["data"])
                    st.success("Query executed successfully!")
                    st.dataframe(df)
                else:
                    st.success(response.get("message", "Query executed"))
            else:
                st.error(f"Error: {response.get('message')}")

        except Exception as e:
            st.error(f"Request failed: {e}")
