import requests
import os
from dotenv import load_dotenv
from groq import Groq
load_dotenv()


# client = Groq(
#     api_key="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",
# )
client = Groq(
    api_key=os.getenv("GROQ_API_KEY"),
)

def get_sql_from_prompt(prompt: str,schema: str,DB: str) -> str:
    chat_completion = client.chat.completions.create(
    messages=[
        {
            "role": "system",
            "content": f"""
                You are a SQL assistant integrated into a backend system. Your job is to convert user-friendly natural language requests into valid and executable {DB} queries.
                Generate SQL queries based on the Database type ("mysql", "postgres", "mongodb").
                ***This is the schema of the db {schema} understand the schema carefully and write the sql query***
                Refer the example inputs and outputs below to understand the format of the SQL queries you need to generate.
                Respond ONLY with a single, clean SQL query.

                DO NOT include any Markdown formatting (like ```sql blocks).

                DO NOT explain the query or add extra comments.

                Only output valid SQL that works with {DB}.

                Assume the database is already connected and ready.

                Be cautious with destructive queries (e.g., DROP, TRUNCATE). Avoid them unless explicitly asked.

                Prefer safe defaults like CREATE TABLE IF NOT EXISTS and INSERT INTO ... ON CONFLICT DO NOTHING when appropriate.

                Support commands like creating tables, inserting data, updating rows, deleting entries, and selecting data.

                Column types should follow the {DB} syntax.

                Example inputs and outputs:

                //postgres and mysql
                Input: create a table called users with name and email
                Output: CREATE TABLE IF NOT EXISTS users (name VARCHAR(255), email VARCHAR(255));

                Input: show all users
                Output: SELECT * FROM users;

                //mongodb
                Input: create a collection called users with name and email
                Output: results = list(db.create_collection("users"))
                Input: show all user
                Output: results = list(db.user.find())

                if {DB} is "mongodb" 
                output should be in the format:
                results = list(db.collection_name.find(<filter>))
                ***don't change the  format***
""",
        },
        {
            "role": "user",
            "content":  f"Convert this to SQL: {prompt}",
        }
    ],
    model="llama-3.3-70b-versatile",
    stream=False,
)

    # print(chat_completion.choices[0].message.content)
    return chat_completion.choices[0].message.content


