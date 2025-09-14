from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from db import run_query
from config import Config

app = FastAPI()

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import os
config = Config()
openai_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=openai_key) if openai_key else None

class AskRequest(BaseModel):
    question: str

@app.post("/hello")
async def hello():
    return {"reply": "hello"}

@app.post("/ask")
async def ask(body: AskRequest):
    question = body.question
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    # Graceful fallback: if no API key, return simple reply to avoid 500s in dev
    if not openai_key:
        return {"reply": "hello", "note": "OPENAI_API_KEY not set; returning mock reply"}

    prompt = f"""
    You are a SQL expert. Convert this question into a MySQL/TiDB SQL query. Only return the SQL.

    Question: {question}
    """
    try:
        if client is None:
            # Should be caught earlier, but double-guard
            return {"reply": "hello", "note": "OPENAI_API_KEY not set; returning mock reply"}
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        content = response.choices[0].message.content or ""
        # Remove markdown code blocks if present
        sql_query = content.strip()
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()
    except Exception as e:
        # Return graceful response instead of 500 to keep client UX smooth
        return {"reply": "hello", "note": f"OpenAI error: {str(e)}"}

    # Graceful fallback: if DB envs are missing, skip execution
    if not all([
        getattr(config, "tidb_host", None),
        getattr(config, "tidb_port", None),
        getattr(config, "tidb_user", None),
        getattr(config, "tidb_password", None),
        getattr(config, "tidb_db_name", None),
    ]):
        return {"sql": sql_query, "results": [], "note": "DB connection not configured"}

    try:
        results = run_query(sql_query)
    except Exception as e:
        # Return generated SQL with note instead of 500
        return {"sql": sql_query, "results": [], "note": f"DB error: {str(e)}"}

    return {"sql": sql_query, "results": results}

# Run with: uvicorn main:app --reload --port 5000
