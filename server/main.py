from fastapi import FastAPI, HTTPException, Form, UploadFile, File
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from db import run_query
from config import Config
from typing import List, Optional
import os
import tempfile
import subprocess
import sys

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
async def ask(
    question: str = Form(...),
    files: List[UploadFile] = File(default=[])
):
    if not question.strip() and not files:
        raise HTTPException(status_code=400, detail="Question or files are required")

    # Process uploaded files - save them and run through pipeline
    file_contents = []
    processed_files = []
    
    if files:
        # Create a temporary directory for uploaded files
        with tempfile.TemporaryDirectory() as temp_dir:
            for file in files:
                try:
                    # Read file content once
                    content = await file.read()
                    file_path = os.path.join(temp_dir, file.filename)
                    
                    # Save file to temporary directory
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    
                    # Run through our pipeline
                    try:
                        pipeline_cmd = [
                            sys.executable, 
                            "server/ingest/pipeline.py",
                            "--input", file_path,
                            "--symbol", "UPLOADED",  # Default symbol for uploaded files
                            "--title", f"Uploaded: {file.filename}"
                        ]
                        
                        result = subprocess.run(
                            pipeline_cmd, 
                            capture_output=True, 
                            text=True, 
                            cwd=os.path.dirname(os.path.dirname(__file__))  # Project root
                        )
                        
                        if result.returncode == 0:
                            processed_files.append(f"✅ {file.filename} - processed and stored in database")
                        else:
                            processed_files.append(f"❌ {file.filename} - processing failed: {result.stderr}")
                            
                    except Exception as e:
                        processed_files.append(f"❌ {file.filename} - pipeline error: {str(e)}")
                    
                    # Try to decode as text for immediate display (using the same content)
                    try:
                        text_content = content.decode('utf-8')
                        file_contents.append(f"File: {file.filename}\n{text_content[:500]}{'...' if len(text_content) > 500 else ''}")
                    except UnicodeDecodeError:
                        file_contents.append(f"File: {file.filename} (binary file, {file.content_type})")
                        
                except Exception as e:
                    file_contents.append(f"File: {file.filename} (error reading: {str(e)})")
                    processed_files.append(f"❌ {file.filename} - error: {str(e)}")
    
    # Combine question with file contents and processing status
    full_question = question
    if file_contents:
        full_question += "\n\nAttached files:\n" + "\n\n".join(file_contents)
    if processed_files:
        full_question += "\n\nFile processing status:\n" + "\n".join(processed_files)

    # Graceful fallback: if no API key, return simple reply to avoid 500s in dev
    if not openai_key:
        reply = f"Received: {question}"
        if processed_files:
            reply += f"\n\nFile processing status:\n" + "\n".join(processed_files)
        return {"reply": reply, "note": "OPENAI_API_KEY not set; returning mock reply"}

    prompt = f"""
    You are a SQL expert. Convert this question into a MySQL/TiDB SQL query. Only return the SQL.
    Consider any attached file contents when generating the query.

    Question: {full_question}
    """
    try:
        if client is None:
            # Should be caught earlier, but double-guard
            reply = f"Received: {question}"
            if processed_files:
                reply += f"\n\nFile processing status:\n" + "\n".join(processed_files)
            return {"reply": reply, "note": "OPENAI_API_KEY not set; returning mock reply"}
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
        reply = f"Received: {question}"
        if processed_files:
            reply += f"\n\nFile processing status:\n" + "\n".join(processed_files)
        return {"reply": reply, "note": f"OpenAI error: {str(e)}"}

    # Graceful fallback: if DB envs are missing, skip execution
    if not all([
        getattr(config, "tidb_host", None),
        getattr(config, "tidb_port", None),
        getattr(config, "tidb_user", None),
        getattr(config, "tidb_password", None),
        getattr(config, "tidb_db_name", None),
    ]):
        return {"sql": sql_query, "results": [], "note": "DB connection not configured", "files_received": len(files), "processed_files": processed_files}

    try:
        results = run_query(sql_query)
    except Exception as e:
        # Return generated SQL with note instead of 500
        return {"sql": sql_query, "results": [], "note": f"DB error: {str(e)}", "files_received": len(files), "processed_files": processed_files}


    return {"sql": sql_query, "results": results, "files_received": len(files), "processed_files": processed_files}

# Run with: uvicorn main:app --reload --port 5000
