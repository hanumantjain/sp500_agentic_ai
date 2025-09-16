from fastapi import FastAPI, HTTPException, Form, UploadFile, File
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from db import run_query
from tools import search_docs_auto
from ingest.extract_text import compute_file_hash
from config import Config
from memory import new_session, add_message, get_recent_context, attach_docs, get_scoped_doc_ids
from agent_core import Agent, AgentFunctionCallingActionLanguage, PythonActionRegistry, Environment, Goal, Memory, generate_response
from typing import List, Optional
import os
import tempfile
import subprocess
import sys
import json

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

# --- Unified agent personas ---
# Financial advisor focused on S&P 500 using TiDB-backed data.
SYSTEM_RAG = (
    "You are a financial advisor specializing in S&P 500 companies. "
    "Answer ONLY using the provided context from our TiDB-backed document store. "
    "Cite every factual statement with its source as [chunk_id]. "
    "If information is missing from context, say what is missing and do not guess."
)

SYSTEM_SQL = (
    "You are a financial advisor specializing in S&P 500 companies. "
    "Your task is to create a single valid MySQL/TiDB SQL query that answers the user's question "
    "using our TiDB schema. Return ONLY the SQL, no prose, no markdown fences."
)

SYSTEM_TOOL_CALLING = (
    "You are a financial advisor specializing in S&P 500 companies. "
    "You have access to various tools to analyze S&P 500 data, search documents, and provide financial insights. "
    "Use the appropriate tools to answer user questions. When you have the information you need, provide a comprehensive response. "
    "Always cite your sources and be specific about the data you're using."
)

# --------------------- Function Calling Setup ---------------------

def create_financial_agent():
    """Create a financial advisor agent with access to S&P 500 tools"""
    goals = [
        Goal(priority=1, name="financial_analysis", description="Provide accurate financial analysis of S&P 500 companies"),
        Goal(priority=2, name="data_retrieval", description="Retrieve relevant financial data using available tools"),
        Goal(priority=3, name="document_analysis", description="Analyze uploaded documents for financial insights")
    ]
    
    # Create action registry with financial and document tools
    action_registry = PythonActionRegistry(tags=["financial", "sp500", "vector", "docs", "search"])
    environment = Environment()
    agent_language = AgentFunctionCallingActionLanguage()
    
    return Agent(
        goals=goals,
        agent_language=agent_language,
        action_registry=action_registry,
        generate_response=generate_response,
        environment=environment
    )

def handle_function_calling(question: str, session_id: str, scoped_doc_ids: List[str] = None) -> dict:
    """Handle function calling with the financial agent"""
    try:
        # Create agent
        agent = create_financial_agent()
        
        # Create memory with conversation context
        memory = Memory()
        
        # Add recent conversation history
        history = get_recent_context(session_id, limit=6)
        for msg in history:
            memory.add_memory({
                "type": msg["role"],
                "content": msg["content"]
            })
        
        # Add context about available documents if any
        if scoped_doc_ids:
            memory.add_memory({
                "type": "system",
                "content": f"Available documents in this session: {', '.join(scoped_doc_ids)}"
            })
        
        # Run the agent
        result_memory = agent.run(question, memory=memory, max_iterations=3)
        
        # Extract the final response
        assistant_messages = [m for m in result_memory.items if m["type"] == "assistant"]
        if assistant_messages:
            final_response = assistant_messages[-1]["content"]
        else:
            final_response = "I've analyzed your request using the available tools. Let me know if you need more specific information."
        
        # Extract tool execution results for sources
        tool_results = [m for m in result_memory.items if m["type"] == "environment"]
        sources = []
        for result in tool_results:
            try:
                result_data = json.loads(result["content"])
                if result_data.get("tool_executed") and "result" in result_data:
                    # Extract relevant source information
                    tool_result = result_data["result"]
                    if isinstance(tool_result, dict):
                        if "sql" in tool_result:
                            sources.append(f"Database query executed")
                        elif "rows" in tool_result:
                            sources.append(f"Retrieved {len(tool_result['rows'])} data points")
            except:
                pass
        
        return {
            "reply": final_response,
            "sources": sources,
            "tool_calls_made": len(tool_results)
        }
        
    except Exception as e:
        return {
            "reply": f"I encountered an error while processing your request: {str(e)}",
            "sources": [],
            "tool_calls_made": 0
        }

class AskRequest(BaseModel):
    question: str

@app.post("/hello")
async def hello():
    return {"reply": "hello"}

@app.post("/ask")
async def ask(
    question: str = Form(...),
    files: List[UploadFile] = File(default=[]),
    session_id: Optional[str] = Form(None),
    user_id: Optional[str] = Form(None),
    k: int = Form(8)
):
    if not question.strip() and not files:
        raise HTTPException(status_code=400, detail="Question or files are required")

    # 1) Session Management
    if not session_id:
        session_id = new_session(user_id=user_id)

    # 2) Process uploaded files and capture doc_ids
    doc_ids_added = []
    processed_files = []
    
    if files:
        with tempfile.TemporaryDirectory() as temp_dir:
            for file in files:
                try:
                    content = await file.read()
                    file_path = os.path.join(temp_dir, file.filename)
                    
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    
                    # Compute doc_id the same way as pipeline
                    try:
                        doc_id = compute_file_hash(file_path)
                        doc_ids_added.append(doc_id)
                    except Exception:
                        doc_id = None
                    
                    # Run through pipeline
                    try:
                        pipeline_cmd = [
                            sys.executable, 
                            "server/ingest/pipeline.py",
                            "--input", file_path,
                            "--symbol", "UPLOADED",
                            "--title", f"Uploaded: {file.filename}"
                        ]
                        
                        result = subprocess.run(
                            pipeline_cmd, 
                            capture_output=True, 
                            text=True, 
                            cwd=os.path.dirname(os.path.dirname(__file__))
                        )
                        
                        if result.returncode == 0:
                            processed_files.append(f"✅ {file.filename} - processed and stored in database")
                        else:
                            processed_files.append(f"❌ {file.filename} - processing failed: {result.stderr}")
                            
                    except Exception as e:
                        processed_files.append(f"❌ {file.filename} - pipeline error: {str(e)}")
                        
                except Exception as e:
                    processed_files.append(f"❌ {file.filename} - error: {str(e)}")

    # 3) Attach new docs to session
    attach_docs(session_id, doc_ids_added)

    # 4) Store user message
    add_message(session_id, "user", question)

    # 5) Get conversation history for context
    history = get_recent_context(session_id, limit=6)
    history_text = "\n".join([f"{m['role']}: {m['content']}" for m in history])

    # 6) Determine the best approach: Function Calling, RAG, or SQL
    scoped_doc_ids = get_scoped_doc_ids(session_id)
    
    # Check if this is a financial data query that would benefit from function calling
    financial_keywords = ["stock", "price", "dividend", "split", "performance", "analysis", "symbol", "company", "sector", "industry"]
    is_financial_query = any(keyword in question.lower() for keyword in financial_keywords)
    
    # Check if user has documents uploaded
    has_documents = bool(scoped_doc_ids) or bool(files) or ("files" in question.lower()) or ("doc:" in question.lower())
    
    # Priority: Function Calling > RAG (with docs) > SQL fallback
    if is_financial_query and openai_key and client:
        # Use function calling for financial queries
        try:
            result = handle_function_calling(question, session_id, scoped_doc_ids)
            add_message(session_id, "assistant", result["reply"])
            
            return {
                "session_id": session_id,
                "reply": result["reply"],
                "sources": result["sources"],
                "tool_calls_made": result["tool_calls_made"],
                "files_received": len(files),
                "processed_files": processed_files,
                "method": "function_calling"
            }
        except Exception as e:
            # Fall back to other methods if function calling fails
            pass
    
    if has_documents:
        # 7) Vector search scoped to session documents
        hits = []
        if scoped_doc_ids:
            # Search with conversation context
            search_query = f"{question}\n\n(Conversation context)\n{history_text}"
            
            # For now, search the most recent doc_id (can be enhanced to search all)
            latest_doc_id = scoped_doc_ids[-1] if scoped_doc_ids else None
            hits = search_docs_auto(question=search_query, k=k, doc_id=latest_doc_id).get("rows", [])
        
        context_blocks = [f"[{h.get('chunk_id','')}] {h.get('snippet','')}" for h in hits]
        context = "\n\n".join(context_blocks) if context_blocks else "(no relevant documents found)"

        if not openai_key or client is None:
            return {
                "session_id": session_id,
                "reply": f"(mock) Context from uploaded documents:\n{context[:1000]}", 
                "sources": [h.get("chunk_id") for h in hits],
                "files_received": len(files),
                "processed_files": processed_files
            }

        # 8) Generate RAG response
        rag_messages = [
            {"role": "system", "content": SYSTEM_RAG},
            {"role": "user", "content": f"Question: {question}\n\nContext from documents:\n{context}"}
        ]
        
        try:
            resp = client.chat.completions.create(model="gpt-4o-mini", messages=rag_messages, temperature=0.2)
            answer = resp.choices[0].message.content or ""
        except Exception as e:
            return {
                "session_id": session_id,
                "reply": f"(mock) Context from documents:\n{context[:1000]}", 
                "note": f"OpenAI error: {str(e)}",
                "sources": [h.get("chunk_id") for h in hits],
                "files_received": len(files),
                "processed_files": processed_files
            }
        
        # 9) Store assistant response
        add_message(session_id, "assistant", answer)
        
        return {
            "session_id": session_id,
            "reply": answer, 
            "sources": [h.get("chunk_id") for h in hits],
            "files_received": len(files),
            "processed_files": processed_files,
            "method": "rag"
        }

    # 10) Fallback to SQL generation for S&P 500 data queries
    if not openai_key:
        return {
            "session_id": session_id,
            "reply": f"Received: {question}",
            "note": "OPENAI_API_KEY not set; returning mock reply",
            "files_received": len(files),
            "processed_files": processed_files
        }

    # 11) Generate SQL query for S&P 500 data
    full_question = question
    if processed_files:
        full_question += f"\n\nFile processing status:\n" + "\n".join(processed_files)

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_SQL},
                {"role": "user", "content": f"Question: {full_question}"}
            ],
            temperature=0
        )
        content = response.choices[0].message.content or ""
        
        # Clean up SQL query
        sql_query = content.strip()
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()
        
    except Exception as e:
        return {
            "session_id": session_id,
            "reply": f"Received: {question}",
            "note": f"OpenAI error: {str(e)}",
            "files_received": len(files),
            "processed_files": processed_files
        }

    # 12) Execute SQL query
    if not all([
        getattr(config, "tidb_host", None),
        getattr(config, "tidb_port", None),
        getattr(config, "tidb_user", None),
        getattr(config, "tidb_password", None),
        getattr(config, "tidb_db_name", None),
    ]):
        return {
            "session_id": session_id,
            "sql": sql_query, 
            "results": [], 
            "note": "DB connection not configured",
            "files_received": len(files),
            "processed_files": processed_files
        }

    try:
        results = run_query(sql_query)
        # Store the SQL response
        sql_response = f"SQL Query: {sql_query}\nResults: {len(results)} rows returned"
        add_message(session_id, "assistant", sql_response)
        
    except Exception as e:
        return {
            "session_id": session_id,
            "sql": sql_query, 
            "results": [], 
            "note": f"DB error: {str(e)}",
            "files_received": len(files),
            "processed_files": processed_files
        }

    return {
        "session_id": session_id,
        "sql": sql_query, 
        "results": results,
        "files_received": len(files),
        "processed_files": processed_files,
        "method": "sql"
    }

# Run with: uvicorn main:app --reload --port 5000
