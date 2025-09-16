from fastapi import FastAPI, HTTPException, Form, UploadFile, File
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import google.generativeai as genai
from db import run_query
from tools import search_docs_auto
from ingest.extract_text import compute_file_hash
from config import Config
from memory import new_session, add_message, get_recent_context, attach_docs, get_scoped_doc_ids, db
import pymysql
from function_calling_agent import agent as function_calling_agent
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

# Function to get fresh API key and configure Gemini
def get_gemini_model():
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        genai.configure(api_key=gemini_key)
        return genai.GenerativeModel('gemini-2.5-flash')
    return None

# Initialize with current API key
gemini_model = get_gemini_model()

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


# --------------------- Function Calling Setup ---------------------

def format_tool_results_for_user(question: str, tool_results: List[dict]) -> str:
    """Format tool execution results into user-friendly text responses"""
    try:
        response_parts = []
        
        for result in tool_results:
            try:
                result_data = json.loads(result["content"])
                if result_data.get("tool_executed") and "result" in result_data:
                    tool_result = result_data["result"]
                    
                    # Handle different tool result types
                    if isinstance(tool_result, dict):
                        # Company search results
                        if "companies" in tool_result:
                            companies = tool_result["companies"]
                            if companies:
                                response_parts.append(f"Here are the S&P 500 companies I found:")
                                for i, company in enumerate(companies[:10], 1):  # Limit to 10
                                    symbol = company.get('symbol', 'N/A')
                                    name = company.get('security', 'N/A')
                                    sector = company.get('gics_sector', 'N/A')
                                    response_parts.append(f"{i}. {symbol} - {name} ({sector})")
                            else:
                                response_parts.append("No companies found matching your criteria.")
                        
                        # Sector breakdown results
                        elif "sector_breakdown" in tool_result:
                            breakdown = tool_result["sector_breakdown"]
                            total = tool_result.get("total_companies", 0)
                            response_parts.append(f"S&P 500 Sector Breakdown (Total: {total} companies):")
                            for sector in breakdown[:10]:  # Limit to top 10
                                name = sector.get('gics_sector', 'N/A')
                                count = sector.get('company_count', 0)
                                pct = sector.get('percentage', 0)
                                response_parts.append(f"• {name}: {count} companies ({pct}%)")
                        
                        # Company details
                        elif "company_info" in tool_result:
                            info = tool_result["company_info"]
                            symbol = info.get('symbol', 'N/A')
                            name = info.get('security', 'N/A')
                            sector = info.get('gics_sector', 'N/A')
                            industry = info.get('gics_sub_ind', 'N/A')
                            location = info.get('headquarters_loc', 'N/A')
                            cik = info.get('cik', 'N/A')
                            response_parts.append(f"Company Details for {symbol}:")
                            response_parts.append(f"• Name: {name}")
                            response_parts.append(f"• Sector: {sector}")
                            response_parts.append(f"• Industry: {industry}")
                            response_parts.append(f"• Headquarters: {location}")
                            response_parts.append(f"• CIK: {cik}")
                        
                        # Stock price data
                        elif "price_data" in tool_result:
                            symbol = tool_result.get("symbol", "N/A")
                            data_points = tool_result.get("data_points", 0)
                            response_parts.append(f"Retrieved {data_points} price data points for {symbol}.")
                        
                        # Error handling
                        elif "error" in tool_result:
                            response_parts.append(f"Error: {tool_result['error']}")
                        
                        # Company comparison results
                        elif "comparison_data" in tool_result:
                            companies = tool_result["comparison_data"]
                            symbols = tool_result.get("symbols_requested", [])
                            response_parts.append(f"Company Comparison for {', '.join(symbols)}:")
                            for company in companies:
                                symbol = company.get('symbol', 'N/A')
                                name = company.get('security', 'N/A')
                                sector = company.get('gics_sector', 'N/A')
                                response_parts.append(f"• {symbol} - {name} ({sector})")
                        
                        # Filtered companies results
                        elif "filters_applied" in tool_result:
                            companies = tool_result["companies"]
                            filters = tool_result["filters_applied"]
                            filter_desc = []
                            for key, value in filters.items():
                                if value:
                                    filter_desc.append(f"{key}: {value}")
                            
                            response_parts.append(f"Companies matching criteria ({', '.join(filter_desc)}):")
                            for i, company in enumerate(companies[:10], 1):
                                symbol = company.get('symbol', 'N/A')
                                name = company.get('security', 'N/A')
                                sector = company.get('gics_sector', 'N/A')
                                response_parts.append(f"{i}. {symbol} - {name} ({sector})")
                        
                        # Statistics results
                        elif "total_companies" in tool_result:
                            stats = tool_result
                            response_parts.append("S&P 500 Statistics:")
                            response_parts.append(f"• Total Companies: {stats.get('total_companies', 0)}")
                            response_parts.append(f"• Total Sectors: {stats.get('total_sectors', 0)}")
                            response_parts.append(f"• Total Sub-Industries: {stats.get('total_sub_industries', 0)}")
                            response_parts.append(f"• Total Locations: {stats.get('total_locations', 0)}")
                            if stats.get('oldest_company_founded'):
                                response_parts.append(f"• Oldest Company Founded: {stats['oldest_company_founded']}")
                            if stats.get('newest_company_founded'):
                                response_parts.append(f"• Newest Company Founded: {stats['newest_company_founded']}")
                        
                        # Geographic distribution results
                        elif "location_distribution" in tool_result:
                            locations = tool_result["location_distribution"]
                            response_parts.append("S&P 500 Geographic Distribution (Top Locations):")
                            for location in locations[:10]:
                                loc = location.get('headquarters_loc', 'N/A')
                                count = location.get('company_count', 0)
                                pct = location.get('percentage', 0)
                                response_parts.append(f"• {loc}: {count} companies ({pct}%)")
                        
                        # Advanced search results
                        elif "search_fields" in tool_result:
                            companies = tool_result["companies"]
                            query = tool_result.get("query", "")
                            response_parts.append(f"Search Results for '{query}':")
                            for i, company in enumerate(companies[:10], 1):
                                symbol = company.get('symbol', 'N/A')
                                name = company.get('security', 'N/A')
                                sector = company.get('gics_sector', 'N/A')
                                response_parts.append(f"{i}. {symbol} - {name} ({sector})")
                        
                        # Sector performance summary
                        elif "sector_summary" in tool_result:
                            sectors = tool_result["sector_summary"]
                            response_parts.append("S&P 500 Sector Performance Summary:")
                            for sector in sectors:
                                name = sector.get('gics_sector', 'N/A')
                                count = sector.get('company_count', 0)
                                pct = sector.get('percentage', 0)
                                sub_industries = sector.get('sub_industries_count', 0)
                                response_parts.append(f"• {name}: {count} companies ({pct}%) - {sub_industries} sub-industries")
                        
                        # Index changes results
                        elif "changes_found" in tool_result:
                            companies = tool_result["companies"]
                            year = tool_result.get("year_filter")
                            year_text = f" in {year}" if year else " (Recent)"
                            response_parts.append(f"S&P 500 Index Changes{year_text}:")
                            for i, company in enumerate(companies[:10], 1):
                                symbol = company.get('symbol', 'N/A')
                                name = company.get('security', 'N/A')
                                date_added = company.get('date_added', 'N/A')
                                response_parts.append(f"{i}. {symbol} - {name} (Added: {date_added})")
                        
                        # Company relationships results
                        elif "related_companies" in tool_result:
                            target = tool_result.get("target_company", "")
                            relationship = tool_result.get("relationship_type", "")
                            companies = tool_result["related_companies"]
                            response_parts.append(f"Companies related to {target} by {relationship}:")
                            for i, company in enumerate(companies[:10], 1):
                                symbol = company.get('symbol', 'N/A')
                                name = company.get('security', 'N/A')
                                sector = company.get('gics_sector', 'N/A')
                                response_parts.append(f"{i}. {symbol} - {name} ({sector})")
                        
                        # Generic data results
                        elif "rows" in tool_result:
                            rows = tool_result["rows"]
                            response_parts.append(f"Retrieved {len(rows)} data records.")
                    
                    # Handle string results
                    elif isinstance(tool_result, str):
                        response_parts.append(tool_result)
                        
            except json.JSONDecodeError:
                continue
        
        if response_parts:
            return "\n\n".join(response_parts)
        else:
            return "I've processed your request using the available tools. Let me know if you need more specific information."
            
    except Exception as e:
        return f"I've analyzed your request, but encountered an issue formatting the response: {str(e)}"


class AskRequest(BaseModel):
    question: str

@app.post("/hello")
async def hello():
    return {"reply": "hello"}

@app.get("/history")
async def get_history(session_id: Optional[str] = None):
    """
    Get chat history and session documents.
    If session_id is provided, returns history for that specific session.
    If no session_id, returns recent sessions overview.
    """
    try:
        if session_id:
            # Get specific session history
            with db().cursor(pymysql.cursors.DictCursor) as cur:
                # Get chat messages for the session
                cur.execute("""
                    SELECT role, content, created_at, tool_name 
                    FROM chat_messages 
                    WHERE session_id=%s 
                    ORDER BY created_at ASC
                """, (session_id,))
                messages = cur.fetchall()
                
                # Get documents attached to this session (if documents table exists)
                try:
                    cur.execute("""
                        SELECT sd.doc_id, d.filename, d.file_hash, d.created_at
                        FROM session_docs sd
                        JOIN documents d ON sd.doc_id = d.doc_id
                        WHERE sd.session_id=%s
                        ORDER BY d.created_at DESC
                    """, (session_id,))
                    documents = cur.fetchall()
                except Exception:
                    # If documents table doesn't exist, just get doc_ids from session_docs
                    cur.execute("""
                        SELECT doc_id as doc_id, doc_id as filename, doc_id as file_hash, NOW() as created_at
                        FROM session_docs
                        WHERE session_id=%s
                    """, (session_id,))
                    documents = cur.fetchall()
                
                # Get session info
                cur.execute("""
                    SELECT session_id, user_id, summary, created_at
                    FROM conversations
                    WHERE session_id=%s
                """, (session_id,))
                session_info = cur.fetchone()
                
                return {
                    "session_id": session_id,
                    "session_info": session_info,
                    "messages": messages,
                    "documents": documents,
                    "total_messages": len(messages),
                    "total_documents": len(documents)
                }
        else:
            # Get recent sessions overview
            with db().cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("""
                    SELECT c.session_id, c.user_id, c.summary, c.created_at,
                           COUNT(cm.session_id) as message_count,
                           COUNT(sd.doc_id) as document_count
                    FROM conversations c
                    LEFT JOIN chat_messages cm ON c.session_id = cm.session_id
                    LEFT JOIN session_docs sd ON c.session_id = sd.session_id
                    GROUP BY c.session_id
                    ORDER BY c.created_at DESC
                    LIMIT 20
                """)
                sessions = cur.fetchall()
                
                return {
                    "sessions": sessions,
                    "total_sessions": len(sessions)
                }
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving history: {str(e)}")


@app.get("/session-docs/{session_id}")
async def get_session_documents(session_id: str):
    """
    Get all documents attached to a specific session from session_docs table.
    """
    try:
        with db().cursor(pymysql.cursors.DictCursor) as cur:
            try:
                cur.execute("""
                    SELECT sd.doc_id, d.filename, d.file_hash, d.file_size, d.content_type, 
                           d.created_at, d.uploaded_by
                    FROM session_docs sd
                    JOIN documents d ON sd.doc_id = d.doc_id
                    WHERE sd.session_id=%s
                    ORDER BY d.created_at DESC
                """, (session_id,))
                documents = cur.fetchall()
            except Exception:
                # If documents table doesn't exist, just get doc_ids from session_docs
                cur.execute("""
                    SELECT doc_id as doc_id, doc_id as filename, doc_id as file_hash, 
                           NULL as file_size, NULL as content_type, NOW() as created_at, NULL as uploaded_by
                    FROM session_docs
                    WHERE session_id=%s
                """, (session_id,))
                documents = cur.fetchall()
            
            return {
                "session_id": session_id,
                "documents": documents,
                "total_documents": len(documents)
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving session documents: {str(e)}")


@app.delete("/delete-session/{session_id}")
async def delete_session(session_id: str):
    """
    Delete a session and all its associated data (messages, documents, etc.).
    """
    try:
        with db().cursor() as cur:
            # Delete session documents first (foreign key constraint)
            cur.execute("DELETE FROM session_docs WHERE session_id=%s", (session_id,))
            
            # Delete chat messages
            cur.execute("DELETE FROM chat_messages WHERE session_id=%s", (session_id,))
            
            # Delete the session itself
            cur.execute("DELETE FROM conversations WHERE session_id=%s", (session_id,))
            
            # Check if any rows were affected
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Session not found")
            
            return {
                "message": "Session deleted successfully",
                "session_id": session_id,
                "deleted_rows": cur.rowcount
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting session: {str(e)}")


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

    # 6) Determine the best approach: Simple Greeting > Function Calling > RAG > SQL
    scoped_doc_ids = get_scoped_doc_ids(session_id)
    
    # Check for simple greetings first (exact word matches only)
    greeting_words = ["hello", "hi", "hey", "good morning", "good afternoon", "good evening", "greetings"]
    question_lower = question.lower()
    # Use word boundaries to avoid false matches like "history" matching "hi"
    is_greeting = any(f" {greeting} " in f" {question_lower} " or 
                     question_lower.startswith(f"{greeting} ") or 
                     question_lower.endswith(f" {greeting}") or
                     question_lower == greeting for greeting in greeting_words)
    
    if is_greeting:
        greeting_response = "Hello! I'm your S&P 500 financial advisor. I can help you with:\n\n" \
                          "• Company research and analysis\n" \
                          "• Stock price and performance data\n" \
                          "• Sector and industry breakdowns\n" \
                          "• Document analysis from uploaded files\n" \
                          "• SEC filings and regulatory data\n\n" \
                          "What would you like to know about S&P 500 companies?"
        
        add_message(session_id, "assistant", greeting_response)
        return {
            "session_id": session_id,
            "reply": greeting_response,
            "files_received": len(files),
            "processed_files": processed_files,
            "method": "greeting"
        }
    
    # Check if user has documents uploaded
    has_documents = bool(scoped_doc_ids) or bool(files) or ("files" in question.lower()) or ("doc:" in question.lower())
    
    # Priority: Function Calling > RAG (with docs) > Simple response
    current_gemini_model = get_gemini_model()
    if current_gemini_model:
        # Use the new function calling agent for all queries
        try:
            # Get document context if documents are available
            doc_context = None
            if scoped_doc_ids:
                # Search for relevant document content
                latest_doc_id = scoped_doc_ids[-1] if scoped_doc_ids else None
                doc_hits = search_docs_auto(question=question, k=k, doc_id=latest_doc_id).get("rows", [])
                if doc_hits:
                    doc_context = "\n\n".join([f"[{h.get('chunk_id','')}] {h.get('snippet','')}" for h in doc_hits])
            
            result = function_calling_agent.run(question, doc_context=doc_context, scoped_doc_ids=scoped_doc_ids)
            add_message(session_id, "assistant", result)
            
            return {
                "session_id": session_id,
                "reply": result,
                "sources": ["S&P 500 Database"],
                "tool_calls_made": 1,  # Simplified for now
                "files_received": len(files),
                "processed_files": processed_files,
                "method": "function_calling"
            }
        except Exception as e:
            # Fall back to other methods if function calling fails
            print(f"Function calling error: {e}")  # Debug output
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

        if not gemini_key or gemini_model is None:
            return {
                "session_id": session_id,
                "reply": f"(mock) Context from uploaded documents:\n{context[:1000]}", 
                "sources": [h.get("chunk_id") for h in hits],
                "files_received": len(files),
                "processed_files": processed_files
            }

        # 8) Generate RAG response using Gemini
        try:
            prompt = f"{SYSTEM_RAG}\n\nQuestion: {question}\n\nContext from documents:\n{context}"
            response = gemini_model.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.2))
            answer = response.text or ""
        except Exception as e:
            return {
                "session_id": session_id,
                "reply": f"(mock) Context from documents:\n{context[:1000]}", 
                "note": f"Gemini error: {str(e)}",
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

    # 10) Fallback to simple response for non-financial queries
    current_gemini_key = os.getenv("GEMINI_API_KEY")
    if not current_gemini_key:
        simple_response = f"I received your message: '{question}'. I'm a financial advisor specializing in S&P 500 companies. " \
                         f"Please ask me about stocks, companies, sectors, or upload documents for analysis."
        add_message(session_id, "assistant", simple_response)
        return {
            "session_id": session_id,
            "reply": simple_response,
            "note": "GEMINI_API_KEY not set; returning simple reply",
            "files_received": len(files),
            "processed_files": processed_files,
            "method": "simple_fallback"
        }

    # 11) For non-financial queries, provide a helpful response
    simple_response = f"I understand you're asking: '{question}'. As your S&P 500 financial advisor, " \
                     f"I can help you with company research, stock analysis, sector breakdowns, and document analysis. " \
                     f"Could you please ask a more specific question about S&P 500 companies, stocks, or financial data?"
    
    add_message(session_id, "assistant", simple_response)
    return {
        "session_id": session_id,
        "reply": simple_response,
        "files_received": len(files),
        "processed_files": processed_files,
        "method": "simple_response"
    }

# Run with: uvicorn main:app --reload --port 5000
