import os, uuid
import pymysql
from typing import List, Optional

def db():
    return pymysql.connect(
        host=os.getenv("TIDB_HOST"),
        port=int(os.getenv("TIDB_PORT","4000")),
        user=os.getenv("TIDB_USER"),
        password=os.getenv("TIDB_PASS"),
        database=os.getenv("TIDB_DB"),
        autocommit=True,
        ssl={"ssl":{}} if os.getenv("TIDB_SSL","1")=="1" else None
    )

def new_session(user_id: Optional[str]=None) -> str:
    sid = str(uuid.uuid4())
    with db().cursor() as cur:
        cur.execute("INSERT INTO conversations (session_id, user_id) VALUES (%s,%s)", (sid, user_id))
    return sid

def add_message(session_id: str, role: str, content: str, tool_name: Optional[str]=None) -> int:
    with db().cursor() as cur:
        cur.execute("""INSERT INTO chat_messages (session_id, role, content, tool_name)
                       VALUES (%s,%s,%s,%s)""", (session_id, role, content, tool_name))
        return cur.lastrowid

def attach_docs(session_id: str, doc_ids: List[str]):
    if not doc_ids: return
    vals = [(session_id, d) for d in doc_ids]
    with db().cursor() as cur:
        cur.executemany("""INSERT IGNORE INTO session_docs (session_id, doc_id) VALUES (%s,%s)""", vals)

def get_recent_context(session_id: str, limit:int=8) -> List[dict]:
    with db().cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("""SELECT role, content FROM chat_messages
                       WHERE session_id=%s ORDER BY created_at DESC LIMIT %s""",
                    (session_id, limit))
        rows = cur.fetchall()
    return list(reversed(rows))

def get_scoped_doc_ids(session_id: str) -> List[str]:
    with db().cursor() as cur:
        cur.execute("SELECT doc_id FROM session_docs WHERE session_id=%s", (session_id,))
        return [r[0] for r in cur.fetchall()]

def save_summary(session_id: str, summary: str):
    with db().cursor() as cur:
        cur.execute("UPDATE conversations SET summary=%s WHERE session_id=%s", (summary, session_id))
