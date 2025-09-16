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

def set_active_doc(session_id: str, doc_id: str) -> None:
    """Persist the current active doc_id for a session using chat_messages as a lightweight store."""
    if not doc_id:
        return
    with db().cursor() as cur:
        cur.execute("""INSERT INTO chat_messages (session_id, role, content, tool_name)
                       VALUES (%s,%s,%s,%s)""",
                    (session_id, "environment", doc_id, "active_doc"))

def get_active_doc(session_id: str) -> Optional[str]:
    """Fetch most recent active doc_id set for this session, if any."""
    with db().cursor() as cur:
        try:
            cur.execute(
                """
                SELECT content
                FROM chat_messages
                WHERE session_id=%s AND tool_name='active_doc'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (session_id,)
            )
            row = cur.fetchone()
            return row[0] if row else None
        except Exception:
            return None

def attach_docs(session_id: str, doc_ids: List[str]):
    if not doc_ids: return
    # Filter out any None/empty doc_ids to avoid bad rows
    filtered = [d for d in doc_ids if d]
    if not filtered: return
    vals = [(session_id, d) for d in filtered]
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
    """Return doc_ids attached to this session, ordered by document created time ascending.

    Falls back gracefully if the documents table or created_at columns are unavailable.
    """
    try:
        with db().cursor() as cur:
            # Prefer ordering by actual document created time when available
            cur.execute(
                """
                SELECT sd.doc_id
                FROM session_docs sd
                JOIN documents d ON sd.doc_id = d.doc_id
                WHERE sd.session_id=%s
                ORDER BY d.created_at ASC
                """,
                (session_id,)
            )
            rows = cur.fetchall()
            if rows:
                return [r[0] for r in rows]
    except Exception:
        pass

    try:
        with db().cursor() as cur:
            # Fallback: order by session_docs created_at if exists
            cur.execute(
                """
                SELECT doc_id
                FROM session_docs
                WHERE session_id=%s
                ORDER BY created_at ASC
                """,
                (session_id,)
            )
            rows = cur.fetchall()
            if rows:
                return [r[0] for r in rows]
    except Exception:
        pass

    # Last resort: unordered
    with db().cursor() as cur:
        cur.execute("SELECT doc_id FROM session_docs WHERE session_id=%s", (session_id,))
        return [r[0] for r in cur.fetchall()]

def save_summary(session_id: str, summary: str):
    with db().cursor() as cur:
        cur.execute("UPDATE conversations SET summary=%s WHERE session_id=%s", (summary, session_id))
