import os
from typing import List, Optional, Dict, Any
from agent_core import register_tool
from db import run_query

def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")

@register_tool(tags=["file_operations", "read"])
def read_project_file(name: str) -> str:
    with open(f"project_files/{name}", "r") as f:
        return f.read()

@register_tool(tags=["file_operations", "list"])
def list_project_files() -> List[str]:
    return sorted([f for f in os.listdir("project_files") if f.endswith(".py")])

@register_tool(tags=["system"], terminal=True)
def terminate(message: str) -> str:
    return f"{message}\nTerminating..."

@register_tool(tags=["vector", "docs", "search"])
def search_docs_auto(question: str,
                     k: int = 8,
                     symbol: Optional[str] = None,
                     doc_id: Optional[str] = None,
                     expand_docs_top_n: int = 0,           # NEW: fetch full docs for top N doc_ids
                     max_chunks_per_doc: int = 50) -> Dict[str, Any]:
    """
    KNN over docs_auto (auto-embedded). Returns top-k chunks.
    If expand_docs_top_n > 0, also returns full context for the top-N doc_ids.
    """
    q = _esc(question)
    inner_limit = max(k * 6, 50)  # fetch more, filter later

    # include distance so we can sort/group
    inner = f"""
      SELECT
        chunk_id, doc_id, page_no, symbol, title, url, text,
        VEC_EMBED_COSINE_DISTANCE(vec, '{q}') AS d
      FROM user_chat_docs
      ORDER BY d
      LIMIT {inner_limit}
    """

    outer = f"SELECT chunk_id, doc_id, page_no, symbol, title, url, LEFT(text, 600) AS snippet, d FROM ({inner}) AS t"
    conds = []
    if symbol:
        conds.append(f"t.symbol = '{_esc(symbol)}'")
    if doc_id:
        conds.append(f"t.doc_id = '{_esc(doc_id)}'")
    if conds:
        outer += " WHERE " + " AND ".join(conds)
    outer += f" ORDER BY d LIMIT {k};"

    hits = run_query(outer)  # list[dict]

    # Optionally expand: fetch full chunks for the top-N doc_ids
    expanded: Dict[str, List[Dict[str, Any]]] = {}
    expand_sqls: List[str] = []
    if expand_docs_top_n > 0 and hits:
        seen = set()
        top_doc_ids: List[str] = []
        for h in hits:
            did = h["doc_id"]
            if did not in seen:
                seen.add(did)
                top_doc_ids.append(did)
            if len(top_doc_ids) >= expand_docs_top_n:
                break

        for did in top_doc_ids:
            exp_sql = f"""
              SELECT chunk_id, doc_id, page_no, symbol, title, url, text
              FROM user_chat_docs
              WHERE doc_id = '{_esc(did)}'
              ORDER BY chunk_no
              LIMIT {max_chunks_per_doc}
            """
            expanded[did] = run_query(exp_sql)
            expand_sqls.append(exp_sql)

    return {
        "sql": {"search": outer, "expand": expand_sqls},
        "rows": hits,
        "expanded_docs": expanded
    }

@register_tool(tags=["vector", "docs"])
def get_doc_context(doc_id: str, max_chunks: int = 50):
    sql = f"""
      SELECT chunk_id, doc_id, page_no, symbol, title, url, text
      FROM user_chat_docs
      WHERE doc_id = '{_esc(doc_id)}'
      ORDER BY chunk_no
      LIMIT {max_chunks}
    """
    return {"sql": sql, "rows": run_query(sql)}

# --------------------- Financial Analysis Tools ---------------------

@register_tool(tags=["financial", "sp500", "data"])
def get_stock_price_data(symbol: str, days: int = 30) -> Dict[str, Any]:
    """
    Get recent stock price data (OHLC) for a given S&P 500 symbol.
    Returns price data from the last N days.
    """
    sql = f"""
      SELECT date, open, high, low, close, volume
      FROM sp500_ohlc 
      WHERE symbol = '{_esc(symbol.upper())}'
      ORDER BY date DESC
      LIMIT {days}
    """
    try:
        results = run_query(sql)
        return {
            "symbol": symbol.upper(),
            "data_points": len(results),
            "price_data": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to fetch price data: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "data"])
def get_dividend_history(symbol: str, years: int = 5) -> Dict[str, Any]:
    """
    Get dividend payment history for a given S&P 500 symbol.
    Returns dividend data from the last N years.
    """
    sql = f"""
      SELECT date, dividend_amount
      FROM dividends 
      WHERE symbol = '{_esc(symbol.upper())}'
      AND date >= DATE_SUB(CURDATE(), INTERVAL {years} YEAR)
      ORDER BY date DESC
    """
    try:
        results = run_query(sql)
        return {
            "symbol": symbol.upper(),
            "dividend_payments": len(results),
            "dividend_data": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to fetch dividend data: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "data"])
def get_stock_splits(symbol: str) -> Dict[str, Any]:
    """
    Get stock split history for a given S&P 500 symbol.
    """
    sql = f"""
      SELECT date, split_ratio
      FROM stock_splits 
      WHERE symbol = '{_esc(symbol.upper())}'
      ORDER BY date DESC
    """
    try:
        results = run_query(sql)
        return {
            "symbol": symbol.upper(),
            "splits": len(results),
            "split_data": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to fetch split data: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "analysis"])
def analyze_stock_performance(symbol: str, days: int = 90) -> Dict[str, Any]:
    """
    Analyze stock performance metrics including price change, volatility, and trends.
    """
    sql = f"""
      SELECT 
        symbol,
        MIN(close) as min_price,
        MAX(close) as max_price,
        AVG(close) as avg_price,
        STDDEV(close) as volatility,
        (SELECT close FROM sp500_ohlc WHERE symbol = '{_esc(symbol.upper())}' ORDER BY date DESC LIMIT 1) as current_price,
        (SELECT close FROM sp500_ohlc WHERE symbol = '{_esc(symbol.upper())}' ORDER BY date DESC LIMIT 1 OFFSET {days-1}) as price_{days}_days_ago
      FROM sp500_ohlc 
      WHERE symbol = '{_esc(symbol.upper())}'
      AND date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
    """
    try:
        results = run_query(sql)
        if results:
            data = results[0]
            current = data.get('current_price', 0)
            old = data.get(f'price_{days}_days_ago', 0)
            change_pct = ((current - old) / old * 100) if old > 0 else 0
            
            return {
                "symbol": symbol.upper(),
                "analysis_period_days": days,
                "current_price": current,
                "price_change_percent": round(change_pct, 2),
                "min_price": data.get('min_price'),
                "max_price": data.get('max_price'),
                "avg_price": round(data.get('avg_price', 0), 2),
                "volatility": round(data.get('volatility', 0), 4),
                "sql": sql
            }
        return {"error": "No data found", "sql": sql}
    except Exception as e:
        return {"error": f"Failed to analyze performance: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "search"])
def search_sp500_companies(query: str, limit: int = 10) -> Dict[str, Any]:
    """
    Search for S&P 500 companies by name or symbol.
    """
    sql = f"""
      SELECT symbol, company_name, sector, industry
      FROM sp500_companies 
      WHERE symbol LIKE '%{_esc(query.upper())}%' 
      OR company_name LIKE '%{_esc(query)}%'
      ORDER BY company_name
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "query": query,
            "results_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to search companies: {str(e)}", "sql": sql}