import os
from typing import List, Optional, Dict, Any, Tuple
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
    Search for S&P 500 companies by name or symbol using the sp500_wik_list table.
    Returns symbol, security name, sector, sub-industry, headquarters, CIK, and founding date.
    """
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE symbol LIKE '%{_esc(query.upper())}%' 
      OR security LIKE '%{_esc(query)}%'
      ORDER BY security
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

@register_tool(tags=["financial", "sp500", "company_info"])
def get_company_details(symbol: str) -> Dict[str, Any]:
    """
    Get comprehensive details for a specific S&P 500 company by symbol.
    Returns all available information including CIK for SEC filings lookup.
    """
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE symbol = '{_esc(symbol.upper())}'
    """
    try:
        results = run_query(sql)
        if results:
            return {
                "symbol": symbol.upper(),
                "company_info": results[0],
                "sql": sql
            }
        else:
            return {"error": f"Company {symbol.upper()} not found in S&P 500", "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get company details: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "sector_analysis"])
def get_companies_by_sector(sector: str, limit: int = 20) -> Dict[str, Any]:
    """
    Get all S&P 500 companies in a specific GICS sector.
    Useful for sector analysis and comparison.
    """
    sql = f"""
      SELECT symbol, security, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE gics_sector LIKE '%{_esc(sector)}%'
      ORDER BY security
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "sector": sector,
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies by sector: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "sector_analysis"])
def get_companies_by_sub_industry(sub_industry: str, limit: int = 15) -> Dict[str, Any]:
    """
    Get all S&P 500 companies in a specific GICS sub-industry.
    More granular than sector analysis.
    """
    sql = f"""
      SELECT symbol, security, gics_sector, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE gics_sub_ind LIKE '%{_esc(sub_industry)}%'
      ORDER BY security
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "sub_industry": sub_industry,
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies by sub-industry: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "geographic"])
def get_companies_by_location(location: str, limit: int = 15) -> Dict[str, Any]:
    """
    Get S&P 500 companies by headquarters location (state, city, or country).
    """
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE headquarters_loc LIKE '%{_esc(location)}%'
      ORDER BY security
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "location": location,
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies by location: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "historical"])
def get_recent_additions(days: int = 365, limit: int = 10) -> Dict[str, Any]:
    """
    Get companies recently added to the S&P 500 index.
    """
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE date_added >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
      ORDER BY date_added DESC
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "period_days": days,
            "recent_additions": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get recent additions: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "analysis"])
def get_sector_breakdown() -> Dict[str, Any]:
    """
    Get a breakdown of S&P 500 companies by GICS sector.
    Shows count and percentage distribution.
    """
    sql = """
      SELECT 
        gics_sector,
        COUNT(*) as company_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sp500_wik_list), 2) as percentage
      FROM sp500_wik_list 
      GROUP BY gics_sector
      ORDER BY company_count DESC
    """
    try:
        results = run_query(sql)
        total_companies = sum(row['company_count'] for row in results)
        return {
            "total_companies": total_companies,
            "sector_breakdown": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get sector breakdown: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "analysis"])
def get_sub_industry_breakdown(sector: str = None, limit: int = 20) -> Dict[str, Any]:
    """
    Get a breakdown of S&P 500 companies by GICS sub-industry.
    Optionally filter by sector.
    """
    where_clause = f"WHERE gics_sector = '{_esc(sector)}'" if sector else ""
    sql = f"""
      SELECT 
        gics_sub_ind,
        gics_sector,
        COUNT(*) as company_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sp500_wik_list {where_clause}), 2) as percentage
      FROM sp500_wik_list 
      {where_clause}
      GROUP BY gics_sub_ind, gics_sector
      ORDER BY company_count DESC
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "sector_filter": sector,
            "sub_industry_breakdown": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get sub-industry breakdown: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "cik_lookup"])
def get_cik_by_symbol(symbol: str) -> Dict[str, Any]:
    """
    Get the CIK (Central Index Key) for a specific S&P 500 company.
    CIK is needed for SEC filings and other regulatory data.
    """
    sql = f"""
      SELECT symbol, security, cik
      FROM sp500_wik_list 
      WHERE symbol = '{_esc(symbol.upper())}'
    """
    try:
        results = run_query(sql)
        if results:
            return {
                "symbol": symbol.upper(),
                "company_name": results[0]['security'],
                "cik": results[0]['cik'],
                "sql": sql
            }
        else:
            return {"error": f"CIK not found for {symbol.upper()}", "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get CIK: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "cik_lookup"])
def get_symbol_by_cik(cik: str) -> Dict[str, Any]:
    """
    Get the S&P 500 company symbol and details by CIK number.
    Useful when you have a CIK from SEC filings.
    """
    # Clean CIK format (remove leading zeros, handle CIK prefix)
    clean_cik = cik.strip()
    if clean_cik.upper().startswith('CIK'):
        clean_cik = clean_cik[3:]
    clean_cik = str(int(clean_cik))  # Remove leading zeros
    
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE cik = '{_esc(clean_cik)}'
    """
    try:
        results = run_query(sql)
        if results:
            return {
                "cik": clean_cik,
                "company_info": results[0],
                "sql": sql
            }
        else:
            return {"error": f"Company not found for CIK {clean_cik}", "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get company by CIK: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "analysis"])
def get_top_sp500_companies(limit: int = 10, sort_by: str = "symbol") -> Dict[str, Any]:
    """
    Get top S&P 500 companies. Can sort by symbol, security name, or sector.
    Default returns first 10 companies alphabetically by symbol.
    """
    valid_sort_options = ["symbol", "security", "gics_sector", "date_added"]
    if sort_by not in valid_sort_options:
        sort_by = "symbol"
    
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      ORDER BY {sort_by}
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "limit": limit,
            "sort_by": sort_by,
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get top companies: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "analysis"])
def get_largest_companies_by_sector(sector: str = None, limit: int = 10) -> Dict[str, Any]:
    """
    Get the largest/most prominent companies in a specific sector, or across all sectors.
    """
    where_clause = f"WHERE gics_sector = '{_esc(sector)}'" if sector else ""
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      {where_clause}
      ORDER BY security
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "sector_filter": sector,
            "limit": limit,
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get largest companies: {str(e)}", "sql": sql}

# --------------------- Comprehensive S&P 500 Analysis Tools ---------------------

@register_tool(tags=["financial", "sp500", "comparison"])
def compare_companies(symbols: List[str], limit: int = 5) -> Dict[str, Any]:
    """
    Compare multiple S&P 500 companies side by side.
    Provide up to 5 company symbols to compare.
    """
    if len(symbols) > limit:
        symbols = symbols[:limit]
    
    symbols_str = "', '".join([_esc(s.upper()) for s in symbols])
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE symbol IN ('{symbols_str}')
      ORDER BY symbol
    """
    try:
        results = run_query(sql)
        return {
            "symbols_requested": symbols,
            "companies_found": len(results),
            "comparison_data": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to compare companies: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "filtering"])
def filter_companies_by_criteria(
    sector: str = None, 
    sub_industry: str = None, 
    location: str = None, 
    founded_after: int = None,
    founded_before: int = None,
    limit: int = 20
) -> Dict[str, Any]:
    """
    Filter S&P 500 companies by multiple criteria: sector, sub-industry, location, founding year.
    """
    conditions = []
    
    if sector:
        conditions.append(f"gics_sector LIKE '%{_esc(sector)}%'")
    if sub_industry:
        conditions.append(f"gics_sub_ind LIKE '%{_esc(sub_industry)}%'")
    if location:
        conditions.append(f"headquarters_loc LIKE '%{_esc(location)}%'")
    if founded_after:
        conditions.append(f"founded >= '{founded_after}'")
    if founded_before:
        conditions.append(f"founded <= '{founded_before}'")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      {where_clause}
      ORDER BY security
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "filters_applied": {
                "sector": sector,
                "sub_industry": sub_industry,
                "location": location,
                "founded_after": founded_after,
                "founded_before": founded_before
            },
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to filter companies: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "statistics"])
def get_sp500_statistics() -> Dict[str, Any]:
    """
    Get comprehensive statistics about the S&P 500 index.
    """
    sql = """
      SELECT 
        COUNT(*) as total_companies,
        COUNT(DISTINCT gics_sector) as total_sectors,
        COUNT(DISTINCT gics_sub_ind) as total_sub_industries,
        COUNT(DISTINCT headquarters_loc) as total_locations,
        MIN(founded) as oldest_company_founded,
        MAX(founded) as newest_company_founded,
        MIN(date_added) as earliest_addition,
        MAX(date_added) as latest_addition
      FROM sp500_wik_list
    """
    try:
        results = run_query(sql)
        if results:
            stats = results[0]
            return {
                "total_companies": stats.get('total_companies', 0),
                "total_sectors": stats.get('total_sectors', 0),
                "total_sub_industries": stats.get('total_sub_industries', 0),
                "total_locations": stats.get('total_locations', 0),
                "oldest_company_founded": stats.get('oldest_company_founded'),
                "newest_company_founded": stats.get('newest_company_founded'),
                "earliest_addition": stats.get('earliest_addition'),
                "latest_addition": stats.get('latest_addition'),
                "sql": sql
            }
        return {"error": "No statistics found", "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get statistics: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "trends"])
def get_companies_by_founding_decade(decade: int = None, limit: int = 20) -> Dict[str, Any]:
    """
    Get companies founded in a specific decade (e.g., 1990 for 1990s).
    If no decade specified, returns distribution by decade.
    """
    if decade:
        decade_start = decade
        decade_end = decade + 9
        where_clause = f"WHERE founded >= '{decade_start}' AND founded <= '{decade_end}'"
        order_clause = "ORDER BY founded, security"
    else:
        where_clause = ""
        order_clause = "ORDER BY founded DESC, security"
    
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      {where_clause}
      {order_clause}
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "decade_filter": decade,
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies by decade: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "geographic"])
def get_companies_by_state(state: str, limit: int = 15) -> Dict[str, Any]:
    """
    Get S&P 500 companies headquartered in a specific US state.
    """
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      WHERE headquarters_loc LIKE '%{_esc(state)}%'
      ORDER BY security
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "state": state,
            "companies_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies by state: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "geographic"])
def get_geographic_distribution() -> Dict[str, Any]:
    """
    Get distribution of S&P 500 companies by headquarters location.
    """
    sql = """
      SELECT 
        headquarters_loc,
        COUNT(*) as company_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sp500_wik_list), 2) as percentage
      FROM sp500_wik_list 
      GROUP BY headquarters_loc
      ORDER BY company_count DESC
      LIMIT 20
    """
    try:
        results = run_query(sql)
        return {
            "location_distribution": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get geographic distribution: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "search"])
def search_companies_advanced(
    query: str, 
    search_fields: List[str] = None, 
    limit: int = 15
) -> Dict[str, Any]:
    """
    Advanced search across multiple fields: symbol, security name, sector, industry, location.
    """
    if search_fields is None:
        search_fields = ["symbol", "security", "gics_sector", "gics_sub_ind", "headquarters_loc"]
    
    conditions = []
    for field in search_fields:
        if field in ["symbol", "security", "gics_sector", "gics_sub_ind", "headquarters_loc"]:
            conditions.append(f"{field} LIKE '%{_esc(query)}%'")
    
    if not conditions:
        conditions = [f"security LIKE '%{_esc(query)}%'"]
    
    where_clause = "WHERE " + " OR ".join(conditions)
    
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      {where_clause}
      ORDER BY 
        CASE 
          WHEN symbol LIKE '{_esc(query.upper())}%' THEN 1
          WHEN security LIKE '{_esc(query)}%' THEN 2
          ELSE 3
        END,
        security
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "query": query,
            "search_fields": search_fields,
            "results_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to search companies: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "analysis"])
def get_sector_performance_summary() -> Dict[str, Any]:
    """
    Get a comprehensive summary of all S&P 500 sectors with company counts and percentages.
    """
    sql = """
      SELECT 
        gics_sector,
        COUNT(*) as company_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sp500_wik_list), 2) as percentage,
        COUNT(DISTINCT gics_sub_ind) as sub_industries_count,
        MIN(founded) as oldest_company,
        MAX(founded) as newest_company
      FROM sp500_wik_list 
      GROUP BY gics_sector
      ORDER BY company_count DESC
    """
    try:
        results = run_query(sql)
        return {
            "sector_summary": results,
            "total_sectors": len(results),
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get sector summary: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "historical"])
def get_index_changes_by_year(year: int = None, limit: int = 20) -> Dict[str, Any]:
    """
    Get companies added to or removed from S&P 500 in a specific year.
    If no year specified, returns recent changes.
    """
    if year:
        where_clause = f"WHERE YEAR(date_added) = {year}"
        order_clause = "ORDER BY date_added DESC"
    else:
        where_clause = "WHERE date_added >= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)"
        order_clause = "ORDER BY date_added DESC"
    
    sql = f"""
      SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
      FROM sp500_wik_list 
      {where_clause}
      {order_clause}
      LIMIT {limit}
    """
    try:
        results = run_query(sql)
        return {
            "year_filter": year,
            "changes_found": len(results),
            "companies": results,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get index changes: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "analysis"])
def get_company_relationships(symbol: str, relationship_type: str = "sector") -> Dict[str, Any]:
    """
    Get companies related to a given company by sector, industry, or location.
    """
    if relationship_type not in ["sector", "industry", "location"]:
        relationship_type = "sector"
    
    # First get the target company's details
    company_sql = f"""
      SELECT gics_sector, gics_sub_ind, headquarters_loc
      FROM sp500_wik_list 
      WHERE symbol = '{_esc(symbol.upper())}'
    """
    
    try:
        company_result = run_query(company_sql)
        if not company_result:
            return {"error": f"Company {symbol.upper()} not found", "sql": company_sql}
        
        company_data = company_result[0]
        
        # Build relationship query based on type
        if relationship_type == "sector":
            where_condition = f"gics_sector = '{_esc(company_data['gics_sector'])}'"
        elif relationship_type == "industry":
            where_condition = f"gics_sub_ind = '{_esc(company_data['gics_sub_ind'])}'"
        else:  # location
            where_condition = f"headquarters_loc = '{_esc(company_data['headquarters_loc'])}'"
        
        related_sql = f"""
          SELECT symbol, security, gics_sector, gics_sub_ind, headquarters_loc, cik, founded, date_added
          FROM sp500_wik_list 
          WHERE {where_condition} AND symbol != '{_esc(symbol.upper())}'
          ORDER BY security
          LIMIT 20
        """
        
        related_results = run_query(related_sql)
        
        return {
            "target_company": symbol.upper(),
            "relationship_type": relationship_type,
            "target_company_data": company_data,
            "related_companies_found": len(related_results),
            "related_companies": related_results,
            "sql": f"{company_sql}; {related_sql}"
        }
        
    except Exception as e:
        return {"error": f"Failed to get company relationships: {str(e)}", "sql": company_sql}

# --------------------- SEC Tools ---------------------

def _company_where(id_type: str, identifier: str) -> Tuple[str, List[Any]]:
    """
    Helper: build WHERE for company identifier
    id_type: 'cik' or 'symbol'
    identifier: value of cik (10/13 chars as stored) or symbol (e.g., AAPL)
    NOTE: If you later add an SP500 mapping table, update the 'symbol' branch to join/lookup CIKs.
    """
    if id_type == "cik":
        return "bf.cik = %s", [identifier]
    elif id_type == "symbol":
        # Placeholder: expect you'll swap this to a JOIN when you add the mapping table.
        # For now, assume you've normalized bronze_sec_facts.cik to be queryable via a helper or view.
        return "bf.cik IN (SELECT cik FROM sp500_symbols WHERE symbol = %s)", [identifier]
    else:
        raise ValueError("id_type must be 'cik' or 'symbol'")

@register_tool(tags=["financial", "sec", "facts"])
def get_sec_fact_timeseries(
    identifier: str,
    id_type: str = "cik",
    tag: str = "",
    taxonomy: str = "us-gaap",
    unit: Optional[str] = None,
    fy_from: Optional[int] = None,
    fy_to: Optional[int] = None,
    fp: Optional[str] = None,            # e.g., 'FY','Q1','Q2','Q3'
    frame: Optional[str] = None,         # e.g., 'CY2023Q4' or 'CY2023'
    order_by: str = "filed",             # 'filed' | 'end_date' | 'fy_fp'
    limit: int = 500
) -> Dict[str, Any]:
    """
    Return a time-series of a single XBRL fact for a company.
    Maps to SEC companyfacts fields: val, fy, fp, start_date, end_date, frame, form, filed, accn.
    """
    base = """
        SELECT
            bf.cik, bf.taxonomy, bf.tag, bf.unit, bf.val,
            bf.fy, bf.fp, bf.start_date, bf.end_date,
            bf.frame, bf.form, bf.filed, bf.accn
        FROM bronze_sec_facts bf
        WHERE {company_where}
          AND bf.taxonomy = %s
          AND bf.tag = %s
    """
    where_company, params = _company_where(id_type, identifier)
    params += [taxonomy, tag]

    if unit:
        base += " AND bf.unit = %s"
        params.append(unit)
    if fy_from is not None:
        base += " AND bf.fy >= %s"
        params.append(fy_from)
    if fy_to is not None:
        base += " AND bf.fy <= %s"
        params.append(fy_to)
    if fp:
        base += " AND bf.fp = %s"
        params.append(fp)
    if frame:
        base += " AND bf.frame = %s"
        params.append(frame)

    order_sql = {
        "filed": "bf.filed DESC, bf.end_date DESC",
        "end_date": "bf.end_date DESC, bf.filed DESC",
        "fy_fp": "bf.fy DESC, bf.fp DESC, bf.filed DESC"
    }.get(order_by, "bf.filed DESC")

    sql = base.format(company_where=where_company) + f" ORDER BY {order_sql} LIMIT %s"
    params.append(min(max(1, limit), 2000))

    try:
        rows = run_query(sql, tuple(params))
        return {"data_type": "sec_fact_timeseries", "results_found": len(rows), "data": rows, "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get time series: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "facts"])
def get_latest_sec_fact(
    identifier: str,
    id_type: str = "cik",
    tag: str = "",
    taxonomy: str = "us-gaap",
    unit: Optional[str] = None
) -> Dict[str, Any]:
    """
    Return the most recently filed value for a company/tag (optionally unit).
    """
    where_company, params = _company_where(id_type, identifier)
    sql = f"""
        WITH cte AS (
          SELECT
            bf.cik, bf.taxonomy, bf.tag, bf.unit, bf.val,
            bf.fy, bf.fp, bf.start_date, bf.end_date,
            bf.frame, bf.form, bf.filed, bf.accn,
            ROW_NUMBER() OVER (ORDER BY bf.filed DESC, bf.end_date DESC) rn
          FROM bronze_sec_facts bf
          WHERE {where_company}
            AND bf.taxonomy = %s
            AND bf.tag = %s
            {"AND bf.unit = %s" if unit else ""}
        )
        SELECT * FROM cte WHERE rn = 1
    """
    params += [taxonomy, tag]
    if unit:
        params.append(unit)
    try:
        rows = run_query(sql, tuple(params))
        return {"data_type": "sec_fact_latest", "results_found": len(rows), "data": rows, "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get latest fact: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "facts"])
def list_company_available_facts(
    identifier: str,
    id_type: str = "cik",
    taxonomy: Optional[str] = None,
    like_tag: Optional[str] = None,
    limit: int = 300
) -> Dict[str, Any]:
    """
    List distinct (taxonomy, tag, unit) a company has disclosed. Optional tag filter (ILIKE).
    """
    where_company, params = _company_where(id_type, identifier)
    sql = f"""
        SELECT bf.taxonomy, bf.tag, bf.unit, COUNT(*) AS n, MAX(bf.filed) AS last_filed
        FROM bronze_sec_facts bf
        WHERE {where_company}
        {"AND bf.taxonomy = %s" if taxonomy else ""}
        {"AND bf.tag ILIKE %s" if like_tag else ""}
        GROUP BY bf.taxonomy, bf.tag, bf.unit
        ORDER BY last_filed DESC, n DESC
        LIMIT %s
    """
    if taxonomy:
        params.append(taxonomy)
    if like_tag:
        params.append(f"%{like_tag}%")
    params.append(min(max(1, limit), 2000))
    try:
        rows = run_query(sql, tuple(params))
        return {"data_type": "sec_available_facts", "results_found": len(rows), "data": rows, "sql": sql}
    except Exception as e:
        return {"error": f"Failed to list facts: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "facts", "peers"])
def get_sec_fact_peers_snapshot(
    identifiers: List[str],
    id_type: str = "cik",         # 'cik' or 'symbol'
    tag: str = "",
    taxonomy: str = "us-gaap",
    unit: Optional[str] = None,
    period_selector: str = "latest",  # 'latest' | 'frame' | 'fy_fp'
    frame: Optional[str] = None,      # e.g., 'CY2024Q4'
    fy: Optional[int] = None,
    fp: Optional[str] = None,         # 'FY','Q1','Q2','Q3'
    limit_per_company: int = 1
) -> Dict[str, Any]:
    """
    Compare the same fact across multiple companies for a chosen period.
    - period_selector='latest' -> latest filed per company
    - period_selector='frame'  -> exact frame match (e.g., CY2024Q4)
    - period_selector='fy_fp'  -> match fiscal year + period
    """
    if not identifiers:
        return {"error": "identifiers cannot be empty"}
    # Build a temp table of companies via UNION ALL to keep it SQL-portable
    placeholders = ", ".join(["%s"] * len(identifiers))
    id_values = list(identifiers)

    entity_sql, entity_params = _company_where(id_type, "{ID_PLACEHOLDER}")
    # Replace a single %s placeholder; we'll expand per row
    entity_sql = entity_sql.replace("%s", "{id_bind}")

    filters = ["bf.taxonomy = %s", "bf.tag = %s"]
    params: List[Any] = [taxonomy, tag]
    if unit:
        filters.append("bf.unit = %s")
        params.append(unit)
    if period_selector == "frame" and frame:
        filters.append("bf.frame = %s")
        params.append(frame)
    if period_selector == "fy_fp" and fy is not None and fp:
        filters.append("bf.fy = %s")
        filters.append("bf.fp = %s")
        params.extend([fy, fp])

    where_union_parts = []
    for _ in identifiers:
        where_union_parts.append("(" + entity_sql.format(id_bind="%s") + ")")
    company_predicate = " OR ".join(where_union_parts)
    params = id_values + params  # identifiers first, then filters

    base = f"""
      WITH base AS (
        SELECT
          bf.cik, bf.taxonomy, bf.tag, bf.unit, bf.val,
          bf.fy, bf.fp, bf.start_date, bf.end_date,
          bf.frame, bf.form, bf.filed, bf.accn,
          ROW_NUMBER() OVER (
            PARTITION BY bf.cik
            ORDER BY bf.filed DESC, bf.end_date DESC
          ) AS rn_latest
        FROM bronze_sec_facts bf
        WHERE ({company_predicate})
          AND {' AND '.join(filters)}
      )
    """
    if period_selector == "latest":
        sql = base + """
          SELECT * FROM base
          WHERE rn_latest <= %s
          ORDER BY filed DESC, end_date DESC
        """
        params.append(min(max(1, limit_per_company), 5))
    else:
        sql = base + """
          SELECT * FROM base
          ORDER BY cik, filed DESC, end_date DESC
        """

    try:
        rows = run_query(sql, tuple(params))
        return {"data_type": "sec_peers_snapshot", "results_found": len(rows), "data": rows, "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get peers snapshot: {str(e)}", "sql": sql}