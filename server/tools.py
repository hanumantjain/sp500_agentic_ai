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
    sql = f"""
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
    sql = f"""
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
    sql = f"""
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
    sql = f"""
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

def _company_where(id_type: str, identifier: str) -> str:
    """
    Helper: build WHERE for company identifier
    id_type: 'cik' or 'symbol'
    identifier: value of cik (10/13 chars as stored) or symbol (e.g., AAPL)
    Maps symbol to CIK using sp500_wik_list table
    """
    if id_type == "cik":
        # bronze_sec_facts stores CIK as CIK0000815097 format
        if not identifier.startswith("CIK"):
            # Convert numeric CIK to CIK format
            identifier = f"CIK{identifier.zfill(10)}"
        return f"bf.cik = '{_esc(identifier)}'"
    elif id_type == "symbol":
        # Map symbol to CIK using sp500_wik_list table
        return f"bf.cik = (SELECT cik FROM sp500_wik_list WHERE symbol = '{_esc(identifier.upper())}')"
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
    where_company = _company_where(id_type, identifier)
    
    conditions = [where_company, f"bf.taxonomy = '{_esc(taxonomy)}'", f"bf.tag = '{_esc(tag)}'"]
    
    if unit:
        conditions.append(f"bf.unit = '{_esc(unit)}'")
    if fy_from is not None:
        conditions.append(f"bf.fy >= {fy_from}")
    if fy_to is not None:
        conditions.append(f"bf.fy <= {fy_to}")
    if fp:
        conditions.append(f"bf.fp = '{_esc(fp)}'")
    if frame:
        conditions.append(f"bf.frame = '{_esc(frame)}'")

    order_sql = {
        "filed": "bf.filed DESC, bf.end_date DESC",
        "end_date": "bf.end_date DESC, bf.filed DESC",
        "fy_fp": "bf.fy DESC, bf.fp DESC, bf.filed DESC"
    }.get(order_by, "bf.filed DESC")

    sql = f"""
        SELECT
            bf.cik, bf.taxonomy, bf.tag, bf.unit, bf.val,
            bf.fy, bf.fp, bf.start_date, bf.end_date,
            bf.frame, bf.form, bf.filed, bf.accn
        FROM bronze_sec_facts bf
        WHERE {' AND '.join(conditions)}
        ORDER BY {order_sql}
        LIMIT {min(max(1, limit), 2000)}
    """

    try:
        rows = run_query(sql)
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
    where_company = _company_where(id_type, identifier)
    
    conditions = [where_company, f"bf.taxonomy = '{_esc(taxonomy)}'", f"bf.tag = '{_esc(tag)}'"]
    if unit:
        conditions.append(f"bf.unit = '{_esc(unit)}'")
    
    sql = f"""
        WITH cte AS (
          SELECT
            bf.cik, bf.taxonomy, bf.tag, bf.unit, bf.val,
            bf.fy, bf.fp, bf.start_date, bf.end_date,
            bf.frame, bf.form, bf.filed, bf.accn,
            ROW_NUMBER() OVER (ORDER BY bf.filed DESC, bf.end_date DESC) rn
          FROM bronze_sec_facts bf
          WHERE {' AND '.join(conditions)}
        )
        SELECT * FROM cte WHERE rn = 1
    """
    
    try:
        rows = run_query(sql)
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
    where_company = _company_where(id_type, identifier)
    
    conditions = [where_company]
    if taxonomy:
        conditions.append(f"bf.taxonomy = '{_esc(taxonomy)}'")
    if like_tag:
        conditions.append(f"bf.tag ILIKE '%{_esc(like_tag)}%'")
    
    sql = f"""
        SELECT bf.taxonomy, bf.tag, bf.unit, COUNT(*) AS n, MAX(bf.filed) AS last_filed
        FROM bronze_sec_facts bf
        WHERE {' AND '.join(conditions)}
        GROUP BY bf.taxonomy, bf.tag, bf.unit
        ORDER BY last_filed DESC, n DESC
        LIMIT {min(max(1, limit), 2000)}
    """
    
    try:
        rows = run_query(sql)
        return {"data_type": "sec_available_facts", "results_found": len(rows), "data": rows, "sql": sql}
    except Exception as e:
        return {"error": f"Failed to list facts: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "facts", "smart"])
def get_sec_facts_smart_search(
    identifier: str,
    id_type: str = "symbol",
    search_term: Optional[str] = None,
    form_type: Optional[str] = None,
    year: Optional[int] = None,
    quarter: Optional[str] = None,
    limit: int = 100
) -> Dict[str, Any]:
    """
    Smart search for SEC facts with flexible parameters.
    Can search by company, form type (10-K, 10-Q, 8-K), year, quarter, or specific terms.
    """
    where_company = _company_where(id_type, identifier)
    
    # Build dynamic WHERE clause
    conditions = [where_company]
    
    if search_term:
        conditions.append(f"(bf.tag ILIKE '%{_esc(search_term)}%' OR bf.taxonomy ILIKE '%{_esc(search_term)}%')")
    
    if form_type:
        conditions.append(f"bf.form = '{_esc(form_type)}'")
    
    if year:
        conditions.append(f"bf.fy = {year}")
    
    if quarter:
        conditions.append(f"bf.fp = '{_esc(quarter)}'")
    
    where_clause = " AND ".join(conditions)
    
    sql = f"""
        SELECT 
            bf.cik, bf.taxonomy, bf.tag, bf.unit, bf.val,
            bf.fy, bf.fp, bf.start_date, bf.end_date,
            bf.frame, bf.form, bf.filed, bf.accn
        FROM bronze_sec_facts bf
        WHERE {where_clause}
        ORDER BY bf.filed DESC, bf.end_date DESC
        LIMIT {min(max(1, limit), 1000)}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "sec_smart_search", 
            "search_params": {
                "identifier": identifier,
                "id_type": id_type,
                "search_term": search_term,
                "form_type": form_type,
                "year": year,
                "quarter": quarter
            },
            "results_found": len(rows), 
            "data": rows, 
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to search facts: {str(e)}", "sql": sql}

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
    
    # Build company conditions for each identifier
    company_conditions = []
    for identifier in identifiers:
        if id_type == "cik":
            if not identifier.startswith("CIK"):
                identifier = f"CIK{identifier.zfill(10)}"
            company_conditions.append(f"bf.cik = '{_esc(identifier)}'")
        else:  # symbol
            company_conditions.append(f"bf.cik = (SELECT cik FROM sp500_wik_list WHERE symbol = '{_esc(identifier.upper())}')")
    
    company_predicate = " OR ".join(company_conditions)
    
    # Build filters
    filters = [f"bf.taxonomy = '{_esc(taxonomy)}'", f"bf.tag = '{_esc(tag)}'"]
    if unit:
        filters.append(f"bf.unit = '{_esc(unit)}'")
    if period_selector == "frame" and frame:
        filters.append(f"bf.frame = '{_esc(frame)}'")
    if period_selector == "fy_fp" and fy is not None and fp:
        filters.append(f"bf.fy = {fy}")
        filters.append(f"bf.fp = '{_esc(fp)}'")

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
        sql = base + f"""
          SELECT * FROM base
          WHERE rn_latest <= {min(max(1, limit_per_company), 5)}
          ORDER BY filed DESC, end_date DESC
        """
    else:
        sql = base + """
          SELECT * FROM base
          ORDER BY cik, filed DESC, end_date DESC
        """

    try:
        rows = run_query(sql)
        return {"data_type": "sec_peers_snapshot", "results_found": len(rows), "data": rows, "sql": sql}
    except Exception as e:
        return {"error": f"Failed to get peers snapshot: {str(e)}", "sql": sql}

# --------------------- OHLC Stock Data Tools ---------------------

@register_tool(tags=["financial", "stock", "price"])
def get_stock_price_data(
    symbol: str,
    days: int = 30,
    include_metrics: bool = True
) -> Dict[str, Any]:
    """
    Get stock price data (OHLC) for a specific symbol with optional performance metrics.
    Data available from 1962-01-02 to 2025-09-09. For very large date ranges, consider using get_stock_historical_analysis.
    """
    try:
        # Get price data - no hard limits, let the database handle it
        sql = f"""
            SELECT ticker, date, open, high, low, close, volume
            FROM sp500_stooq_ohcl 
            WHERE ticker = '{_esc(symbol.upper())}'
            ORDER BY date DESC
            LIMIT {max(1, days)}
        """
        price_data = run_query(sql)
        
        if not price_data:
            # Check if symbol exists in our data
            check_sql = f"SELECT MIN(date) as earliest, MAX(date) as latest FROM sp500_stooq_ohcl WHERE ticker = '{_esc(symbol.upper())}'"
            availability = run_query(check_sql)
            if availability:
                return {
                    "error": f"No recent data found for symbol {symbol}. Data available from {availability[0]['earliest']} to {availability[0]['latest']}",
                    "data_availability": {
                        "earliest_date": availability[0]['earliest'],
                        "latest_date": availability[0]['latest']
                    }
                }
            else:
                return {"error": f"Symbol {symbol} not found in our database. Available symbols: 501 S&P 500 companies"}
        
        result = {
            "data_type": "stock_price_data",
            "symbol": symbol.upper(),
            "days_requested": days,
            "records_found": len(price_data),
            "price_data": price_data,
            "data_availability": {
                "earliest_date": "1962-01-02",
                "latest_date": "2025-09-09"
            },
            "sql": sql
        }
        
        # Add performance metrics if requested
        if include_metrics and len(price_data) > 1:
            latest = price_data[0]
            oldest = price_data[-1]
            
            # Calculate basic metrics
            price_change = latest['close'] - oldest['close']
            price_change_pct = (price_change / oldest['close']) * 100
            
            # Get min/max in period
            min_price = min(row['low'] for row in price_data)
            max_price = max(row['high'] for row in price_data)
            avg_volume = sum(row['volume'] for row in price_data) / len(price_data)
            
            result["performance_metrics"] = {
                "latest_close": latest['close'],
                "period_start_close": oldest['close'],
                "price_change": round(price_change, 2),
                "price_change_pct": round(price_change_pct, 2),
                "period_high": max_price,
                "period_low": min_price,
                "avg_volume": round(avg_volume, 0),
                "latest_volume": latest['volume']
            }
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to get stock price data: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stock", "comparison"])
def compare_stock_prices(
    symbols: List[str],
    days: int = 30,
    metric: str = "close"
) -> Dict[str, Any]:
    """
    Compare stock prices across multiple symbols.
    """
    if not symbols or len(symbols) > 10:
        return {"error": "Please provide 1-10 stock symbols"}
    
    try:
        symbols_str = "', '".join([_esc(s.upper()) for s in symbols])
        
        # Get latest price data for all symbols
        sql = f"""
            SELECT 
                s1.ticker,
                s1.date,
                s1.close as latest_close,
                s1.volume as latest_volume,
                s2.avg_close_period,
                ROUND(((s1.close - s2.avg_close_period) / s2.avg_close_period) * 100, 2) as change_pct
            FROM sp500_stooq_ohcl s1
            JOIN (
                SELECT ticker, AVG(close) as avg_close_period
                FROM sp500_stooq_ohcl 
                WHERE ticker IN ('{symbols_str}')
                  AND date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
                GROUP BY ticker
            ) s2 ON s1.ticker = s2.ticker
            WHERE s1.ticker IN ('{symbols_str}')
              AND s1.date = (
                  SELECT MAX(date) 
                  FROM sp500_stooq_ohcl s3 
                  WHERE s3.ticker = s1.ticker
              )
            ORDER BY s1.close DESC
        """
        
        comparison_data = run_query(sql)
        
        return {
            "data_type": "stock_price_comparison",
            "symbols": [s.upper() for s in symbols],
            "days_period": days,
            "comparison_metric": metric,
            "records_found": len(comparison_data),
            "comparison_data": comparison_data,
            "sql": sql
        }
        
    except Exception as e:
        return {"error": f"Failed to compare stock prices: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stock", "analysis"])
def get_stock_performance_analysis(
    symbol: str,
    days: int = 30
) -> Dict[str, Any]:
    """
    Get comprehensive stock performance analysis including volatility, moving averages, and trends.
    """
    try:
        # Get price data with moving averages
        sql = f"""
            SELECT 
                date,
                open,
                high,
                low,
                close,
                volume,
                AVG(close) OVER (ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma_10,
                AVG(close) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as ma_30
            FROM sp500_stooq_ohcl 
            WHERE ticker = '{_esc(symbol.upper())}'
              AND date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
            ORDER BY date DESC
            LIMIT {min(max(1, days), 365)}
        """
        
        price_data = run_query(sql)
        
        if not price_data:
            return {"error": f"No price data found for symbol {symbol}"}
        
        # Calculate volatility metrics
        closes = [row['close'] for row in price_data]
        min_price = min(row['low'] for row in price_data)
        max_price = max(row['high'] for row in price_data)
        avg_close = sum(closes) / len(closes)
        volatility_pct = ((max_price - min_price) / avg_close) * 100
        
        # Get latest moving averages
        latest = price_data[0]
        
        return {
            "data_type": "stock_performance_analysis",
            "symbol": symbol.upper(),
            "analysis_period_days": days,
            "records_analyzed": len(price_data),
            "performance_metrics": {
                "latest_close": latest['close'],
                "latest_ma_10": round(latest['ma_10'], 2),
                "latest_ma_30": round(latest['ma_30'], 2),
                "period_high": max_price,
                "period_low": min_price,
                "avg_close": round(avg_close, 2),
                "volatility_pct": round(volatility_pct, 2),
                "latest_volume": latest['volume']
            },
            "price_data": price_data[:10],  # Return first 10 days for trend analysis
            "sql": sql
        }
        
    except Exception as e:
        return {"error": f"Failed to analyze stock performance: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stock", "sector"])
def get_sector_stock_performance(
    sector: str,
    limit: int = 10,
    sort_by: str = "market_cap"
) -> Dict[str, Any]:
    """
    Get stock performance data for all companies in a specific sector.
    """
    try:
        # Get sector stocks with latest price data
        sql = f"""
            SELECT 
                c.symbol,
                c.security,
                c.gics_sector,
                s.close as latest_close,
                s.volume as latest_volume,
                s.date as latest_date,
                ROUND(AVG(s.close) OVER (PARTITION BY c.symbol ORDER BY s.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as avg_30d
            FROM sp500_wik_list c
            JOIN sp500_stooq_ohcl s ON c.symbol = s.ticker
            WHERE c.gics_sector LIKE '%{_esc(sector)}%'
              AND s.date = (
                  SELECT MAX(date) 
                  FROM sp500_stooq_ohcl s2 
                  WHERE s2.ticker = c.symbol
              )
            ORDER BY s.close DESC
            LIMIT {min(max(1, limit), 50)}
        """
        
        sector_data = run_query(sql)
        
        return {
            "data_type": "sector_stock_performance",
            "sector": sector,
            "stocks_found": len(sector_data),
            "sort_by": sort_by,
            "sector_data": sector_data,
            "sql": sql
        }
        
    except Exception as e:
        return {"error": f"Failed to get sector performance: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stock", "volume"])
def get_high_volume_stocks(
    days: int = 1,
    limit: int = 20,
    min_volume: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get stocks with highest trading volume for recent days.
    """
    try:
        volume_filter = f"AND volume >= {min_volume}" if min_volume else ""
        
        sql = f"""
            SELECT 
                s.ticker,
                s.date,
                s.close,
                s.volume,
                c.security,
                c.gics_sector
            FROM sp500_stooq_ohcl s
            LEFT JOIN sp500_wik_list c ON s.ticker = c.symbol
            WHERE s.date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
              {volume_filter}
            ORDER BY s.volume DESC
            LIMIT {min(max(1, limit), 100)}
        """
        
        volume_data = run_query(sql)
        
        return {
            "data_type": "high_volume_stocks",
            "days_analyzed": days,
            "min_volume_filter": min_volume,
            "stocks_found": len(volume_data),
            "volume_data": volume_data,
            "sql": sql
        }
        
    except Exception as e:
        return {"error": f"Failed to get high volume stocks: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stock", "integrated"])
def get_stock_comprehensive_analysis(
    symbol: str,
    include_sec_data: bool = True,
    days: int = 30
) -> Dict[str, Any]:
    """
    Get comprehensive analysis combining stock price data with company info and SEC data.
    """
    try:
        # Get company info with latest stock data
        sql = f"""
            SELECT 
                c.symbol,
                c.security,
                c.gics_sector,
                c.gics_sub_ind,
                c.headquarters_loc,
                c.cik,
                s.close as latest_close,
                s.volume as latest_volume,
                s.date as latest_date,
                ROUND(AVG(s.close) OVER (ORDER BY s.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as avg_30d
            FROM sp500_wik_list c
            JOIN sp500_stooq_ohcl s ON c.symbol = s.ticker
            WHERE c.symbol = '{_esc(symbol.upper())}'
              AND s.date = (
                  SELECT MAX(date) 
                  FROM sp500_stooq_ohcl s2 
                  WHERE s2.ticker = c.symbol
              )
        """
        
        company_data = run_query(sql)
        
        if not company_data:
            return {"error": f"No data found for symbol {symbol}"}
        
        result = {
            "data_type": "comprehensive_stock_analysis",
            "symbol": symbol.upper(),
            "company_info": company_data[0],
            "analysis_period_days": days
        }
        
        # Get recent price history
        price_sql = f"""
            SELECT date, open, high, low, close, volume
            FROM sp500_stooq_ohcl 
            WHERE ticker = '{_esc(symbol.upper())}'
            ORDER BY date DESC
            LIMIT {days}
        """
        price_data = run_query(price_sql)
        result["price_history"] = price_data
        
        # Get SEC data if requested
        if include_sec_data:
            sec_sql = f"""
                SELECT bf.taxonomy, bf.tag, bf.unit, bf.val, bf.fy, bf.fp, bf.filed
                FROM bronze_sec_facts bf
                WHERE bf.cik = (SELECT cik FROM sp500_wik_list WHERE symbol = '{_esc(symbol.upper())}')
                  AND bf.taxonomy = 'us-gaap'
                  AND bf.tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'NetIncomeLoss', 'Assets')
                ORDER BY bf.filed DESC
                LIMIT 5
            """
            sec_data = run_query(sec_sql)
            result["recent_sec_facts"] = sec_data
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to get comprehensive analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stock", "historical"])
def get_stock_historical_analysis(
    symbol: str,
    start_year: int = None,
    end_year: int = None,
    analysis_type: str = "average"
) -> Dict[str, Any]:
    """
    Get historical stock analysis for a specific symbol over a multi-year period.
    Analysis types: 'average', 'yearly', 'performance'
    Data available from 1962-01-02 to 2025-09-09. If no years specified, uses all available data.
    """
    try:
        # Build flexible query based on available parameters
        where_conditions = [f"ticker = '{_esc(symbol.upper())}'"]
        
        if start_year is not None:
            where_conditions.append(f"YEAR(date) >= {start_year}")
        if end_year is not None:
            where_conditions.append(f"YEAR(date) <= {end_year}")
        
        sql = f"""
            SELECT 
                ticker, 
                date, 
                open, 
                high, 
                low, 
                close, 
                volume,
                YEAR(date) as year
            FROM sp500_stooq_ohcl 
            WHERE {' AND '.join(where_conditions)}
            ORDER BY date ASC
        """
        
        historical_data = run_query(sql)
        
        if not historical_data:
            # Check data availability for this symbol
            check_sql = f"SELECT MIN(date) as earliest, MAX(date) as latest FROM sp500_stooq_ohcl WHERE ticker = '{_esc(symbol.upper())}'"
            availability = run_query(check_sql)
            if availability:
                earliest = availability[0]['earliest']
                latest = availability[0]['latest']
                return {
                    "error": f"No data found for {symbol} in the requested period. Data available from {earliest} to {latest}",
                    "data_availability": {
                        "earliest_date": earliest,
                        "latest_date": latest
                    }
                }
            else:
                return {"error": f"Symbol {symbol} not found in our database. Available symbols: 501 S&P 500 companies"}
        
        # Determine actual date range from data
        actual_start = historical_data[0]['date']
        actual_end = historical_data[-1]['date']
        
        result = {
            "data_type": "stock_historical_analysis",
            "symbol": symbol.upper(),
            "requested_start_year": start_year,
            "requested_end_year": end_year,
            "actual_start_date": actual_start,
            "actual_end_date": actual_end,
            "analysis_type": analysis_type,
            "total_records": len(historical_data),
            "data_availability": {
                "earliest_date": "1962-01-02",
                "latest_date": "2025-09-09"
            },
            "sql": sql
        }
        
        if analysis_type == "average":
            # Calculate overall average price for the period
            avg_close = sum(row['close'] for row in historical_data) / len(historical_data)
            avg_volume = sum(row['volume'] for row in historical_data) / len(historical_data)
            min_price = min(row['low'] for row in historical_data)
            max_price = max(row['high'] for row in historical_data)
            
            result["analysis_results"] = {
                "average_close_price": round(avg_close, 2),
                "average_volume": round(avg_volume, 0),
                "period_low": min_price,
                "period_high": max_price,
                "price_range": round(max_price - min_price, 2),
                "total_trading_days": len(historical_data)
            }
            
        elif analysis_type == "yearly":
            # Calculate yearly averages
            yearly_data = {}
            for row in historical_data:
                year = row['year']
                if year not in yearly_data:
                    yearly_data[year] = []
                yearly_data[year].append(row['close'])
            
            yearly_averages = {}
            for year, prices in yearly_data.items():
                yearly_averages[year] = {
                    "average_price": round(sum(prices) / len(prices), 2),
                    "trading_days": len(prices)
                }
            
            result["analysis_results"] = yearly_averages
            
        elif analysis_type == "performance":
            # Calculate performance metrics
            first_price = historical_data[0]['close']
            last_price = historical_data[-1]['close']
            total_return = ((last_price - first_price) / first_price) * 100
            
            # Calculate volatility (standard deviation of daily returns)
            daily_returns = []
            for i in range(1, len(historical_data)):
                prev_close = historical_data[i-1]['close']
                curr_close = historical_data[i]['close']
                daily_return = (curr_close - prev_close) / prev_close
                daily_returns.append(daily_return)
            
            if daily_returns:
                avg_return = sum(daily_returns) / len(daily_returns)
                variance = sum((r - avg_return) ** 2 for r in daily_returns) / len(daily_returns)
                volatility = (variance ** 0.5) * 100  # Annualized volatility approximation
            else:
                volatility = 0
            
            result["analysis_results"] = {
                "starting_price": first_price,
                "ending_price": last_price,
                "total_return_percent": round(total_return, 2),
                "annualized_volatility": round(volatility, 2),
                "years_analyzed": end_year - start_year + 1
            }
        
        # Add sample data for context
        result["sample_data"] = historical_data[:10]  # First 10 records
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to get historical analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stock", "extremes"])
def get_stock_extremes(
    symbol: str,
    start_year: int = None,
    end_year: int = None
) -> Dict[str, Any]:
    """
    Get all-time high and low prices for a specific symbol.
    Data available from 1962-01-02 to 2025-09-09. If no years specified, uses all available data.
    """
    try:
        # Build flexible query based on available parameters
        where_conditions = [f"ticker = '{_esc(symbol.upper())}'"]
        
        if start_year is not None:
            where_conditions.append(f"YEAR(date) >= {start_year}")
        if end_year is not None:
            where_conditions.append(f"YEAR(date) <= {end_year}")
        
        # Get all-time high
        high_sql = f"""
            SELECT date, high, close, volume
            FROM sp500_stooq_ohcl 
            WHERE {' AND '.join(where_conditions)}
            ORDER BY high DESC
            LIMIT 1
        """
        
        # Get all-time low
        low_sql = f"""
            SELECT date, low, close, volume
            FROM sp500_stooq_ohcl 
            WHERE {' AND '.join(where_conditions)}
            ORDER BY low ASC
            LIMIT 1
        """
        
        high_result = run_query(high_sql)
        low_result = run_query(low_sql)
        
        if not high_result or not low_result:
            # Check data availability for this symbol
            check_sql = f"SELECT MIN(date) as earliest, MAX(date) as latest FROM sp500_stooq_ohcl WHERE ticker = '{_esc(symbol.upper())}'"
            availability = run_query(check_sql)
            if availability:
                earliest = availability[0]['earliest']
                latest = availability[0]['latest']
                return {
                    "error": f"No data found for {symbol} in the requested period. Data available from {earliest} to {latest}",
                    "data_availability": {
                        "earliest_date": earliest,
                        "latest_date": latest
                    }
                }
            else:
                return {"error": f"Symbol {symbol} not found in our database. Available symbols: 501 S&P 500 companies"}
        
        # Get data range
        range_sql = f"""
            SELECT MIN(date) as earliest, MAX(date) as latest, COUNT(*) as total_records
            FROM sp500_stooq_ohcl 
            WHERE {' AND '.join(where_conditions)}
        """
        range_result = run_query(range_sql)
        
        result = {
            "data_type": "stock_extremes",
            "symbol": symbol.upper(),
            "requested_start_year": start_year,
            "requested_end_year": end_year,
            "data_availability": {
                "earliest_date": "1962-01-02",
                "latest_date": "2025-09-09"
            },
            "sql": f"High: {high_sql}, Low: {low_sql}"
        }
        
        if range_result:
            result["actual_data_period"] = {
                "earliest_date": range_result[0]['earliest'],
                "latest_date": range_result[0]['latest'],
                "total_records": range_result[0]['total_records']
            }
        
        # Format high result
        if high_result:
            high_data = high_result[0]
            result["all_time_high"] = {
                "date": high_data['date'],
                "high_price": high_data['high'],
                "close_price": high_data['close'],
                "volume": high_data['volume']
            }
        
        # Format low result
        if low_result:
            low_data = low_result[0]
            result["all_time_low"] = {
                "date": low_data['date'],
                "low_price": low_data['low'],
                "close_price": low_data['close'],
                "volume": low_data['volume']
            }
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to get stock extremes: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "news", "company"])
def get_company_news(
    symbol: str,
    limit: int = 5,
    days_back: int = 30
) -> Dict[str, Any]:
    """
    Get recent news articles for a specific company symbol.
    Data available from 2020-03-27 to present. Returns headlines, summaries, sources, and URLs for citations.
    """
    try:
        sql = f"""
            SELECT 
                symbol, 
                datetime, 
                headline, 
                summary, 
                source, 
                url,
                category
            FROM sp500_finnhub_news 
            WHERE symbol = '{_esc(symbol.upper())}'
            AND datetime >= DATE_SUB(NOW(), INTERVAL {max(1, days_back)} DAY)
            ORDER BY datetime DESC 
            LIMIT {max(1, min(limit, 20))}
        """
        
        news_data = run_query(sql)
        
        if not news_data:
            # Check if symbol exists in news data
            check_sql = f"SELECT COUNT(*) as count FROM sp500_finnhub_news WHERE symbol = '{_esc(symbol.upper())}'"
            count_result = run_query(check_sql)
            if count_result and count_result[0]['count'] > 0:
                return {
                    "error": f"No recent news found for {symbol} in the last {days_back} days. Try increasing the days_back parameter.",
                    "data_availability": {
                        "symbol": symbol.upper(),
                        "has_historical_news": True,
                        "suggestion": "Try with a larger days_back value (e.g., 90 or 365)"
                    }
                }
            else:
                return {"error": f"Symbol {symbol} not found in our news database. Available symbols: 500 S&P 500 companies"}
        
        result = {
            "data_type": "company_news",
            "symbol": symbol.upper(),
            "days_back": days_back,
            "limit": limit,
            "records_found": len(news_data),
            "news_articles": news_data,
            "data_availability": {
                "earliest_news": "2020-03-27",
                "latest_news": "Present",
                "total_sources": 20
            },
            "sql": sql
        }
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to get company news: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "news", "search"])
def search_news_by_keywords(
    keywords: str,
    limit: int = 5,
    days_back: int = 30
) -> Dict[str, Any]:
    """
    Search news articles by keywords in headline or summary.
    Data available from 2020-03-27 to present. Returns relevant articles with sources and URLs for citations.
    """
    try:
        # Clean and prepare keywords for search
        clean_keywords = _esc(keywords.strip())
        search_terms = clean_keywords.split()
        
        # Build search conditions
        search_conditions = []
        for term in search_terms:
            search_conditions.append(f"(headline LIKE '%{term}%' OR summary LIKE '%{term}%')")
        
        where_clause = " AND ".join(search_conditions)
        
        sql = f"""
            SELECT 
                symbol, 
                datetime, 
                headline, 
                summary, 
                source, 
                url,
                category
            FROM sp500_finnhub_news 
            WHERE {where_clause}
            AND datetime >= DATE_SUB(NOW(), INTERVAL {max(1, days_back)} DAY)
            ORDER BY datetime DESC 
            LIMIT {max(1, min(limit, 20))}
        """
        
        news_data = run_query(sql)
        
        if not news_data:
            return {
                "error": f"No news found matching keywords '{keywords}' in the last {days_back} days. Try different keywords or increase days_back.",
                "data_availability": {
                    "keywords_searched": keywords,
                    "days_back": days_back,
                    "suggestion": "Try broader keywords or increase the search period"
                }
            }
        
        result = {
            "data_type": "news_search",
            "keywords": keywords,
            "days_back": days_back,
            "limit": limit,
            "records_found": len(news_data),
            "news_articles": news_data,
            "data_availability": {
                "earliest_news": "2020-03-27",
                "latest_news": "Present",
                "total_sources": 20
            },
            "sql": sql
        }
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to search news: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "news", "market"])
def get_market_news(
    limit: int = 10,
    days_back: int = 7
) -> Dict[str, Any]:
    """
    Get recent market-wide news articles across all S&P 500 companies.
    Data available from 2020-03-27 to present. Returns latest market news with sources and URLs for citations.
    """
    try:
        sql = f"""
            SELECT 
                symbol, 
                datetime, 
                headline, 
                summary, 
                source, 
                url,
                category
            FROM sp500_finnhub_news 
            WHERE datetime >= DATE_SUB(NOW(), INTERVAL {max(1, days_back)} DAY)
            ORDER BY datetime DESC 
            LIMIT {max(1, min(limit, 50))}
        """
        
        news_data = run_query(sql)
        
        if not news_data:
            return {
                "error": f"No market news found in the last {days_back} days.",
                "data_availability": {
                    "days_back": days_back,
                    "suggestion": "Try increasing the days_back parameter"
                }
            }
        
        # Get unique symbols and sources for context
        unique_symbols = list(set(article['symbol'] for article in news_data))
        unique_sources = list(set(article['source'] for article in news_data))
        
        result = {
            "data_type": "market_news",
            "days_back": days_back,
            "limit": limit,
            "records_found": len(news_data),
            "unique_symbols": len(unique_symbols),
            "unique_sources": len(unique_sources),
            "news_articles": news_data,
            "data_availability": {
                "earliest_news": "2020-03-27",
                "latest_news": "Present",
                "total_sources": 20
            },
            "sql": sql
        }
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to get market news: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "news", "sector"])
def get_sector_news(
    sector: str,
    limit: int = 5,
    days_back: int = 30
) -> Dict[str, Any]:
    """
    Get recent news for companies in a specific sector.
    Data available from 2020-03-27 to present. Returns sector-specific news with sources and URLs for citations.
    """
    try:
        # First get companies in the sector
        sector_sql = f"""
            SELECT DISTINCT symbol 
            FROM sp500_wik_list 
            WHERE gics_sector LIKE '%{_esc(sector)}%'
        """
        sector_companies = run_query(sector_sql)
        
        if not sector_companies:
            return {"error": f"Sector '{sector}' not found. Available sectors: Technology, Healthcare, Financials, Consumer Discretionary, Industrials, Consumer Staples, Energy, Utilities, Real Estate, Materials, Communication Services"}
        
        # Get symbols for the sector
        symbols = [company['symbol'] for company in sector_companies]
        symbols_str = "', '".join([_esc(sym) for sym in symbols])
        
        sql = f"""
            SELECT 
                symbol, 
                datetime, 
                headline, 
                summary, 
                source, 
                url,
                category
            FROM sp500_finnhub_news 
            WHERE symbol IN ('{symbols_str}')
            AND datetime >= DATE_SUB(NOW(), INTERVAL {max(1, days_back)} DAY)
            ORDER BY datetime DESC 
            LIMIT {max(1, min(limit, 20))}
        """
        
        news_data = run_query(sql)
        
        if not news_data:
            return {
                "error": f"No recent news found for {sector} sector in the last {days_back} days. Try increasing the days_back parameter.",
                "data_availability": {
                    "sector": sector,
                    "companies_in_sector": len(symbols),
                    "days_back": days_back,
                    "suggestion": "Try with a larger days_back value (e.g., 90 or 365)"
                }
            }
        
        # Get unique companies and sources
        unique_companies = list(set(article['symbol'] for article in news_data))
        unique_sources = list(set(article['source'] for article in news_data))
        
        result = {
            "data_type": "sector_news",
            "sector": sector,
            "days_back": days_back,
            "limit": limit,
            "records_found": len(news_data),
            "companies_in_sector": len(symbols),
            "companies_with_news": len(unique_companies),
            "unique_sources": len(unique_sources),
            "news_articles": news_data,
            "data_availability": {
                "earliest_news": "2020-03-27",
                "latest_news": "Present",
                "total_sources": 20
            },
            "sql": sql
        }
        
        return result
        
    except Exception as e:
        return {"error": f"Failed to get sector news: {str(e)}", "sql": sql}

# =============================================================================
# COMPREHENSIVE FINANCIAL ANALYSIS TOOLS
# =============================================================================

@register_tool(tags=["financial", "stocks", "analysis"])
def get_latest_stock_prices(limit: int = 20) -> Dict[str, Any]:
    """
    Get the latest stock prices for all S&P 500 companies with company details.
    """
    sql = f"""
        SELECT s.ticker, s.close as latest_price, s.volume as latest_volume, s.date as latest_date,
               w.security, w.gics_sector, w.headquarters_loc
        FROM sp500_stooq_ohcl s
        JOIN sp500_wik_list w ON s.ticker = w.symbol
        WHERE s.date = (
            SELECT MAX(date) FROM sp500_stooq_ohcl s2 WHERE s2.ticker = s.ticker
        )
        ORDER BY s.close DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "latest_stock_prices",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get latest stock prices: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stocks", "performance"])
def get_stock_performance_analysis(period_days: int = 30, limit: int = 20) -> Dict[str, Any]:
    """
    Get stock performance analysis over specified period with percentage changes.
    """
    sql = f"""
        SELECT 
            s1.ticker,
            s1.close as current_price,
            s2.close as price_period_ago,
            ROUND(((s1.close - s2.close) / s2.close) * 100, 2) as change_pct,
            w.security,
            w.gics_sector
        FROM sp500_stooq_ohcl s1
        JOIN sp500_stooq_ohcl s2 ON s1.ticker = s2.ticker
        JOIN sp500_wik_list w ON s1.ticker = w.symbol
        WHERE s1.date = (SELECT MAX(date) FROM sp500_stooq_ohcl WHERE ticker = s1.ticker)
        AND s2.date = (
            SELECT MAX(date) FROM sp500_stooq_ohcl 
            WHERE ticker = s1.ticker AND date <= DATE_SUB(s1.date, INTERVAL {period_days} DAY)
        )
        ORDER BY change_pct DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "stock_performance_analysis",
            "period_days": period_days,
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get stock performance analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stocks", "volume"])
def get_highest_volume_stocks(limit: int = 20) -> Dict[str, Any]:
    """
    Get stocks with highest trading volume for the latest trading day.
    """
    sql = f"""
        SELECT s.ticker, s.volume, s.close, s.date,
               w.security, w.gics_sector
        FROM sp500_stooq_ohcl s
        JOIN sp500_wik_list w ON s.ticker = w.symbol
        WHERE s.date = (SELECT MAX(date) FROM sp500_stooq_ohcl)
        ORDER BY s.volume DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "highest_volume_stocks",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get highest volume stocks: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stocks", "volatility"])
def get_stock_volatility_analysis(period_days: int = 90, min_trading_days: int = 60, limit: int = 20) -> Dict[str, Any]:
    """
    Get stock volatility analysis with price statistics over specified period.
    """
    sql = f"""
        SELECT 
            ticker,
            COUNT(*) as trading_days,
            ROUND(AVG(close), 2) as avg_price,
            ROUND(MIN(close), 2) as min_price,
            ROUND(MAX(close), 2) as max_price,
            ROUND(STDDEV(close), 2) as price_stddev,
            ROUND((MAX(close) - MIN(close)) / AVG(close) * 100, 2) as price_range_pct
        FROM sp500_stooq_ohcl
        WHERE date >= DATE_SUB((SELECT MAX(date) FROM sp500_stooq_ohcl), INTERVAL {period_days} DAY)
        GROUP BY ticker
        HAVING trading_days >= {min_trading_days}
        ORDER BY price_stddev DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "stock_volatility_analysis",
            "period_days": period_days,
            "min_trading_days": min_trading_days,
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get stock volatility analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stocks", "technical"])
def get_moving_averages_analysis(ma_short: int = 20, ma_long: int = 50, limit: int = 20) -> Dict[str, Any]:
    """
    Get moving averages analysis for stocks with current price vs moving averages.
    """
    sql = f"""
        SELECT 
            s1.ticker,
            s1.close as current_price,
            ROUND(AVG(s2.close), 2) as ma_short,
            ROUND(AVG(s3.close), 2) as ma_long,
            w.security,
            w.gics_sector
        FROM sp500_stooq_ohcl s1
        JOIN sp500_stooq_ohcl s2 ON s1.ticker = s2.ticker 
            AND s2.date BETWEEN DATE_SUB(s1.date, INTERVAL {ma_short} DAY) AND s1.date
        JOIN sp500_stooq_ohcl s3 ON s1.ticker = s3.ticker 
            AND s3.date BETWEEN DATE_SUB(s1.date, INTERVAL {ma_long} DAY) AND s1.date
        JOIN sp500_wik_list w ON s1.ticker = w.symbol
        WHERE s1.date = (SELECT MAX(date) FROM sp500_stooq_ohcl WHERE ticker = s1.ticker)
        GROUP BY s1.ticker, s1.close, w.security, w.gics_sector
        ORDER BY s1.close DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "moving_averages_analysis",
            "ma_short": ma_short,
            "ma_long": ma_long,
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get moving averages analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sector", "analysis"])
def get_sector_performance_analysis(limit: int = 20) -> Dict[str, Any]:
    """
    Get sector performance analysis with average prices and volumes.
    """
    sql = f"""
        SELECT 
            w.gics_sector,
            COUNT(DISTINCT s.ticker) as companies_count,
            ROUND(AVG(s.close), 2) as avg_sector_price,
            ROUND(SUM(s.volume), 0) as total_sector_volume,
            ROUND(AVG(s.volume), 0) as avg_volume_per_stock
        FROM sp500_stooq_ohcl s
        JOIN sp500_wik_list w ON s.ticker = w.symbol
        WHERE s.date = (SELECT MAX(date) FROM sp500_stooq_ohcl)
        GROUP BY w.gics_sector
        ORDER BY avg_sector_price DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "sector_performance_analysis",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get sector performance analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stocks", "extremes"])
def get_all_time_highs_lows(limit: int = 20) -> Dict[str, Any]:
    """
    Get all-time highs and lows for stocks with dates when they occurred.
    """
    sql = f"""
        SELECT 
            ticker,
            MAX(close) as all_time_high,
            MIN(close) as all_time_low,
            MAX(date) as latest_date,
            (SELECT date FROM sp500_stooq_ohcl s2 
             WHERE s2.ticker = s1.ticker AND s2.close = MAX(s1.close) 
             ORDER BY date DESC LIMIT 1) as high_date,
            (SELECT date FROM sp500_stooq_ohcl s3 
             WHERE s3.ticker = s1.ticker AND s3.close = MIN(s1.close) 
             ORDER BY date DESC LIMIT 1) as low_date
        FROM sp500_stooq_ohcl s1
        GROUP BY ticker
        ORDER BY all_time_high DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "all_time_highs_lows",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get all-time highs and lows: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "companies", "details"])
def get_company_details_with_stock(limit: int = 20) -> Dict[str, Any]:
    """
    Get comprehensive company details with latest stock information.
    """
    sql = f"""
        SELECT 
            w.symbol,
            w.security,
            w.gics_sector,
            w.gics_sub_ind,
            w.headquarters_loc,
            w.cik,
            w.founded,
            s.close as latest_price,
            s.volume as latest_volume,
            s.date as latest_trading_date
        FROM sp500_wik_list w
        LEFT JOIN (
            SELECT ticker, close, volume, date,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM sp500_stooq_ohcl
        ) s ON w.symbol = s.ticker AND s.rn = 1
        ORDER BY w.symbol
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "company_details_with_stock",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get company details with stock: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "companies", "sector"])
def get_companies_by_sector_detailed(limit: int = 20) -> Dict[str, Any]:
    """
    Get detailed breakdown of companies by sector with market statistics.
    """
    sql = f"""
        SELECT 
            w.gics_sector,
            COUNT(*) as company_count,
            GROUP_CONCAT(DISTINCT w.symbol ORDER BY w.symbol SEPARATOR ', ') as symbols,
            ROUND(AVG(s.close), 2) as avg_stock_price,
            ROUND(SUM(s.volume), 0) as total_volume
        FROM sp500_wik_list w
        LEFT JOIN (
            SELECT ticker, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM sp500_stooq_ohcl
        ) s ON w.symbol = s.ticker AND s.rn = 1
        GROUP BY w.gics_sector
        ORDER BY company_count DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "companies_by_sector_detailed",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies by sector detailed: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "companies", "location"])
def get_companies_by_location_detailed(limit: int = 20) -> Dict[str, Any]:
    """
    Get detailed breakdown of companies by headquarters location.
    """
    sql = f"""
        SELECT 
            w.headquarters_loc,
            COUNT(*) as company_count,
            GROUP_CONCAT(DISTINCT w.symbol ORDER BY w.symbol SEPARATOR ', ') as symbols,
            w.gics_sector
        FROM sp500_wik_list w
        GROUP BY w.headquarters_loc, w.gics_sector
        ORDER BY company_count DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "companies_by_location_detailed",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies by location detailed: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "companies", "newest"])
def get_newest_sp500_companies(limit: int = 20) -> Dict[str, Any]:
    """
    Get the newest companies added to the S&P 500 with current stock prices.
    """
    sql = f"""
        SELECT 
            w.symbol,
            w.security,
            w.date_added,
            w.gics_sector,
            w.headquarters_loc,
            s.close as current_price
        FROM sp500_wik_list w
        LEFT JOIN (
            SELECT ticker, close,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM sp500_stooq_ohcl
        ) s ON w.symbol = s.ticker AND s.rn = 1
        ORDER BY w.date_added DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "newest_sp500_companies",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get newest S&P 500 companies: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stocks", "highest_priced"])
def get_highest_priced_stocks(limit: int = 20) -> Dict[str, Any]:
    """
    Get stocks with the highest current prices.
    """
    sql = f"""
        SELECT 
            w.symbol,
            w.security,
            w.gics_sector,
            s.close as current_price,
            s.volume as current_volume,
            s.date as latest_date
        FROM sp500_wik_list w
        JOIN (
            SELECT ticker, close, volume, date,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM sp500_stooq_ohcl
        ) s ON w.symbol = s.ticker AND s.rn = 1
        ORDER BY s.close DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "highest_priced_stocks",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get highest priced stocks: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "revenue"])
def get_latest_revenue_data(limit: int = 20) -> Dict[str, Any]:
    """
    Get the latest revenue data for S&P 500 companies from SEC filings.
    """
    sql = f"""
        SELECT 
            bf.cik,
            w.symbol,
            w.security,
            bf.tag,
            bf.val as revenue_value,
            bf.unit,
            bf.fy as fiscal_year,
            bf.fp as fiscal_period,
            bf.filed as filing_date,
            bf.form as form_type
        FROM bronze_sec_facts bf
        JOIN sp500_wik_list w ON bf.cik = w.cik
        WHERE bf.tag LIKE '%Revenue%' 
        AND bf.taxonomy = 'us-gaap'
        AND bf.fy >= 2020
        ORDER BY bf.filed DESC, bf.val DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "latest_revenue_data",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get latest revenue data: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "assets"])
def get_company_assets_analysis(limit: int = 20) -> Dict[str, Any]:
    """
    Get company assets analysis from SEC filings.
    """
    sql = f"""
        SELECT 
            bf.cik,
            w.symbol,
            w.security,
            bf.tag,
            bf.val as asset_value,
            bf.unit,
            bf.fy as fiscal_year,
            bf.fp as fiscal_period,
            bf.filed as filing_date
        FROM bronze_sec_facts bf
        JOIN sp500_wik_list w ON bf.cik = w.cik
        WHERE bf.tag LIKE '%Assets%' 
        AND bf.taxonomy = 'us-gaap'
        AND bf.fy >= 2020
        ORDER BY bf.val DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "company_assets_analysis",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get company assets analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "profitability"])
def get_profitability_metrics(limit: int = 20) -> Dict[str, Any]:
    """
    Get profitability metrics (Net Income, Operating Income, Gross Profit) from SEC filings.
    """
    sql = f"""
        SELECT 
            bf.cik,
            w.symbol,
            w.security,
            bf.tag,
            bf.val as metric_value,
            bf.unit,
            bf.fy as fiscal_year,
            bf.fp as fiscal_period,
            bf.filed as filing_date
        FROM bronze_sec_facts bf
        JOIN sp500_wik_list w ON bf.cik = w.cik
        WHERE bf.tag IN ('NetIncomeLoss', 'OperatingIncomeLoss', 'GrossProfit')
        AND bf.taxonomy = 'us-gaap'
        AND bf.fy >= 2020
        ORDER BY bf.filed DESC, bf.val DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "profitability_metrics",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get profitability metrics: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "cash_flow"])
def get_cash_flow_analysis(limit: int = 20) -> Dict[str, Any]:
    """
    Get cash flow analysis from SEC filings.
    """
    sql = f"""
        SELECT 
            bf.cik,
            w.symbol,
            w.security,
            bf.tag,
            bf.val as cash_flow_value,
            bf.unit,
            bf.fy as fiscal_year,
            bf.fp as fiscal_period,
            bf.filed as filing_date
        FROM bronze_sec_facts bf
        JOIN sp500_wik_list w ON bf.cik = w.cik
        WHERE bf.tag LIKE '%CashFlow%'
        AND bf.taxonomy = 'us-gaap'
        AND bf.fy >= 2020
        ORDER BY bf.filed DESC, ABS(bf.val) DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "cash_flow_analysis",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get cash flow analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sec", "debt_equity"])
def get_debt_equity_analysis(limit: int = 20) -> Dict[str, Any]:
    """
    Get debt and equity analysis from SEC filings.
    """
    sql = f"""
        SELECT 
            bf.cik,
            w.symbol,
            w.security,
            bf.tag,
            bf.val as debt_equity_value,
            bf.unit,
            bf.fy as fiscal_year,
            bf.fp as fiscal_period,
            bf.filed as filing_date
        FROM bronze_sec_facts bf
        JOIN sp500_wik_list w ON bf.cik = w.cik
        WHERE (bf.tag LIKE '%Debt%' OR bf.tag LIKE '%Equity%')
        AND bf.taxonomy = 'us-gaap'
        AND bf.fy >= 2020
        ORDER BY bf.filed DESC, bf.val DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "debt_equity_analysis",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get debt equity analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "news", "latest"])
def get_latest_news_by_company(limit: int = 20) -> Dict[str, Any]:
    """
    Get the latest news for S&P 500 companies.
    """
    sql = f"""
        SELECT 
            n.symbol,
            w.security,
            n.headline,
            n.summary,
            n.source,
            n.datetime,
            n.url,
            n.category
        FROM sp500_finnhub_news n
        JOIN sp500_wik_list w ON n.symbol = w.symbol
        WHERE n.datetime >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY n.datetime DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "latest_news_by_company",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get latest news by company: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "news", "sector"])
def get_news_by_sector_analysis(limit: int = 20) -> Dict[str, Any]:
    """
    Get news analysis by sector with company coverage.
    """
    sql = f"""
        SELECT 
            w.gics_sector,
            COUNT(*) as news_count,
            GROUP_CONCAT(DISTINCT n.symbol ORDER BY n.symbol SEPARATOR ', ') as companies_with_news,
            MAX(n.datetime) as latest_news_time
        FROM sp500_finnhub_news n
        JOIN sp500_wik_list w ON n.symbol = w.symbol
        WHERE n.datetime >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        GROUP BY w.gics_sector
        ORDER BY news_count DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "news_by_sector_analysis",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get news by sector analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "news", "sources"])
def get_most_active_news_sources(limit: int = 20) -> Dict[str, Any]:
    """
    Get the most active news sources covering S&P 500 companies.
    """
    sql = f"""
        SELECT 
            n.source,
            COUNT(*) as article_count,
            COUNT(DISTINCT n.symbol) as companies_covered,
            MAX(n.datetime) as latest_article
        FROM sp500_finnhub_news n
        WHERE n.datetime >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        GROUP BY n.source
        ORDER BY article_count DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "most_active_news_sources",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get most active news sources: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "changes"])
def get_recent_sp500_changes(limit: int = 20) -> Dict[str, Any]:
    """
    Get recent S&P 500 component changes (additions and removals).
    """
    sql = f"""
        SELECT 
            effective_date,
            added_ticker,
            added_security,
            removed_ticker,
            removed_security,
            reason
        FROM selected_changes_sp500
        WHERE effective_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
        ORDER BY effective_date DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "recent_sp500_changes",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get recent S&P 500 changes: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "additions"])
def get_companies_added_to_sp500(limit: int = 20) -> Dict[str, Any]:
    """
    Get companies recently added to the S&P 500 with details.
    """
    sql = f"""
        SELECT 
            effective_date,
            added_ticker,
            added_security,
            reason,
            w.gics_sector,
            w.headquarters_loc
        FROM selected_changes_sp500 sc
        LEFT JOIN sp500_wik_list w ON sc.added_ticker = w.symbol
        WHERE added_ticker IS NOT NULL
        AND effective_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
        ORDER BY effective_date DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "companies_added_to_sp500",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies added to S&P 500: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sp500", "removals"])
def get_companies_removed_from_sp500(limit: int = 20) -> Dict[str, Any]:
    """
    Get companies recently removed from the S&P 500 with details.
    """
    sql = f"""
        SELECT 
            effective_date,
            removed_ticker,
            removed_security,
            reason,
            w.gics_sector,
            w.headquarters_loc
        FROM selected_changes_sp500 sc
        LEFT JOIN sp500_wik_list w ON sc.removed_ticker = w.symbol
        WHERE removed_ticker IS NOT NULL
        AND effective_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
        ORDER BY effective_date DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "companies_removed_from_sp500",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get companies removed from S&P 500: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "stocks", "correlation"])
def get_stock_correlation_analysis(limit: int = 20) -> Dict[str, Any]:
    """
    Get stock correlation analysis showing similar performing stocks.
    """
    sql = f"""
        WITH stock_returns AS (
            SELECT 
                ticker,
                date,
                close,
                LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close,
                (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) / 
                LAG(close) OVER (PARTITION BY ticker ORDER BY date) as daily_return
            FROM sp500_stooq_ohcl
            WHERE date >= DATE_SUB((SELECT MAX(date) FROM sp500_stooq_ohcl), INTERVAL 90 DAY)
        )
        SELECT 
            s1.ticker as ticker1,
            s2.ticker as ticker2,
            COUNT(*) as common_days,
            ROUND(AVG(s1.daily_return), 4) as avg_return_1,
            ROUND(AVG(s2.daily_return), 4) as avg_return_2,
            ROUND(STDDEV(s1.daily_return), 4) as stddev_1,
            ROUND(STDDEV(s2.daily_return), 4) as stddev_2
        FROM stock_returns s1
        JOIN stock_returns s2 ON s1.date = s2.date AND s1.ticker < s2.ticker
        WHERE s1.daily_return IS NOT NULL AND s2.daily_return IS NOT NULL
        GROUP BY s1.ticker, s2.ticker
        HAVING COUNT(*) >= 60
        ORDER BY ABS(AVG(s1.daily_return) - AVG(s2.daily_return)) ASC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "stock_correlation_analysis",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get stock correlation analysis: {str(e)}", "sql": sql}

@register_tool(tags=["financial", "sector", "rotation"])
def get_sector_rotation_analysis(limit: int = 20) -> Dict[str, Any]:
    """
    Get sector rotation analysis showing sector performance trends.
    """
    sql = f"""
        WITH sector_performance AS (
            SELECT 
                w.gics_sector,
                s.date,
                AVG(s.close) as avg_sector_price,
                AVG(s.volume) as avg_sector_volume
            FROM sp500_stooq_ohcl s
            JOIN sp500_wik_list w ON s.ticker = w.symbol
            WHERE s.date >= DATE_SUB((SELECT MAX(date) FROM sp500_stooq_ohcl), INTERVAL 90 DAY)
            GROUP BY w.gics_sector, s.date
        ),
        sector_changes AS (
            SELECT 
                gics_sector,
                date,
                avg_sector_price,
                LAG(avg_sector_price) OVER (PARTITION BY gics_sector ORDER BY date) as prev_price,
                (avg_sector_price - LAG(avg_sector_price) OVER (PARTITION BY gics_sector ORDER BY date)) / 
                LAG(avg_sector_price) OVER (PARTITION BY gics_sector ORDER BY date) as daily_change
            FROM sector_performance
        )
        SELECT 
            gics_sector,
            COUNT(*) as trading_days,
            ROUND(AVG(daily_change) * 100, 2) as avg_daily_change_pct,
            ROUND(STDDEV(daily_change) * 100, 2) as volatility_pct,
            ROUND(SUM(CASE WHEN daily_change > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) as positive_days_pct
        FROM sector_changes
        WHERE daily_change IS NOT NULL
        GROUP BY gics_sector
        ORDER BY avg_daily_change_pct DESC
        LIMIT {limit}
    """
    
    try:
        rows = run_query(sql)
        return {
            "data_type": "sector_rotation_analysis",
            "results_found": len(rows),
            "data": rows,
            "sql": sql
        }
    except Exception as e:
        return {"error": f"Failed to get sector rotation analysis: {str(e)}", "sql": sql}