"""
Router → Executor → Composer pattern for S&P 500 financial agent
"""

import json
import os
import time
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from decimal import Decimal
import google.generativeai as genai
from tool_schemas import TOOLS, ok, fail

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle date, datetime, and Decimal objects"""
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)
from tools import (
    get_company_details, compare_companies, get_top_sp500_companies,
    get_companies_by_sector, get_companies_by_location, get_sector_breakdown,
    get_sp500_statistics, search_companies_advanced, get_geographic_distribution,
    get_company_relationships, get_sec_fact_timeseries, get_latest_sec_fact,
    list_company_available_facts, get_sec_fact_peers_snapshot, get_sec_facts_smart_search,
    get_stock_price_data, compare_stock_prices, get_stock_performance_analysis,
    get_sector_stock_performance, get_high_volume_stocks,     get_stock_comprehensive_analysis,
    get_stock_historical_analysis,
    get_stock_extremes,
    get_company_news,
    search_news_by_keywords,
    get_market_news,
    get_sector_news,
    search_docs_auto,
    # New comprehensive financial analysis tools
    get_latest_stock_prices,
    get_stock_performance_analysis,
    get_highest_volume_stocks,
    get_stock_volatility_analysis,
    get_moving_averages_analysis,
    get_sector_performance_analysis,
    get_all_time_highs_lows,
    get_company_details_with_stock,
    get_companies_by_sector_detailed,
    get_companies_by_location_detailed,
    get_newest_sp500_companies,
    get_highest_priced_stocks,
    get_latest_revenue_data,
    get_company_assets_analysis,
    get_profitability_metrics,
    get_cash_flow_analysis,
    get_debt_equity_analysis,
    get_latest_news_by_company,
    get_news_by_sector_analysis,
    get_most_active_news_sources,
    get_recent_sp500_changes,
    get_companies_added_to_sp500,
    get_companies_removed_from_sp500,
    get_stock_correlation_analysis,
    get_sector_rotation_analysis
)

# Function to get fresh Gemini model with current API key
def get_gemini_model():
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        genai.configure(api_key=gemini_key)
        return genai.GenerativeModel('gemini-1.5-flash')
    return None

# Initialize with current API key
model = get_gemini_model()

SYSTEM_PROMPT = """You are a financial advisor specializing in S&P 500 companies.

CRITICAL: You MUST respond with JSON when tools are needed. Do NOT use natural language for tool calls.

IMPORTANT: If the user mentions "uploaded document", "document", "report", or asks questions about document content, you should use the search_docs_auto tool to search through the uploaded documents.

When a user asks a question:

1. If you need to use a tool, respond with EXACTLY this JSON format:
{"tool": "tool_name", "args": {"param1": "value1"}}

2. If no tools are needed, respond with natural language.

Available tools and their parameters:
- get_company_details: {"tool": "get_company_details", "args": {"symbol": "AAPL"}}
- compare_companies: {"tool": "compare_companies", "args": {"symbols": ["AAPL", "MSFT", "GOOGL"]}}
- get_top_sp500_companies: {"tool": "get_top_sp500_companies", "args": {"limit": 10, "sort_by": "symbol"}}
- get_companies_by_sector: {"tool": "get_companies_by_sector", "args": {"sector": "Information Technology", "limit": 10}}
- get_companies_by_location: {"tool": "get_companies_by_location", "args": {"location": "California", "limit": 10}}
- get_sector_breakdown: {"tool": "get_sector_breakdown", "args": {"limit": 10}}
- get_sp500_statistics: {"tool": "get_sp500_statistics", "args": {}}
- search_companies_advanced: {"tool": "search_companies_advanced", "args": {"query": "Apple", "limit": 10}}
- get_geographic_distribution: {"tool": "get_geographic_distribution", "args": {"limit": 10}}
- get_company_relationships: {"tool": "get_company_relationships", "args": {"symbol": "AAPL", "relationship_type": "sector"}}
- get_sec_fact_timeseries: {"tool": "get_sec_fact_timeseries", "args": {"identifier": "AAPL", "id_type": "symbol", "tag": "Revenues"}}
- get_latest_sec_fact: {"tool": "get_latest_sec_fact", "args": {"identifier": "AAPL", "id_type": "symbol", "tag": "Revenues"}}
- list_company_available_facts: {"tool": "list_company_available_facts", "args": {"identifier": "AAPL", "id_type": "symbol"}}
- get_sec_fact_peers_snapshot: {"tool": "get_sec_fact_peers_snapshot", "args": {"identifiers": ["AAPL", "MSFT"], "tag": "Revenues"}}
- get_sec_facts_smart_search: {"tool": "get_sec_facts_smart_search", "args": {"identifier": "AAPL", "search_term": "revenue", "form_type": "10-K"}}
- get_stock_price_data: {"tool": "get_stock_price_data", "args": {"symbol": "AAPL", "days": 30}}
- compare_stock_prices: {"tool": "compare_stock_prices", "args": {"symbols": ["AAPL", "MSFT", "GOOGL"]}}
- get_stock_performance_analysis: {"tool": "get_stock_performance_analysis", "args": {"symbol": "AAPL", "days": 30}}
- get_sector_stock_performance: {"tool": "get_sector_stock_performance", "args": {"sector": "Information Technology", "limit": 10}}
- get_high_volume_stocks: {"tool": "get_high_volume_stocks", "args": {"days": 1, "limit": 20}}
- get_stock_comprehensive_analysis: {"tool": "get_stock_comprehensive_analysis", "args": {"symbol": "AAPL", "include_sec_data": True}}
- get_stock_historical_analysis: {"tool": "get_stock_historical_analysis", "args": {"symbol": "AAPL", "start_year": 2012, "end_year": 2023, "analysis_type": "average"}}

EXAMPLES:
User: "compare Apple, Microsoft, and Google"
Response: {"tool": "compare_companies", "args": {"symbols": ["AAPL", "MSFT", "GOOGL"]}}

User: "top 10 stocks"
Response: {"tool": "get_top_sp500_companies", "args": {"limit": 10}}

User: "technology companies"
Response: {"tool": "get_companies_by_sector", "args": {"sector": "Information Technology", "limit": 10}}

User: "S&P 500 statistics"
Response: {"tool": "get_sp500_statistics", "args": {}}

User: "Apple stock price"
Response: {"tool": "get_stock_price_data", "args": {"symbol": "AAPL", "days": 30}}

User: "compare Apple and Microsoft stock prices"
Response: {"tool": "compare_stock_prices", "args": {"symbols": ["AAPL", "MSFT"]}}

User: "high volume stocks today"
Response: {"tool": "get_high_volume_stocks", "args": {"days": 1, "limit": 20}}

User: "Apple average stock price from 2012 to 2023"
Response: {"tool": "get_stock_historical_analysis", "args": {"symbol": "AAPL", "start_year": 2012, "end_year": 2023, "analysis_type": "average"}}

User: "Apple stock price for the last 5 years"
Response: {"tool": "get_stock_price_data", "args": {"symbol": "AAPL", "days": 1825}}

User: "Microsoft historical analysis"
Response: {"tool": "get_stock_historical_analysis", "args": {"symbol": "MSFT", "analysis_type": "average"}}

User: "Apple all-time high stock price"
Response: {"tool": "get_stock_historical_analysis", "args": {"symbol": "AAPL", "analysis_type": "average"}}

User: "when was Apple stock highest priced ever"
Response: {"tool": "get_stock_extremes", "args": {"symbol": "AAPL"}}

User: "Apple all-time high and low prices"
Response: {"tool": "get_stock_extremes", "args": {"symbol": "AAPL"}}

User: "What was Tesla's highest stock price ever"
Response: {"tool": "get_stock_extremes", "args": {"symbol": "TSLA"}}

User: "Show me recent Apple news"
Response: {"tool": "get_company_news", "args": {"symbol": "AAPL", "limit": 5}}

User: "Find news about Tesla earnings"
Response: {"tool": "search_news_by_keywords", "args": {"keywords": "Tesla earnings", "limit": 3}}

User: "What's the latest market news"
Response: {"tool": "get_market_news", "args": {"limit": 10, "days_back": 7}}

User: "Show me technology sector news"
Response: {"tool": "get_sector_news", "args": {"sector": "Technology", "limit": 5}}

User: "What does the uploaded document say about revenue?"
Response: {"tool": "search_docs_auto", "args": {"question": "What does the document say about revenue?", "k": 8}}

User: "Analyze the uploaded Microsoft report"
Response: {"tool": "search_docs_auto", "args": {"question": "Analyze the Microsoft report", "k": 8}}

User: "Show me the latest stock prices"
Response: {"tool": "get_latest_stock_prices", "args": {"limit": 20}}

User: "Which stocks have the highest volume today?"
Response: {"tool": "get_highest_volume_stocks", "args": {"limit": 10}}

User: "Show me stock performance over the last 30 days"
Response: {"tool": "get_stock_performance_analysis", "args": {"period_days": 30, "limit": 20}}

User: "What are the most volatile stocks?"
Response: {"tool": "get_stock_volatility_analysis", "args": {"period_days": 90, "limit": 10}}

User: "Show me moving averages analysis"
Response: {"tool": "get_moving_averages_analysis", "args": {"ma_short": 20, "ma_long": 50, "limit": 20}}

User: "Which sectors are performing best?"
Response: {"tool": "get_sector_performance_analysis", "args": {"limit": 10}}

User: "Show me all-time highs and lows"
Response: {"tool": "get_all_time_highs_lows", "args": {"limit": 20}}

User: "What are the newest companies in S&P 500?"
Response: {"tool": "get_newest_sp500_companies", "args": {"limit": 10}}

User: "Show me highest priced stocks"
Response: {"tool": "get_highest_priced_stocks", "args": {"limit": 10}}

User: "What's the latest revenue data?"
Response: {"tool": "get_latest_revenue_data", "args": {"limit": 20}}

User: "Show me company assets analysis"
Response: {"tool": "get_company_assets_analysis", "args": {"limit": 20}}

User: "What are the profitability metrics?"
Response: {"tool": "get_profitability_metrics", "args": {"limit": 20}}

User: "Show me cash flow analysis"
Response: {"tool": "get_cash_flow_analysis", "args": {"limit": 20}}

User: "What's the debt and equity analysis?"
Response: {"tool": "get_debt_equity_analysis", "args": {"limit": 20}}

User: "Show me latest company news"
Response: {"tool": "get_latest_news_by_company", "args": {"limit": 10}}

User: "What are the most active news sources?"
Response: {"tool": "get_most_active_news_sources", "args": {"limit": 10}}

User: "Show me recent S&P 500 changes"
Response: {"tool": "get_recent_sp500_changes", "args": {"limit": 10}}

User: "Which companies were added to S&P 500?"
Response: {"tool": "get_companies_added_to_sp500", "args": {"limit": 10}}

User: "Show me stock correlation analysis"
Response: {"tool": "get_stock_correlation_analysis", "args": {"limit": 20}}

User: "What's the sector rotation analysis?"
Response: {"tool": "get_sector_rotation_analysis", "args": {"limit": 10}}

REMEMBER: Only JSON for tool calls, natural language for final answers."""

class FunctionCallingAgent:
    def __init__(self, max_steps: int = 4):
        self.max_steps = max_steps
        self.tool_functions = {
            "get_company_details": get_company_details,
            "compare_companies": compare_companies,
            "get_top_sp500_companies": get_top_sp500_companies,
            "get_companies_by_sector": get_companies_by_sector,
            "get_companies_by_location": get_companies_by_location,
            "get_sector_breakdown": get_sector_breakdown,
            "get_sp500_statistics": get_sp500_statistics,
            "search_companies_advanced": search_companies_advanced,
            "get_geographic_distribution": get_geographic_distribution,
            "get_company_relationships": get_company_relationships,
            "get_sec_fact_timeseries": get_sec_fact_timeseries,
            "get_latest_sec_fact": get_latest_sec_fact,
            "list_company_available_facts": list_company_available_facts,
            "get_sec_fact_peers_snapshot": get_sec_fact_peers_snapshot,
            "get_sec_facts_smart_search": get_sec_facts_smart_search,
            "get_stock_price_data": get_stock_price_data,
            "compare_stock_prices": compare_stock_prices,
            "get_stock_performance_analysis": get_stock_performance_analysis,
            "get_sector_stock_performance": get_sector_stock_performance,
            "get_high_volume_stocks": get_high_volume_stocks,
            "get_stock_comprehensive_analysis": get_stock_comprehensive_analysis,
            "get_stock_historical_analysis": get_stock_historical_analysis,
            "get_stock_extremes": get_stock_extremes,
            "get_company_news": get_company_news,
            "search_news_by_keywords": search_news_by_keywords,
            "get_market_news": get_market_news,
            "get_sector_news": get_sector_news,
            "search_docs_auto": search_docs_auto,
            # New comprehensive financial analysis tools
            "get_latest_stock_prices": get_latest_stock_prices,
            "get_stock_performance_analysis": get_stock_performance_analysis,
            "get_highest_volume_stocks": get_highest_volume_stocks,
            "get_stock_volatility_analysis": get_stock_volatility_analysis,
            "get_moving_averages_analysis": get_moving_averages_analysis,
            "get_sector_performance_analysis": get_sector_performance_analysis,
            "get_all_time_highs_lows": get_all_time_highs_lows,
            "get_company_details_with_stock": get_company_details_with_stock,
            "get_companies_by_sector_detailed": get_companies_by_sector_detailed,
            "get_companies_by_location_detailed": get_companies_by_location_detailed,
            "get_newest_sp500_companies": get_newest_sp500_companies,
            "get_highest_priced_stocks": get_highest_priced_stocks,
            "get_latest_revenue_data": get_latest_revenue_data,
            "get_company_assets_analysis": get_company_assets_analysis,
            "get_profitability_metrics": get_profitability_metrics,
            "get_cash_flow_analysis": get_cash_flow_analysis,
            "get_debt_equity_analysis": get_debt_equity_analysis,
            "get_latest_news_by_company": get_latest_news_by_company,
            "get_news_by_sector_analysis": get_news_by_sector_analysis,
            "get_most_active_news_sources": get_most_active_news_sources,
            "get_recent_sp500_changes": get_recent_sp500_changes,
            "get_companies_added_to_sp500": get_companies_added_to_sp500,
            "get_companies_removed_from_sp500": get_companies_removed_from_sp500,
            "get_stock_correlation_analysis": get_stock_correlation_analysis,
            "get_sector_rotation_analysis": get_sector_rotation_analysis,
        }
    
    def call_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Executor: Call the actual tool function"""
        try:
            if name not in self.tool_functions:
                return fail(f"Unknown tool: {name}", code="UNKNOWN_TOOL")
            
            # Call the tool function
            result = self.tool_functions[name](**args)
            
            # Normalize the response to canonical format
            if isinstance(result, dict):
                if "error" in result:
                    return fail(result["error"], code="TOOL_ERROR")
                else:
                    return ok(result)
            else:
                return ok(result)
                
        except TimeoutError:
            return fail("Request timed out", code="TIMEOUT")
        except Exception as e:
            return fail(str(e), code="TOOL_ERROR")
    
    def router(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """Router: LLM decides which tools to call"""
        try:
            # Convert messages to Gemini format
            prompt = SYSTEM_PROMPT + "\n\n"
            for msg in messages:
                if msg["role"] == "user":
                    prompt += f"User: {msg['content']}\n\n"
                elif msg["role"] == "assistant":
                    prompt += f"Assistant: {msg['content']}\n\n"
                elif msg["role"] == "tool":
                    prompt += f"Tool Result: {msg['content']}\n\n"
            
            prompt += "Respond with JSON for tool calls or natural language for final answers:"
            
            current_model = get_gemini_model()
            if not current_model:
                return "GEMINI_API_KEY not configured"
            
            response = current_model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(temperature=0.1)
            )
            
            content = response.text or ""
            content = content.strip()
            
            # Try to parse as JSON
            try:
                if content.startswith('{') and content.endswith('}'):
                    return json.loads(content)
                elif '{' in content and '}' in content:
                    # Extract JSON from mixed content
                    start = content.find('{')
                    end = content.rfind('}') + 1
                    return json.loads(content[start:end])
            except json.JSONDecodeError:
                pass
            
            # If not JSON, return as natural language response
            return {"type": "text", "content": content}
            
        except Exception as e:
            return {"type": "error", "content": f"Router error: {str(e)}"}
    
    def composer(self, tool_results: List[Dict[str, Any]], user_question: str) -> str:
        """Composer: Turn tool results into user-friendly response"""
        try:
            # Build context from tool results
            context_parts = []
            sources = []
            
            for result in tool_results:
                # Handle both old format (status/data) and new format (data_type)
                if result.get("status") == "ok" and result.get("data"):
                    data = result["data"]
                    if isinstance(data, dict):
                        # Format different types of data
                        if "companies" in data:
                            companies = data["companies"]
                            context_parts.append(f"Found {len(companies)} companies")
                            for i, company in enumerate(companies[:5], 1):
                                symbol = company.get('symbol', 'N/A')
                                name = company.get('security', 'N/A')
                                sector = company.get('gics_sector', 'N/A')
                                context_parts.append(f"{i}. {symbol} - {name} ({sector})")
                        
                        elif "sector_breakdown" in data:
                            breakdown = data["sector_breakdown"]
                            context_parts.append("Sector breakdown:")
                            for sector in breakdown[:5]:
                                name = sector.get('gics_sector', 'N/A')
                                count = sector.get('company_count', 0)
                                pct = sector.get('percentage', 0)
                                context_parts.append(f"• {name}: {count} companies ({pct}%)")
                        
                        elif "total_companies" in data:
                            stats = data
                            context_parts.append("S&P 500 Statistics:")
                            context_parts.append(f"• Total Companies: {stats.get('total_companies', 0)}")
                            context_parts.append(f"• Total Sectors: {stats.get('total_sectors', 0)}")
                            context_parts.append(f"• Total Sub-Industries: {stats.get('total_sub_industries', 0)}")
                        
                        elif "location_distribution" in data:
                            locations = data["location_distribution"]
                            context_parts.append("Geographic Distribution:")
                            for location in locations[:5]:
                                loc = location.get('headquarters_loc', 'N/A')
                                count = location.get('company_count', 0)
                                pct = location.get('percentage', 0)
                                context_parts.append(f"• {loc}: {count} companies ({pct}%)")
                        
                        elif "comparison_data" in data:
                            companies = data["comparison_data"]
                            context_parts.append("Company Comparison:")
                            for company in companies:
                                symbol = company.get('symbol', 'N/A')
                                name = company.get('security', 'N/A')
                                sector = company.get('gics_sector', 'N/A')
                                context_parts.append(f"• {symbol} - {name} ({sector})")
                        
                        elif "company_info" in data:
                            info = data["company_info"]
                            symbol = info.get('symbol', 'N/A')
                            name = info.get('security', 'N/A')
                            sector = info.get('gics_sector', 'N/A')
                            industry = info.get('gics_sub_ind', 'N/A')
                            location = info.get('headquarters_loc', 'N/A')
                            context_parts.append(f"Company Details for {symbol}:")
                            context_parts.append(f"• Name: {name}")
                            context_parts.append(f"• Sector: {sector}")
                            context_parts.append(f"• Industry: {industry}")
                            context_parts.append(f"• Headquarters: {location}")
                        
                        # SEC fact timeseries results
                        elif result.get("data_type") == "sec_fact_timeseries":
                            data = result["data"]
                            context_parts.append(f"SEC Fact Time Series ({len(data)} rows):")
                            for r in data[:10]:
                                context_parts.append(
                                    f"• {r.get('taxonomy')}/{r.get('tag')} {r.get('unit')} "
                                    f"{r.get('fy') or ''}{r.get('fp') or ''} "
                                    f"{'['+r.get('frame')+']' if r.get('frame') else ''} "
                                    f"= {r.get('val')} (filed {r.get('filed')}, accn {r.get('accn')})"
                                )
                        
                        # SEC latest fact results
                        elif result.get("data_type") == "sec_fact_latest":
                            rows = result["data"]
                            if rows:
                                r = rows[0]
                                context_parts.append(
                                    f"Latest {r.get('taxonomy')}/{r.get('tag')} {r.get('unit')}: "
                                    f"{r.get('val')} for {r.get('fy')}{r.get('fp') or ''} "
                                    f"({'frame ' + r.get('frame') if r.get('frame') else 'period end ' + str(r.get('end_date'))}); "
                                    f"filed {r.get('filed')} ({r.get('form')} {r.get('accn')})"
                                )
                            else:
                                context_parts.append("No recent fact found.")
                        
                        # SEC available facts results
                        elif result.get("data_type") == "sec_available_facts":
                            data = result["data"]
                            context_parts.append(f"Available SEC Facts ({len(data)} unique tag/units):")
                            for r in data[:15]:
                                context_parts.append(
                                    f"• {r.get('taxonomy')}/{r.get('tag')} [{r.get('unit')}] "
                                    f"(n={r.get('n')}, last filed {r.get('last_filed')})"
                                )
                        
                        # SEC peers snapshot results
                        elif result.get("data_type") == "sec_peers_snapshot":
                            data = result["data"]
                            context_parts.append(f"SEC Peers Snapshot ({len(data)} rows):")
                            for r in data[:20]:
                                context_parts.append(
                                    f"• CIK {r.get('cik')}: {r.get('taxonomy')}/{r.get('tag')} {r.get('unit')} "
                                    f"{r.get('fy') or ''}{r.get('fp') or ''} "
                                    f"{'['+r.get('frame')+']' if r.get('frame') else ''} "
                                    f"= {r.get('val')} (filed {r.get('filed')})"
                                )
                        
                        # SEC smart search results
                        elif result.get("data_type") == "sec_smart_search":
                            data = result["data"]
                            search_params = result.get("search_params", {})
                            context_parts.append(f"SEC Smart Search Results ({len(data)} facts found):")
                            context_parts.append(f"Search: {search_params.get('identifier')} - {search_params.get('search_term', 'all facts')}")
                            for r in data[:15]:
                                context_parts.append(
                                    f"• {r.get('taxonomy')}/{r.get('tag')} {r.get('unit')} "
                                    f"{r.get('fy') or ''}{r.get('fp') or ''} "
                                    f"{'['+r.get('frame')+']' if r.get('frame') else ''} "
                                    f"= {r.get('val')} ({r.get('form')} filed {r.get('filed')})"
                                )
                        
                        # Stock price data results
                        elif result.get("data_type") == "stock_price_data":
                            symbol = result.get("symbol", "Unknown")
                            price_data = result.get("price_data", [])
                            metrics = result.get("performance_metrics", {})
                            context_parts.append(f"Stock Price Data for {symbol} ({len(price_data)} days):")
                            if metrics:
                                context_parts.append(f"Latest Close: ${metrics.get('latest_close', 'N/A')}")
                                context_parts.append(f"Price Change: ${metrics.get('price_change', 'N/A')} ({metrics.get('price_change_pct', 'N/A')}%)")
                                context_parts.append(f"Period High: ${metrics.get('period_high', 'N/A')} | Low: ${metrics.get('period_low', 'N/A')}")
                                context_parts.append(f"Avg Volume: {metrics.get('avg_volume', 'N/A'):,.0f}")
                            for r in price_data[:5]:
                                context_parts.append(f"• {r.get('date')}: O:{r.get('open')} H:{r.get('high')} L:{r.get('low')} C:{r.get('close')} V:{r.get('volume'):,}")
                        
                        # Stock price comparison results
                        elif result.get("data_type") == "stock_price_comparison":
                            symbols = result.get("symbols", [])
                            comparison_data = result.get("comparison_data", [])
                            context_parts.append(f"Stock Price Comparison ({len(comparison_data)} stocks):")
                            for r in comparison_data:
                                context_parts.append(
                                    f"• {r.get('ticker')}: ${r.get('latest_close', 'N/A')} "
                                    f"(30d change: {r.get('change_pct', 'N/A')}%) "
                                    f"Volume: {r.get('latest_volume', 'N/A'):,}"
                                )
                        
                        # Stock performance analysis results
                        elif result.get("data_type") == "stock_performance_analysis":
                            symbol = result.get("symbol", "Unknown")
                            metrics = result.get("performance_metrics", {})
                            context_parts.append(f"Stock Performance Analysis for {symbol}:")
                            context_parts.append(f"Latest Close: ${metrics.get('latest_close', 'N/A')}")
                            context_parts.append(f"Moving Averages: MA10=${metrics.get('latest_ma_10', 'N/A')}, MA30=${metrics.get('latest_ma_30', 'N/A')}")
                            context_parts.append(f"Volatility: {metrics.get('volatility_pct', 'N/A')}%")
                            context_parts.append(f"Period Range: ${metrics.get('period_low', 'N/A')} - ${metrics.get('period_high', 'N/A')}")
                            context_parts.append(f"Average Close: ${metrics.get('avg_close', 'N/A')}")
                        
                        # Sector stock performance results
                        elif result.get("data_type") == "sector_stock_performance":
                            sector = result.get("sector", "Unknown")
                            sector_data = result.get("sector_data", [])
                            context_parts.append(f"Sector Stock Performance - {sector} ({len(sector_data)} stocks):")
                            for r in sector_data[:10]:
                                context_parts.append(
                                    f"• {r.get('symbol')} ({r.get('security', 'N/A')}): "
                                    f"${r.get('latest_close', 'N/A')} "
                                    f"Volume: {r.get('latest_volume', 'N/A'):,}"
                                )
                        
                        # High volume stocks results
                        elif result.get("data_type") == "high_volume_stocks":
                            volume_data = result.get("volume_data", [])
                            days = result.get("days_analyzed", 1)
                            context_parts.append(f"High Volume Stocks (Last {days} day{'s' if days > 1 else ''}):")
                            for r in volume_data[:10]:
                                context_parts.append(
                                    f"• {r.get('ticker')} ({r.get('security', 'N/A')}): "
                                    f"${r.get('close', 'N/A')} "
                                    f"Volume: {r.get('volume', 'N/A'):,}"
                                )
                        
                        # Comprehensive stock analysis results
                        elif result.get("data_type") == "comprehensive_stock_analysis":
                            symbol = result.get("symbol", "Unknown")
                            company_info = result.get("company_info", {})
                            price_history = result.get("price_history", [])
                            sec_facts = result.get("recent_sec_facts", [])
                            context_parts.append(f"Comprehensive Analysis for {symbol}:")
                            context_parts.append(f"Company: {company_info.get('security', 'N/A')}")
                            context_parts.append(f"Sector: {company_info.get('gics_sector', 'N/A')}")
                            context_parts.append(f"Location: {company_info.get('headquarters_loc', 'N/A')}")
                            context_parts.append(f"Latest Price: ${company_info.get('latest_close', 'N/A')}")
                            context_parts.append(f"Latest Volume: {company_info.get('latest_volume', 'N/A'):,}")
                            if sec_facts:
                                context_parts.append("Recent SEC Facts:")
                                for r in sec_facts[:3]:
                                    context_parts.append(f"• {r.get('tag', 'N/A')}: {r.get('val', 'N/A')} {r.get('unit', '')} ({r.get('fy', 'N/A')}{r.get('fp', '')})")
                        
                # Handle new format with data_type
                elif result.get("data_type"):
                    tool_result = result  # Use result directly for new format
                    
                    # Stock historical analysis results
                    if result.get("data_type") == "stock_historical_analysis":
                        symbol = result.get("symbol", "Unknown")
                        requested_start = result.get("requested_start_year", "N/A")
                        requested_end = result.get("requested_end_year", "N/A")
                        actual_start = result.get("actual_start_date", "N/A")
                        actual_end = result.get("actual_end_date", "N/A")
                        analysis_type = result.get("analysis_type", "average")
                        analysis_results = result.get("analysis_results", {})
                        total_records = result.get("total_records", 0)
                        data_availability = result.get("data_availability", {})
                        
                        context_parts.append(f"Historical Analysis for {symbol}:")
                        if requested_start != "N/A" and requested_end != "N/A":
                            context_parts.append(f"Requested Period: {requested_start}-{requested_end}")
                        context_parts.append(f"Actual Data Period: {actual_start} to {actual_end}")
                        context_parts.append(f"Analysis Type: {analysis_type.title()}")
                        context_parts.append(f"Total Trading Days: {total_records:,}")
                        if data_availability:
                            context_parts.append(f"Data Available: {data_availability.get('earliest_date', 'N/A')} to {data_availability.get('latest_date', 'N/A')}")
                        
                        if analysis_type == "average":
                            context_parts.append(f"Average Close Price: ${analysis_results.get('average_close_price', 'N/A')}")
                            context_parts.append(f"Average Volume: {analysis_results.get('average_volume', 'N/A'):,.0f}")
                            context_parts.append(f"Period High: ${analysis_results.get('period_high', 'N/A')}")
                            context_parts.append(f"Period Low: ${analysis_results.get('period_low', 'N/A')}")
                            context_parts.append(f"Price Range: ${analysis_results.get('price_range', 'N/A')}")
                            
                        elif analysis_type == "yearly":
                            context_parts.append("Yearly Averages:")
                            for year, data in list(analysis_results.items())[:5]:  # Show first 5 years
                                context_parts.append(f"• {year}: ${data.get('average_price', 'N/A')} ({data.get('trading_days', 'N/A')} days)")
                                
                        elif analysis_type == "performance":
                            context_parts.append(f"Starting Price: ${analysis_results.get('starting_price', 'N/A')}")
                            context_parts.append(f"Ending Price: ${analysis_results.get('ending_price', 'N/A')}")
                            context_parts.append(f"Total Return: {analysis_results.get('total_return_percent', 'N/A')}%")
                            context_parts.append(f"Annualized Volatility: {analysis_results.get('annualized_volatility', 'N/A')}%")
                            context_parts.append(f"Years Analyzed: {analysis_results.get('years_analyzed', 'N/A')}")
                    
                    # Stock extremes results
                    elif result.get("data_type") == "stock_extremes":
                        symbol = result.get("symbol", "Unknown")
                        all_time_high = result.get("all_time_high", {})
                        all_time_low = result.get("all_time_low", {})
                        actual_period = result.get("actual_data_period", {})
                        
                        context_parts.append(f"Stock Extremes for {symbol}:")
                        if actual_period:
                            context_parts.append(f"Data Period: {actual_period.get('earliest_date', 'N/A')} to {actual_period.get('latest_date', 'N/A')}")
                            context_parts.append(f"Total Records: {actual_period.get('total_records', 'N/A'):,}")
                        
                        if all_time_high:
                            context_parts.append(f"All-Time High: ${all_time_high.get('high_price', 'N/A')} on {all_time_high.get('date', 'N/A')}")
                            context_parts.append(f"  - Close Price: ${all_time_high.get('close_price', 'N/A')}")
                            context_parts.append(f"  - Volume: {all_time_high.get('volume', 'N/A'):,}")
                        
                        if all_time_low:
                            context_parts.append(f"All-Time Low: ${all_time_low.get('low_price', 'N/A')} on {all_time_low.get('date', 'N/A')}")
                            context_parts.append(f"  - Close Price: ${all_time_low.get('close_price', 'N/A')}")
                            context_parts.append(f"  - Volume: {all_time_low.get('volume', 'N/A'):,}")
                    
                    # Company news results
                    elif result.get("data_type") == "company_news":
                        symbol = result.get("symbol", "Unknown")
                        articles = result.get("news_articles", [])
                        records_found = result.get("records_found", 0)
                        days_back = result.get("days_back", 30)
                        
                        context_parts.append(f"Recent News for {symbol} (Last {days_back} days):")
                        context_parts.append(f"Found {records_found} news articles")
                        
                        for i, article in enumerate(articles[:3], 1):  # Show first 3 articles
                            context_parts.append(f"{i}. {article.get('headline', 'N/A')}")
                            context_parts.append(f"   Date: {article.get('datetime', 'N/A')}")
                            context_parts.append(f"   Source: {article.get('source', 'N/A')}")
                            context_parts.append(f"   Summary: {article.get('summary', 'N/A')[:100]}...")
                            context_parts.append(f"   URL: {article.get('url', 'N/A')}")
                            context_parts.append("")
                    
                    # News search results
                    elif result.get("data_type") == "news_search":
                        keywords = result.get("keywords", "Unknown")
                        articles = result.get("news_articles", [])
                        records_found = result.get("records_found", 0)
                        days_back = result.get("days_back", 30)
                        
                        context_parts.append(f"News Search Results for '{keywords}' (Last {days_back} days):")
                        context_parts.append(f"Found {records_found} relevant articles")
                        
                        for i, article in enumerate(articles[:3], 1):  # Show first 3 articles
                            context_parts.append(f"{i}. {article.get('headline', 'N/A')}")
                            context_parts.append(f"   Symbol: {article.get('symbol', 'N/A')}, Date: {article.get('datetime', 'N/A')}")
                            context_parts.append(f"   Source: {article.get('source', 'N/A')}")
                            context_parts.append(f"   Summary: {article.get('summary', 'N/A')[:100]}...")
                            context_parts.append(f"   URL: {article.get('url', 'N/A')}")
                            context_parts.append("")
                    
                    # Market news results
                    elif result.get("data_type") == "market_news":
                        articles = result.get("news_articles", [])
                        records_found = result.get("records_found", 0)
                        unique_symbols = result.get("unique_symbols", 0)
                        unique_sources = result.get("unique_sources", 0)
                        days_back = result.get("days_back", 7)
                        
                        context_parts.append(f"Latest Market News (Last {days_back} days):")
                        context_parts.append(f"Found {records_found} articles from {unique_symbols} companies and {unique_sources} sources")
                        
                        for i, article in enumerate(articles[:5], 1):  # Show first 5 articles
                            context_parts.append(f"{i}. {article.get('headline', 'N/A')}")
                            context_parts.append(f"   Symbol: {article.get('symbol', 'N/A')}, Date: {article.get('datetime', 'N/A')}")
                            context_parts.append(f"   Source: {article.get('source', 'N/A')}")
                            context_parts.append(f"   Summary: {article.get('summary', 'N/A')[:100]}...")
                            context_parts.append(f"   URL: {article.get('url', 'N/A')}")
                            context_parts.append("")
                    
                    # Sector news results
                    elif result.get("data_type") == "sector_news":
                        sector = result.get("sector", "Unknown")
                        articles = result.get("news_articles", [])
                        records_found = result.get("records_found", 0)
                        companies_in_sector = result.get("companies_in_sector", 0)
                        companies_with_news = result.get("companies_with_news", 0)
                        days_back = result.get("days_back", 30)
                        
                        context_parts.append(f"{sector} Sector News (Last {days_back} days):")
                        context_parts.append(f"Found {records_found} articles from {companies_with_news} companies (out of {companies_in_sector} total in sector)")
                        
                        for i, article in enumerate(articles[:3], 1):  # Show first 3 articles
                            context_parts.append(f"{i}. {article.get('headline', 'N/A')}")
                            context_parts.append(f"   Symbol: {article.get('symbol', 'N/A')}, Date: {article.get('datetime', 'N/A')}")
                            context_parts.append(f"   Source: {article.get('source', 'N/A')}")
                            context_parts.append(f"   Summary: {article.get('summary', 'N/A')[:100]}...")
                            context_parts.append(f"   URL: {article.get('url', 'N/A')}")
                            context_parts.append("")
                    
                    # Add other data_type handlers here as needed
                    sources.append("S&P 500 Database")
            
            # Create final response
            context = "\n".join(context_parts) if context_parts else "No data available"
            
            composer_prompt = f"""Based on the following data, provide a user-friendly response to: "{user_question}"

Data:
{context}

STRICT OUTPUT REQUIREMENTS:
- Write in clean Markdown suitable for a web app.
- Start with a bold one-line takeaway summarizing the answer.
- Follow with 3-7 concise bullet points with actual numbers/percentages.
- If numeric series are present (prices, returns, MAs, volatility), include a small Markdown table.
- Never include placeholders like [insert ...] or TODOs. Always use actual values or omit the line.
- If a metric is unavailable, omit it instead of writing N/A.
- End with a short disclaimer: "This is informational, not financial advice."

Formatting examples (adapt to the data available):
- **Key Takeaway:** Microsoft gained 12.4% over the last year with neutral momentum.
- **Performance**: 1Y total return 12.4%; 30D change 2.1%
- **Moving Averages**: MA20 512.3; MA50 505.8; MA200 472.9 (golden cross on 2025-06-12)
- **Volatility**: 20-day volatility 1.8%

Optional table example:
| Metric | Value |
|---|---|
| Latest Close | $498.41 |
| 30D High | $538.00 |
| 30D Low | $495.00 |

Keep it concise and informative."""

            current_model = get_gemini_model()
            if not current_model:
                return "GEMINI_API_KEY not configured"
            
            response = current_model.generate_content(
                composer_prompt,
                generation_config=genai.types.GenerationConfig(temperature=0.3)
            )
            
            return response.text or "I've analyzed the data but couldn't generate a response."
            
        except Exception as e:
            return f"Error composing response: {str(e)}"
    
    def run(self, user_input: str, doc_context: Optional[str] = None, scoped_doc_ids: Optional[List[str]] = None) -> str:
        """Main orchestration loop: Router → Executor → Composer"""
        # Build the user input with document context if available
        full_user_input = user_input
        if doc_context:
            full_user_input = f"Document Context:\n{doc_context}\n\nUser Question: {user_input}"
        
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": full_user_input}
        ]
        
        tool_results = []
        
        for step in range(self.max_steps):
            # Router: Decide what to do
            router_response = self.router(messages)
            
            if router_response.get("type") == "error":
                return f"Error: {router_response['content']}"



            if router_response.get("type") == "text":
                # No more tool calls needed, return the text response
                return router_response["content"]
            
            # Check if it's a tool call
            if "tool" in router_response and "args" in router_response:
                tool_name = router_response["tool"]
                tool_args = router_response["args"]
                
                # Executor: Call the tool
                tool_result = self.call_tool(tool_name, tool_args)
                tool_results.append(tool_result)
                
                # Add tool result to messages
                messages.append({
                    "role": "tool",
                    "content": json.dumps(tool_result, cls=DateTimeEncoder)
                })
                
                # Continue the loop to see if more tools are needed
                continue
            
            # If we get here, something unexpected happened
            break
        
        # Composer: Generate final response from all tool results
        if tool_results:
            return self.composer(tool_results, user_input)
        else:
            return "I couldn't process your request. Please try rephrasing your question."

# Global agent instance
agent = FunctionCallingAgent()
