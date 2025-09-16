"""
Router → Executor → Composer pattern for S&P 500 financial agent
"""

import json
import os
import time
from typing import List, Dict, Any, Optional
from datetime import date, datetime
import google.generativeai as genai
from tool_schemas import TOOLS, ok, fail

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle date and datetime objects"""
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)
from tools import (
    get_company_details, compare_companies, get_top_sp500_companies,
    get_companies_by_sector, get_companies_by_location, get_sector_breakdown,
    get_sp500_statistics, search_companies_advanced, get_geographic_distribution,
    get_company_relationships, get_sec_fact_timeseries, get_latest_sec_fact,
    list_company_available_facts, get_sec_fact_peers_snapshot, get_sec_facts_smart_search
)

# Configure Gemini
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-2.0-flash-exp')

SYSTEM_PROMPT = """You are a financial advisor specializing in S&P 500 companies.

CRITICAL: You MUST respond with JSON when tools are needed. Do NOT use natural language for tool calls.

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

EXAMPLES:
User: "compare Apple, Microsoft, and Google"
Response: {"tool": "compare_companies", "args": {"symbols": ["AAPL", "MSFT", "GOOGL"]}}

User: "top 10 stocks"
Response: {"tool": "get_top_sp500_companies", "args": {"limit": 10}}

User: "technology companies"
Response: {"tool": "get_companies_by_sector", "args": {"sector": "Information Technology", "limit": 10}}

User: "S&P 500 statistics"
Response: {"tool": "get_sp500_statistics", "args": {}}

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
            
            response = model.generate_content(
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
                        elif tool_result.get("data_type") == "sec_fact_timeseries":
                            data = tool_result["data"]
                            context_parts.append(f"SEC Fact Time Series ({len(data)} rows):")
                            for r in data[:10]:
                                context_parts.append(
                                    f"• {r.get('taxonomy')}/{r.get('tag')} {r.get('unit')} "
                                    f"{r.get('fy') or ''}{r.get('fp') or ''} "
                                    f"{'['+r.get('frame')+']' if r.get('frame') else ''} "
                                    f"= {r.get('val')} (filed {r.get('filed')}, accn {r.get('accn')})"
                                )
                        
                        # SEC latest fact results
                        elif tool_result.get("data_type") == "sec_fact_latest":
                            rows = tool_result["data"]
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
                        elif tool_result.get("data_type") == "sec_available_facts":
                            data = tool_result["data"]
                            context_parts.append(f"Available SEC Facts ({len(data)} unique tag/units):")
                            for r in data[:15]:
                                context_parts.append(
                                    f"• {r.get('taxonomy')}/{r.get('tag')} [{r.get('unit')}] "
                                    f"(n={r.get('n')}, last filed {r.get('last_filed')})"
                                )
                        
                        # SEC peers snapshot results
                        elif tool_result.get("data_type") == "sec_peers_snapshot":
                            data = tool_result["data"]
                            context_parts.append(f"SEC Peers Snapshot ({len(data)} rows):")
                            for r in data[:20]:
                                context_parts.append(
                                    f"• CIK {r.get('cik')}: {r.get('taxonomy')}/{r.get('tag')} {r.get('unit')} "
                                    f"{r.get('fy') or ''}{r.get('fp') or ''} "
                                    f"{'['+r.get('frame')+']' if r.get('frame') else ''} "
                                    f"= {r.get('val')} (filed {r.get('filed')})"
                                )
                        
                        # SEC smart search results
                        elif tool_result.get("data_type") == "sec_smart_search":
                            data = tool_result["data"]
                            search_params = tool_result.get("search_params", {})
                            context_parts.append(f"SEC Smart Search Results ({len(data)} facts found):")
                            context_parts.append(f"Search: {search_params.get('identifier')} - {search_params.get('search_term', 'all facts')}")
                            for r in data[:15]:
                                context_parts.append(
                                    f"• {r.get('taxonomy')}/{r.get('tag')} {r.get('unit')} "
                                    f"{r.get('fy') or ''}{r.get('fp') or ''} "
                                    f"{'['+r.get('frame')+']' if r.get('frame') else ''} "
                                    f"= {r.get('val')} ({r.get('form')} filed {r.get('filed')})"
                                )
                    
                    sources.append("S&P 500 Database")
            
            # Create final response
            context = "\n".join(context_parts) if context_parts else "No data available"
            
            composer_prompt = f"""Based on the following data, provide a user-friendly response to: "{user_question}"

Data:
{context}

Format your response as:
1. One-line takeaway at the top
2. 3-5 bullet points with key information
3. Use plain English and include numbers/percentages
4. Mention sources briefly

Keep it concise and informative."""

            response = model.generate_content(
                composer_prompt,
                generation_config=genai.types.GenerationConfig(temperature=0.3)
            )
            
            return response.text or "I've analyzed the data but couldn't generate a response."
            
        except Exception as e:
            return f"Error composing response: {str(e)}"
    
    def run(self, user_input: str) -> str:
        """Main orchestration loop: Router → Executor → Composer"""
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_input}
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
