"""
Tool schemas for S&P 500 financial agent
Following the Router → Executor → Composer pattern
"""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_company_details",
            "description": "Get comprehensive details for a specific S&P 500 company by symbol",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)"
                    }
                },
                "required": ["symbol"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compare_companies",
            "description": "Compare multiple S&P 500 companies side by side",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbols": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of stock ticker symbols to compare (max 5)",
                        "maxItems": 5
                    }
                },
                "required": ["symbols"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_sp500_companies",
            "description": "Get top S&P 500 companies with optional sorting",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of companies to return (default 10, max 50)",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10
                    },
                    "sort_by": {
                        "type": "string",
                        "enum": ["symbol", "security", "gics_sector", "date_added"],
                        "description": "Sort companies by this field",
                        "default": "symbol"
                    }
                },
                "required": [],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_companies_by_sector",
            "description": "Get S&P 500 companies in a specific sector",
            "parameters": {
                "type": "object",
                "properties": {
                    "sector": {
                        "type": "string",
                        "description": "GICS sector name (e.g., Information Technology, Health Care, Financials)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of companies to return (default 10, max 50)",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10
                    }
                },
                "required": ["sector"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_companies_by_location",
            "description": "Get S&P 500 companies by headquarters location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "Headquarters location (e.g., California, New York, Texas)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of companies to return (default 10, max 50)",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10
                    }
                },
                "required": ["location"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_sector_breakdown",
            "description": "Get breakdown of S&P 500 companies by sector",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of sectors to return (default 10, max 20)",
                        "minimum": 1,
                        "maximum": 20,
                        "default": 10
                    }
                },
                "required": [],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_sp500_statistics",
            "description": "Get comprehensive statistics about the S&P 500 index",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_companies_advanced",
            "description": "Search S&P 500 companies by name, symbol, or other criteria",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (company name, symbol, or keywords)"
                    },
                    "search_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fields to search in (default: all fields)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results to return (default 10, max 20)",
                        "minimum": 1,
                        "maximum": 20,
                        "default": 10
                    }
                },
                "required": ["query"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_geographic_distribution",
            "description": "Get geographic distribution of S&P 500 companies by headquarters",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of locations to return (default 10, max 20)",
                        "minimum": 1,
                        "maximum": 20,
                        "default": 10
                    }
                },
                "required": [],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_company_relationships",
            "description": "Get companies related to a given company by sector, industry, or location",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol of the target company"
                    },
                    "relationship_type": {
                        "type": "string",
                        "enum": ["sector", "industry", "location"],
                        "description": "Type of relationship to find",
                        "default": "sector"
                    }
                },
                "required": ["symbol"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_sec_fact_timeseries",
            "description": "Time-series of a single SEC XBRL fact for a company from bronze_sec_facts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "CIK or symbol value, controlled by id_type"},
                    "id_type": {"type": "string", "enum": ["cik", "symbol"], "default": "cik"},
                    "tag": {"type": "string", "description": "XBRL tag, e.g., Revenues"},
                    "taxonomy": {"type": "string", "default": "us-gaap"},
                    "unit": {"type": "string", "description": "Unit filter (e.g., USD)"},
                    "fy_from": {"type": "integer"},
                    "fy_to": {"type": "integer"},
                    "fp": {"type": "string", "description": "FY, Q1, Q2, Q3"},
                    "frame": {"type": "string", "description": "e.g., CY2024 or CY2024Q4"},
                    "order_by": {"type": "string", "enum": ["filed", "end_date", "fy_fp"], "default": "filed"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 2000, "default": 500}
                },
                "required": ["identifier", "tag"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_latest_sec_fact",
            "description": "Most recently filed value for a company/tag (optionally unit).",
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string"},
                    "id_type": {"type": "string", "enum": ["cik", "symbol"], "default": "cik"},
                    "tag": {"type": "string"},
                    "taxonomy": {"type": "string", "default": "us-gaap"},
                    "unit": {"type": "string"}
                },
                "required": ["identifier", "tag"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_company_available_facts",
            "description": "List distinct (taxonomy, tag, unit) disclosed by a company; supports fuzzy tag filter.",
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string"},
                    "id_type": {"type": "string", "enum": ["cik", "symbol"], "default": "cik"},
                    "taxonomy": {"type": "string"},
                    "like_tag": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 2000, "default": 300}
                },
                "required": ["identifier"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_sec_fact_peers_snapshot",
            "description": "Compare same XBRL fact across multiple companies for a chosen period.",
            "parameters": {
                "type": "object",
                "properties": {
                    "identifiers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 25
                    },
                    "id_type": {"type": "string", "enum": ["cik", "symbol"], "default": "cik"},
                    "tag": {"type": "string"},
                    "taxonomy": {"type": "string", "default": "us-gaap"},
                    "unit": {"type": "string"},
                    "period_selector": {"type": "string", "enum": ["latest", "frame", "fy_fp"], "default": "latest"},
                    "frame": {"type": "string"},
                    "fy": {"type": "integer"},
                    "fp": {"type": "string"},
                    "limit_per_company": {"type": "integer", "minimum": 1, "maximum": 5, "default": 1}
                },
                "required": ["identifiers", "tag"],
                "additionalProperties": False
            },
            "strict": True
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_sec_facts_smart_search",
            "description": "Smart search for SEC facts with flexible parameters - can search by company, form type, year, quarter, or specific terms.",
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "Company symbol or CIK"},
                    "id_type": {"type": "string", "enum": ["cik", "symbol"], "default": "symbol"},
                    "search_term": {"type": "string", "description": "Search term for tags or taxonomy (e.g., 'revenue', 'assets')"},
                    "form_type": {"type": "string", "description": "SEC form type (e.g., '10-K', '10-Q', '8-K')"},
                    "year": {"type": "integer", "description": "Fiscal year"},
                    "quarter": {"type": "string", "description": "Quarter (e.g., 'Q1', 'Q2', 'Q3', 'FY')"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}
                },
                "required": ["identifier"],
                "additionalProperties": False
            },
            "strict": True
        }
    }
]

# Canonical response format helpers
def ok(data, meta=None):
    """Return successful tool response"""
    return {
        "status": "ok",
        "data": data,
        "error": None,
        "meta": meta or {}
    }

def fail(msg, code="TOOL_ERROR", meta=None):
    """Return failed tool response"""
    return {
        "status": "error",
        "data": None,
        "error": {"code": code, "message": msg},
        "meta": meta or {}
    }

# --------------------- OHLC Stock Data Tool Schemas ---------------------

OHLC_TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_stock_price_data",
            "description": "Get stock price data (OHLC) for a specific symbol with performance metrics.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock symbol (e.g., AAPL, MSFT)"},
                    "days": {"type": "integer", "minimum": 1, "default": 30, "description": "Number of days of data to retrieve (data available from 1962-2025)"},
                    "include_metrics": {"type": "boolean", "default": True, "description": "Include performance metrics calculation"}
                },
                "required": ["symbol"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_stock_price_data", "args": {"symbol": "AAPL", "days": 30}},
                {"tool": "get_stock_price_data", "args": {"symbol": "MSFT", "days": 7, "include_metrics": True}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compare_stock_prices",
            "description": "Compare stock prices across multiple symbols with performance metrics.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbols": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 10, "description": "List of stock symbols to compare"},
                    "days": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30, "description": "Period for comparison"},
                    "metric": {"type": "string", "enum": ["close", "volume", "high", "low"], "default": "close", "description": "Metric to compare"}
                },
                "required": ["symbols"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "compare_stock_prices", "args": {"symbols": ["AAPL", "MSFT", "GOOGL"]}},
                {"tool": "compare_stock_prices", "args": {"symbols": ["TSLA", "NVDA"], "days": 7, "metric": "volume"}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_performance_analysis",
            "description": "Get comprehensive stock performance analysis including volatility, moving averages, and trends.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock symbol to analyze"},
                    "days": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30, "description": "Analysis period in days"}
                },
                "required": ["symbol"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_stock_performance_analysis", "args": {"symbol": "AAPL", "days": 30}},
                {"tool": "get_stock_performance_analysis", "args": {"symbol": "TSLA", "days": 90}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_sector_stock_performance",
            "description": "Get stock performance data for all companies in a specific sector.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sector": {"type": "string", "description": "GICS sector name (e.g., Information Technology, Health Care)"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10, "description": "Maximum number of stocks to return"},
                    "sort_by": {"type": "string", "enum": ["market_cap", "price", "volume"], "default": "market_cap", "description": "Sort criteria"}
                },
                "required": ["sector"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_sector_stock_performance", "args": {"sector": "Information Technology", "limit": 10}},
                {"tool": "get_sector_stock_performance", "args": {"sector": "Health Care", "limit": 20, "sort_by": "price"}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_high_volume_stocks",
            "description": "Get stocks with highest trading volume for recent days.",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "maximum": 30, "default": 1, "description": "Number of recent days to analyze"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20, "description": "Maximum number of stocks to return"},
                    "min_volume": {"type": "integer", "minimum": 0, "description": "Minimum volume filter"}
                },
                "required": [],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_high_volume_stocks", "args": {"days": 1, "limit": 20}},
                {"tool": "get_high_volume_stocks", "args": {"days": 3, "limit": 10, "min_volume": 10000000}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_comprehensive_analysis",
            "description": "Get comprehensive analysis combining stock price data with company info and SEC data.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock symbol for comprehensive analysis"},
                    "include_sec_data": {"type": "boolean", "default": True, "description": "Include recent SEC filing data"},
                    "days": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30, "description": "Price history period"}
                },
                "required": ["symbol"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_stock_comprehensive_analysis", "args": {"symbol": "AAPL", "include_sec_data": True}},
                {"tool": "get_stock_comprehensive_analysis", "args": {"symbol": "MSFT", "days": 60, "include_sec_data": False}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_historical_analysis",
            "description": "Get historical stock analysis for a specific symbol over a multi-year period.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock symbol (e.g., AAPL, MSFT)"},
                    "start_year": {"type": "integer", "minimum": 1962, "maximum": 2025, "description": "Starting year for analysis (optional, data available from 1962-2025)"},
                    "end_year": {"type": "integer", "minimum": 1962, "maximum": 2025, "description": "Ending year for analysis (optional, data available from 1962-2025)"},
                    "analysis_type": {"type": "string", "enum": ["average", "yearly", "performance"], "default": "average", "description": "Type of analysis to perform"}
                },
                "required": ["symbol"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_stock_historical_analysis", "args": {"symbol": "AAPL", "start_year": 2012, "end_year": 2023, "analysis_type": "average"}},
                {"tool": "get_stock_historical_analysis", "args": {"symbol": "MSFT", "start_year": 2020, "end_year": 2023, "analysis_type": "yearly"}},
                {"tool": "get_stock_historical_analysis", "args": {"symbol": "TSLA", "start_year": 2015, "end_year": 2023, "analysis_type": "performance"}},
                {"tool": "get_stock_historical_analysis", "args": {"symbol": "AAPL", "analysis_type": "average"}},
                {"tool": "get_stock_historical_analysis", "args": {"symbol": "MSFT", "start_year": 2020}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_extremes",
            "description": "Get all-time high and low prices for a specific symbol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock symbol (e.g., AAPL, MSFT)"},
                    "start_year": {"type": "integer", "minimum": 1962, "maximum": 2025, "description": "Starting year for analysis (optional, data available from 1962-2025)"},
                    "end_year": {"type": "integer", "minimum": 1962, "maximum": 2025, "description": "Ending year for analysis (optional, data available from 1962-2025)"}
                },
                "required": ["symbol"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_stock_extremes", "args": {"symbol": "AAPL"}},
                {"tool": "get_stock_extremes", "args": {"symbol": "MSFT", "start_year": 2020}},
                {"tool": "get_stock_extremes", "args": {"symbol": "TSLA", "start_year": 2015, "end_year": 2023}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_company_news",
            "description": "Get recent news articles for a specific company symbol with sources and URLs for citations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock symbol (e.g., AAPL, MSFT)"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5, "description": "Number of news articles to retrieve"},
                    "days_back": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30, "description": "Number of days back to search for news"}
                },
                "required": ["symbol"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_company_news", "args": {"symbol": "AAPL", "limit": 5}},
                {"tool": "get_company_news", "args": {"symbol": "MSFT", "limit": 3, "days_back": 7}},
                {"tool": "get_company_news", "args": {"symbol": "TSLA", "days_back": 14}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_news_by_keywords",
            "description": "Search news articles by keywords in headline or summary with sources and URLs for citations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "keywords": {"type": "string", "description": "Keywords to search for in news headlines and summaries"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5, "description": "Number of news articles to retrieve"},
                    "days_back": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30, "description": "Number of days back to search for news"}
                },
                "required": ["keywords"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "search_news_by_keywords", "args": {"keywords": "Apple earnings", "limit": 5}},
                {"tool": "search_news_by_keywords", "args": {"keywords": "Tesla stock split", "days_back": 14}},
                {"tool": "search_news_by_keywords", "args": {"keywords": "Microsoft AI", "limit": 3, "days_back": 7}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_market_news",
            "description": "Get recent market-wide news articles across all S&P 500 companies with sources and URLs for citations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10, "description": "Number of news articles to retrieve"},
                    "days_back": {"type": "integer", "minimum": 1, "maximum": 365, "default": 7, "description": "Number of days back to search for news"}
                },
                "required": [],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_market_news", "args": {"limit": 10}},
                {"tool": "get_market_news", "args": {"limit": 5, "days_back": 3}},
                {"tool": "get_market_news", "args": {"days_back": 14}}
            ]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_sector_news",
            "description": "Get recent news for companies in a specific sector with sources and URLs for citations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sector": {"type": "string", "description": "Sector name (e.g., Technology, Healthcare, Financials)"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5, "description": "Number of news articles to retrieve"},
                    "days_back": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30, "description": "Number of days back to search for news"}
                },
                "required": ["sector"],
                "additionalProperties": False,
                "strict": True
            },
            "examples": [
                {"tool": "get_sector_news", "args": {"sector": "Technology", "limit": 5}},
                {"tool": "get_sector_news", "args": {"sector": "Healthcare", "days_back": 14}},
                {"tool": "get_sector_news", "args": {"sector": "Financials", "limit": 3, "days_back": 7}}
            ]
        }
    }
]
