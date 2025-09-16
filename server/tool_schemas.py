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
