import json
import os
import time
import traceback
import inspect
from dataclasses import dataclass, field
from typing import List, Dict, Callable, Any, get_type_hints

import google.generativeai as genai

tools = {}
tools_by_tag = {}

# --------------------- Tool registration ---------------------
def register_tool(tool_name=None, description=None, parameters_override=None, terminal=False, tags=None):
    def decorator(func):
        nonlocal tool_name, description, parameters_override
        tool_name = tool_name or func.__name__
        description = description or (func.__doc__ or "No description provided").strip()

        if parameters_override is None:
            signature = inspect.signature(func)
            type_hints = get_type_hints(func)
            properties = {}
            required = []
            for param_name, param in signature.parameters.items():
                if param_name in ["action_context", "action_agent"]:
                    continue
                param_type = type_hints.get(param_name, str)
                json_type = {
                    str: "string", int: "integer", float: "number",
                    bool: "boolean", list: "array", dict: "object"
                }.get(param_type, "string")
                properties[param_name] = {"type": json_type}
                if param.default == inspect.Parameter.empty:
                    required.append(param_name)
            parameters_override = {"type": "object", "properties": properties, "required": required}

        tools[tool_name] = {
            "description": description,
            "parameters": parameters_override,
            "function": func,
            "terminal": terminal,
            "tags": tags or []
        }
        for tag in tags or []:
            tools_by_tag.setdefault(tag, []).append(tool_name)

        return func
    return decorator

# --------------------- Core Classes ---------------------
@dataclass(frozen=True)
class Goal:
    priority: int
    name: str
    description: str

@dataclass
class Memory:
    items: List[Dict] = field(default_factory=list)

    def add_memory(self, memory: dict):
        self.items.append(memory)

    def get_memories(self, limit: int = None):
        return self.items[:limit]

    def copy_without_system_memories(self):
        filtered = [m for m in self.items if m["type"] != "system"]
        mem = Memory()
        mem.items = filtered
        return mem

class Environment:
    def execute_action(self, action, args):
        try:
            result = action.function(**args)
            return {"tool_executed": True, "result": result, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
        except Exception as e:
            return {"tool_executed": False, "error": str(e), "traceback": traceback.format_exc()}

class Action:
    def __init__(self, name, function, description, parameters, terminal=False):
        self.name = name
        self.function = function
        self.description = description
        self.parameters = parameters
        self.terminal = terminal

class ActionRegistry:
    def __init__(self):
        self.actions = {}

    def register(self, action: Action):
        self.actions[action.name] = action

    def get_action(self, name: str):
        return self.actions.get(name)

    def get_actions(self):
        return list(self.actions.values())

class PythonActionRegistry(ActionRegistry):
    def __init__(self, tags: List[str] = None, tool_names: List[str] = None):
        super().__init__()
        for name, desc in tools.items():
            if tool_names and name not in tool_names:
                continue
            if tags and not any(tag in desc.get("tags", []) for tag in tags):
                continue
            self.register(Action(
                name=name,
                function=desc["function"],
                description=desc["description"],
                parameters=desc.get("parameters", {}),
                terminal=desc.get("terminal", False)
            ))

# --------------------- Agent Language ---------------------
class AgentLanguage:
    def construct_prompt(self, actions, environment, goals, memory):
        raise NotImplementedError

    def parse_response(self, response: str) -> dict:
        raise NotImplementedError

class AgentFunctionCallingActionLanguage(AgentLanguage):
    def format_goals(self, goals):
        sep = "\n---\n"
        goal_text = "\n".join(f"{g.name}:{sep}{g.description}{sep}" for g in goals)
        return [{"role": "system", "content": goal_text}]

    def format_memory(self, memory):
        mapped = []
        for item in memory.get_memories():
            content = item.get("content") or json.dumps(item, indent=4)
            role = "assistant" if item["type"] in ["assistant", "environment"] else "user"
            mapped.append({"role": role, "content": content})
        return mapped

    def format_actions(self, actions):
        return [
            {"type": "function", "function": {
                "name": a.name, "description": a.description[:1024], "parameters": a.parameters
            }} for a in actions
        ]

    def construct_prompt(self, actions, environment, goals, memory):
        prompt = self.format_goals(goals) + self.format_memory(memory)
        tools = self.format_actions(actions)
        return {"messages": prompt, "tools": tools}

    def parse_response(self, response: str):
        try:
            # Try to parse as JSON first
            parsed = json.loads(response)
            if isinstance(parsed, dict) and "tool" in parsed:
                return parsed
            else:
                return {"tool": "terminate", "args": {"message": response}}
        except json.JSONDecodeError:
            # If JSON parsing fails, try to extract tool information from text
            response_lower = response.lower()
            
        # Check for common tool patterns in the response
        if "compare" in response_lower or "comparison" in response_lower:
            # Try to extract company symbols from the response
            import re
            # Look for common company names and convert to symbols
            company_mappings = {
                "APPLE": "AAPL", "MICROSOFT": "MSFT", "GOOGLE": "GOOGL", "GOOGLEL": "GOOGL",
                "AMAZON": "AMZN", "TESLA": "TSLA", "META": "META", "FACEBOOK": "META",
                "NETFLIX": "NFLX", "NVIDIA": "NVDA", "INTEL": "INTC", "IBM": "IBM",
                "ORACLE": "ORCL", "SALESFORCE": "CRM", "ADOBE": "ADBE", "CISCO": "CSCO"
            }
            
            # Extract potential company names (2-10 characters, not common words)
            words = re.findall(r'\b[A-Z]{2,10}\b', response.upper())
            exclude_words = {"AND", "THE", "OR", "IN", "OF", "TO", "FOR", "WITH", "BY", "FROM"}
            symbols = []
            
            for word in words:
                if word not in exclude_words:
                    # Check if it's a known company name
                    if word in company_mappings:
                        symbols.append(company_mappings[word])
                    elif len(word) <= 5:  # Likely a ticker symbol
                        symbols.append(word)
            
            if len(symbols) >= 2:
                return {"tool": "compare_companies", "args": {"symbols": symbols[:5]}}
            
            if "top" in response_lower and ("stock" in response_lower or "company" in response_lower):
                # Extract number if present
                import re
                numbers = re.findall(r'\d+', response)
                limit = int(numbers[0]) if numbers else 10
                return {"tool": "get_top_sp500_companies", "args": {"limit": limit}}
            
        if "sector" in response_lower or "industry" in response_lower:
            # Try to identify sector
            sector_mappings = {
                "technology": "Information Technology",
                "tech": "Information Technology", 
                "healthcare": "Health Care",
                "health": "Health Care",
                "finance": "Financials",
                "financial": "Financials",
                "energy": "Energy",
                "consumer": "Consumer Discretionary",
                "industrial": "Industrials",
                "materials": "Materials",
                "utilities": "Utilities",
                "real estate": "Real Estate",
                "communication": "Communication Services",
                "communications": "Communication Services"
            }
            
            for keyword, sector_name in sector_mappings.items():
                if keyword in response_lower:
                    return {"tool": "get_companies_by_sector", "args": {"sector": sector_name, "limit": 10}}
            
            # Default fallback
            return {"tool": "terminate", "args": {"message": response}}

# --------------------- LLM Integration ---------------------
def generate_response(prompt):
    messages = prompt["messages"]
    tools = prompt.get("tools", [])
    
    # Convert messages to Gemini format
    gemini_prompt = ""
    for msg in messages:
        if msg["role"] == "system":
            gemini_prompt += f"System: {msg['content']}\n\n"
        elif msg["role"] == "user":
            gemini_prompt += f"User: {msg['content']}\n\n"
        elif msg["role"] == "assistant":
            gemini_prompt += f"Assistant: {msg['content']}\n\n"
    
    # Configure Gemini model
    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        return "GEMINI_API_KEY not configured"
    
    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel('gemini-2.0-flash-exp')
    
    try:
        if tools:
            # Enhanced function calling prompt for Gemini
            tool_descriptions = "\n".join([f"- {tool['function']['name']}: {tool['function']['description']}" for tool in tools])
            
            # Create a more specific prompt for function calling
            function_prompt = f"""{gemini_prompt}

Available tools:
{tool_descriptions}

IMPORTANT: You must respond with ONLY a JSON object in this exact format:
{{"tool": "tool_name", "args": {{"param1": "value1", "param2": "value2"}}}}

Do not include any other text, explanations, or markdown. Just the JSON object.

Examples:
- For "compare Apple, Microsoft, Google": {{"tool": "compare_companies", "args": {{"symbols": ["AAPL", "MSFT", "GOOGL"]}}}}
- For "top 10 stocks": {{"tool": "get_top_sp500_companies", "args": {{"limit": 10}}}}
- For "technology companies": {{"tool": "get_companies_by_sector", "args": {{"sector": "Information Technology", "limit": 10}}}}

Choose the most appropriate tool and respond with JSON only:"""
            
            response = model.generate_content(function_prompt, generation_config=genai.types.GenerationConfig(temperature=0.1))
            content = response.text or ""
            
            # Clean up the response and try to extract JSON
            content = content.strip()
            
            # Try to find JSON in the response
            if content.startswith('{') and content.endswith('}'):
                return content
            elif '{' in content and '}' in content:
                # Extract JSON from mixed content
                start = content.find('{')
                end = content.rfind('}') + 1
                json_part = content[start:end]
                return json_part
            
            # If no JSON found, return a default tool call
            return '{"tool": "terminate", "args": {"message": "No valid tool call found"}}'
        else:
            response = model.generate_content(gemini_prompt, generation_config=genai.types.GenerationConfig(temperature=0.1))
            return response.text or ""
    except Exception as e:
        return f"Error generating response: {str(e)}"

# --------------------- Agent ---------------------
class Agent:
    def __init__(self, goals, agent_language, action_registry, generate_response, environment):
        self.goals = goals
        self.agent_language = agent_language
        self.actions = action_registry
        self.generate_response = generate_response
        self.environment = environment

    def get_action(self, response):
        invocation = self.agent_language.parse_response(response)
        action = self.actions.get_action(invocation["tool"])
        return action, invocation

    def should_terminate(self, response: str):
        action, _ = self.get_action(response)
        return action.terminal if action else False

    def set_current_task(self, memory, task):
        memory.add_memory({"type": "user", "content": task})

    def update_memory(self, memory, response, result):
        memory.add_memory({"type": "assistant", "content": response})
        memory.add_memory({"type": "environment", "content": json.dumps(result)})

    def run(self, user_input: str, memory=None, max_iterations=50):
        memory = memory or Memory()
        self.set_current_task(memory, user_input)
        for _ in range(max_iterations):
            prompt = self.agent_language.construct_prompt(
                actions=self.actions.get_actions(),
                environment=self.environment,
                goals=self.goals,
                memory=memory
            )
            response = self.generate_response(prompt)
            action, invocation = self.get_action(response)
            if not action:
                break
            result = self.environment.execute_action(action, invocation.get("args", {}))
            self.update_memory(memory, response, result)
            if self.should_terminate(response):
                break
        return memory
