import json
import os
import time
import traceback
import inspect
from dataclasses import dataclass, field
from typing import List, Dict, Callable, Any, get_type_hints

from litellm import completion

tools = {}
tools_by_tag = {}

# --------------------- Tool registration ---------------------
def register_tool(tool_name=None, description=None, parameters_override=None, terminal=False, tags=None):
    def decorator(func):
        nonlocal tool_name, description
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
            return json.loads(response)
        except:
            return {"tool": "terminate", "args": {"message": response}}

# --------------------- LLM Integration ---------------------
def generate_response(prompt):
    messages = prompt["messages"]
    tools = prompt.get("tools", [])
    if not tools:
        resp = completion(model="openai/gpt-4o", messages=messages, max_tokens=1024)
        return resp.choices[0].message.content
    else:
        resp = completion(model="openai/gpt-4o", messages=messages, tools=tools, max_tokens=1024)
        tool_calls = resp.choices[0].message.tool_calls
        if tool_calls:
            tool = tool_calls[0]
            return json.dumps({"tool": tool.function.name, "args": json.loads(tool.function.arguments)})
        return resp.choices[0].message.content

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
