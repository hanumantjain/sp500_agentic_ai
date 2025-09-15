import os
from typing import List
from app.agent_core import register_tool

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
