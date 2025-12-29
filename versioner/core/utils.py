"""
Created: Nov 4, 2025
By: Adam M.
Generalised: 2025-12-29
Objective: General utility functions (e.g. .env loading).
"""
import os

def load_dotenv(path: str = ".env") -> None:
    """Loads environment variables from a .env file."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#") or "=" not in ln:
                continue
            k, v = ln.split("=", 1)
            k = k.strip()
            v = v.strip().strip("'\"")
            if k and k not in os.environ:
                os.environ[k] = v
