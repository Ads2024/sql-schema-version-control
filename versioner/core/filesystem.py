
import os
import re
import hashlib
import tempfile
from typing import Optional

def sanitise_filename(name: str) -> str:
    """Sanitises a string to be safe for filenames."""
    name = re.sub(r"[^\w.-]", "_", name)
    name = re.sub(r"__+", "_", name).strip("_")
    return name or "unnamed"

def is_different(path: str, content: str) -> bool:
    """Checks if the content is different from the file at path."""
    content_bytes = content.encode("utf-8")
    if os.path.exists(path):
        with open(path, "rb") as f:
            existing_content = f.read()
        return hashlib.sha256(existing_content).hexdigest() != hashlib.sha256(content_bytes).hexdigest()
    return True

def write_if_changed(path: str, content: str) -> bool:
    """Writes content to path only if it has changed. Returns True if written."""
    if not is_different(path, content):
        return False
    
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    content_bytes = content.encode("utf-8")

    
    dirn = os.path.dirname(path) or "."
    fd, tmppath = tempfile.mkstemp(dir=dirn, prefix=".tmp_")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content_bytes)
        # atomic replace
        os.replace(tmppath, path)
        return True
    except Exception:
        if os.path.exists(tmppath):
            try:
                os.remove(tmppath)
            except Exception:
                pass
        raise
