
import pyodbc
import re
from typing import List, Optional
from .auth import AuthManager

def ensure_driver_available(driver_name: str) -> str:
    """Checks if the requested driver is available, or finds a suitable fallback."""
    installed = pyodbc.drivers()
    if driver_name in installed:
        return driver_name
    
    low_installed = [d.lower() for d in installed]
    if driver_name and driver_name.lower() in low_installed:
        return installed[low_installed.index(driver_name.lower())]
        
    for d in installed:
        if "18" in d:
            return d
    # If no 18, check for 17
    for d in installed:
        if "17" in d:
            return d
            
    # Raise error if explicitly requested driver not found.
    raise RuntimeError(f"Requested ODBC driver {driver_name} not found, available drivers: {installed}")

def replace_db_in_conn(conn: str, dbname: str) -> str:
    """Replaces the database/initial catalog in the connection string."""
    out = re.sub(r'(?i)\b(database|initial catalog)\s*=\s*[^;]+;?', '', conn).strip()
    if out and not out.endswith(";"):
        out = out + ";"
    out = out + f"DATABASE={dbname};"
    return out

def replace_server_in_conn(conn: str, servername: str) -> str:
    """Replaces the server in the connection string."""
    out = re.sub(r'(?i)\bserver\s*=[^;]+;?', '', conn).strip()
    if out and not out.endswith(";"):
        out = out + ";"
    out = out + f"SERVER={servername};"
    return out

def get_server_name_from_conn(conn: str) -> str:
    """Extracts server name from connection string."""
    m = re.search(r'(?i)\bserver\s*=\s*([^;]+)', conn)
    if m:
        return m.group(1).strip()
    return "unknown_server"

def build_connection_string(
    server: str,
    database: Optional[str] = None,
    driver: str = "ODBC Driver 17 for SQL Server",
    auth_interactive: bool = False,
    auth_sp: bool = False,
    sp_legacy: bool = False,
    sp_client_id: str = None,
    sp_client_secret: str = None
) -> str:
    """Builds an ODBC connection string."""
    use_driver = ensure_driver_available(driver)
    
    db_part = f"DATABASE={database};" if database else ""
    
    if auth_interactive:
        return (
            f"DRIVER={{{use_driver}}};SERVER={server};{db_part}"
            "Authentication=ActiveDirectoryInteractive;Encrypt=yes;TrustServerCertificate=no;"
        )
    
    if auth_sp and sp_legacy:
        # Legacy UID/PWD fallback
        return (
            f"DRIVER={{{use_driver}}};SERVER={server};{db_part}"
            f"UID={sp_client_id};PWD={sp_client_secret};Authentication=ActiveDirectoryServicePrincipal;"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        

    # Original code: "DRIVER={...};SERVER={...};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    base = f"DRIVER={{{use_driver}}};SERVER={server};{db_part}Encrypt=yes;TrustServerCertificate=no;"
    if not auth_interactive and not sp_legacy:
        pass
    return base

def list_databases(conn_str: str, auth_manager: AuthManager = None, verbose: bool = False) -> List[str]:
    """Lists user databases from the server."""
    master_conn = replace_db_in_conn(conn_str, "master")
    dbs = []
    
    if verbose:
        masked = re.sub(r'(?i)\b(pwd|password)=[^;]+', r'\1=***', master_conn)
        print(f"DEBUG: Listing databases using connection string: {masked}")

    try:
        connect_args = {"autocommit": True}
        if auth_manager and auth_manager.get_token_credential():
            token_bytes = auth_manager.get_access_token()
            if token_bytes:
                connect_args["attrs_before"] = {1256: token_bytes}
        
        with pyodbc.connect(master_conn, **connect_args) as conn:
            cur = conn.cursor()
            cur.execute(r"""
                SELECT name
                FROM sys.databases
                WHERE database_id > 4 --- skips system databases 
                    AND state = 0 --- skips offline databases
                ORDER BY name;
            """)
            rows = cur.fetchall()
            
        for row in rows:
            try:
                dbs.append(row.name)
            except Exception:
                dbs.append(row[0]) # Fallback for tuple access
                
    except Exception as e:
        if verbose:
            traceback.print_exc()
        print(f"WARN: Failed to list databases: {e}")
        
    if verbose:
        print(f"DEBUG: Found {len(dbs)} databases: {', '.join(dbs)}")
    return dbs
