
import os
import argparse
from datetime import datetime, timezone
import yaml
import pyodbc
from ..core.auth import AuthManager
from ..core.connection import build_connection_string, replace_server_in_conn, replace_db_in_conn, list_databases
from ..core.tracking import read_last_run, write_last_run
from .sql_objects import extract_sql_objects

def run_fabric_extraction(args: argparse.Namespace, config: dict):
    """
    Orchestrates extraction for Fabric/Azure SQL.
    """
    verbose = args.verbose
    dry_run = args.dry_run
    repo_root = args.repo_root
    
    servers = []
    if args.server:
        servers.append(args.server)
    
    if not servers and config.get("environments", {}).get("fabric", {}).get("servers"):
         servers.extend(config["environments"]["fabric"]["servers"])
         
    # Also support SECOND_SERVER env var if flag is set (legacy support)
    if args.include_second_server:
        sec = os.environ.get("SECOND_SERVER") or os.environ.get("FABRIC_SECOND_SERVER")
        if sec:
            servers.append(sec)
            
    if not servers:
        conn_str = args.conn or os.environ.get("SQL_CONN")
        if conn_str:
            from ..core.connection import get_server_name_from_conn
            s = get_server_name_from_conn(conn_str)
            if s and s != "unknown_server":
                servers.append(s)
                
    if not servers:
        print("ERROR: No server specified. Use --server, config.yaml, or include SERVER= in connection string.")
        return


    auth = AuthManager(
        tenant_id=args.sp_tenant,
        client_id=args.sp_client_id,
        client_secret=args.sp_client_secret
    )
    

    last_run_key = "Fabric"
    last_run_dt = read_last_run(key=last_run_key)
    max_seen = last_run_dt
    
    total_changed = 0
    total_skipped = 0
    

    for server in servers:
        if verbose:
            print(f"\n{'='*60}\nProcessing server: {server}\n{'='*60}\n")
            

        # Initialize base connection string from args or env
        base_conn_str = args.conn or os.environ.get("SQL_CONN")

        # Driver Selection Logic
        driver_to_use = args.driver
        from ..core.connection import ensure_driver_available
        if args.sp_fallback and not args.conn:
             # Check if requested driver works, otherwise grab first available
             try:
                 ensure_driver_available(args.driver)
             except RuntimeError:
                 import pyodbc
                 drivers = pyodbc.drivers()
                 if drivers:
                     driver_to_use = drivers[0]
                     if verbose:
                         print(f"WARN: Driver fallback enabled. Using installed driver: '{driver_to_use}'")

        # If SP fallback requested and no credentials found in conn string, rebuild
        if args.sp_fallback and auth.has_sp_credentials() and base_conn_str:
            if "UID=" not in base_conn_str.upper() and "AUTHENTICATION=" not in base_conn_str.upper():
                 if verbose:
                     print("DEBUG: SQL_CONN found but lacks credentials while --sp-fallback is requested. Rebuilding connection string with SP credentials.")
                 base_conn_str = None # Force rebuild in the next block

        if not base_conn_str:
             if args.ad_interactive:
                 base_conn_str = build_connection_string(server=server, driver=driver_to_use, auth_interactive=True)
             elif auth.has_sp_credentials():
                 base_conn_str = build_connection_string(
                     server=server, 
                     driver=driver_to_use, 
                     auth_sp=True, 
                     sp_legacy=args.sp_fallback,
                     sp_client_id=auth.client_id,
                     sp_client_secret=auth.client_secret
                 )
             else:
                 base_conn_str = build_connection_string(server=server, driver=driver_to_use)
        
        # Ensure server name in conn string matches current loop
        server_conn_str = replace_server_in_conn(base_conn_str, server)
        
        # List Databases
        dbs = []
        # Determine strict auth manager for listing: 
        # If using legacy fallback, DO NOT pass auth token manager to list_databases (avoids token injection mixing)
        list_db_auth = auth if (auth.has_sp_credentials() and not args.sp_fallback) else None
        
        if args.databases_file:
             if os.path.exists(args.databases_file):
                 with open(args.databases_file, 'r') as f:
                     dbs = [line.strip() for line in f if line.strip()]
        elif args.databases:
            dbs = [d.strip() for d in args.databases.split(',') if d.strip()]
        elif args.all_databases:
            dbs = list_databases(server_conn_str, auth_manager=list_db_auth, verbose=verbose)
        elif args.database:
            dbs = [args.database]
        else:
             # Try to infer from conn string
             import re
             m = re.search(r'(?i)\b(database|initial catalog)\s*=\s*([^;]+)', server_conn_str)
             if m:
                 dbs = [m.group(2)]
             else:
                 print(f"ERROR: No database specified for server {server}. Skipping.")
                 continue
                 
        for db_name in dbs:
            if verbose:
                print(f"Processing database: {db_name}")
                
            db_conn_str = replace_db_in_conn(server_conn_str, db_name)
            
            if args.export_env:
                os.environ["SQL_CONN"] = db_conn_str

            # Connect
            try:
                connect_args = {"autocommit": True}
                # Check if we should inject token
                # Logic: If using SP tokens (not legacy fallback)
                if auth.has_sp_credentials() and not args.sp_fallback and not args.ad_interactive:
                     token_bytes = auth.get_access_token()
                     if token_bytes:
                         connect_args["attrs_before"] = {1256: token_bytes}
                
                with pyodbc.connect(db_conn_str, **connect_args) as conn:
                    c, s, m = extract_sql_objects(
                        conn=conn,
                        server_name=server,
                        db_name=db_name,
                        base_repo_root=repo_root,
                        type_str="Fabric",
                        last_run_dt=last_run_dt,
                        include_drop=args.include_drop,
                        include_header=args.header,
                        dry_run=dry_run,
                        verbose=verbose
                    )
                    total_changed += c
                    total_skipped += s
                    if m > max_seen:
                        max_seen = m
                        
            except Exception as e:
                print(f"ERROR: [Server: {server}] Failed to process database {db_name}: {e}")
                if verbose:
                    import traceback
                    traceback.print_exc()

    print(f"Total changed: {total_changed}, skipped: {total_skipped}")
    
    # Update Last Run
    if max_seen > last_run_dt and not dry_run:
        write_last_run("last_run.yaml", last_run_key, max_seen)
        if verbose:
            print(f"Updated last_run.yaml {last_run_key} = {max_seen.isoformat()}")
