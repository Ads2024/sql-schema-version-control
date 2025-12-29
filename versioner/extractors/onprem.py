"""
Created: Nov 4, 2025
By: Adam M.
Generalised: 2025-12-29
Objective: Extraction logic for On-Premise SQL Server.
"""
import os
import argparse
from datetime import datetime, timezone
from ..core.connection import build_connection_string, replace_server_in_conn, replace_db_in_conn, list_databases
from ..core.tracking import read_last_run, write_last_run
from .sql_objects import extract_sql_objects
from .sql_agent import extract_sql_agent_jobs

def run_onprem_extraction(args: argparse.Namespace, config: dict):
    """
    Orchestrates extraction for On-Premise SQL.
    """
    verbose = args.verbose
    dry_run = args.dry_run
    repo_root = args.repo_root
    

    servers = []
    # CLI overrides config
    if args.servers_list: 
        servers.extend(args.servers_list)
    elif config.get("environments", {}).get("onprem", {}).get("servers"):
        servers.extend(config["environments"]["onprem"]["servers"])
    
    if args.server:
        servers.append(args.server)
        
    servers = list(set(servers)) # Deduplicate
    
    if not servers:
        print("ERROR: No servers specified. Use --servers or configure config.yaml.")
        return

    
    last_run_key = "On-Prem"
    last_run_dt = read_last_run(key=last_run_key)
    max_seen = last_run_dt
    
    total_changed = 0
    total_skipped = 0
    
   
    for server in servers:
        if verbose:
            print(f"\n{'='*60}\nProcessing server: {server}\n{'='*60}\n")
            
        
        base_conn_str = f"DRIVER={{{args.driver}}};SERVER={server};Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
        if args.conn:
             base_conn_str = replace_server_in_conn(args.conn, server)

        dbs = []
        if args.all_databases:
            dbs = list_databases(base_conn_str, verbose=verbose)
        elif args.databases:
             dbs = [d.strip() for d in args.databases.split(',') if d.strip()]
        elif args.database:
             dbs = [args.database]
        else:
             pass

        
        
        do_agent_jobs = False
        if args.include_sql_agent_jobs:
            do_agent_jobs = True
        elif config.get("environments", {}).get("onprem", {}).get("extract_agent_jobs"):
            do_agent_jobs = True
            
        import pyodbc
        
        # SQL Agent Jobs
        if do_agent_jobs:
            try:
                msdb_conn_str = replace_db_in_conn(base_conn_str, "msdb")
                with pyodbc.connect(msdb_conn_str, autocommit=True) as conn:
                    c, s, m = extract_sql_agent_jobs(
                        conn=conn,
                        server_name=server,
                        base_repo_root=repo_root,
                        type_str="OnPrem",
                        last_run_dt=last_run_dt,
                        dry_run=dry_run,
                        verbose=verbose
                    )
                    total_changed += c
                    total_skipped += s
                    if m > max_seen:
                        max_seen = m
            except Exception as e:
                print(f"ERROR: [Server: {server}] Agent Job extraction failed: {e}")
                if verbose:
                    import traceback
                    traceback.print_exc()

        # SQL Objects (Views/Procs)
        if dbs:
            for db_name in dbs:
                if verbose:
                    print(f"Processing database: {db_name}")
                
                db_conn_str = replace_db_in_conn(base_conn_str, db_name)
                
                try:
                    with pyodbc.connect(db_conn_str, autocommit=True) as conn:
                        c, s, m = extract_sql_objects(
                            conn=conn,
                            server_name=server,
                            db_name=db_name,
                            base_repo_root=repo_root,
                            type_str="OnPrem",
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
                    print(f"ERROR: [Server: {server}] DB extraction failed for {db_name}: {e}")
                    if verbose:
                        import traceback
                        traceback.print_exc()

    print(f"Total changed: {total_changed}, skipped: {total_skipped}")

    # Update Last Run
    if max_seen > last_run_dt and not dry_run:
        write_last_run("last_run.yaml", last_run_key, max_seen)
        if verbose:
             print(f"Updated last_run.yaml {last_run_key} = {max_seen.isoformat()}")
