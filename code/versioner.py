from inspect import trace
import pyodbc
import hashlib
from logging import exception
import faulthandler
import argparse
import os
import sys
import faulthandler
import pyodbc
import hashlib
import re
import yaml
import traceback
import tempfile
from datetime import datetime, timezone
import requests
from azure.identity import ClientSecretCredential


faulthandler.enable(file=sys.stderr, all_threads=True)

def _load_dotenv(path: str = ".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#") or "=" not in ln:
                continue
            k,v = ln.split("=", 1)
            k = k.strip()
            v= v.strip().strip("'\"")
            if k and k not in os.environ:
                os.environ[k] = v


def ensure_driver_available(driver_name: str):
    installed = pyodbc.drivers()
    if driver_name in installed:
        return driver_name
    low_installed = [d.lower() for d in installed]
    if driver_name and driver_name.lower() in low_installed:
        return installed[low_installed.index(driver_name.lower())]
    for d in installed:
        if "18" in d:
            return d
    raise RuntimeError(f" Requested ODBC driver {driver_name} not found, available drivers: {installed}")

_ = _load_dotenv()

TENANT = (
    os.environ.get("FABRIC_SP_TENANT")
    or os.environ.get("FABRIC_TENANT_ID")
    or os.environ.get("TENANT")
)

CLIENT = (
    os.environ.get("FABRIC_SP_CLIENT_ID")
    or os.environ.get("FABRIC_CLIENT_ID")
    or os.environ.get("CLIENT")
)

SECRET = (
    os.environ.get("FABRIC_SP_CLIENT_SECRET")
    or os.environ.get("FABRIC_CLIENT_SECRET")
    or os.environ.get("CLIENT_SECRET")
)

SECOND_SERVER = (
    os.environ.get("SECOND_SERVER")
    or os.environ.get("FABRIC_SECOND_SERVER")
    or os.environ.get("SECONDARY_SERVER")
)
SOURCE_FOLDER = "src"
VIEW_TYPE = {"V"}
PROC_TYPE = {"P"}
ALLOWED_TYPES = VIEW_TYPE | PROC_TYPE
QUERY = r"""
    SELECT
           DB_NAME() AS DatabaseName,
           SCHEMA_NAME(o.schema_id) AS SchemaName,
           o.name AS ObjectName,
           o.type AS ObjectType,
           COALESCE(m.definition, OBJECT_DEFINITION(o.object_id)) AS ObjectDefinition,
           OBJECTPROPERTY(o.object_id, 'IsEncrypted') AS IsEncrypted,
           o.modify_date AS ModifiedDate
    FROM sys.objects o
    LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
    WHERE o.type IN ('V', 'P')
    ORDER BY SchemaName, ObjectName
"""


def sanitise_filename(name: str):
    name = re.sub(r"[^\w.-]", "_", name)
    name = re.sub(r"__+", "_", name).strip("_")
    return name or "unnamed"


def _parse_datetime_to_utc(value):
    if value is None:
        raise ValueError("No date value")
    if isinstance(value,datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    s = str(value).strip()

    try:

        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError):
        pass
    

    iso_pattern = r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?([+-]\d{2}:\d{2}|Z)$"
    match = re.match(iso_pattern, s)
    if match:
        year, month, day, hour, minute, second, microsecond_str, tzinfo = match.groups()
        
        microsecond = int(microsecond_str.ljust(6, '0')[:6] ) if microsecond_str else 0

        if tz_str == 'Z' or tz_str == '+00:00':
            tz = timezone.utc
        elif tz_str:
            sign = 1 if tz_str[0] == '+' else -1
            tz_hours, tz_mins = map(int, tz_str[1:].split(':'))
            tz = timezone(timedelta(hours=sign * tz_hours, minutes=sign * tz_mins))
        else:
            tz = timezone.utc
        dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), microsecond, tz)
        return dt.astimezone(timezone.utc)


    fmts = ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]
    for f in fmts:
        try:
            dt = datetime.strptime(s,f)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue 
    raise ValueError(f"Unrecognised date format: {value}")



def read_last_run(path: str = "last_run.yaml", key: str = "Fabric"):
    if not os.path.exists(path):
        return datetime(1940, 1, 1, tzinfo=timezone.utc)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        v = data.get(key)
        if not v:
            return datetime(1940, 1, 1, tzinfo=timezone.utc)
        return _parse_datetime_to_utc(v)
    except Exception as e:
        print(f"WARNING: Failed to read last run date from {path}: {e}. Using default date 1940-01-01")
        traceback.print_exc()
        return datetime(1940, 1, 1, tzinfo=timezone.utc)    


def write_last_run(path: str = "last_run.yaml", key: str = "Fabric", dt: datetime = None):
    if dt is None:
        dt = datetime.now(timezone.utc)
    data = {}

    if os.path.exists(path):
        try:
            with open(path, "r", encoding = "utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            data = {}
    data[key] = dt.astimezone(timezone.utc).isoformat()
    dirn = os.path.dirname(path) or "."
    fd, tmppath = tempfile.mkstemp(dir=dirn, prefix=".last_run_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f)
        os.replace(tmppath,path)
    finally:
        if os.path.exists(tmppath):
            try:
                os.remove(tmppath)
            except Exception:
                pass

def bracket_ident(name: str):
    return "[" + name.replace("]", "]]") + "]"



def write_if_changed(path: str, content: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok = True)
    content_bytes = content.encode("utf-8")
    if os.path.exists(path):
        with open(path, "rb") as f:
            existing_content = f.read() 
        if hashlib.sha256(existing_content).hexdigest() == hashlib.sha256(content_bytes).hexdigest():
            return
    with open(path, "wb") as f:
        f.write(content_bytes)  
    return True

def is_different(path: str, content: str):
    content_bytes = content.encode("utf-8")
    if os.path.exists(path):
        with open(path, "rb") as f:
            existing_content = f.read() 
        return hashlib.sha256(existing_content).hexdigest() != hashlib.sha256(content_bytes).hexdigest() 


def replace_db_in_conn(conn: str, dbname: str):
    out = re.sub(r'(?i)\b(database|initial catalog)\s*=\s*[^;]+;?', '',conn).strip()
    if out and not out.endswith(";"):
        out = out + ";"
    out = out + f"DATABASE={dbname};"
    return out


def list_databases(conn_str: str, credential: "ClientSecretCredential" = None, verbose: bool = False):
    master_conn = replace_db_in_conn(conn_str, "master")
    dbs = []

    if verbose:
        masked = re.sub(r'(?i)\b(pwd|password)=[^;]+',r'\1=***', master_conn)
        print(f"DEBUG: Listing databases using connection string: {masked}")

    try:
        if credential:
            token = credential.get_token("https://database.windows.net/.default").token
            token_bytes = token.encode("utf-8")
            with pyodbc.connect(master_conn, autocommit=True, attrs_before={1256: token_bytes}) as conn:
                cur = cn.cursor()
                cur.execute(r"""
                    SELECT name
                    FROM sys.databases
                    WHERE database_id > 4 --- skips system databases 
                        AND state = 0 --- skips offline databases
                    ORDER BY name;
                """)
                rows = cur.fetchall()
        else:
            with pyodbc.connect(master_conn, autocommit=True) as conn:
                cur = cn.cursor()
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
                dbs.append(row[0])
        if verbose:
            print(f"DEBUG: Found {len(dbs)} databases")
    except Exception as e:
        if verbose:
            traceback.print_exc()
    if verbose:
        print(f"DEBUG: Found {len(dbs)} databases: {', '.join(dbs)}")
    return dbs


def main():
    parser = argparse.ArgumentParser(description="Extract SQL Server views and stored procedures to files.")
    parser.add_argument("--type", choices=["Fabric", "OnPrem"], help="Type of Object source.")
    parser.add_argument("--conn", help="ODBC connection string to the SQL Server database. If omitted, reads from SQL_CONN environment variable.")
    parser.add_argument("--ad-interactive", action="store_true", help="Use Azure/AD interactive authentication. When set and --conn is not provided, a connection string will be built from --server/--database.")
    parser.add_argument("--server", help="SQL Server host (server[:port]) when using --ad-interactive.")
    parser.add_argument("--database", help="Database name when using --ad-interactive (optional; ignored when using multi-db mode).")
    parser.add_argument("--driver", default="ODBC Driver 17 for SQL Server", help="ODBC driver to use when building connection string for AD interactive (default: %(default)s).")
    parser.add_argument("--repo-root", default=".", help="Root directory to store the extracted SQL files. Defaults to current directory.")
    parser.add_argument("--include-drop", action="store_true", help="Include DROP statements in the SQL files.")
    parser.add_argument("--header", action="store_true", help="Include header comments in the SQL files.")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without writing files.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose diagnostics for troubleshooting.")
    parser.add_argument("--all-databases", action="store_true", help="Iterate all user databases on the server (overrides --database).")
    parser.add_argument("--databases", help="Comma-separated list of databases to process (overrides --all-databases).")
    parser.add_argument("--databases-file", help="Path to a file with one database name per line to process (overrides --all-databases).")
    parser.add_argument("--export-env", action="store_true", help="Update the .env SQL_CONN entry for each database as it's processed (overwrites .env). Use with caution.")
    # Service Principal / SP credentials (preferred via environment/.env)
    parser.add_argument("--sp-tenant", help="Azure Tenant ID for Service Principal authentication (overrides env).")
    parser.add_argument("--sp-client-id", help="Service Principal Client ID (overrides env).")
    parser.add_argument("--sp-client-secret", help="Service Principal Client Secret (overrides env).")
    parser.add_argument("--sp-fallback", action="store_true", help="If ODBC Driver 18 is not available, attempt legacy SP auth by embedding UID/PWD in the connection string (may fail on Driver 17).")
    parser.add_argument("--include-second-server", action="store_true", help="Process a second server using SECOND_SERVER environment variable (uses same SP credentials).")
    args = parser.parse_args()

    # Allow connection string to be provided via environment variable for security
    conn_str = args.conn or os.environ.get("SQL_CONN")

    # Support Active Directory Interactive authentication: if requested and no conn provided,
    # build a connection string using the provided server/database and driver.
    if not conn_str and args.ad_interactive:
        if not args.server:
            parser.error("--server is required when using --ad-interactive and --conn/SQL_CONN is not provided.")
        database_part = f"DATABASE={args.database};" if args.database else ""
        conn_str = (
            f"DRIVER={{{args.driver}}};SERVER={args.server};{database_part}"
            "Authentication=ActiveDirectoryInteractive;Encrypt=yes;TrustServerCertificate=no;"
        )

    # Allow overriding SP creds via CLI args (they are primarily expected from environment/.env)
    sp_tenant = args.sp_tenant or TENANT
    sp_client = args.sp_client_id or CLIENT
    sp_secret = args.sp_client_secret or CLIENT_SECRET

    # If SP credentials are available and the user did not pass an explicit --conn,
    # prefer token-based Service Principal authentication (no interactive prompt).
    token_credential = None
    if not args.conn and sp_tenant and sp_client and sp_secret:
        if not args.server:
            parser.error("--server is required when using Service Principal authentication and --conn/SQL_CONN is not provided.")
        # Decide on an ODBC driver to use. Prefer the requested driver if available.
        try:
            use_driver = ensure_driver_available(args.driver)
        except RuntimeError as err:
            # If the driver helper couldn't find a driver 18 and the user asked for fallback,
            # allow any installed driver to be used (legacy UID/PWD fallback path).
            if args.sp_fallback:
                drivers = pyodbc.drivers()
                if not drivers:
                    parser.error("No ODBC drivers are installed on this system. Install an ODBC driver for SQL Server.")
                use_driver = drivers[0]
                if args.verbose:
                    print(f"WARN: ensure_driver_available: {err}; continuing with installed driver '{use_driver}' because --sp-fallback was specified")
            else:
                parser.error(str(err))

        # If we will attempt token flow, build a connection string without Authentication/UID/PWD; we'll supply an access token
        if not args.sp_fallback:
            database_part = f"DATABASE={args.database};" if args.database else ""
            conn_str = (
                f"DRIVER={{{use_driver}}};SERVER={args.server};{database_part}Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            # Prepare the credential for token acquisition
            try:
                token_credential = ClientSecretCredential(sp_tenant, sp_client, sp_secret)
            except Exception as exc:
                parser.error(f"Failed to create ClientSecretCredential: {exc}")
        else:
            # Fallback: attempt legacy SP auth by embedding UID/PWD in conn string (may or may not work with older drivers)
            database_part = f"DATABASE={args.database};" if args.database else ""
            conn_str = (
                f"DRIVER={{{use_driver}}};SERVER={args.server};{database_part}"
                f"UID={sp_client};PWD={sp_secret};Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;TrustServerCertificate=no;"
            )
            if args.verbose:
                print("WARN: using SP legacy fallback (UID/PWD in conn). This may fail on older drivers; prefer Driver 18 and token-based auth.")

    if not conn_str:
        parser.error("Connection string must be provided via --conn or SQL_CONN environment variable, or use --ad-interactive with --server.")

    # determine databases to process (priority: --databases-file, --databases, --all-databases, --database, conn_str)
    db_list = []
    if args.databases_file:
        if not os.path.exists(args.databases_file):
            parser.error(f"Databases file not found: {args.databases_file}")
        with open(args.databases_file, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if ln:
                    db_list.append(ln)
    elif args.databases:
        db_list = [d.strip() for d in args.databases.split(",") if d.strip()]
    elif args.all_databases:
        db_list = list_databases(conn_str, credential=token_credential, verbose=args.verbose)
        if not db_list:
            print("No user databases found to process via sys.databases. You can supply a list with --databases or --databases-file.")
            return
    else:
        if args.database:
            db_list = [args.database]
        else:
            # if user didn't specify a database and not using all-databases, try to infer from conn_str
            m = re.search(r'(?i)\b(database|initial catalog)\s*=\s*([^;]+)', conn_str)
            if m:
                db_list = [m.group(2)]
            else:
                parser.error("No database specified. Use --database, include DATABASE= in --conn / SQL_CONN, use --databases or --databases-file, or use --all-databases.")

    # Read last run for Fabric and initialize max-seen tracker
    # Determine which last_run key to use (default to Fabric)
    run_key = args.type or "Fabric"
    # Read last run for the selected type and initialize max-seen tracker
    last_run_dt = read_last_run(key=run_key)
    if args.verbose:
        print(f"DEBUG: {run_key} last_run = {last_run_dt.isoformat()}")
    max_seen = last_run_dt

    total_changed = 0
    total_skipped = 0

    # Build list of servers to process
    servers_to_process = []
    
    # Extract primary server from connection string or --server argument
    if args.server:
        servers_to_process.append(args.server)
    else:
        # Try to extract from connection string
        m = re.search(r'(?i)\bSERVER\s*=\s*([^;]+)', conn_str)
        if m:
            servers_to_process.append(m.group(1))
    
    # Add second server if requested
    if args.include_second_server:
        if not SECOND_SERVER:
            parser.error("--include-second-server requires SECOND_SERVER environment variable to be set.")
        servers_to_process.append(SECOND_SERVER)
        if args.verbose:
            print(f"DEBUG: Including second server: {SECOND_SERVER}")
    
    if not servers_to_process:
        parser.error("No server specified. Use --server or include SERVER= in connection string.")

    # Process each server
    for server in servers_to_process:
        if args.verbose:
            print(f"\n{'='*60}")
            print(f"Processing server: {server}")
            print(f"{'='*60}\n")
        
        # Update connection string for current server
        current_conn_str = replace_server_in_conn(conn_str, server)
        
        # Resolve database list for this server
        server_db_list = []
        if args.databases_file:
            server_db_list = db_list  # Use the pre-loaded list from file
        elif args.databases:
            server_db_list = db_list  # Use the pre-loaded list from argument
        elif args.all_databases:
            server_db_list = list_databases(current_conn_str, credential=token_credential, verbose=args.verbose)
            if not server_db_list:
                print(f"WARN: No user databases found on server {server}. Skipping.")
                continue
        else:
            if args.database:
                server_db_list = [args.database]
            else:
                # Try to infer from conn_str
                m = re.search(r'(?i)\b(database|initial catalog)\s*=\s*([^;]+)', current_conn_str)
                if m:
                    server_db_list = [m.group(2)]
                else:
                    print(f"ERROR: No database specified for server {server}. Skipping.")
                    continue

        for db_name in server_db_list:
            if args.verbose:
                print(f"DEBUG: [Server: {server}] processing database {db_name}")

            conn_db = replace_db_in_conn(current_conn_str, db_name)
            # expose the per-db connection string in the environment for downstream tools
            os.environ["SQL_CONN"] = conn_db
            if args.export_env:
                # update .env file to reflect the current DB connection (overwrite SQL_CONN line)
                try:
                    dotenv_path = ".env"
                    if os.path.exists(dotenv_path):
                        with open(dotenv_path, "r", encoding="utf-8") as f:
                            lines = f.readlines()
                        wrote = False
                        with open(dotenv_path, "w", encoding="utf-8") as f:
                            for ln in lines:
                                if ln.strip().startswith("SQL_CONN"):
                                    # write the conn_db value quoted
                                    f.write(f"SQL_CONN = '{conn_db}'\n")
                                    wrote = True
                                else:
                                    f.write(ln)
                        if not wrote:
                            with open(dotenv_path, "a", encoding="utf-8") as f:
                                f.write(f"\nSQL_CONN = '{conn_db}'\n")
                        if args.verbose:
                            print(f"DEBUG: updated {dotenv_path} with Database={db_name}")
                    else:
                        # create new .env
                        with open(dotenv_path, "w", encoding="utf-8") as f:
                            f.write(f"SQL_CONN = '{conn_db}'\n")
                        if args.verbose:
                            print(f"DEBUG: created {dotenv_path} with Database={db_name}")
                except Exception as exc:
                    print(f"ERROR: failed to update .env: {exc}")
            # Use token-based connection if we prepared a token credential, otherwise normal connect
            if token_credential:
                try:
                    token = token_credential.get_token("https://database.windows.net/.default").token
                    token_bytes = token.encode("utf-16-le")
                    with pyodbc.connect(conn_db, autocommit=True, attrs_before={1256: token_bytes}) as cn:
                        cur = cn.cursor()
                        cur.execute(QUERY)
                        rows = cur.fetchall()
                except Exception as exc:
                    print(f"ERROR: [Server: {server}] token-based connection failed for {db_name}: {exc}")
                    if args.verbose:
                        import traceback
                        traceback.print_exc()
                    rows = []
            else:
                with pyodbc.connect(conn_db, autocommit=True) as cn:
                    cur = cn.cursor()
                    cur.execute(QUERY)
                    rows = cur.fetchall()

            if args.verbose:
                masked = re.sub(r"(?i)(pwd|password)=[^;]+", r"\1=***", conn_db)
                print(f"DEBUG: [Server: {server}] fetched {len(rows)} rows from database {db_name} using {masked}")

            if not rows:
                if args.verbose:
                    print(f"DEBUG: [Server: {server}] no rows returned for {db_name}")
                continue

            # Save per-database at the top-level under repo-root to avoid mixing
            # objects from different DBs when iterating multiple databases.
            # Layout: <repo-root>/<sanitised-db-name>/src/<type>/<ObjectType>/<Schema>/<Object>.sql
            base_dir = os.path.join(args.repo_root, SOURCE_FOLDER, args.type or "", sanitise_filename(db_name))
            changed = 0
            skipped = 0

            for row in rows:
                schema_name = row.SchemaName
                object_name = row.ObjectName
                object_type = (row.ObjectType or "").strip()
                object_definition = row.ObjectDefinition
                modified_dt = row.ModifiedDate
                # normalize/parse modified date to timezone-aware UTC
                try:
                    mod_dt = _parse_datetime_to_utc(modified_dt)
                except Exception:
                    mod_dt = None
                # update max seen modification time for this run
                if mod_dt is not None and mod_dt > max_seen:
                    max_seen = mod_dt

                if object_type not in ALLOWED_TYPES:
                    if args.verbose:
                        print(f"SKIP: {schema_name}.{object_name} - object_type '{object_type}' not in allowed types")
                    skipped += 1
                    continue
                # Skip if the object has no retrievable definition or appears encrypted
                if not object_definition:
                    if args.verbose:
                        print(f"SKIP: {schema_name}.{object_name} - no definition (NULL or empty) or encrypted")
                    skipped += 1
                    continue

                if object_type in VIEW_TYPE:
                    obj_type_str = "VIEW"
                    obj_type_code = "V"
                elif object_type in PROC_TYPE:
                    obj_type_str = "PROCEDURE"
                    obj_type_code = "P"
                else:
                    # defensive fallback
                    if args.verbose:
                        print(f"SKIP: {schema_name}.{object_name} - unknown type '{object_type}'")
                    skipped += 1
                    continue

                # Determine destination file path
                dest_dir = os.path.join(base_dir, obj_type_str, sanitise_filename(schema_name))
                dest_file = os.path.join(dest_dir, f"{sanitise_filename(object_name)}.sql")
                file_exists = os.path.exists(dest_file)

                # Skip based on last_run filter - but only if file already exists
                # If file doesn't exist, always extract it (first time addition)
                if file_exists:
                    if mod_dt is not None:
                        if mod_dt <= last_run_dt:  # Changed from < to <= to skip objects modified at or before last_run
                            if args.verbose:
                                print(f"SKIP: {schema_name}.{object_name} - modified {mod_dt.isoformat()} <= last_run {last_run_dt.isoformat()}")
                            skipped += 1
                            continue
                    else:
                        # If we don't have a modified timestamp, be conservative and skip
                        if args.verbose:
                            print(f"SKIP: {schema_name}.{object_name} - unable to determine ModifiedDate")
                        skipped += 1
                        continue
                else:
                    if args.verbose:
                        print(f"NEW: {schema_name}.{object_name} - file doesn't exist, will be added regardless of modified date")

                schema_q = bracket_ident(schema_name)
                object_q = bracket_ident(object_name)
                drop_smt = (
                    f"IF OBJECT_ID(N'{schema_q}.{object_q}', '{obj_type_code}') IS NOT NULL "
                    f"DROP {obj_type_str} {schema_q}.{object_q};\nGO\n\n"
                ) if args.include_drop else ""

                # dest_file already calculated above
                body = object_definition.replace("\r\n", "\n").rstrip() + "\n"

                parts = []
                if args.header:
                    header = f"""-- =============================================================
-- Script generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- Database: {db_name}
-- Schema: {schema_name}
-- Object: {object_name}
-- Type: {obj_type_str}
-- Modified: {modified_dt}
-- =============================================================

"""
                    parts.append(header)

                parts.append(drop_smt)
                parts.append(body)

                sql = "\n".join([p for p in parts if p])  # drop empty parts

                if args.dry_run:
                    # In dry-run mode, report what would be written and update counters
                    if is_different(dest_file, sql):
                        changed += 1
                        if args.verbose:
                            print(f"WOULD WRITE: {dest_file}")
                        else:
                            print(f"Dry run - {dest_file}:\n{sql}\n")
                    else:
                        skipped += 1
                        if args.verbose:
                            print(f"WOULD SKIP (unchanged): {dest_file}")
                    continue

                if write_if_changed(dest_file, sql):
                    changed += 1
                    if args.verbose:
                        print(f"WROTE: {dest_file}")
                else:
                    skipped += 1
                    if args.verbose:
                        print(f"SKIPPED (unchanged): {dest_file}")

            total_changed += changed
            total_skipped += skipped

            if args.verbose:
                print(f"DEBUG: [Server: {server}] {db_name} -> changed: {changed}, skipped: {skipped}")

    print(f"Total changed: {total_changed}, skipped: {total_skipped}")

    # Update last_run.yaml for Fabric using the max-seen modified datetime (or now if none newer)
    try:
        if max_seen and max_seen > last_run_dt:
            new_last = max_seen
        else:
            new_last = datetime.now(timezone.utc)
        write_last_run("last_run.yaml", run_key, new_last)
        if args.verbose:
            print(f"DEBUG: updated last_run.yaml {run_key} = {new_last.isoformat()}")
    except Exception as exc:
        print(f"ERROR: failed to update last_run.yaml: {exc}")

if __name__ == "__main__":
    main()