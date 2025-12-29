import pyodbc
from code.versioner import VIEW_TYPE
from code.versioner import SOURCE_FOLDER
import argparse
import os
import pyodbc
import re
import hashlib
import yaml
import tempfile
import traceback
from datetime import datetime, timezone


SOURCE_FOLDER = "src"
VIEW_TYPE = {"V"}
PROC_TYPE = {"P"}
ALLOWED_TYPES = VIEW_TYPE | PROC_TYPE
SERVER_LIST = []
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

SQL_AGENT_JOB_QUERY = r"""
  SELECT
       j.job_id,
       j.name AS JobName,
       j.enabled AS IsEnabled,
       j.description AS JobDescription,
       j.date_created AS DateCreated,
       j.date_modified AS DateModified,
       s.step_id AS StepId,
       s.step_name AS StepName,
       s.command AS Command,
       s.subsystem AS Subsystem,
       s.command AS Command,
       s.database_name AS DatabaseName,
       s.on_success_action AS OnSuccessAction,
       s.on_fail_action AS OnFailAction,
       s.retry_attempts AS RetryAttempts,
       s.retry_interval AS RetryInterval
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobsteps s ON j.job_id = s.job_id
WHERE j.enabled = 1 -- Only enabled jobs
ORDER BY j.name, s.step_id
"""

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

_load_dotenv()


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


def read_last_run(path: str = "last_run.yaml", key: str = "On-Prem"):
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


def write_last_run(path: str = "last_run.yaml", key: str = "On-Prem", dt: datetime = None):
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


def get_server_name_from_conn(conn: str):
    m = re.search(r'(?i)\bserver\s*([^;]+)', conn)
    if m:
        return m.group(1).strip()
    return "unknown_server"

def replace_db_in_conn(conn: str, dbname: str):
    out = re.sub(r'(?i)\b(database|initial catalog)\s*=\s*[^;]+;?', '',conn).strip()
    if out and not out.endswith(";"):
        out = out + ";"
    out = out + f"DATABASE={dbname};"
    return out


def replace_server_in_conn(conn:str,servername:str):
    out = re.sub(r'(?i)\bserver\s*=[^;]+;?', '',conn).strip()
    if out and not out.endswith(";"):
        out = out + ";"
    out = out + f"SERVER={servername};"
    return out


def list_databases(conn_str: str, verbose: bool = False):
    master_conn = replace_db_in_conn(conn_str, "master")
    dbs = []

    if verbose:
        masked = re.sub(r'(?i)\b(pwd|password)=[^;]+',r'\1=***', master_conn)
        print(f"DEBUG: Listing databases using connection string: {masked}")

    try:
            with pyodbc.connect(master_conn, autocommit=True) as conn:
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
        if verbose:
            print(f"DEBUG: Found {len(dbs)} databases: {', '.join(dbs)}")
    except Exception as e:
        if verbose:
            traceback.print_exc()
    if verbose:
        print(f"DEBUG: Found {len(dbs)} databases: {', '.join(dbs)}")
    return dbs

def extract_sql_agent_jobs(conn_str: str, server_name: str, repo_root: str, type_str: str, verbose: bool = False, dry_run: bool = False, last_run_dt: datetime = None):
    changed = 0
    skipped = 0
    max_seen = last_run_dt if last_run_dt else datetime(1940, 1, 1, tzinfo=timezone.utc)

    try:
        msdb_conn = replace_db_in_conn(conn_str, "msdb")

    except Exception as e:
        print(f"ERROR: [{server_name}] Failed to connect to msdb: {e}")
        if verbose:
            traceback.print_exc()
        return 0, 0, max_seen

    if verbose:
        masked = re.sub(r'(?i)\b(pwd|password)=[^;]+',r'\1=***', msdb_conn)
        print(f"DEBUG: [{server_name}] Extracting SQL Agent jobs using connection string: {masked}")  

    print(f"[{server_name}] Connecting to msdb for SQL Agent jobs...")

    try:
        with pyodbc.connect(msdb_conn, timeout=60, autocommit=True) as conn:
            cur = conn.cursor()
            if verbose:
                print(f"DEBUG: [{server_name}] Connected to msdb for SQL Agent jobs.")
            cur.execute(SQL_AGENT_JOB_QUERY)
            row_iter  = []
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    row_iter.append(row)
                row = row_iter
        print(f"[{server_name}] Found {len(row)} SQL Agent job step records.")

        if not rows:
            print(f"DEBUG: [{server_name}] No SQL Agent job step records found.")
            if verbose:
                print(f"DEBUG: [{server_name}] Query returned 0 rows - check if jobs exist and are enabled.")
            return changed, skipped, max_seen


        jobs_dict = {}
        for row in rows:
            job_id = str(row.job_id)
            if job_id not in jobs_dict:
                jobs_dict[job_id] = {
                    "name": row.JobName,
                    "enabled": row.IsEnabled,
                    "description": row.JobDescription,
                    "date_created": row.DateCreated,
                    "date_modified": row.DateModified,
                    "steps": []
                }

            if row.StepID is not None:
                jobs_dict[job_id]["steps"].append({
                    "step_id": row.StepID,
                    "step_name": row.StepName,
                    "subsystem": row.Subsystem,
                    "command": row.Command or '',
                    "database": row.DatabaseName or '',
                    "on_success_action": row.OnSuccessAction,
                    "on_fail_action": row.OnFailAction,
                    "retry_attempts": row.RetryAttempts,
                    "retry_interval": row.RetryInterval
                })


            try:
                mod_dt = _parse_datetime_to_utc(row.DateModified)
                if mod_dt and mod_dt > max_seen:
                    max_seen = mod_dt
            except Exception as e:
                pass



        base_dir = os.path.join(repo_root, SOURCE_FOLDER, type_str or "", sanitise_filename(server_name), "SQL_AGENT_JOBS")

        for job_id, job_data in jobs_dict.items():
            job_name = job_data["name"]
            date_modified = job_data["date_modified"]

            dest_file = os.path.join(base_dir, f"{sanitise_filename(job_name)}.txt")
            file_exists = os.path.exists(dest_file)
            
            if file_exists:
                try:
                    mod_dt = _parse_datetime_to_utc(date_modified)
                    if mod_dt and last_run_dt and mod_dt <= last_run_dt:
                        if verbose:
                            print(f"SKIP: SQL Agent Job '{job_name}' - modified {mod_dt.isoformat()} <= last_run {last_run_dt.isoformat()}")
                        skipped += 1
                        continue
                except Exception:
                    if verbose:
                        print(f"SKIP: SQL Agent Job '{job_name}' unable to determine modified date - skipping")
                    skipped += 1
                    continue
            else:
                if verbose:
                    print(f"NEW: SQL Agent Job '{job_name}' does not exist in repository - will be added regardless of modified date.")

            content_parts = []
            content_parts.append("=" * 60)
            content_parts.append(f"SQL Agent Job: {job_name}")
            content_parts.append("=" * 60)
            content_parts.append(f"JobID: {job_id}")
            content_parts.append(f"Enabled: {"Yes" if job_data["enabled"] else "No"}")
            content_parts.append(f"Description: {job_data["description"]}")
            content_parts.append(f"Date Created: {job_data["date_created"]}")
            content_parts.append(f"Date Modified: {date_modified}")
            content_parts.append("=" * 60)

            if job_data['steps']:
                content_parts.append(f"Total Steps: {len(job_data['steps'])}")
                content_parts.append("")

                for step in sorted(job_data['steps'], key=lambda x: x['step_id']):
                    content_parts.append("-" * 60)
                    content_parts.append(f"Step {step['step_id']}: {step['step_name']}")
                    content_parts.append("-" * 60)
                    content_parts.append(f"Subsystem: {step['subsystem']}")
                    content_parts.append(f"Database: {step['database']}")
                    content_parts.append(f"On Success Action: {step['on_success_action']}")
                    content_parts.append(f"On Fail Action: {step['on_fail_action']}")
                    content_parts.append(f"Retry Attempts: {step['retry_attempts']}")
                    content_parts.append(f"Retry Interval: {step['retry_interval']}")
                    content_parts.append("")
                    content_parts.append("Command")
                    content_parts.append("-" * 40)
                    content_parts.append(step['command'])
                    content_parts.append("-" * 40)
                    content_parts.append("")
            else:
                content_parts.append("No steps found")
                content_parts.append("")

            content = "\n".join(content_parts)

            if dry_run:
                if is_different(dest_file, content):
                    changed += 1
                    if verbose:
                        print(f"WOULD WRITE: {dest_file}")
                else:
                    skipped += 1
                    if verbose:
                        print(f"WOULD SKIP: {dest_file}")
            
            else:
                if write_if_changed(dest_file, content):
                    changed += 1
                    if verbose:
                        print(f"WROTE SQL Agent Job: {dest_file}")
                else:
                    skipped += 1
                    if verbose:
                        print(f"SKIPPED SQL Agent Job: {dest_file}")

        print(f"[{server_name}] Wrote {changed} SQL Agent Job records, skipped {skipped} records.")

        if verbose:
            print(f"DEBUG: [{server_name}] SQL Agent Jobs extraction completed. Changed: {changed}, Skipped: {skipped}")

        return changed, skipped, max_seen
    except pyodbc.Error as db_err:
        print(f"ERROR: [{server_name}] Database error while extracting SQL Agent Jobs: {db_err}")
        print(f"This could be due to permissions issues. Ensure the user has SELECT permissions on the msdb database.")
        if verbose:
            traceback.print_exc()
        return 0, 0, max_seen
    except Exception as e:
        print(f"ERROR: [{server_name}] Unexpected error while extracting SQL Agent Jobs: {e}")
        if verbose:
            traceback.print_exc()
        return 0, 0, max_seen


        


            
