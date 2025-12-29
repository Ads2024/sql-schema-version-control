
import os
import pyodbc
from datetime import datetime, timezone
from typing import Tuple
from ..core.filesystem import sanitise_filename, write_if_changed, is_different
from ..core.tracking import _parse_datetime_to_utc

SOURCE_FOLDER = "src"


def extract_sql_agent_jobs(
    conn: pyodbc.Connection,
    server_name: str,
    base_repo_root: str,
    type_str: str,
    last_run_dt: datetime,
    dry_run: bool = False,
    verbose: bool = False
) -> Tuple[int, int, datetime]:
    """
    Extracts SQL Agent Jobs from msdb.
    Returns (changed_count, skipped_count, max_modified_dt).
    """
    query_path = os.path.join(os.path.dirname(__file__), "..", "queries", "sql_agent_jobs.sql")
    with open(query_path, "r", encoding="utf-8") as f:
        query_sql = f.read()

    print(f"[{server_name}] Connecting to msdb for SQL Agent jobs...")
    
    try:
        cur = conn.cursor()
        cur.execute(query_sql)
        rows = cur.fetchall()
    except Exception as e:
        print(f"ERROR: [{server_name}] Failed to query msdb: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 0, 0, last_run_dt

    if not rows:
        print(f"[{server_name}] No SQL Agent job records found.")
        return 0, 0, last_run_dt
        
    print(f"[{server_name}] Found {len(rows)} SQL Agent job step records.")

    # Group rows by JobId
    jobs_dict = {}
    
    max_seen = last_run_dt
    
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

        # Check max modified date
        try:
            mod_dt = _parse_datetime_to_utc(row.DateModified)
            if mod_dt and mod_dt > max_seen:
                max_seen = mod_dt
        except Exception:
            pass

        if row.StepId is not None:
             jobs_dict[job_id]["steps"].append({
                "step_id": row.StepId,
                "step_name": row.StepName,
                "subsystem": row.Subsystem,
                "command": row.Command or '',
                "database": row.DatabaseName or '',
                "on_success_action": row.OnSuccessAction,
                "on_fail_action": row.OnFailAction,
                "retry_attempts": row.RetryAttempts,
                "retry_interval": row.RetryInterval
             })
             
    changed = 0
    skipped = 0
    base_dir = os.path.join(base_repo_root, SOURCE_FOLDER, type_str or "", sanitise_filename(server_name), "SQL_AGENT_JOBS")

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
                        print(f"SKIP: Agent Job '{job_name}' - modified {mod_dt.isoformat()} <= last_run {last_run_dt.isoformat()}")
                    skipped += 1
                    continue
            except Exception:
                pass
        else:
            if verbose:
                print(f"NEW: Agent Job '{job_name}' - file doesn't exist, will be added")
        
        content_parts = []
        content_parts.append("=" * 60)
        content_parts.append(f"SQL Agent Job: {job_name}")
        content_parts.append("=" * 60)
        content_parts.append(f"JobID: {job_id}")
        content_parts.append(f"Enabled: {'Yes' if job_data['enabled'] else 'No'}")
        content_parts.append(f"Description: {job_data['description']}")
        content_parts.append(f"Date Created: {job_data['date_created']}")
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
                    print(f"WROTE Agent Job: {dest_file}")
            else:
                skipped += 1
                if verbose:
                    print(f"SKIPPED Agent Job: {dest_file}")

    print(f"[{server_name}] Extracted agent jobs: {changed} changed, {skipped} skipped.")
    return changed, skipped, max_seen
