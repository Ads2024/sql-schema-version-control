
import os
import pyodbc
from datetime import datetime, timezone
from ..core.filesystem import sanitise_filename, write_if_changed, is_different
from ..core.tracking import _parse_datetime_to_utc

SOURCE_FOLDER = "src"
VIEW_TYPE = {"V"}
PROC_TYPE = {"P"}
ALLOWED_TYPES = VIEW_TYPE | PROC_TYPE

def bracket_ident(name: str):
    return "[" + name.replace("]", "]]") + "]"

def extract_sql_objects(
    conn: pyodbc.Connection,
    server_name: str,
    db_name: str,
    base_repo_root: str,
    type_str: str,
    last_run_dt: datetime,
    include_drop: bool = False,
    include_header: bool = False,
    dry_run: bool = False,
    verbose: bool = False
) -> tuple[int, int, datetime]:
    """
    Extracts SQL objects (Views, Procedures) from the database.
    Returns (changed_count, skipped_count, max_modified_dt).
    """
    query_path = os.path.join(os.path.dirname(__file__), "..", "queries", "sql_objects.sql")
    with open(query_path, "r", encoding="utf-8") as f:
        query_sql = f.read()

    try:
        cur = conn.cursor()
        cur.execute(query_sql)
        rows = cur.fetchall()
    except Exception as e:
        print(f"ERROR: [Server: {server_name}] query failed for {db_name}: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 0, 0, last_run_dt

    if verbose:
        print(f"DEBUG: [Server: {server_name}] fetched {len(rows)} rows from database {db_name}")

    if not rows:
        return 0, 0, last_run_dt

    # Layout: <repo-root>/src/<type>/<sanitised-db-name>/<ObjectType>/<Schema>/<Object>.sql
    # NOTE: Original versioner.py layout:
    # base_dir = os.path.join(args.repo_root, SOURCE_FOLDER, args.type or "", sanitise_filename(db_name))
    # dest_dir = os.path.join(base_dir, obj_type_str, sanitise_filename(schema_name))
    # dest_file = os.path.join(dest_dir, f"{sanitise_filename(object_name)}.sql")
    
    base_dir = os.path.join(base_repo_root, SOURCE_FOLDER, type_str or "", sanitise_filename(db_name))
    
    changed = 0
    skipped = 0
    max_seen = last_run_dt

    for row in rows:
        schema_name = row.SchemaName
        object_name = row.ObjectName
        object_type = (row.ObjectType or "").strip()
        object_definition = row.ObjectDefinition
        modified_dt = row.ModifiedDate

        try:
            mod_dt = _parse_datetime_to_utc(modified_dt)
        except Exception:
            mod_dt = None

        if mod_dt is not None and mod_dt > max_seen:
            max_seen = mod_dt

        if object_type not in ALLOWED_TYPES:
            if verbose:
                print(f"SKIP: {schema_name}.{object_name} - object_type '{object_type}' not in allowed types")
            skipped += 1
            continue

        if not object_definition:
            if verbose:
                print(f"SKIP: {schema_name}.{object_name} - no definition or encrypted")
            skipped += 1
            continue

        if object_type in VIEW_TYPE:
            obj_type_str = "VIEW"
            obj_type_code = "V"
        elif object_type in PROC_TYPE:
            obj_type_str = "PROCEDURE"
            obj_type_code = "P"
        else:
            skipped += 1
            continue

        dest_dir = os.path.join(base_dir, obj_type_str, sanitise_filename(schema_name))
        dest_file = os.path.join(dest_dir, f"{sanitise_filename(object_name)}.sql")
        
        file_exists = os.path.exists(dest_file)

        # Logic: if file exists, check mod date. If logic says skip, skip.
        if file_exists:
            if mod_dt is not None:
                if mod_dt <= last_run_dt:
                    if verbose:
                        print(f"SKIP: {schema_name}.{object_name} - modified {mod_dt.isoformat()} <= last_run {last_run_dt.isoformat()}")
                    skipped += 1
                    continue
            else:
                if verbose:
                    print(f"SKIP: {schema_name}.{object_name} - unable to determine ModifiedDate")
                skipped += 1
                continue
        else:
            if verbose:
                print(f"NEW: {schema_name}.{object_name} - file doesn't exist, will be added")

        schema_q = bracket_ident(schema_name)
        object_q = bracket_ident(object_name)
        
        drop_stmt = (
            f"IF OBJECT_ID(N'{schema_q}.{object_q}', '{obj_type_code}') IS NOT NULL "
            f"DROP {obj_type_str} {schema_q}.{object_q};\nGO\n\n"
        ) if include_drop else ""

        body = object_definition.replace("\r\n", "\n").rstrip() + "\n"

        parts = []
        if include_header:
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

        parts.append(drop_stmt)
        parts.append(body)

        sql = "\n".join([p for p in parts if p])

        if dry_run:
            if is_different(dest_file, sql):
                changed += 1
                if verbose:
                    print(f"WOULD WRITE: {dest_file}")
            else:
                skipped += 1
                if verbose:
                    print(f"WOULD SKIP (unchanged): {dest_file}")
            continue

        if write_if_changed(dest_file, sql):
            changed += 1
            if verbose:
                print(f"WROTE: {dest_file}")
        else:
            skipped += 1
            if verbose:
                print(f"SKIPPED (unchanged): {dest_file}")

    return changed, skipped, max_seen
