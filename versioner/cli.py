
import argparse
import sys
import os
import yaml
from .core.utils import load_dotenv
from .extractors.fabric import run_fabric_extraction
from .extractors.onprem import run_onprem_extraction

def load_config(path: str = "config.yaml") -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}

def main():
    parser = argparse.ArgumentParser(description="Extract SQL Server objects for version control.")
    
    # Core arguments
    parser.add_argument("--type", choices=["fabric", "onprem"], required=True, help="Type of environment: fabric or onprem")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml (default: config.yaml)")
    parser.add_argument("--repo-root", default=".", help="Root directory to store extracted files.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate writes.")
    
    # Connection / Server arguments
    parser.add_argument("--conn", help="ODBC connection string (overrides server/db args).")
    parser.add_argument("--server", help="Primary SQL Server hostname.")
    parser.add_argument("--servers", nargs="+", dest="servers_list", help="List of SQL Servers to process (OnPrem only).")
    parser.add_argument("--database", help="Specific database to process.")
    parser.add_argument("--all-databases", action="store_true", help="Iterate all databases.")
    parser.add_argument("--databases", help="Comma-separated list of databases.")
    parser.add_argument("--databases-file", help="File containing list of databases.")
    
    # Auth arguments
    parser.add_argument("--driver", default="ODBC Driver 17 for SQL Server", help="ODBC Driver to use.")
    parser.add_argument("--ad-interactive", action="store_true", help="Use Active Directory Interactive auth.")
    parser.add_argument("--sp-tenant", help="Service Principal Tenant ID.")
    parser.add_argument("--sp-client-id", help="Service Principal Client ID.")
    parser.add_argument("--sp-client-secret", help="Service Principal Client Secret.")
    parser.add_argument("--sp-fallback", action="store_true", help="Fallback to legacy UID/PWD for SP auth.")
    
    # Extraction flags
    parser.add_argument("--include-drop", action="store_true", help="Include DROP statements in SQL.")
    parser.add_argument("--header", action="store_true", help="Include header comments in SQL.")
    parser.add_argument("--include-sql-agent-jobs", action="store_true", help="Extract SQL Agent Jobs (OnPrem).")
    
    # Legacy flag support
    parser.add_argument("--export-env", action="store_true", help="Update .env with current DB (Fabric).")
    parser.add_argument("--include-second-server", action="store_true", help="Process secondary server (Fabric).")
    parser.add_argument("--all-servers", action="store_true", help="No-op flag for compatibility (use config for list).")
    
    args = parser.parse_args()
    
    # Load .env
    load_dotenv()
    
    config_path = args.config
    if not os.path.exists(config_path):
         package_dir = os.path.dirname(os.path.dirname(__file__))
         potential = os.path.join(package_dir, "config.yaml")
         if os.path.exists(potential):
             config_path = potential
             
    config = load_config(config_path)
    
    if args.verbose:
        print(f"Using configuration from: {config_path if os.path.exists(config_path) else 'Defaults (empty)'}")
        
    if args.type.lower() == "fabric":
        run_fabric_extraction(args, config)
    elif args.type.lower() == "onprem":
        run_onprem_extraction(args, config)
        
if __name__ == "__main__":
    main()
