# sql-schema-version-control


A utility for version controlling SQL objects across hybrid on-premises and Microsoft Fabric environments.

## Features

- **Incremental extraction** - Timestamp-based delta tracking (only extracts changed objects)
- **Hybrid cloud support** - Works with on-prem SQL Server (Windows Auth) and Microsoft Fabric (Service Principal)
- **SQL Agent extraction** - Captures SQL Agent jobs, steps, and schedules (on-prem only)
- **Atomic file operations** - Writes to temporary files and uses atomic replacement to ensure integrity
- **Multi-environment orchestration** - Supports execution via Windows Task Scheduler (on-prem) and GitHub Actions (Fabric)

## Architecture
```
┌───────────────────────────────────────────────────────┐
│                   SQL Object Versioner                │
├───────────────────────────────────────────────────────┤
│                                                       │
│  ┌──────────────────┐         ┌──────────────────┐    │
│  │  On-Prem Runner  │         │  Fabric Runner   │    │
│  │  (Task Scheduler)│         │ (GitHub Actions) │    │
│  └────────┬─────────┘         └────────┬─────────┘    │
│           │                            │              │
│           ▼                            ▼              │
│  ┌──────────────────────────────────────────────────┐ │
│  │           versioner.cli (Orchestration)          │ │
│  └───────────────────┬──────────────────────────────┘ │
│                      │                                │
│         ┌────────────┴────────────┐                   │
│         ▼                         ▼                   │
│  ┌─────────────┐          ┌─────────────┐             │
│  │ On-Prem     │          │ Fabric      │             │
│  │ Extractor   │          │ Extractor   │             │
│  └──────┬──────┘          └──────┬──────┘             │
│         │                        │                    │
│         ▼                        ▼                    │
│  ┌──────────────────────────────────────┐             │
│  │  Shared: SQL Objects + SQL Agent     │             │
│  │  (Views, Procedures, Agent Jobs)     │             │
│  └──────────────────────────────────────┘             │
│                      │                                │
│                      ▼                                │
│         ┌────────────────────────┐                    │
│         │  Git Repository        │                    │
│         │  (Versioned Objects)   │                    │
│         └────────────────────────┘                    │
└───────────────────────────────────────────────────────┘
```

## Installation
```bash
git clone https://github.com/yourusername/sql-object-versioner
cd sql-object-versioner
pip install -r requirements.txt
```

## Configuration

## Configuration

### 1. Scope (`config.yaml`)
Use `config.yaml` to define **what** to extract (Server lists, Environment flags). This file should be committed to the repo.

```yaml
environments:
  onprem:
    servers:
      - "sql-prod-01"
    extract_agent_jobs: true
    
  fabric:
    servers:
      - "xyz.datawarehouse.fabric.microsoft.com"
```

### 2. Authentication (Secrets)
**NEVER** commit secrets to `config.yaml`. Use Environment Variables or CLI arguments.

**Local Development (`.env`):**
Create a `.env` file in the root directory to store secrets locally. The tool automatically loads this file.
```properties
FABRIC_SP_TENANT=00000000-0000...
FABRIC_SP_CLIENT_ID=00000000-0000...
FABRIC_SP_CLIENT_SECRET=your-secret...
# Optional: Default connection string
SQL_CONN="DRIVER={ODBC Driver 17 for SQL Server};SERVER=...;..."
```

**CI/CD (GitHub Actions):**
Inject these as Environment Variables from GitHub Secrets. The workflow then passes them as arguments or relies on the process environment.

## Usage

### Fabric Extraction
```bash
# Extract from Fabric endpoint
python -m versioner.cli \
  --type fabric \
  --server xyz.datawarehouse.fabric.microsoft.com \
  --all-databases \
  --verbose

# Dry run (preview changes without writing)
python -m versioner.cli \
  --type fabric \
  --server xyz.datawarehouse.fabric.microsoft.com \
  --database Lakehouse_Prod \
  --dry-run
```

### On-Premises Extraction
```bash
# Extract from multiple servers
python -m versioner.cli \
  --type onprem \
  --servers sql-prod-01 sql-prod-02 \
  --all-databases \
  --include-sql-agent-jobs \
  --verbose

# Extract specific database
python -m versioner.cli \
  --type onprem \
  --servers sql-analytics \
  --database ReportingDB \
  --include-drop \
  --header
```

## How It Works

### Delta Tracking

The system uses `last_run.yaml` to track the last extraction timestamp:
```yaml
Fabric: 2024-12-29T10:30:00+00:00
OnPrem: 2024-12-29T09:15:00+00:00
```

Only objects modified **after** this timestamp are extracted, ensuring fast incremental updates.

### File Organization
```
src/
├── Fabric/
│   ├── Lakehouse_Prod/
│   │   ├── VIEW/
│   │   │   ├── dbo/
│   │   │   │   └── vw_sales_summary.sql
│   │   ├── PROCEDURE/
│   │   │   └── etl/
│   │   │       └── sp_load_data.sql
├── OnPrem/
    ├── sql-prod-01/
    │   ├── AppDB/
    │   │   ├── VIEW/
    │   │   ├── PROCEDURE/
    │   ├── SQL_AGENT_JOBS/
    │   │   ├── DailyETL.txt
    │   │   └── WeeklyBackup.txt
```

## Production Deployment

### On-Premises (Windows Task Scheduler)
```batch
REM run_onprem.bat
@echo off
cd C:\sql-versioner
python -m versioner.cli --type onprem --servers-list sql-prod-01 sql-prod-02 --all-databases --include-sql-agent-jobs
git add src/OnPrem
git commit -m "Automated on-prem extraction %date% %time%"
git push
```

Schedule via Task Scheduler: Daily at 2:00 AM

### Automated Issue Reporting
The project includes a PowerShell script to create GitHub Issues when changes are detected.

```powershell
./scripts/create_github_issue.ps1 -Type "OnPrem" -Token $env:GITHUB_TOKEN -Assignee "your-username"
```

**Features:**
- Reports server, timestamp, and list of changed files.
- **Auto-assignment**: Assigns issue to specific user via `-Assignee` arg, `ISSUE_ASSIGNEE` env var, or defaults to the token owner.

### Fabric (GitHub Actions)
```yaml
# .github/workflows/fabric-extraction.yml
name: Fabric SQL Object Extraction
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
  workflow_dispatch:

jobs:
  extract:
    runs-on: windows-latest
    env:
      FABRIC_SP_TENANT: ${{ secrets.FABRIC_TENANT_ID }}
      FABRIC_SP_CLIENT_ID: ${{ secrets.FABRIC_CLIENT_ID }}
      FABRIC_SP_CLIENT_SECRET: ${{ secrets.FABRIC_CLIENT_SECRET }}
      SQL_CONN: ${{ secrets.FABRIC_ENDPOINT }}
      ISSUE_ASSIGNEE: ${{ vars.ISSUE_ASSIGNEE }} # Optional: GitHub user to assign issues to
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: actions/setup-python@v5
      
      - name: Extract Fabric objects
        run: python -m versioner.cli --type fabric --all-databases
      
      - name: Commit and Push
        id: commit
        run: |
          git add src/Fabric last_run.yaml
          git commit -m "Automated Fabric extraction"
          git push
          echo "changes_detected=true" >> $env:GITHUB_OUTPUT

      - name: Create GitHub Issue
        if: steps.commit.outputs.changes_detected == 'true'
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: ./scripts/create_github_issue.ps1 -Type "Fabric" -Token $env:GH_TOKEN
```

## Technical Details

### Core Components

- **auth.py** - Service Principal authentication for Fabric
- **connection.py** - ODBC connection string building and database discovery
- **filesystem.py** - Atomic file writes with SHA256 change detection
- **tracking.py** - State management for incremental extraction
- **sql_objects.py** - Shared extraction logic for Views and Procedures
- **sql_agent.py** - SQL Agent job extraction (on-prem only)

### Key Design Decisions

**Why tempfile for atomic writes?**
Prevents corrupted state if process is killed mid-write. Uses `os.replace()` for atomic filesystem operations.

**Why separate last_run keys?**
On-prem and Fabric environments may have different schedules. Separate keys prevent cross-contamination.

**Why .txt for Agent jobs vs .sql for objects?**
Agent jobs contain metadata (schedules, retry logic) not just SQL, so plain text is more appropriate.

## Troubleshooting

### "No objects extracted"

Check `last_run.yaml` - if timestamp is recent, no objects have been modified since last run. Use `--dry-run` to see what would be extracted.

### "Authentication failed" (Fabric)

Verify Service Principal has:
- **SQL Database Contributor** role on Fabric Workspace
- **Fabric Administrator** or **Workspace Admin** permissions

### "Driver not found"

Install ODBC Driver 18 for SQL Server:
```bash
# Ubuntu
sudo apt-get install msodbcsql18

# Windows
# Download from: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server
```
# Limitations & Requirements

## Platform Requirements
- **Operating System**: Windows (primary), Linux/macOS (untested)
  - On-prem automation script (`scripts/run_onprem.bat`) is Windows-only
  - Core Python modules should work cross-platform but not validated
- **Cloud Provider**: Microsoft Azure/Fabric
  - Authentication designed for Azure AD/Entra ID
  - Connection patterns optimized for Fabric SQL endpoints
- **Database**: Microsoft SQL Server 2012+
  - Uses `sys.objects`, `sys.sql_modules`, `MSDB` system views
  - Not compatible with PostgreSQL, MySQL, Oracle

## Extraction Scope
**Currently Extracts**:
- Views (`sys.objects.type = 'V'`)
- Stored Procedures (`sys.objects.type = 'P'`)
- SQL Agent Jobs (from `MSDB`, on-prem only)

**Does NOT Extract**:
- Functions (scalar, inline table-valued, multi-statement)
- Triggers (DDL, DML)
- User-Defined Types
- Table schemas (column definitions, constraints, indexes)
- Security objects (logins, users, roles, permissions)
- Server-level configuration

**Rationale**: Focus on executable code that changes frequently. Table schemas 
typically require migration tools (SSDT, Flyway, Liquibase) rather than simple 
DDL extraction.

## Authentication Support
- **Azure/Fabric**: Service Principal (OAuth), Interactive (browser)
- **On-Premise**: Windows Authentication (Integrated Security)
- **Not Supported**: SQL Authentication (username/password)

## Notification Requirements
- GitHub Issues integration requires GitHub-hosted repository
- Other Git providers (GitLab, Bitbucket, Azure DevOps) not supported for notifications
- Tool functions without notifications if GitHub token not provided

## Known Limitations
- **Encrypted objects**: Cannot extract definitions of objects created WITH ENCRYPTION
- **System databases**: Excludes master, model, msdb, tempdb by design
- **Offline databases**: Skips databases where `state != 0`
- **Large objects**: No size limit enforcement, but files >100MB may cause Git performance issues
- **Concurrent runs**: No lock file protection; stagger schedules if running from multiple sources

## Future Considerations
- Cross-platform automation scripts (bash equivalents)
- Extended object types (functions, triggers)
- Table schema extraction
- Support for other database platforms (PostgreSQL, MySQL)
- GitLab/Azure DevOps notification integrations

## Production Deployment
Tested on:
- Windows Server 2016+
- MS Fabric (November 2025 to present)
- GitHub Actions (ubuntu-latest, windows-latest)

**Not Supported**:
- Windows Server 2008 (non-R2) - Python 3.11 incompatible

If deploying to other environments, validate thoroughly in non-production first.

## License

MIT
