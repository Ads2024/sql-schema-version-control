# sql-schema-version-control


Production-grade database schema versioning system used to manage 40,000+ SQL objects across hybrid on-premises and Microsoft Fabric environments, generalised for this project.

## Features

- **Incremental extraction** - Timestamp-based delta tracking (only extracts changed objects)
- **Hybrid cloud support** - Works with on-prem SQL Server (Windows Auth) and Microsoft Fabric (Service Principal)
- **SQL Agent extraction** - Captures SQL Agent jobs, steps, and schedules (on-prem only)
- **Atomic state management** - Corruption-resistant tracking via tempfile writes
- **Multi-environment orchestration** - On-prem (Windows Task Scheduler) + Fabric (GitHub Actions)
- **Scale proven** - Handles 40,000+ objects with 10-year retention history

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

### config.yaml
```yaml
environments:
  onprem:
    servers:
      - "sql-prod-01.corp.local"
      - "sql-prod-02.corp.local"
    extract_agent_jobs: true
    
  fabric:
    servers:
      - "abc123.datawarehouse.fabric.microsoft.com"
```

### Environment Variables

For Fabric (Service Principal):
```bash
export FABRIC_TENANT_ID="your-tenant-id"
export FABRIC_CLIENT_ID="your-client-id"
export FABRIC_CLIENT_SECRET="your-secret"
```

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


## License

MIT

## Author

Adam M. - Data Engineer

---

**Questions?** Open an issue or reach out at `abdia980@gmail.com`