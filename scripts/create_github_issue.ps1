<#
Created: Nov 6, 2025
By: Adam M.
Generalised: 2025-12-29
Objective: PowerShell script to report extraction results to GitHub Issues.
#>
param (
    [string]$Type = "OnPrem",
    [string]$Token,
    [string]$Assignee
)

$ErrorActionPreference = "Stop"

if (-not $Token) {
    Write-Warning "No Github Token provided. Exiting."
    exit 0
}


$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$Server = $env:COMPUTERNAME
$CommitSHA = git rev-parse --short HEAD
$CommitFullSHA = git rev-parse HEAD


$Changes = git show --name-only --format="" HEAD
if ($Changes -is [string]) {
    $ChangesList = $Changes.Split("`n") | Where-Object { $_.Trim() -ne "" }
}
else {
    $ChangesList = $Changes
}
$ChangeCount = ($ChangesList | Measure-Object).Count


$Body = @"
**SQL objects have been successfully extracted and committed to the repo.**

**Environment Details:**
- **Server:** $Server
- **Time:** $Timestamp
- **Commit:** $CommitFullSHA

**Changes Summary:**
- **Total Files:** $ChangeCount

**Changed Files:**
```
$($ChangesList -join "`n")
```
"@

$Title = "[$Type] SQL Extraction Report - $Timestamp"


$Headers = @{
    "Authorization" = "token $Token"
    "Accept"        = "application/vnd.github.v3+json"
}

$Assignees = @()

# Check for override via Env Var or Argument
if ($Assignee) {
    $Assignees += $Assignee
}
elseif ($env:ISSUE_ASSIGNEE) {
    $Assignees += $env:ISSUE_ASSIGNEE
}

# If no assignee specified, try to auto-assign to token owner
if ($Assignees.Count -eq 0) {
    try {
        # Get current user (token owner)
        $UserResp = Invoke-RestMethod -Uri "https://api.github.com/user" -Method Get -Headers $Headers -ErrorAction SilentlyContinue
        if ($UserResp -and $UserResp.login) {
            $Assignees += $UserResp.login
            Write-Host "Will auto-assign issue to token owner: $($UserResp.login)"
        }
    }
    catch {
        Write-Warning "Could not determine GitHub user from token for assignment."
    }
}
else {
    Write-Host "Will assign issue to: $($Assignees -join ', ')"
}


# Parse Repo Owner/Name from git config
$RemoteUrl = git config --get remote.origin.url
# Helper regex for SSH and HTTPS
if ($RemoteUrl -match "github\.com[:/]([^/]+)/([^/.]+?)(\.git)?$") {
    $Owner = $Matches[1]
    $Repo = $Matches[2]
}
else {
    Write-Error "Could not parse GitHub repo from remote URL: $RemoteUrl"
    exit 1
}

$IssueData = @{
    title = $Title
    body  = $Body
}

if ($Assignees.Count -gt 0) {
    $IssueData["assignees"] = $Assignees
}

$Uri = "https://api.github.com/repos/$Owner/$Repo/issues"
$JsonPayload = $IssueData | ConvertTo-Json -Depth 10

try {
    $IssueResp = Invoke-RestMethod -Uri $Uri -Method Post -Headers $Headers -Body $JsonPayload -ContentType "application/json"
    Write-Host "Successfully created GitHub issue: $($IssueResp.html_url)"
}
catch {
    Write-Error "Failed to create GitHub issue: $_"
    # Print error details if available
    try {
        $ErrorJson = $_.Exception.Response.GetResponseStream()
        $Reader = New-Object System.IO.StreamReader($ErrorJson)
        Write-Error $Reader.ReadToEnd()
    }
    catch {}
    exit 1
}
