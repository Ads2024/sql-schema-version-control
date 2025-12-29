@echo off
REM ======================================================================================================
REM Automated On-Prem SQL Objects Extraction and Versioning
REM Created: November 6, 2025
REM Author: Adam M
REM Purpose: Extract on-prem SQL objects, commit changes and push to Github Origin Repo
REM Usage: Schedule this batch file in windows Task Scheduler
REM ======================================================================================================

SETLOCAL EnableDelayedExpansion

REM Set repository path -change path accordingly
SET REPO_PATH=%userprofile%\Repos\sql-schema-version-control
CD /D "%REPO_PATH%"

REM Log File for troubleshooting
SET LOG_FILE=%REPO_PATH%\logs\onprem_extraction_%DATE:~4,4%%DATE:~7,2%%DATE:~10,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%.log
SET LOG_FILE=%LOG_FILE: =0%
IF NOT EXIST "%REPO_PATH%\logs" mkdir "%REPO_PATH%\logs"

echo ========================================================================== > "%LOG_FILE%"
echo Automated On-Prem SQL Objects Extraction and Versioning - %DATE% %TIME% >> "%LOG_FILE%"
echo ========================================================================== >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM Pull latest changes from origin
echo [%TIME%] Pulling latest changes from Github...>> "%LOG_FILE%"
git pull --all >> "%LOG_FILE%" 2>&1

IF ERRORLEVEL 1 (
    echo [%TIME%] Failed to pull latest changes from Github.>> "%LOG_FILE%"
    echo ERROR: Git Pull Failed. Check log file for details.>> "%LOG_FILE%"
    exit /b 1
)


REM Run the on-prem extractor using local venv
echo [%TIME%] Running on-prem extractor...>> "%LOG_FILE%"
echo Command: %REPO_PATH%\venv\Scripts\python.exe %REPO_PATH%\code\versioner_onprem.py --all-servers --all-databases --include-sql-agent-jobs --type OnPrem --repo-root . -v >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

"%REPO_PATH%\venv\Scripts\python.exe" "%REPO_PATH%\code\versioner_onprem.py" --all-servers --all-databases --include-sql-agent-jobs --type OnPrem --repo-root . -v >> "%LOG_FILE%" 2>&1

IF ERRORLEVEL 1 (
    echo [%TIME%] Failed to run on-prem extractor.>> 
    "%LOG_FILE%"
    echo ERROR: On-Prem Extractor Failed. Check log file for details.>> "%LOG_FILE%"
    exit /b 1
)

echo [%TIME%] On-Prem Extractor completed successfully.>> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM Check of there are changes to commit
echo [%TIME%] Checking for changes to commit...>> "%LOG_FILE%"
git status --short >> "%LOG_FILE%" 2>&1
git diff --quiet --exit-code
SET WORKING_TREE_CHANGED=!ERRORLEVEL!

git diff --cached --quiet --exit-code
SET INDEX_CHANGED=!ERRORLEVEL!

IF !WORKING_TREE_CHANGED! EQU 0 if !INDEX_CHANGED! EQU 0 (
    echo [%TIME%] No changes detected.>> "%LOG_FILE%"
    echo No changes to commit
    exit /b 0
)

REM stage all changes in src/ and last_run.yaml
echo [%TIME%] Staging changes...>> "%LOG_FILE%"
git add src/ >> "%LOG_FILE%" 2>&1
git add last_run.yaml >> "%LOG_FILE%" 2>&1

REM commit with timestamp
SET COMMIT_MSG = Automated On-Prem SQL Objects Extraction and Versioning - %DATE% %TIME%
echo [%TIME%] Committing change: %COMMIT_MSG%>> "%LOG_FILE%"
git commit -m "%COMMIT_MSG%" >> "%LOG_FILE%" 2>&1

if ERRORLEVEL 1 (
    echo [%TIME%] Failed to commit changes.>> "%LOG_FILE%"
    echo ERROR: Git Commit Failed. Check log file for details.>> "%LOG_FILE%"
    exit /b 1
)
echo [%TIME%] Changes committed successfully.>> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM push changes to origin
echo [%TIME%] Pushing changes to origin...>> "%LOG_FILE%"
git push origin main >> "%LOG_FILE%" 2>&1

if ERRORLEVEL 1 (
    echo [%TIME%] Failed to push changes to origin.>> "%LOG_FILE%"
    echo ERROR: Git Push Failed. Check log file for details.>> "%LOG_FILE%"
    exit /b 1
)
echo [%TIME%] Changes pushed successfully.>> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM Create Github notification issue
echo [%TIME%] Creating Github notification issue...>> "%LOG_FILE%"
if defined GITHUB_TOKEN (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%REPO_PATH%\scripts\create_github_issue.ps1" -Type "OnPrem" -token "%GITHUB_TOKEN%" >> "%LOG_FILE%" 2>&1
    if ERRORLEVEL 1(
        echo [%TIME%] Warning: Failed to create Github notification issue.>> "%LOG_FILE%"
        echo Warning: Github notification issue not created. See log for details %LOG_FILE%
        REM Dont exit - notification is optional
    )
) else(
    echo [%TIME%] Github token not found.>> "%LOG_FILE%"
    echo Github token not found.>> "%LOG_FILE%"
    
)

echo ================================================================================================ >> "%LOG_FILE%"
echo SUCCESS: On-Prem SQL Objects Extraction and Versioning completed successfully
echo Log: %LOG_FILE%

exit /b 0

