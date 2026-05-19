@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo =========================================
echo  Mandarin Trainer - Updater
echo =========================================
echo.
echo Please make sure Mandarin Trainer is stopped before continuing.
echo.
set /p READY="Ready to update? [y/n]: "
if /i not "!READY!"=="y" (
    echo Update cancelled.
    pause & exit /b 0
)
echo.

:: ── Check git ────────────────────────────────────────────────────────────
git --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] git is not installed or not in PATH.
    echo         Download it from https://git-scm.com
    pause & exit /b 1
)

:: ── Check we are inside a git repo ───────────────────────────────────────
git rev-parse --git-dir >nul 2>&1
if errorlevel 1 (
    echo [ERROR] This folder is not a git repository.
    echo         Clone the repo first, then run update.bat from inside it.
    pause & exit /b 1
)

:: ── Backup vocab.db ──────────────────────────────────────────────────────
set "DB_BAK="
if exist vocab.db (
    echo [1/4] Backing up vocab.db...
    copy /Y vocab.db vocab.db.update_bak >nul
    set "DB_BAK=1"
) else (
    echo [1/4] No vocab.db found, skipping backup.
)

:: ── Fetch and show what will change ──────────────────────────────────────
echo [2/4] Fetching latest changes...
for /f %%i in ('git rev-parse --abbrev-ref HEAD') do set BRANCH=%%i
git fetch origin 2>nul

echo.
git diff --quiet HEAD origin/!BRANCH! 2>nul
if errorlevel 1 (
    echo Files in this update:
    echo -----------------------------------------
    git --no-pager diff --name-status HEAD origin/!BRANCH!
    echo -----------------------------------------
) else (
    echo Already up to date. No changes to apply.
)
echo.

:: ── Apply update ─────────────────────────────────────────────────────────
echo [3/4] Applying update...
if exist ".git\index.lock" del ".git\index.lock"
if exist vocab.db del vocab.db
git reset --hard origin/!BRANCH!
if errorlevel 1 (
    echo.
    echo [ERROR] Update failed.
    if defined DB_BAK (
        echo         Restoring vocab.db from backup...
        copy /Y vocab.db.update_bak vocab.db >nul
        del vocab.db.update_bak
    )
    pause & exit /b 1
)

:: ── Restore vocab.db ─────────────────────────────────────────────────────
if defined DB_BAK (
    echo         Restoring vocab.db...
    copy /Y vocab.db.update_bak vocab.db >nul
    del vocab.db.update_bak
)

:: ── Update Python dependencies ───────────────────────────────────────────
echo [4/4] Updating dependencies...
if exist .venv\Scripts\pip.exe (
    .venv\Scripts\pip.exe install -r requirements.txt -q
    echo         Done.
) else (
    echo         No venv found - run start.bat first to set it up.
)

echo.
echo =========================================
echo  Update complete! Run start.bat to launch.
echo =========================================
pause
