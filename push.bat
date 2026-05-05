@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo =========================================
echo  Mandarin Trainer - Push to GitHub
echo =========================================
echo.

:: ── Check git ─────────────────────────────────────────────────────────────
git --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] git is not installed or not in PATH.
    echo         Download it from https://git-scm.com
    pause & exit /b 1
)

:: ── Init repo if needed ────────────────────────────────────────────────────
git rev-parse --git-dir >nul 2>&1
if errorlevel 1 (
    echo [1/4] Initialising git repository...
    git init
    git remote add origin https://github.com/trainingDay25/MandarinTrainer.git
    echo       Done.
) else (
    echo [1/4] Git repository already initialised.
    :: Ensure remote is set
    git remote get-url origin >nul 2>&1
    if errorlevel 1 (
        git remote add origin https://github.com/trainingDay25/MandarinTrainer.git
        echo       Remote added.
    )
)
echo.

:: ── Stage files ───────────────────────────────────────────────────────────
echo [2/4] Staging files...

git add app.py
git add generate_lemonade_examples.py
git add requirements.txt
git add start.bat
git add update.bat
git add push.bat
git add README.md
git add vocab.db
git add templates\
git add static\

echo       Staged. Summary of changes:
echo.
git diff --cached --stat
echo.

:: ── Check if there is anything to commit ──────────────────────────────────
git diff --cached --quiet
if not errorlevel 1 (
    echo Nothing new to commit - everything is already up to date.
    pause & exit /b 0
)

:: ── Commit message ────────────────────────────────────────────────────────
echo [3/4] Enter a commit message (or press Enter for a timestamp):
set /p MSG="  > "
if "!MSG!"=="" (
    for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set D=%%c-%%b-%%a
    for /f "tokens=1-2 delims=: " %%a in ('time /t') do set T=%%a:%%b
    set MSG=update !D! !T!
)

git commit -m "!MSG!"
if errorlevel 1 (
    echo [ERROR] Commit failed.
    pause & exit /b 1
)
echo.

:: ── Push ──────────────────────────────────────────────────────────────────
echo [4/4] Pushing to GitHub...
git push -u origin main 2>nul
if errorlevel 1 (
    echo       Trying branch "master"...
    git push -u origin master
    if errorlevel 1 (
        echo.
        echo [ERROR] Push failed. Things to check:
        echo         - Are you logged in? Run: git credential-manager
        echo         - Does the remote repo exist at:
        echo           https://github.com/trainingDay25/MandarinTrainer.git
        pause & exit /b 1
    )
)

echo.
echo =========================================
echo  Done! Changes pushed to GitHub.
echo =========================================
pause
