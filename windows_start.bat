@echo off
cd /d "%~dp0"

:: Check Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Python not found. Please install Python 3.10+ from https://python.org
    echo Make sure to tick "Add Python to PATH" during installation.
    pause
    exit /b 1
)

:: Create venv and install dependencies if not already set up
if not exist ".venv\Scripts\python.exe" (
    echo First run: setting up virtual environment...
    python -m venv .venv
    echo Installing dependencies...
    .venv\Scripts\pip install -r requirements.txt
    echo Setup complete!
    echo.
)

start "" http://localhost:5001
.venv\Scripts\python.exe app.py
