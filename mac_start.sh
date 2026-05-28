#!/bin/bash
cd "$(dirname "$0")"

# Check Python is available
if ! command -v python3 &>/dev/null; then
    echo "Python not found. Please install Python 3.10+ from https://python.org"
    exit 1
fi

# Create venv and install dependencies if not already set up
if [ ! -f ".venv/bin/python" ]; then
    echo "First run: setting up virtual environment..."
    python3 -m venv .venv
    echo "Installing dependencies..."
    .venv/bin/pip install -r requirements.txt
    echo "Setup complete!"
    echo
fi

open http://localhost:5001
.venv/bin/python app.py
