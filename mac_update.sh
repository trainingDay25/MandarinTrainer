#!/bin/bash
cd "$(dirname "$0")"

echo "========================================="
echo " Mandarin Trainer - Updater"
echo "========================================="
echo
echo "Please make sure Mandarin Trainer is stopped before continuing."
echo
read -rp "Ready to update? [y/n]: " READY
if [[ ! "$READY" =~ ^[Yy]$ ]]; then
    echo "Update cancelled."
    exit 0
fi
echo

# ── Check git ────────────────────────────────────────────────────────────────
if ! command -v git &>/dev/null; then
    echo "[ERROR] git is not installed or not in PATH."
    echo "        Install it via: xcode-select --install"
    exit 1
fi

# ── Check we are inside a git repo ───────────────────────────────────────────
if ! git rev-parse --git-dir &>/dev/null; then
    echo "[ERROR] This folder is not a git repository."
    echo "        Clone the repo first, then run update.sh from inside it."
    exit 1
fi

# ── Backup vocab.db ──────────────────────────────────────────────────────────
DB_BAK=""
if [ -f vocab.db ]; then
    echo "[1/4] Backing up vocab.db..."
    cp vocab.db vocab.db.update_bak
    DB_BAK=1
else
    echo "[1/4] No vocab.db found, skipping backup."
fi

# ── Fetch and show what will change ──────────────────────────────────────────
echo "[2/4] Fetching latest changes..."
BRANCH=$(git rev-parse --abbrev-ref HEAD)
git fetch origin 2>/dev/null

echo
if ! git diff --quiet HEAD "origin/$BRANCH" 2>/dev/null; then
    echo "Files in this update:"
    echo "-----------------------------------------"
    git --no-pager diff --name-status HEAD "origin/$BRANCH"
    echo "-----------------------------------------"
else
    echo "Already up to date. No changes to apply."
fi
echo

# ── Apply update ─────────────────────────────────────────────────────────────
echo "[3/4] Applying update..."
rm -f .git/index.lock
[ -f vocab.db ] && rm vocab.db
if ! git reset --hard "origin/$BRANCH"; then
    echo
    echo "[ERROR] Update failed."
    if [ -n "$DB_BAK" ]; then
        echo "        Restoring vocab.db from backup..."
        mv vocab.db.update_bak vocab.db
    fi
    exit 1
fi

# ── Restore vocab.db ─────────────────────────────────────────────────────────
if [ -n "$DB_BAK" ]; then
    echo "        Restoring vocab.db..."
    mv vocab.db.update_bak vocab.db
fi

# ── Update Python dependencies ────────────────────────────────────────────────
echo "[4/4] Updating dependencies..."
if [ -f .venv/bin/pip ]; then
    .venv/bin/pip install -r requirements.txt -q
    echo "        Done."
else
    echo "        No venv found - run start.sh first to set it up."
fi

echo
echo "========================================="
echo " Update complete! Run start.sh to launch."
echo "========================================="
