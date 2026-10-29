#!/usr/bin/env bash
# Production start script (Render). Uses the persistent disk at /var/data when it is attached,
# otherwise falls back to local folders (data then relies on the Supabase copy after restarts).
set -e
DATA_DIR="${DATA_DIR:-/var/data}"
if [ -d "$DATA_DIR" ] && [ -w "$DATA_DIR" ]; then
  mkdir -p "$DATA_DIR/db_storage" "$DATA_DIR/uploads"
  rm -rf db_storage uploads 2>/dev/null || true
  ln -sfn "$DATA_DIR/db_storage" db_storage
  ln -sfn "$DATA_DIR/uploads" uploads
  echo "[start] Using persistent disk at $DATA_DIR"
else
  mkdir -p db_storage uploads
  echo "[start] WARNING: no persistent disk at $DATA_DIR - add one in Render > Disks (mount path /var/data)"
fi
exec python3 server.py
