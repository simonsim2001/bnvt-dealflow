#!/usr/bin/env bash
# BNVT Dealflow OS - Perpetual Service Manager for macOS
# Keeps the local server running 24/7 in the background with auto-restart & startup on boot.

PLIST_NAME="com.bnvt.dealflow.plist"
PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_NAME"
PROJECT_DIR="/Users/simonlevy/.gemini/antigravity/scratch/bnvt-dealflow"
LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"
mkdir -p "$HOME/Library/LaunchAgents"

case "$1" in
  install)
    echo "⚙️ Creating launchd plist agent at $PLIST_PATH..."
    cat << EOF > "$PLIST_PATH"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.bnvt.dealflow</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>$PROJECT_DIR/server.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$PROJECT_DIR</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/server.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/server_error.log</string>
</dict>
</plist>
EOF
    echo "🚀 Loading background service into launchctl..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    launchctl load "$PLIST_PATH"
    echo "✓ BNVT Dealflow is now running constantly in the background on http://localhost:8000!"
    echo "  It will auto-start whenever your Mac turns on."
    ;;

  start)
    echo "🚀 Starting BNVT Dealflow background service..."
    launchctl load "$PLIST_PATH" 2>/dev/null || true
    launchctl start com.bnvt.dealflow
    echo "✓ Service started."
    ;;

  stop)
    echo "⏹️ Stopping BNVT Dealflow background service..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    echo "✓ Service stopped."
    ;;

  restart)
    echo "🔄 Restarting service..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    sleep 1
    launchctl load "$PLIST_PATH"
    echo "✓ Service restarted."
    ;;

  status)
    echo "🔍 Checking status of BNVT Dealflow service..."
    if launchctl list | grep -q "com.bnvt.dealflow"; then
      echo "  - launchd service status: REGISTERED & RUNNING"
    else
      echo "  - launchd service status: NOT REGISTERED"
    fi
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8000 | grep -q "200"; then
      echo "  - HTTP server: LIVE at http://localhost:8000 (HTTP 200 OK)"
    else
      echo "  - HTTP server: NOT RESPONDING on http://localhost:8000"
    fi
    ;;

  logs)
    echo "📜 Showing latest 50 lines of server logs ($LOG_DIR/server.log):"
    tail -n 50 "$LOG_DIR/server.log" "$LOG_DIR/server_error.log" 2>/dev/null
    ;;

  *)
    echo "Usage: bash service.sh {install|start|stop|restart|status|logs}"
    exit 1
    ;;
esac
