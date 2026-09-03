#!/bin/bash
# Installs the submission watcher as a launchd LaunchAgent so it runs at
# login — HANDOFF.md §6: "the user should not need to do more than hit ok."
#
#   ./install-watcher.sh            install and start
#   ./install-watcher.sh uninstall  stop and remove
#   ./install-watcher.sh status     is it running
#
# A LaunchAgent (not a LaunchDaemon): it runs as Matt, in his GUI session,
# which is the whole point — it has to be able to open a Terminal window
# and a Chrome window on his actual screen.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LABEL=com.mattbarr.job-applier-watcher
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

case "${1:-install}" in
  status)
    launchctl list | grep -q "$LABEL" && echo "watcher: running" || echo "watcher: not running"
    [ -f "$PLIST" ] && echo "plist:   $PLIST" || echo "plist:   not installed"
    exit 0
    ;;
  uninstall)
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    rm -f "$PLIST"
    echo "watcher uninstalled. Approved applications will sit until you run worker.py by hand."
    exit 0
    ;;
  install) ;;
  *) echo "usage: $0 {install|uninstall|status}" >&2; exit 2 ;;
esac

mkdir -p "$HOME/Library/LaunchAgents"
cat > "$PLIST" <<PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$HERE/.venv/bin/python</string>
    <string>$HERE/watcher.py</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>AWS_PROFILE</key><string>job-applier</string>
    <key>HOME</key><string>$HOME</string>
    <!-- review until the first real submission has gone through; then
         set this to auto, which is ARCHITECTURE.md §3's steady state. -->
    <key>JOB_APPLIER_WORKER_MODE</key><string>review</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$HOME/Library/Logs/job-applier-watcher.log</string>
  <key>StandardErrorPath</key><string>$HOME/Library/Logs/job-applier-watcher.log</string>
</dict>
</plist>
PLIST_EOF

launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
echo "watcher installed and running."
echo "  log:       ~/Library/Logs/job-applier-watcher.log"
echo "  mode:      review (worker fills the form, you click submit)"
echo "  switch to auto: edit JOB_APPLIER_WORKER_MODE in $PLIST, then re-run this script"
echo
echo "From here, replying \"ok\" to an approval email is the whole job."
