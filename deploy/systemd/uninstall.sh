#!/usr/bin/env bash
set -euo pipefail

print_banner() {
    local cyan='' reset='' animate=0
    if [ -t 1 ] && [ "${TERM:-dumb}" != "dumb" ]; then
        cyan=$'\033[1;36m'
        reset=$'\033[0m'
        if sleep 0.001 2>/dev/null; then
            animate=1
        fi
    fi

    printf '%s' "$cyan"
    while IFS= read -r line; do
        printf '%s\n' "$line"
        if [ "$animate" -eq 1 ]; then
            sleep 0.03
        fi
    done <<'BANNER'
  __ _                                                
 / _\ |__   __ _ _ __ ___  _ __   ___   ___  _ __ ___ 
 \ \| '_ \ / _` | '_ ` _ \| '_ \ / _ \ / _ \| '__/ _ \
 _\ \ | | | (_| | | | | | | |_) | (_) | (_) | | |  __/
 \__/_| |_|\__,_|_| |_| |_| .__/ \___/ \___/|_|  \___|
                          |_|
BANNER
    printf '%s' "$reset"
    echo
}

print_banner

PURGE=0
for arg in "$@"; do
    case "$arg" in
        --purge) PURGE=1 ;;
        -h|--help)
            echo "usage: $0 [--purge]"
            exit 0
            ;;
        *) echo "unknown arg: $arg" >&2; exit 1 ;;
    esac
done

[ "${EUID:-0}" -eq 0 ] || { echo "run with sudo" >&2; exit 1; }

echo "==> stopping service"
systemctl stop shampoore 2>/dev/null || true
systemctl disable shampoore 2>/dev/null || true

echo "==> removing unit"
rm -f /etc/systemd/system/shampoore.service
systemctl daemon-reload
systemctl reset-failed shampoore 2>/dev/null || true

if [ "$PURGE" -eq 1 ]; then
    PANEL_BIN="/opt/shampoore/bin/shampoore-panel"
    PANEL_DB_PATH="/var/lib/shampoore/panel.db"
    if [ -r /etc/shampoore/panel.env ]; then
        ENV_DB="$(grep -E '^PANEL_DB=' /etc/shampoore/panel.env | tail -n1 | cut -d= -f2- | tr -d '\r' || true)"
        if [ -n "${ENV_DB:-}" ]; then
            PANEL_DB_PATH="$ENV_DB"
        fi
    fi
    if [ -x "$PANEL_BIN" ] && [ -f "$PANEL_DB_PATH" ]; then
        echo "==> cleaning up remote nodes (best effort)"
        if ! "$PANEL_BIN" --cleanup-all-nodes --db="$PANEL_DB_PATH"; then
            echo "warning: remote node cleanup finished with errors; continuing with purge" >&2
        fi
    else
        echo "==> skipping remote node cleanup (panel binary or database not found)"
    fi
fi

echo "==> removing binaries"
rm -rf /opt/shampoore

if [ "$PURGE" -eq 1 ]; then
    echo "==> removing state and config"
    rm -rf /var/lib/shampoore /etc/shampoore
    if id -u shampoore >/dev/null 2>&1; then
        userdel shampoore 2>/dev/null || true
    fi
    echo "✓ purged"
else
    echo "kept: /var/lib/shampoore  /etc/shampoore  (use --purge to remove)"
    echo "✓ uninstalled"
fi