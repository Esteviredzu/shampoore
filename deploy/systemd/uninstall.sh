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