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

REPO="${SHAMPOORE_REPO:-Esteviredzu/shampoore}"
VERSION="${SHAMPOORE_VERSION:-latest}"
LISTEN="${SHAMPOORE_LISTEN:-127.0.0.1:8080}"
TOKEN_OVERRIDE="${SHAMPOORE_TOKEN:-}"

PREFIX="/opt/shampoore"
STATE_DIR="/var/lib/shampoore"
CONFIG_DIR="/etc/shampoore"
USER_NAME="shampoore"

die() { echo "error: $*" >&2; exit 1; }
log() { echo "==> $*"; }

[ "${EUID:-0}" -eq 0 ] || die "run with sudo"

case "$(uname -m)" in
    x86_64|amd64) ARCH=amd64 ;;
    aarch64|arm64) ARCH=arm64 ;;
    *) die "unsupported architecture: $(uname -m)" ;;
esac

SCRIPT_DIR=""
if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

if [ -n "$SCRIPT_DIR" ] \
   && [ -x "$SCRIPT_DIR/shampoore-panel" ] \
   && [ -x "$SCRIPT_DIR/shampoore-server" ] \
   && [ -f "$SCRIPT_DIR/shampoore.service" ]; then
    SRC_DIR="$SCRIPT_DIR"
    log "using local binaries from $SRC_DIR"
else
    command -v curl >/dev/null || die "curl is required"
    command -v tar  >/dev/null || die "tar is required"

    if [ "$VERSION" = "latest" ]; then
        URL="https://github.com/$REPO/releases/latest/download/shampoore-linux-$ARCH.tar.gz"
    else
        URL="https://github.com/$REPO/releases/download/$VERSION/shampoore-linux-$ARCH.tar.gz"
    fi

    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT

    log "downloading $URL"
    curl -fL --retry 3 --connect-timeout 10 -o "$TMP/shampoore.tar.gz" "$URL" \
        || die "download failed"
    log "extracting"
    tar -xzf "$TMP/shampoore.tar.gz" -C "$TMP" || die "extract failed"

    SRC_DIR="$(find "$TMP" -mindepth 1 -maxdepth 1 -type d -name 'shampoore-*' | head -n1)"
    [ -n "$SRC_DIR" ] && [ -d "$SRC_DIR" ] || die "unexpected tarball layout"
fi

[ -x "$SRC_DIR/shampoore-panel" ]   || die "missing shampoore-panel"
[ -x "$SRC_DIR/shampoore-server" ]  || die "missing shampoore-server"
[ -f "$SRC_DIR/shampoore.service" ] || die "missing shampoore.service"

if ! id -u "$USER_NAME" >/dev/null 2>&1; then
    log "creating system user $USER_NAME"
    useradd --system --no-create-home --shell /usr/sbin/nologin "$USER_NAME"
fi

log "installing binaries to $PREFIX/bin"
install -d -m 0755 "$PREFIX/bin"
install -o root -g root -m 0755 "$SRC_DIR/shampoore-panel"  "$PREFIX/bin/shampoore-panel"
install -o root -g root -m 0755 "$SRC_DIR/shampoore-server" "$PREFIX/bin/shampoore-server"
for extra in shampoore-relay shampoore-node-agent; do
    if [ -x "$SRC_DIR/$extra" ]; then
        install -o root -g root -m 0755 "$SRC_DIR/$extra" "$PREFIX/bin/$extra"
    else
        log "warning: $extra is missing; remote node deployment will require a manual build"
    fi
done

log "preparing state directory"
install -d -o "$USER_NAME" -g "$USER_NAME" -m 0755 "$STATE_DIR/configs"

if [ -f "$SRC_DIR/uninstall.sh" ]; then
    install -o root -g root -m 0755 "$SRC_DIR/uninstall.sh" "$PREFIX/uninstall.sh"
fi

install -d -m 0750 -o root -g "$USER_NAME" "$CONFIG_DIR"

if [ ! -f "$CONFIG_DIR/panel.env" ]; then
    if [ -n "$TOKEN_OVERRIDE" ]; then
        TOKEN="$TOKEN_OVERRIDE"
        log "using provided SHAMPOORE_TOKEN"
    else
        log "generating random panel token"
        if command -v openssl >/dev/null 2>&1; then
            TOKEN="$(openssl rand -hex 32)"
        else
            TOKEN="$(head -c32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
        fi
    fi
    umask 0027
    cat > "$CONFIG_DIR/panel.env" <<EOF
PANEL_TOKEN=$TOKEN
PANEL_LISTEN=$LISTEN
PANEL_DB=/var/lib/shampoore/panel.db
PUBLIC_LISTEN=
PUBLIC_URL=
EOF
else
    # Backward compatible migration of an existing env file.
    ensure_env() {
        local key="$1" value="$2"
        grep -q "^${key}=" "$CONFIG_DIR/panel.env" || printf '%s=%s\n' "$key" "$value" >> "$CONFIG_DIR/panel.env"
    }
    ensure_env PANEL_DB /var/lib/shampoore/panel.db
    ensure_env PUBLIC_LISTEN ""
    ensure_env PUBLIC_URL ""
fi
chown root:"$USER_NAME" "$CONFIG_DIR/panel.env"
chmod 0640 "$CONFIG_DIR/panel.env"

log "installing systemd unit"
install -o root -g root -m 0644 "$SRC_DIR/shampoore.service" /etc/systemd/system/shampoore.service

log "reloading systemd"
systemctl daemon-reload
systemctl enable shampoore >/dev/null
systemctl restart shampoore

sleep 1
if systemctl is-active --quiet shampoore; then
    TOKEN_NOW="$(grep '^PANEL_TOKEN=' "$CONFIG_DIR/panel.env" | cut -d= -f2-)"
    LISTEN_NOW="$(grep '^PANEL_LISTEN=' "$CONFIG_DIR/panel.env" | cut -d= -f2-)"
    cat <<EOF

  ✓ shampoore is running

    panel:     http://$LISTEN_NOW
    open:      http://$LISTEN_NOW/
    token:     $TOKEN_NOW

  Enter the token on the panel login page (it is kept in an HttpOnly cookie).

    logs:      journalctl -u shampoore -f
    configs:   $STATE_DIR/configs/
    uninstall: sudo $PREFIX/uninstall.sh --purge

EOF
else
    echo "✗ shampoore failed to start; recent logs:" >&2
    journalctl -u shampoore -n 40 --no-pager >&2
    exit 1
fi