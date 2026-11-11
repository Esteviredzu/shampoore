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

cd "$(dirname "$0")"

REPO="Esteviredzu/shampoore"

VERSION_RAW="${1:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
VERSION="${VERSION_RAW#v}"
if [ -z "$VERSION" ]; then
    VERSION="dev"
fi

ARCHES=(amd64 arm64)
DIST="dist"
STAGE_ROOT="$DIST/.stage"

die() { echo "error: $*" >&2; exit 1; }
log() { echo "==> $*"; }

command -v go  >/dev/null || die "go is required"
command -v tar >/dev/null || die "tar is required"
[ -f go.mod ] || die "run from repo root (go.mod not found)"
[ -f deploy/systemd/install.sh ]         || die "deploy/systemd/install.sh is missing"
[ -f deploy/systemd/uninstall.sh ]       || die "deploy/systemd/uninstall.sh is missing"
[ -f deploy/systemd/shampoore.service ]  || die "deploy/systemd/shampoore.service is missing"

log "version: $VERSION"
log "arches:  ${ARCHES[*]}"

rm -rf "$DIST"
mkdir -p "$DIST" "$STAGE_ROOT"

LDFLAGS="-s -w"

for ARCH in "${ARCHES[@]}"; do
    STAGE="$STAGE_ROOT/shampoore-linux-$ARCH"
    mkdir -p "$STAGE"

    log "building linux/$ARCH"
    CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
        go build -trimpath -ldflags="$LDFLAGS" \
        -o "$STAGE/shampoore-panel"  ./cmd/shampoore-panel
    CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
        go build -trimpath -ldflags="$LDFLAGS" \
        -o "$STAGE/shampoore-server" ./cmd/shampoore-server

    log "copying install helpers for $ARCH"
    install -m 0755 deploy/systemd/install.sh        "$STAGE/install.sh"
    install -m 0755 deploy/systemd/uninstall.sh      "$STAGE/uninstall.sh"
    install -m 0644 deploy/systemd/shampoore.service "$STAGE/shampoore.service"

    OUT="$DIST/shampoore-linux-$ARCH.tar.gz"
    log "packing $OUT"
    tar -czf "$OUT" -C "$STAGE_ROOT" "shampoore-linux-$ARCH"
done

log "copying install.sh and uninstall.sh to dist/ root"
cp -p deploy/systemd/install.sh   "$DIST/install.sh"
cp -p deploy/systemd/uninstall.sh "$DIST/uninstall.sh"
chmod +x "$DIST/install.sh" "$DIST/uninstall.sh"

# --- Android AAR (optional, gracefully skipped) ---------------------------
AAR_BUILT=0

if [ -z "${ANDROID_HOME:-}" ] && [ -z "${ANDROID_SDK_ROOT:-}" ]; then
    for d in "$HOME/Android/Sdk" "$HOME/Android/sdk" "$HOME/android-sdk" "/opt/android-sdk" "/usr/lib/android-sdk"; do
        if [ -d "$d/cmdline-tools" ] || [ -d "$d/platform-tools" ]; then
            export ANDROID_HOME="$d"
            break
        fi
    done
fi

if [ -z "${ANDROID_NDK_HOME:-}" ] && [ -n "${ANDROID_HOME:-}" ] && [ -d "$ANDROID_HOME/ndk" ]; then
    NDK_LATEST="$(ls -1 "$ANDROID_HOME/ndk" 2>/dev/null | sort -V | tail -n1 || true)"
    if [ -n "$NDK_LATEST" ]; then
        export ANDROID_NDK_HOME="$ANDROID_HOME/ndk/$NDK_LATEST"
    fi
fi

if command -v gomobile >/dev/null 2>&1 \
   && [ -n "${ANDROID_HOME:-}${ANDROID_SDK_ROOT:-}" ] \
   && [ -n "${ANDROID_NDK_HOME:-}" ]; then
    log "building Android AAR"
    log "  ANDROID_HOME=$ANDROID_HOME"
    log "  ANDROID_NDK_HOME=$ANDROID_NDK_HOME"

    rm -f "$DIST/shampoore.aar" "$DIST/shampoore-sources.jar"
    if gomobile bind \
        -target=android/arm64,android/arm \
        -androidapi 21 \
        -ldflags="-checklinkname=0" \
        -o "$DIST/shampoore.aar" \
        ./mobile; then
        rm -f "$DIST/shampoore-sources.jar"
        AAR_BUILT=1
        log "AAR built: $DIST/shampoore.aar"
    else
        log "warning: gomobile bind failed; skipping AAR"
    fi
else
    log "skipping AAR"
    log "  gomobile:         $(command -v gomobile >/dev/null 2>&1 && echo yes || echo no)"
    log "  ANDROID_HOME:     ${ANDROID_HOME:-<unset>}"
    log "  ANDROID_NDK_HOME: ${ANDROID_NDK_HOME:-<unset>}"
fi

log "generating SHA256SUMS"
(
    cd "$DIST"
    sha256sum install.sh uninstall.sh shampoore-linux-*.tar.gz > SHA256SUMS
    if [ "$AAR_BUILT" -eq 1 ]; then
        sha256sum shampoore.aar >> SHA256SUMS
    fi
)

rm -rf "$STAGE_ROOT"

echo
echo "✓ release v$VERSION built"
echo
ls -lh "$DIST" | sed 's/^/  /'
echo

if command -v gh >/dev/null 2>&1; then
    echo "publish:  gh release create v$VERSION dist/* --title \"v$VERSION\" --notes \"Release v$VERSION\""
else
    echo "publish:  git tag -a v$VERSION -m \"v$VERSION\" && git push origin v$VERSION"
    echo "          then upload ./dist/* to the GitHub release page for that tag"
fi
echo