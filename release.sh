#!/usr/bin/env bash
set -euo pipefail

print_banner() {
    local cyan='' reset='' animate=0
    if [ -t 1 ] && [ "${TERM:-dumb}" != "dumb" ]; then
        cyan=$'\033[1;36m'
        reset=$'\033[0m'
        if sleep 0.001 2>/dev/null; then animate=1; fi
    fi
    printf '%s' "$cyan"
    while IFS= read -r line; do
        printf '%s\n' "$line"
        if [ "$animate" -eq 1 ]; then sleep 0.03; fi
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

usage() {
    cat <<EOF
Usage:
  $0 [VERSION] [build options]
  $0 --list
  $0 --delete TAG [-d TAG ...]
  $0 --delete-all [--yes]
  $0 -h | --help

Build:
  VERSION           Exact version to build (e.g. 0.33, 0.33.0,
                    0.33.0-beta.1, v0.33). Short versions are normalized
                    to full SemVer (0.33 -> 0.33.0).
                    If omitted, derived from the current git state.
  -b, --beta        Force a beta (prerelease) build
  -s, --stable      Force a stable release build
  --no-publish      Build only; do not create a GitHub release

Release management (requires gh CLI):
  -l, --list        List releases, newest first
  -d, --delete TAG  Delete release with this tag AND the remote git tag
                    (repeat: -d v0.33.0-beta.1 -d v0.33.0-beta.2)
  --delete-all      Delete ALL releases and their remote tags
  -y, --yes         Skip confirmation prompt (only for --delete-all)

  -h, --help        Show this help

Beta numbering (--beta):
  When --beta is used with a clean version (0.33 or 0.33.0) or without a
  version, the next -beta.N number is picked automatically from existing
  GitHub releases and git tags. No commit hash is appended.
    ./release.sh --beta 0.33        # -> v0.33.0-beta.1, then .2, .3, ...
    ./release.sh --beta             # base version from the latest git tag
    ./release.sh --beta 0.33-beta.5 # explicit suffix, used as-is
  The release is published on GitHub with --prerelease.

Stable releases (--stable):
  The -beta.N suffix is stripped and the final release is published without
  --prerelease.
    ./release.sh --stable 0.33        # -> v0.33.0
    ./release.sh --stable             # -> v0.33.0 after v0.33.0-beta.N
    ./release.sh 0.33                 # same as --stable 0.33

Version resolution (when VERSION is not given and no --beta/--stable):
  - exact tag vX.Y.Z            -> stable X.Y.Z
  - N commits after a tag      -> beta <tag>-dev.<sha>
  - no tags                    -> beta 0.0.0-dev.<sha>

Examples:
  $0                              # build from current git state
  $0 0.33                         # stable v0.33.0
  $0 --beta 0.33                  # beta v0.33.0-beta.<next>
  $0 --stable 0.33                # final v0.33.0
  $0 0.33.0-beta.4                # explicit beta
  $0 --list
  $0 --delete v0.33.0-beta.1
  $0 --delete-all --yes
EOF
}

die() { echo "error: $*" >&2; exit 1; }
log() { echo "==> $*"; }

# normalize_core prints MAJOR.MINOR.PATCH (e.g. 0.33 -> 0.33.0) or fails.
normalize_core() {
    local raw="${1#[vV]}" core part
    core="${raw%%-*}"
    core="${core%%+*}"
    [[ "$core" =~ ^[0-9]+(\.[0-9]+){0,2}$ ]] || return 1
    local -a parts=()
    IFS='.' read -r -a parts <<< "$core"
    if [ "${#parts[@]}" -lt 1 ] || [ "${#parts[@]}" -gt 3 ]; then
        return 1
    fi
    local -a out=()
    for part in "${parts[@]}"; do
        [[ "$part" =~ ^[0-9]+$ ]] || return 1
        out+=("$((10#$part))")
    done
    while [ "${#out[@]}" -lt 3 ]; do out+=(0); done
    printf '%s.%s.%s' "${out[0]}" "${out[1]}" "${out[2]}"
}

# normalize_version prints a full SemVer without the leading "v", keeping an
# explicit prerelease suffix (0.33 -> 0.33.0, 0.33-beta.2 -> 0.33.0-beta.2).
normalize_version() {
    local core pre="" rest="$1"
    rest="${rest#[vV]}"
    rest="${rest//[[:space:]]/}"
    if [ -z "$rest" ]; then
        return 1
    fi
    if [[ "$rest" == *-* ]]; then
        pre="${rest#*-}"
        rest="${rest%%-*}"
        [ -n "$pre" ] || return 1
    fi
    rest="${rest%%+*}"
    core="$(normalize_core "$rest")" || return 1
    if [ -n "$pre" ]; then
        printf '%s-%s' "$core" "$pre"
    else
        printf '%s' "$core"
    fi
}

# latest_base_version returns the X.Y.Z core of the current/latest git tag.
latest_base_version() {
    local tag
    tag="$(git describe --tags --exact-match 2>/dev/null || git describe --tags --abbrev=0 2>/dev/null || true)"
    if [ -z "$tag" ]; then
        echo "0.0.0"
        return
    fi
    normalize_core "$tag" 2>/dev/null || echo "0.0.0"
}

# next_beta_number looks at GitHub releases, local tags and remote tags and
# returns the next free N for v<base>-beta.N.
next_beta_number() {
    local base="$1" max=0 n t line src=""
    count_tags() {
        local tag
        while IFS= read -r tag; do
            tag="${tag##*refs/tags/}"
            tag="${tag%%^*}"
            case "$tag" in
                "v${base}-beta."*)
                    n="${tag#v${base}-beta.}"
                    if [[ "$n" =~ ^[0-9]+$ ]]; then
                        n=$((10#$n))
                        if (( n > max )); then max=$n; fi
                    fi
                    ;;
            esac
        done <<< "$1"
    }
    if command -v gh >/dev/null 2>&1; then
        if t="$(gh release list --limit 1000 --json tagName -q '.[].tagName' 2>/dev/null)" && [ -n "$t" ]; then
            src="gh release list"
            count_tags "$t"
        fi
    fi
    if t="$(git tag -l "v${base}-beta.*" 2>/dev/null)" && [ -n "$t" ]; then
        src="${src:+$src, }git tag"
        count_tags "$t"
    fi
    # git ls-remote is authoritative: a pushed tag is a real collision.
    if t="$(git ls-remote --tags origin "refs/tags/v${base}-beta.*" 2>/dev/null)" && [ -n "$t" ]; then
        src="${src:+$src, }git ls-remote"
        count_tags "$t"
    fi
    if [ -z "$src" ]; then
        echo "==> warning: could not query GitHub/git for v${base}-beta.* ; defaulting to .1" >&2
    fi
    echo $((max + 1))
}

MODE=""
VERSION_ARG=""
DO_LIST=0
DO_DELETE_ALL=0
ASSUME_YES=0
SHOW_HELP=0
NO_PUBLISH=0
DELETE_TAGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        -b|--beta)     MODE="beta";     shift ;;
        -s|--stable)   MODE="stable";   shift ;;
        -l|--list)     DO_LIST=1;       shift ;;
        --delete-all)  DO_DELETE_ALL=1; shift ;;
        -y|--yes)      ASSUME_YES=1;    shift ;;
        --no-publish)  NO_PUBLISH=1;    shift ;;
        -h|--help)     SHOW_HELP=1;     shift ;;
        -d|--delete)
            shift
            [ $# -gt 0 ] || die "--delete requires a tag argument"
            [ "${1#-}" = "$1" ] || die "--delete requires a tag, got option: $1"
            DELETE_TAGS+=("$1"); shift
            ;;
        --) shift; break ;;
        -*) die "unknown option: $1 (try --help)" ;;
        *)
            [ -z "$VERSION_ARG" ] || die "unexpected argument: $1"
            VERSION_ARG="$1"; shift ;;
    esac
done

[ $# -eq 0 ] || die "unexpected trailing arguments: $*"

if [ "$SHOW_HELP" -eq 1 ]; then
    usage; exit 0
fi

cd "$(dirname "$0")"

active_ops=0
[ "$DO_LIST" -eq 1 ]       && active_ops=$((active_ops + 1))
[ "$DO_DELETE_ALL" -eq 1 ] && active_ops=$((active_ops + 1))
[ ${#DELETE_TAGS[@]} -gt 0 ] && active_ops=$((active_ops + 1))
if [ "$active_ops" -gt 1 ]; then
    die "conflicting operations: pick one of --list / --delete / --delete-all"
fi
if [ ${#DELETE_TAGS[@]} -gt 0 ] && [ -n "$VERSION_ARG" ]; then
    die "can't combine --delete with a version argument; use -d TAG for each"
fi

if [ "$DO_LIST" -eq 1 ]; then
    command -v gh >/dev/null || die "gh CLI is required for --list"
    log "listing releases"
    gh release list --limit 1000
    exit 0
fi

if [ "$DO_DELETE_ALL" -eq 1 ]; then
    command -v gh >/dev/null || die "gh CLI is required for --delete-all"
    tag_list="$(gh release list --limit 1000 --json tagName -q '.[].tagName')" \
        || die "failed to list releases (try 'gh auth status')"
    all_tags=()
    while IFS= read -r t; do
        [ -n "$t" ] && all_tags+=("$t")
    done <<< "$tag_list"
    if [ ${#all_tags[@]} -eq 0 ]; then
        log "no releases to delete"; exit 0
    fi
    echo "About to delete ${#all_tags[@]} release(s):"
    printf '  %s\n' "${all_tags[@]}"
    if [ "$ASSUME_YES" -ne 1 ]; then
        [ -t 0 ] || die "refusing to delete all without --yes (stdin is not a tty)"
        read -r -p "Type 'yes' to confirm: " answer
        [ "$answer" = "yes" ] || die "aborted"
    fi
    failed=0
    for t in "${all_tags[@]}"; do
        if gh release delete "$t" --yes --cleanup-tag; then
            log "deleted $t"
        else
            log "failed to delete $t"; failed=$((failed + 1))
        fi
    done
    [ "$failed" -eq 0 ] || die "$failed deletion(s) failed"
    exit 0
fi

if [ ${#DELETE_TAGS[@]} -gt 0 ]; then
    command -v gh >/dev/null || die "gh CLI is required for --delete"
    failed=0
    for t in "${DELETE_TAGS[@]}"; do
        if gh release delete "$t" --yes --cleanup-tag; then
            log "deleted $t"
        else
            log "failed to delete $t"; failed=$((failed + 1))
        fi
    done
    [ "$failed" -eq 0 ] || die "$failed deletion(s) failed"
    exit 0
fi

# --- Build path ------------------------------------------------------
print_banner

command -v go  >/dev/null || die "go is required"
command -v tar >/dev/null || die "tar is required"
[ -f go.mod ] || die "run from repo root (go.mod not found)"
[ -f deploy/systemd/install.sh ]         || die "deploy/systemd/install.sh is missing"
[ -f deploy/systemd/uninstall.sh ]       || die "deploy/systemd/uninstall.sh is missing"
[ -f deploy/systemd/shampoore.service ]  || die "deploy/systemd/shampoore.service is missing"

SHORT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"

if [ -n "$VERSION_ARG" ]; then
    if ! RAW="$(normalize_version "$VERSION_ARG")"; then
        die "invalid version '$VERSION_ARG' (expected e.g. 0.33, 0.33.0, 0.33.0-beta.2)"
    fi
elif [ "$MODE" = "beta" ] || [ "$MODE" = "stable" ]; then
    # --beta / --stable without a version: take the base from the latest tag
    # and drop any -beta.N / -dev.<sha> suffix.
    RAW="$(latest_base_version)"
else
    if TAG="$(git describe --tags --exact-match 2>/dev/null)"; then
        if ! RAW="$(normalize_version "$TAG")"; then
            die "invalid git tag '$TAG'"
        fi
    else
        RAW="$(latest_base_version)-dev.${SHORT_SHA}"
    fi
fi
[ -n "$RAW" ] || RAW="0.0.0-dev.${SHORT_SHA}"

if [ -z "$MODE" ]; then
    if [[ "$RAW" == *-* ]]; then MODE="beta"; else MODE="stable"; fi
fi

VERSION="$RAW"
if [ "$MODE" = "stable" ]; then
    if ! CORE="$(normalize_core "$RAW")"; then
        die "invalid stable version '$RAW'"
    fi
    if [[ "$RAW" != "$CORE" ]]; then
        log "stripping prerelease suffix: $RAW -> $CORE"
    fi
    VERSION="$CORE"
else
    if [[ "$RAW" == *-* ]]; then
        # Explicit -beta.N (or a -dev.<sha> snapshot) is used as-is.
        VERSION="$RAW"
    else
        NEXT_N="$(next_beta_number "$RAW")"
        VERSION="${RAW}-beta.${NEXT_N}"
        log "auto-numbered beta: $VERSION"
    fi
fi

VERSION_TAG="v${VERSION}"

ARCHES=(amd64 arm64)
DIST="dist"
STAGE_ROOT="$DIST/.stage"

log "mode:    $MODE"
log "version: $VERSION_TAG"
log "commit:  ${SHORT_SHA}"
log "arches:  ${ARCHES[*]}"

rm -rf "$DIST"
mkdir -p "$DIST" "$STAGE_ROOT"

LDFLAGS="-s -w -X github.com/Esteviredzu/shampoore/internal/version.Version=$VERSION_TAG"

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
    CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
        go build -trimpath -ldflags="$LDFLAGS" \
        -o "$STAGE/shampoore-relay"  ./cmd/shampoore-relay
    CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
        go build -trimpath -ldflags="$LDFLAGS" \
        -o "$STAGE/shampoore-node-agent" ./cmd/shampoore-node-agent

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

AAR_BUILT=0
if [ -z "${ANDROID_HOME:-}" ] && [ -z "${ANDROID_SDK_ROOT:-}" ]; then
    for d in "$HOME/Android/Sdk" "$HOME/Android/sdk" "$HOME/android-sdk" "/opt/android-sdk" "/usr/lib/android-sdk"; do
        if [ -d "$d/cmdline-tools" ] || [ -d "$d/platform-tools" ]; then
            export ANDROID_HOME="$d"; break
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
    if gomobile bind -target=android/arm64,android/arm -androidapi 21 \
        -ldflags="-checklinkname=0" -o "$DIST/shampoore.aar" ./mobile; then
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
    if [ "$AAR_BUILT" -eq 1 ]; then sha256sum shampoore.aar >> SHA256SUMS; fi
)

rm -rf "$STAGE_ROOT"

echo
if [ "$MODE" = "beta" ]; then
    echo "✓ BETA build $VERSION_TAG"
else
    echo "✓ STABLE release $VERSION_TAG"
fi
echo
ls -lh "$DIST" | sed 's/^/  /'
echo

PRERELEASE_FLAG=""
if [ "$MODE" = "beta" ]; then PRERELEASE_FLAG="--prerelease"; fi

print_publish_instructions() {
    echo "publish:  gh release create $VERSION_TAG dist/* \\"
    echo "            --title \"$VERSION_TAG\" --notes \"Release $VERSION_TAG\" ${PRERELEASE_FLAG}"
    echo "          (or: git tag -a $VERSION_TAG -m \"$VERSION_TAG\" && git push origin $VERSION_TAG,"
    echo "           then upload ./dist/* to the GitHub release for that tag)"
    if [ "$MODE" = "beta" ]; then
        echo "          (mark the release as 'Pre-release' on GitHub)"
    fi
}

if [ "$NO_PUBLISH" -eq 1 ]; then
    log "skipping GitHub publish (--no-publish)"
elif [[ "$VERSION" == *-dev.* ]]; then
    log "development snapshot $VERSION_TAG is built locally; publish it manually if needed"
    print_publish_instructions
elif command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
    log "publishing $VERSION_TAG to GitHub"
    if gh release create "$VERSION_TAG" dist/* \
        --title "$VERSION_TAG" \
        --notes "Release $VERSION_TAG" \
        --target "$(git rev-parse HEAD)" \
        ${PRERELEASE_FLAG:+"$PRERELEASE_FLAG"}; then
        log "published $VERSION_TAG"
    else
        die "failed to publish $VERSION_TAG"
    fi
else
    log "warning: gh CLI is unavailable or not authenticated; printing manual publish commands"
    print_publish_instructions
fi
echo