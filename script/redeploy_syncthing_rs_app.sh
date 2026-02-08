#!/bin/bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP_SRC="${APP_SRC:-/Applications/Syncthing.app}"
APP_DST="${APP_DST:-/Applications/Syncthing-Optimized.app}"
APP_RES="$APP_DST/Contents/Resources/syncthing"
WRAPPER_SRC="$REPO_ROOT/script/syncthing_rs_app_wrapper.sh"
RUST_BIN="$REPO_ROOT/syncthing-rs/target/release/syncthing-rs"

if [[ ! -d "$APP_SRC" ]]; then
  echo "source app bundle not found: $APP_SRC" >&2
  exit 1
fi

echo "[1/6] Building syncthing-rs release binary"
cargo build --release --manifest-path "$REPO_ROOT/syncthing-rs/Cargo.toml" --bin syncthing-rs

if [[ ! -x "$RUST_BIN" ]]; then
  echo "rust binary missing after build: $RUST_BIN" >&2
  exit 1
fi

echo "[2/6] Recreating optimized app bundle"
rm -rf "$APP_DST"
cp -R "$APP_SRC" "$APP_DST"

echo "[3/6] Installing syncthing-rs runtime into app bundle"
mkdir -p "$APP_RES"
if [[ -x "$APP_RES/syncthing" ]]; then
  cp "$APP_RES/syncthing" "$APP_RES/syncthing.go.bin"
fi
cp "$RUST_BIN" "$APP_RES/syncthing-rs"
cp "$WRAPPER_SRC" "$APP_RES/syncthing"
chmod +x "$APP_RES/syncthing-rs" "$APP_RES/syncthing"

echo "[4/6] Updating app metadata"
/usr/libexec/PlistBuddy -c "Set :CFBundleName Syncthing-Optimized" "$APP_DST/Contents/Info.plist" || true
/usr/libexec/PlistBuddy -c "Set :CFBundleDisplayName Syncthing-Optimized" "$APP_DST/Contents/Info.plist" || true

echo "[5/6] Sanitizing and codesigning"
xattr -cr "$APP_DST"
codesign --force --deep --sign - "$APP_DST"

echo "[6/6] Verifying deployed binaries"
ls -la "$APP_RES/syncthing" "$APP_RES/syncthing-rs"

echo
echo "Redeploy complete."
echo "Launch: open -a \"$APP_DST\""
echo "Runtime launch log: /tmp/syncthing-rs-app-launch.log"
