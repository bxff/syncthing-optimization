#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
RUST_BIN="$APP_DIR/syncthing-rs"
CONFIG_XML="${SYNCTHING_CONFIG_XML:-$HOME/Library/Application Support/Syncthing/config.xml}"
DB_ROOT="${SYNCTHING_RS_DB_ROOT:-$HOME/Library/Application Support/Syncthing/index-rs}"
API_LISTEN="${SYNCTHING_RS_API_LISTEN:-127.0.0.1:8384}"
BEP_LISTEN="${SYNCTHING_RS_LISTEN:-0.0.0.0:22000}"
MEM_MAX_MB="${SYNCTHING_RS_MEMORY_MAX_MB:-50}"
MAX_PEERS="${SYNCTHING_RS_MAX_PEERS:-32}"

expand_tilde() {
  local p="$1"
  case "$p" in
    "~") printf '%s\n' "$HOME" ;;
    "~/"*) printf '%s\n' "$HOME/${p#\~/}" ;;
    *) printf '%s\n' "$p" ;;
  esac
}

declare -a FOLDER_ARGS
FOLDER_ARGS=()
if [[ -f "$CONFIG_XML" ]]; then
  while IFS=$'\t' read -r folder_id folder_path; do
    [[ -z "$folder_id" || -z "$folder_path" ]] && continue
    folder_path="$(expand_tilde "$folder_path")"
    FOLDER_ARGS+=(--folder "${folder_id}:${folder_path}")
  done < <(/usr/bin/perl -ne 'if (/<folder\b/) { my ($id) = /id="([^"]+)"/; my ($path) = /path="([^"]+)"/; if (defined $id && defined $path) { print "$id\t$path\n"; } }' "$CONFIG_XML")

  gui_addr="$(/usr/bin/perl -0777 -ne 'if (/<gui\b[^>]*>.*?<address>([^<]+)<\/address>/s) { print $1; }' "$CONFIG_XML" 2>/dev/null || true)"
  if [[ -n "$gui_addr" ]]; then
    API_LISTEN="$gui_addr"
  fi
fi

if [[ ${#FOLDER_ARGS[@]} -eq 0 ]]; then
  default_path="${SYNCTHING_RS_DEFAULT_FOLDER_PATH:-$HOME/Sync}"
  FOLDER_ARGS=(--folder "default:${default_path}")
fi

mkdir -p "$DB_ROOT"

declare -a FILTERED_ARGS
FILTERED_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --no-browser|--no-restart|--no-default-folder|--logflags=*|--logfile=*|--audit|--auditfile=*)
      ;;
    *)
      FILTERED_ARGS+=("$arg")
      ;;
  esac
done

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] launching syncthing-rs via app wrapper"
  echo "config_xml=$CONFIG_XML"
  echo "db_root=$DB_ROOT"
  echo "api_listen=$API_LISTEN"
  echo "bep_listen=$BEP_LISTEN"
  printf 'folders='; printf '%s ' "${FOLDER_ARGS[@]}"; echo
} >> /tmp/syncthing-rs-app-launch.log

exec "$RUST_BIN" daemon \
  "${FOLDER_ARGS[@]}" \
  --api-listen "$API_LISTEN" \
  --listen "$BEP_LISTEN" \
  --db-root "$DB_ROOT" \
  --memory-max-mb "$MEM_MAX_MB" \
  --max-peers "$MAX_PEERS" \
  "${FILTERED_ARGS[@]}"
