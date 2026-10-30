#!/usr/bin/env bash
# CD step: publish dags/ + the twse_etl package into the folder Airflow mounts as its DAGs folder.
#
#   scripts/deploy_dags.sh                      # local: deploy/airflow_dags_mount (docker-compose mount)
#   scripts/deploy_dags.sh /path/to/target      # CI: a runner folder that is uploaded as an artifact
#
# AWS (MWAA) uses the same bundle: `aws s3 sync "$TARGET" "s3://$MWAA_BUCKET/dags/" --delete`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-$ROOT/deploy/airflow_dags_mount}"
LOG="${DEPLOY_LOG:-$ROOT/.local/deploy.log}"
VERSION="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
mkdir -p "$(dirname "$LOG")"

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG"; }

# 1. Guard against anomaly scenario 3 ("after a merge every DAG disappeared"): never ship an empty bundle.
dag_count=$(find "$ROOT/dags" -maxdepth 1 -name '*.py' | wc -l | tr -d ' ')
if [ "$dag_count" -eq 0 ]; then
  log "ABORT version=$VERSION: dags/ contains no DAG file, refusing to wipe the target"
  exit 1
fi

# 2. Everything we ship must at least compile.
python3 -m py_compile "$ROOT"/dags/*.py "$ROOT"/src/twse_etl/*.py

# 3. Build the bundle in a temp dir, then mirror it (rsync --delete also removes deleted DAGs).
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
cp "$ROOT"/dags/*.py "$STAGE"/
cp -R "$ROOT/src/twse_etl" "$STAGE/twse_etl"
find "$STAGE" -name __pycache__ -prune -exec rm -rf {} +
printf 'twse_etl/\n' > "$STAGE/.airflowignore"  # library code, not DAG files
printf '%s\n' "$VERSION" > "$STAGE/DEPLOYED_VERSION"

mkdir -p "$TARGET"
rsync -a --delete "$STAGE"/ "$TARGET"/

# 4. Keep every released bundle for a one-command rollback: tar -xzf deploy/releases/<sha>.tgz -C <target>
mkdir -p "$ROOT/deploy/releases"
tar -czf "$ROOT/deploy/releases/$VERSION.tgz" -C "$STAGE" .

log "deployed version=$VERSION dag_files=$dag_count target=$TARGET"
