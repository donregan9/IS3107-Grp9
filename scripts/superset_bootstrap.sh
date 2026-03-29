#!/bin/sh
set -e

echo "[superset] Running database migrations..."
superset db upgrade

echo "[superset] Ensuring admin user exists..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

echo "[superset] Initializing app..."
superset init

IMPORT_DIR="${SUPERSET_IMPORT_DIR:-/app/superset_exports}"

if [ -d "$IMPORT_DIR" ]; then
  imported_any=0
  for bundle in "$IMPORT_DIR"/*.zip; do
    if [ ! -e "$bundle" ]; then
      continue
    fi

    imported_any=1
    echo "[superset] Importing dashboard bundle: $bundle"

    # Try newer CLI first, then legacy syntax for compatibility.
    if superset import-dashboards --path "$bundle" --username "${SUPERSET_ADMIN_USERNAME:-admin}" --overwrite; then
      echo "[superset] Imported with modern CLI options."
    elif superset import-dashboards -p "$bundle" -u "${SUPERSET_ADMIN_USERNAME:-admin}"; then
      echo "[superset] Imported with legacy CLI options."
    else
      echo "[superset] Warning: failed to import $bundle."
    fi
  done

  if [ "$imported_any" -eq 0 ]; then
    echo "[superset] No dashboard export bundles found in $IMPORT_DIR"
  fi
else
  echo "[superset] Import directory not found: $IMPORT_DIR"
fi

echo "[superset] Starting web server..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads