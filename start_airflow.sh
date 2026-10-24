#!/bin/bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example to .env and set private credentials first." >&2
  exit 1
fi

docker compose up --build -d
echo "Airflow UI: http://localhost:8080"
echo "Inspect services with: docker compose ps"
