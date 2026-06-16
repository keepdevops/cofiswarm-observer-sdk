#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
test -f "${ROOT}/examples/structlog_setup.py"
test -f "${ROOT}/examples/sink-agent-logs.yaml"
echo "ok: observer-sdk examples"
