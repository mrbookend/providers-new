#!/usr/bin/env bash
set -euo pipefail
# lit: no-op commit to nudge Streamlit Cloud
"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/redeploy-nudge.sh"
