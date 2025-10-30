#!/usr/bin/env bash
set -euo pipefail
# cloud: venv + deps + compile + streamlit run
"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/dev-run.sh"
