# Shared paths for scripts in subdirectories (max one level under scripts/).
# Source from any script: source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
_ESPX_COMMON="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ESPX_SCRIPTS_ROOT="$(cd "$_ESPX_COMMON/.." && pwd)"
ESPX_REPO_ROOT="$(cd "$ESPX_SCRIPTS_ROOT/.." && pwd)"
ROOT="$ESPX_REPO_ROOT"
SCRIPTS="$ESPX_SCRIPTS_ROOT"
export ROOT SCRIPTS ESPX_SCRIPTS_ROOT ESPX_REPO_ROOT

# Optional: source "$SCRIPTS/_common/safe_paths.sh" for rm/rsync/codegen guards.
