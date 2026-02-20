#!/usr/bin/env bash
# HadoopScope — Install script
# Usage: curl -fsSL https://raw.githubusercontent.com/disoardi/hadoopscope/main/install.sh | bash
# Or:    ./install.sh [--update]

set -euo pipefail

REPO="https://github.com/disoardi/hadoopscope"
INSTALL_DIR="${HADOOPSCOPE_DIR:-$HOME/.hadoopscope}"
BIN_DIR="${HOME}/.local/bin"
TOOL_DIR="${INSTALL_DIR}/repo"

RED='\033[0;31m'
GRN='\033[0;32m'
YEL='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GRN}[hadoopscope]${NC} $*"; }
warn()  { echo -e "${YEL}[hadoopscope]${NC} $*"; }
error() { echo -e "${RED}[hadoopscope] ERROR:${NC} $*" >&2; exit 1; }

# --- Check Python 3.6+ ---
if ! command -v python3 &>/dev/null; then
    error "python3 not found. Install Python 3.6+ first."
fi

py_version=$(python3 -c "import sys; print('{}.{}'.format(*sys.version_info[:2]))")
py_major=$(python3 -c "import sys; print(sys.version_info.major)")
py_minor=$(python3 -c "import sys; print(sys.version_info.minor)")

if [ "$py_major" -lt 3 ] || ([ "$py_major" -eq 3 ] && [ "$py_minor" -lt 6 ]); then
    error "Python 3.6+ required, found $py_version"
fi
info "Python $py_version OK"

# --- Check git ---
if ! command -v git &>/dev/null; then
    error "git not found. Install git first."
fi

# --- Install or update ---
if [ -d "${TOOL_DIR}/.git" ]; then
    if [ "${1:-}" = "--update" ] || [ "${1:-}" = "-u" ]; then
        info "Updating HadoopScope..."
        git -C "${TOOL_DIR}" fetch --quiet origin
        git -C "${TOOL_DIR}" reset --hard origin/main --quiet
        info "Updated to $(git -C "${TOOL_DIR}" describe --tags --always)"
    else
        info "HadoopScope already installed at ${TOOL_DIR}"
        info "Run with --update to upgrade to the latest version."
    fi
else
    info "Installing HadoopScope into ${TOOL_DIR}..."
    mkdir -p "${INSTALL_DIR}"
    git clone --depth=1 --quiet "${REPO}.git" "${TOOL_DIR}"
    info "Cloned $(git -C "${TOOL_DIR}" describe --tags --always)"
fi

# --- Symlink CLI ---
mkdir -p "${BIN_DIR}"
WRAPPER="${BIN_DIR}/hadoopscope"

cat > "${WRAPPER}" <<'WRAPPER_EOF'
#!/usr/bin/env bash
exec python3 "${HOME}/.hadoopscope/repo/hadoopscope.py" "$@"
WRAPPER_EOF

chmod +x "${WRAPPER}"
info "Installed CLI wrapper at ${WRAPPER}"

# --- Config example ---
CFG_DIR="${INSTALL_DIR}/config"
if [ ! -f "${CFG_DIR}/hadoopscope.yaml" ]; then
    mkdir -p "${CFG_DIR}"
    cp "${TOOL_DIR}/config/example.yaml" "${CFG_DIR}/hadoopscope.yaml"
    warn "Config example copied to ${CFG_DIR}/hadoopscope.yaml — edit before use!"
fi

# --- PATH reminder ---
if ! echo "$PATH" | grep -q "${BIN_DIR}"; then
    warn "${BIN_DIR} is not in your PATH."
    warn "Add this to your shell profile:"
    warn "  export PATH=\"\${HOME}/.local/bin:\$PATH\""
fi

echo ""
info "Installation complete!"
info ""
info "Next steps:"
info "  1. Edit config:  ${CFG_DIR}/hadoopscope.yaml"
info "  2. Set password: export AMBARI_PASS=yourpassword"
info "  3. Run:          hadoopscope --env prod-hdp --dry-run"
info ""
info "For help: hadoopscope --help"
