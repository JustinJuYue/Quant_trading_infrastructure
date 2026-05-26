#!/usr/bin/env bash
# ============================================================================
# Quant Trading Infrastructure - Bootstrap & Deployment Script
# Target: Ubuntu 24.04 LTS (DigitalOcean Droplet)
# Description: Fully idempotent setup of Julia/Python environments, 
#              systemd service registration, and safety validation.
# ============================================================================
set -euo pipefail

# --- Colored Log Helpers ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[INFO] $1${NC}"; }
warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }
fail() { echo -e "${RED}[FATAL] $1${NC}"; exit 1; }

# --- Path Resolution ---
# Resolve absolute path to project root regardless of where script is called
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
ENV_FILE="$ROOT_DIR/.env"

# Ensure we have root/sudo privileges before we start
if ! sudo -v >/dev/null 2>&1; then
    fail "This script requires sudo privileges to install dependencies and configure systemd."
fi

# ============================================================================
# Step 1: Directory Setup
# ============================================================================
log "1. Initializing system directories..."
mkdir -p "$LOG_DIR"

# ============================================================================
# Step 1.5: System Dependencies (Fix 3)
# ============================================================================
log "1.5 Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y \
    python3-venv \
    python3-pip \
    libzmq3-dev \
    wget \
    curl \
    sqlite3

# ============================================================================
# Step 2: Safety Enforcement & Credential Validation
# ============================================================================
log "2. Enforcing safety checks and .env configuration..."

if [ ! -f "$ENV_FILE" ]; then
    warn ".env file not found. Generating a safety template..."
    cat <<EOF > "$ENV_FILE"
KRAKEN_API_KEY=
KRAKEN_SECRET_KEY=
DRY_RUN=true
ZMQ_PORT=5555
EOF
    chmod 600 "$ENV_FILE"
    fail "A template .env file was generated at $ENV_FILE.\nPlease populate your Kraken API keys and re-run this script."
fi

# Secure the environment file
chmod 600 "$ENV_FILE"

# Helper function to extract env variables cleanly
get_env() {
    grep "^$1=" "$ENV_FILE" | cut -d '=' -f 2- | tr -d '"' | tr -d "'" | xargs
}

DRY_RUN_VAL=$(get_env "DRY_RUN" | tr '[:upper:]' '[:lower:]')
API_KEY=$(get_env "KRAKEN_API_KEY")
SECRET_KEY=$(get_env "KRAKEN_SECRET_KEY")

if [ "$DRY_RUN_VAL" != "true" ]; then
    fail "DRY_RUN is not set to 'true' in .env. Initial deployments MUST run in DRY_RUN=true to prevent accidental capital exposure. Aborting."
fi

if [ -z "$API_KEY" ] || [ -z "$SECRET_KEY" ]; then
    fail "KRAKEN_API_KEY or KRAKEN_SECRET_KEY is empty in .env. Please configure them and re-run."
fi

log "Safety checks passed: DRY_RUN is enforced."

# ============================================================================
# Step 3: Julia Environment Provisioning
# ============================================================================
log "3. Checking Julia environment..."
JULIA_VERSION="1.10.9"
INSTALL_JULIA=true

if command -v julia >/dev/null 2>&1; then
    # Fix 1: Strip whitespaces/newlines from version output
    INSTALLED_JULIA=$(julia -e 'print(VERSION)' | tr -d '[:space:]')
    if [ "$INSTALLED_JULIA" == "$JULIA_VERSION" ]; then
        log "Julia $JULIA_VERSION is already installed."
        INSTALL_JULIA=false
    else
        warn "Julia $INSTALLED_JULIA detected, but we require exactly $JULIA_VERSION."
    fi
fi

if [ "$INSTALL_JULIA" = true ]; then
    log "Downloading and installing Julia $JULIA_VERSION..."
    JULIA_TAR="julia-${JULIA_VERSION}-linux-x86_64.tar.gz"
    wget -q --show-progress "https://julialang-s3.julialang.org/bin/linux/x64/1.10/${JULIA_TAR}" -O "/tmp/${JULIA_TAR}"
    
    sudo tar -zxf "/tmp/${JULIA_TAR}" -C /opt
    sudo rm -rf /opt/julia
    sudo mv "/opt/julia-${JULIA_VERSION}" /opt/julia
    sudo ln -sf /opt/julia/bin/julia /usr/local/bin/julia
    rm "/tmp/${JULIA_TAR}"
    log "Julia $JULIA_VERSION installed to /opt/julia."
fi

log "Instantiating and precompiling Julia environment..."
(
    cd "$ROOT_DIR/julia_app"

    # Remove old Manifest.toml to avoid Julia version conflicts
    if [ -f "Manifest.toml" ]; then
        rm -f Manifest.toml
        log "Removed old Manifest.toml — will regenerate for current Julia version"
    fi

    julia --project=. -e 'using Pkg; Pkg.instantiate(); Pkg.precompile()'

    # Fix 4: Dynamically parse Project.toml to only verify actual dependencies
    if [ -f "Project.toml" ]; then
        # Extract package names from [deps] section (e.g., "ZMQ = ...")
        DEPS=$(grep -E '^[A-Za-z0-9_]+ =' Project.toml | cut -d' ' -f1 | paste -sd "," -)
        if [ -n "$DEPS" ]; then
            log "Verifying actual Julia packages: $DEPS"
            julia --project=. -e "using $DEPS; println(\"Julia dependencies successfully verified.\")" || fail "Julia package verification failed."
        else
            warn "No dependencies found to verify in Project.toml."
        fi
    fi
)

# ============================================================================
# Step 4: Python Environment Provisioning
# ============================================================================
log "4. Checking Python environment..."
(
    cd "$ROOT_DIR/python_app"
    
    if [ ! -d ".venv" ]; then
        log "Creating Python virtual environment (.venv)..."
        python3 -m venv .venv
    fi

    log "Installing Python dependencies..."
    .venv/bin/pip install --quiet --upgrade pip
    .venv/bin/pip install --quiet -r requirements.txt

    log "Verifying Python ZMQ bindings..."
    .venv/bin/python -c 'import zmq; print(f"ZMQ version bound successfully: {zmq.zmq_version()}")' || fail "Python ZMQ verification failed."
)

# ============================================================================
# Step 5: systemd Service Registration
# ============================================================================
log "5. Configuring systemd services..."

USER_NAME=$(whoami)
JULIA_SVC="/etc/systemd/system/quant-julia.service"
PYTHON_SVC="/etc/systemd/system/quant-python.service"

# Generate Julia Service
sudo bash -c "cat <<EOF > $JULIA_SVC
[Unit]
Description=Quant Trading Julia ZMQ Server (Strategy Engine)
After=network.target

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$ROOT_DIR/julia_app
EnvironmentFile=$ENV_FILE
ExecStart=/usr/local/bin/julia --project=. server.jl
Restart=on-failure
RestartSec=10
# Require systemd v240+ native logging streams
StandardOutput=append:$LOG_DIR/julia.log
StandardError=append:$LOG_DIR/julia.err

[Install]
WantedBy=multi-user.target
EOF"

# Generate Python Service
sudo bash -c "cat <<EOF > $PYTHON_SVC
[Unit]
Description=Quant Trading Python Live Feed (Execution Gateway)
After=network.target quant-julia.service
# Fix 2: Changed from Requires to Wants so Python doesn't crash if Julia restarts
Wants=quant-julia.service

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$ROOT_DIR/python_app
EnvironmentFile=$ENV_FILE
ExecStart=$ROOT_DIR/python_app/.venv/bin/python live_feed_integration.py
Restart=on-failure
RestartSec=10
# Require systemd v240+ native logging streams
StandardOutput=append:$LOG_DIR/python.log
StandardError=append:$LOG_DIR/python.err

[Install]
WantedBy=multi-user.target
EOF"

log "Reloading systemd daemon and enabling services..."
sudo systemctl daemon-reload
sudo systemctl enable quant-julia.service
sudo systemctl enable quant-python.service

log "Starting quant-julia.service and quant-python.service..."
sudo systemctl restart quant-julia.service
sudo systemctl restart quant-python.service

# ============================================================================
# Step 6: Post-Deploy Health Check
# ============================================================================
log "6. Waiting 8 seconds for ZMQ bindings and data feeds to stabilize..."
sleep 8

JULIA_STATUS=$(systemctl is-active quant-julia.service || echo "failed")
PYTHON_STATUS=$(systemctl is-active quant-python.service || echo "failed")

echo ""
echo "=================================================="
echo "           DEPLOYMENT SUMMARY"
echo "=================================================="
echo "  Julia  service: [$JULIA_STATUS]"
echo "  Python service: [$PYTHON_STATUS]"
echo "  DRY_RUN:        $DRY_RUN_VAL"
echo "  ZMQ Port:       $(get_env "ZMQ_PORT")"
echo "  Log directory:  $LOG_DIR/"
echo "=================================================="
echo ""

if [ "$JULIA_STATUS" != "active" ] || [ "$PYTHON_STATUS" != "active" ]; then
    warn "One or more services failed to reach active state!"
    echo -e "\n${YELLOW}To debug, run the following commands:${NC}"
    [ "$JULIA_STATUS" != "active" ] && echo "  sudo journalctl -u quant-julia.service -n 50 --no-pager"
    [ "$PYTHON_STATUS" != "active" ] && echo "  sudo journalctl -u quant-python.service -n 50 --no-pager"
    echo "  tail -n 50 $LOG_DIR/julia.err"
    echo "  tail -n 50 $LOG_DIR/python.err"
    fail "Deployment finished with errors. Please investigate the logs above."
else
    log "Deployment completed successfully! The trading infrastructure is running safely in DRY_RUN mode."
fi