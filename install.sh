#!/bin/bash

# Exit on any error
set -e

echo "Starting Market Monitor installation..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Get the actual username (if script was run with sudo)
ACTUAL_USER=${SUDO_USER:-$USER}
echo "Installing for user: $ACTUAL_USER"

# Get the directory where the install script is located (project root)
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo "Installing in: $PROJECT_ROOT"

# Use the existing directory structure
APP_DIR=$PROJECT_ROOT
DATA_DIR="$APP_DIR/data"
LOG_DIR="$APP_DIR/logs"
VENV_DIR="$APP_DIR/venv"

# Create necessary directories
echo "Creating application directories..."
mkdir -p $DATA_DIR
mkdir -p $LOG_DIR
mkdir -p /var/log/market_monitor

# Set up Python virtual environment
echo "Setting up Python virtual environment..."
apt-get update
apt-get install -y python3-pip python3-venv

# Create and activate virtual environment
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Install requirements
echo "Installing Python dependencies..."
pip install -r $APP_DIR/requirements.txt

# Set up log rotation
echo "Setting up log rotation..."
cat > /etc/logrotate.d/market-monitor << EOL
/var/log/market_monitor/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 $ACTUAL_USER $ACTUAL_USER
}

$LOG_DIR/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 $ACTUAL_USER $ACTUAL_USER
}
EOL

# Copy systemd service file if it exists in project, otherwise create it
if [ -f "$PROJECT_ROOT/market-monitor.service" ]; then
    echo "Using provided service file..."
    cp "$PROJECT_ROOT/market-monitor.service" /etc/systemd/system/
else
    echo "Creating service file..."
    cat > /etc/systemd/system/market-monitor.service << EOL
[Unit]
Description=Market Monitor Service
After=network.target

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$APP_DIR
Environment=PYTHONPATH=$APP_DIR
Environment=PATH=$VENV_DIR/bin:$PATH
ExecStart=$VENV_DIR/bin/python main.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/market_monitor/output.log
StandardError=append:/var/log/market_monitor/error.log

[Install]
WantedBy=multi-user.target
EOL
fi

# Set correct permissions
echo "Setting permissions..."
chown -R $ACTUAL_USER:$ACTUAL_USER $APP_DIR
chown -R $ACTUAL_USER:$ACTUAL_USER /var/log/market_monitor

# Reload systemd and enable service
echo "Enabling service..."
systemctl daemon-reload
systemctl enable market-monitor

echo "Installation complete!"
echo ""
echo "To start the service:"
echo "  sudo systemctl start market-monitor"
echo ""
echo "To check status:"
echo "  sudo systemctl status market-monitor"
echo ""
echo "To view logs:"
echo "  tail -f /var/log/market_monitor/output.log"
echo "  tail -f /var/log/market_monitor/error.log"