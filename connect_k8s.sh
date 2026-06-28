#!/bin/bash

# Configuration
OVPN_CONFIG="./cslab-k8s.ovpn"
LOG_FILE="./openvpn.log"

# Check if OVPN file exists
if [ ! -f "$OVPN_CONFIG" ]; then
    echo "Error: $OVPN_CONFIG not found in current directory."
    exit 1
fi

# Function to stop VPN
stop_vpn() {
    echo ""
    echo "Shutting down OpenVPN..."
    sudo pkill -F /tmp/cslab-vpn.pid 2>/dev/null
    sudo rm -f /tmp/cslab-vpn.pid
    echo "Done."
    exit 0
}

# Trap interrupt signals (Ctrl+C)
trap stop_vpn SIGINT SIGTERM

echo "Starting OpenVPN connection in the background..."
echo "Note: You may be prompted for your sudo password."

# Truncate log file to avoid matching old entries
sudo truncate -s 0 "$LOG_FILE" 2>/dev/null || sudo touch "$LOG_FILE"
sudo chmod 666 "$LOG_FILE"

# Start OpenVPN in background
sudo openvpn --config "$OVPN_CONFIG" --daemon --writepid /tmp/cslab-vpn.pid --log "$LOG_FILE"

echo "Waiting for connection to establish (checking log)..."
MAX_RETRIES=60
COUNT=0
while ! sudo grep -q "Initialization Sequence Completed" "$LOG_FILE" 2>/dev/null; do
    sleep 1
    ((COUNT++))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo "Timeout: VPN took too long to connect. Check $LOG_FILE for details."
        sudo kill $(cat /tmp/cslab-vpn.pid) 2>/dev/null
        exit 1
    fi
    printf "."
done
echo -e "\nConnected!"

echo "Testing Kubernetes connection..."
if kubectl get nodes &>/dev/null; then
    echo "Success: Kubernetes cluster is reachable."
    echo "Current context: $(kubectl config current-context)"
else
    echo "Warning: VPN is up but 'kubectl get nodes' failed. You might need to check your permissions or cluster status."
fi

echo "-------------------------------------------------------"
echo "VPN is running in the background."
echo "Press Ctrl+C to disconnect and exit."
echo "-------------------------------------------------------"

# Keep script running to maintain the trap
while true; do sleep 1; done
