#!/usr/bin/env bash
# Bring up the CSLab VPN AND register the cluster DNS so that short names like
# `hdfs-namenode` resolve. OpenVPN pushes DNS 10.233.0.3 + domain
# default.svc.cluster.local but cannot apply it on systemd-resolved hosts, so we
# wire it up manually with resolvectl.
#
# Run with sudo from the repo root:   sudo ./scripts/vpn_up.sh
set -euo pipefail

OVPN_CONFIG="./cslab-k8s.ovpn"
LOG_FILE="./openvpn.log"
PIDFILE="/tmp/cslab-vpn.pid"
CLUSTER_DNS="10.233.0.3"          # CoreDNS (pushed by VPN)
CLUSTER_DOMAINS=(default.svc.cluster.local svc.cluster.local cluster.local)

[ -f "$OVPN_CONFIG" ] || { echo "Error: $OVPN_CONFIG not found (run from repo root)."; exit 1; }

echo "==> Killing any existing openvpn instances"
pkill -f "openvpn --config $OVPN_CONFIG" 2>/dev/null || true
rm -f "$PIDFILE"
sleep 2

echo "==> Starting single OpenVPN daemon"
: > "$LOG_FILE"; chmod 666 "$LOG_FILE"
openvpn --config "$OVPN_CONFIG" --daemon --writepid "$PIDFILE" --log "$LOG_FILE"

echo -n "==> Waiting for tunnel"
for i in $(seq 1 60); do
  grep -q "Initialization Sequence Completed" "$LOG_FILE" 2>/dev/null && break
  sleep 1; printf "."
done
echo
grep -q "Initialization Sequence Completed" "$LOG_FILE" || { echo "Timeout. See $LOG_FILE"; exit 1; }

# Find the tun device that got our VPN address
TUN_DEV=$(ip -o addr show | awk '/10\.8\.0\./{print $2}' | head -1)
[ -n "$TUN_DEV" ] || { echo "No tun device with 10.8.0.x found"; exit 1; }
echo "==> Tunnel up on $TUN_DEV"

echo "==> Registering cluster DNS ($CLUSTER_DNS) for ${CLUSTER_DOMAINS[*]}"
resolvectl dns "$TUN_DEV" "$CLUSTER_DNS"
# Plain domains => used as search suffixes (so short `hdfs-namenode` resolves)
# ~cluster.local => routing domain (FQDN queries go to this link's DNS)
resolvectl domain "$TUN_DEV" "${CLUSTER_DOMAINS[@]}" "~cluster.local"
resolvectl flush-caches 2>/dev/null || true

echo "==> Verifying resolution"
if getent hosts hdfs-namenode >/dev/null; then
  echo "OK: hdfs-namenode -> $(getent hosts hdfs-namenode | awk '{print $1}')"
else
  echo "WARN: hdfs-namenode still unresolved. Check 'resolvectl status $TUN_DEV'."
  exit 1
fi

echo
echo "VPN ready. DNS registered on $TUN_DEV. Leave this connected while you work."
echo "To stop:  sudo pkill -F $PIDFILE"
