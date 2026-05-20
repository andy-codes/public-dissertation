#!/bin/bash
set -euo pipefail

RIAK_ROOT="/opt/openriak"
RIAK_CONF="${RIAK_ROOT}/etc/riak.conf"
GEN_DIR="${RIAK_ROOT}/generated.conf"

# Derive private IPv4 (works without IMDS)
IP="$(ip -4 route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')"
NODENAME="riak@${IP}"

# Ensure config exists
if [ ! -f "${RIAK_CONF}" ]; then
  echo "[openriak-userdata] ERROR: ${RIAK_CONF} not found"
  exit 1
fi

# Helper: set key = value (replace if exists, else append)
set_kv () {
  local key="$1" value="$2"
  if grep -qE "^${key}\s*=" "${RIAK_CONF}"; then
    sed -i -E "s|^${key}\s*=.*|${key} = ${value}|" "${RIAK_CONF}"
  else
    printf "\n%s = %s\n" "${key}" "${value}" >> "${RIAK_CONF}"
  fi
}

echo "[openriak-userdata] Using IP=${IP}, nodename=${NODENAME}"

# 1) Set nodename
set_kv "nodename" "${NODENAME}"

# 2) Bind listeners to the instance IP (NOT loopback)
set_kv "listener.http.internal" "${IP}:8098"
set_kv "listener.protobuf.internal" "${IP}:8087"

# 3) Ensure cuttlefish output dir is writable (cf_config writes here)
mkdir -p "${GEN_DIR}"
chown -R riak:riak "${GEN_DIR}"
chmod 0775 "${GEN_DIR}"

# -------------------------------------------------------------------
# 3.5) HARDENING: ensure node-local riak state is clean for this nodename
#      (prevents crashes when AMI has old ring/state from another nodename)
# -------------------------------------------------------------------
STATE_DIR="/var/lib/openriak"
MARKER="${STATE_DIR}/initialized"
LAST_NODE="${STATE_DIR}/last_nodename"

mkdir -p "${STATE_DIR}"
chmod 0755 "${STATE_DIR}"

if [ ! -f "${MARKER}" ]; then
  echo "[openriak-userdata] First boot: wiping ${RIAK_ROOT}/data to ensure clean node identity"
  rm -rf "${RIAK_ROOT}/data/"*
  rm -rf /tmp/erl_pipes/* || true
  touch "${MARKER}"
fi

if [ -f "${LAST_NODE}" ] && [ "$(cat "${LAST_NODE}" || true)" != "${NODENAME}" ]; then
  echo "[openriak-userdata] Nodename changed ($(cat "${LAST_NODE}" || true) -> ${NODENAME}): wiping ${RIAK_ROOT}/data"
  rm -rf "${RIAK_ROOT}/data/"*
  rm -rf /tmp/erl_pipes/* || true
fi

echo "${NODENAME}" > "${LAST_NODE}"
chmod 0644 "${LAST_NODE}"

# 4) Start riak via relx runner (triggers cf_config hook)
sudo -u riak "${RIAK_ROOT}/bin/riak" stop || true
sudo pkill -f 'beam.smp|run_erl|epmd' || true
sudo -u riak "${RIAK_ROOT}/bin/riak" daemon

# 5) Log a tiny verification into cloud-init output
sleep 3
echo "[openriak-userdata] riak.conf nodename: $(grep -E '^\s*nodename\s*=' -n "${RIAK_CONF}" || true)"
echo "[openriak-userdata] riak.conf http:     $(grep -E '^\s*listener\.http\.internal\s*=' -n "${RIAK_CONF}" || true)"
echo "[openriak-userdata] riak.conf pb:       $(grep -E '^\s*listener\.protobuf\.internal\s*=' -n "${RIAK_CONF}" || true)"
echo "[openriak-userdata] vm.args -name:      $(awk '/^-name /{print $0; exit}' /opt/openriak/vm.args 2>/dev/null || true)"
