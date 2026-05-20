#!/usr/bin/env bash
set -euo pipefail

# ---------- Config ----------
KEY_PATH="${KEY_PATH:-$HOME/.ssh/yugabyte_test}"
SSH_USER="${SSH_USER:-ubuntu}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"

NAME_BASE="openriak-bench"

RIAK_ADMIN="/opt/openriak/bin/riak-admin"
RIAK_SUDO=(sudo -u riak)

SSH_OPTS=(
  -i "$KEY_PATH"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=10
)

# ---------- Helpers ----------
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

aws_cli() {
  if [[ -n "${REGION}" ]]; then
    aws --region "$REGION" "$@"
  else
    aws "$@"
  fi
}

ssh_run() {
  local host="$1"; shift
  local cmd="$*"
  log "SSH -> ${SSH_USER}@${host}: ${cmd}"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "$cmd"
}

wait_for_ssh() {
  local host="$1"
  local tries=30
  local i=1
  while (( i <= tries )); do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "echo ok" >/dev/null 2>&1; then
      log "SSH ready on ${host}"
      return 0
    fi
    log "Waiting for SSH on ${host} (attempt ${i}/${tries})..."
    sleep 5
    ((i++))
  done
  return 1
}

get_instance_ips_by_name() {
  local name="$1"

  # We query a strict 2-element list: [PublicIpAddress, PrivateIpAddress]
  # Then read them in that exact order.
  local pub priv
  read -r pub priv < <(
    aws_cli ec2 describe-instances \
      --filters "Name=tag:Name,Values=${name}" "Name=instance-state-name,Values=running" \
      --query "Reservations[].Instances[0].[PublicIpAddress,PrivateIpAddress]" \
      --output text
  ) || return 1

  [[ "${pub:-}" == "None" ]] && pub=""
  [[ "${priv:-}" == "None" ]] && priv=""

  [[ -n "${pub}" ]] || return 2
  [[ -n "${priv}" ]] || return 3

  echo "${pub} ${priv}"
}

# ---------- Main ----------
command -v aws >/dev/null 2>&1 || die "Missing required command: aws"
command -v ssh >/dev/null 2>&1 || die "Missing required command: ssh"
[[ -f "$KEY_PATH" ]] || die "SSH key not found at: $KEY_PATH"

log "Discovering Riak nodes by Name tag: ${NAME_BASE}-0..2"
log "Using key: ${KEY_PATH}"
log "Using SSH user: ${SSH_USER}"
[[ -n "${REGION}" ]] && log "Using AWS region: ${REGION}" || log "Using AWS region from AWS CLI config (no override)"

declare -A PUB PRIV

for i in 0 1 2; do
  name="${NAME_BASE}-${i}"
  log "Looking up instance: ${name}"

  if ips="$(get_instance_ips_by_name "$name")"; then
    PUB["$i"]="$(awk '{print $1}' <<<"$ips")"
    PRIV["$i"]="$(awk '{print $2}' <<<"$ips")"
    log "Found ${name}: public='${PUB[$i]}' private='${PRIV[$i]}'"
  else
    rc=$?
    if [[ $rc -eq 2 ]]; then
      die "Instance ${name} is running but has no public IP (cannot SSH from your laptop)."
    else
      die "Could not find a *running* instance with tag Name=${name}"
    fi
  fi
done

SEED_PUBLIC="${PUB[0]}"
SEED_PRIVATE="${PRIV[0]}"

log "Seed node is ${NAME_BASE}-0"
log "Seed public  IP: ${SEED_PUBLIC}"
log "Seed private IP: ${SEED_PRIVATE}"

# Wait for SSH readiness on all nodes (PUBLIC IPS ONLY)
for i in 0 1 2; do
  log "Checking SSH readiness for ${NAME_BASE}-${i} (${PUB[$i]})"
  wait_for_ssh "${PUB[$i]}" || die "SSH never became ready on ${NAME_BASE}-${i} (${PUB[$i]})"
done

# Join non-seed nodes to cluster (ssh via PUBLIC, join via SEED PRIVATE)
for i in 1 2; do
  log "-----"
  log "Joining ${NAME_BASE}-${i} to seed using: riak@${SEED_PRIVATE}"
  ssh_run "${PUB[$i]}" "${RIAK_SUDO[*]} ${RIAK_ADMIN} cluster join \"riak@${SEED_PRIVATE}\""
  ssh_run "${PUB[$i]}" "${RIAK_SUDO[*]} ${RIAK_ADMIN} member-status"
done

# Plan/commit on seed
log "-----"
log "Planning cluster changes on seed ${NAME_BASE}-0"
ssh_run "$SEED_PUBLIC" "${RIAK_SUDO[*]} ${RIAK_ADMIN} cluster plan"

log "Committing cluster changes on seed ${NAME_BASE}-0"
ssh_run "$SEED_PUBLIC" "${RIAK_SUDO[*]} ${RIAK_ADMIN} cluster commit"

# Final status on seed
log "-----"
log "Final member-status on seed ${NAME_BASE}-0"
ssh_run "$SEED_PUBLIC" "${RIAK_SUDO[*]} ${RIAK_ADMIN} member-status"

log "Final cluster status on seed ${NAME_BASE}-0"
ssh_run "$SEED_PUBLIC" "${RIAK_SUDO[*]} ${RIAK_ADMIN} cluster status"

log "Cluster Join Finished"

log "----------"
log "Creating default counter bucket"
log "----------"
log "Creating default counter bucket"
ssh_run "$SEED_PUBLIC" "$(cat <<'EOF'
/opt/openriak/bin/riak-admin bucket-type create counters '{"props":{"datatype":"counter"}}' &&
/opt/openriak/bin/riak-admin bucket-type activate counters &&
/opt/openriak/bin/riak-admin bucket-type status counters
EOF
)"

log "Initialisation Complete :)"