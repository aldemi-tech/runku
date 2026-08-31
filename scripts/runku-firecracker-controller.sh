#!/usr/bin/env bash
set -euo pipefail
set -f

action="${1:-}"
worker_index="${2:-}"
asset_directory="${RUNKU_FIRECRACKER_ASSET_DIR:?RUNKU_FIRECRACKER_ASSET_DIR is required}"
state_root="${RUNKU_FIRECRACKER_STATE_ROOT:-/var/lib/runku/firecracker}"
token_file="${RUNKU_FIRECRACKER_TOKEN_FILE:?RUNKU_FIRECRACKER_TOKEN_FILE is required}"
image_reference="${RUNKU_FIRECRACKER_IMAGE_REFERENCE:?RUNKU_FIRECRACKER_IMAGE_REFERENCE is required}"
egress_mode="${RUNKU_FIRECRACKER_EGRESS_MODE:?RUNKU_FIRECRACKER_EGRESS_MODE is required}"
egress_allow="${RUNKU_FIRECRACKER_EGRESS_ALLOW:-}"
egress_deny="${RUNKU_FIRECRACKER_EGRESS_DENY:-}"
dns_servers="${RUNKU_FIRECRACKER_DNS_SERVERS:-}"
workers="${RUNKU_FIRECRACKER_WORKERS:-4}"
memory_mib="${RUNKU_FIRECRACKER_MEMORY_MIB:-256}"
vcpu_count="${RUNKU_FIRECRACKER_VCPU_COUNT:-1}"
network_octet_base="${RUNKU_FIRECRACKER_NETWORK_OCTET_BASE:-220}"
uid="${RUNKU_FIRECRACKER_UID:-65532}"
gid="${RUNKU_FIRECRACKER_GID:-65532}"
cpu_set="${RUNKU_FIRECRACKER_CPUSET:-}"
runner_port="${RUNKU_FIRECRACKER_RUNNER_PORT:-32110}"
firecracker="$asset_directory/firecracker"
jailer="$asset_directory/jailer"
kernel="$asset_directory/vmlinux"
rootfs="$asset_directory/rootfs.ext4"
expected_image_file="$asset_directory/image-reference.txt"

case "$action" in ensure|replace|shutdown) ;; *) exit 2 ;; esac
[[ "$worker_index" =~ ^[0-9]+$ ]] || exit 2
[[ "$workers" =~ ^[0-9]+$ ]] || exit 2
[[ "$memory_mib" =~ ^[0-9]+$ ]] || exit 2
[[ "$vcpu_count" =~ ^[0-9]+$ ]] || exit 2
[[ "$network_octet_base" =~ ^[0-9]+$ ]] || exit 2
[[ "$runner_port" =~ ^[0-9]+$ ]] || exit 2
test "$workers" -ge 1 && test "$workers" -le 32
test "$worker_index" -lt "$workers"
test "$memory_mib" -ge 128 && test "$memory_mib" -le 32768
test "$vcpu_count" -ge 1 && test "$vcpu_count" -le 32
test "$runner_port" -ge 1 && test "$runner_port" -le 65535
case "$egress_mode" in none|public|restricted) ;; *) exit 2 ;; esac
test "$(id -u)" -eq 0
test "$(uname -s)" = Linux
for command in flock ip nft; do command -v "$command" >/dev/null; done
ipc_token=""
if test "$action" != shutdown; then
  test -r /dev/kvm
  command -v getent >/dev/null
  for path in "$firecracker" "$jailer" "$kernel" "$rootfs" "$expected_image_file" "$token_file"; do
    test -r "$path"
  done
  test "$(cat "$expected_image_file")" = "$image_reference"
  ipc_token="$(cat "$token_file")"
  test "${#ipc_token}" -ge 32 && test "${#ipc_token}" -le 256
fi

mkdir -p "$state_root/locks" "$state_root/jailer"
chmod 0700 "$state_root" "$state_root/locks"
exec 9>"$state_root/locks/worker-$worker_index.lock"
flock -x 9

third_octet=$((network_octet_base + worker_index))
test "$third_octet" -le 254
id="runku-worker-$worker_index"
namespace="runku-fc-$worker_index"
host_device="rfch$worker_index"
peer_device="rfcp$worker_index"
gateway="172.31.${third_octet}.1"
guest="172.31.${third_octet}.2"
mac="06:00:ac:1f:$(printf '%02x' "$third_octet"):02"
jail_root="$state_root/jailer/firecracker/$id/root"
pid_file="$jail_root/firecracker.pid"
cgroup="/sys/fs/cgroup/runku-firecracker/$id"
filter_table="runku_fc_$worker_index"
nat_table="runku_fc_nat_$worker_index"

delete_nftables() {
  nft delete table inet "$filter_table" >/dev/null 2>&1 || true
  nft delete table ip "$nat_table" >/dev/null 2>&1 || true
}

stop_worker() {
  if test -s "$pid_file"; then
    pid="$(cat "$pid_file")"
    kill -TERM "$pid" >/dev/null 2>&1 || true
    for _ in $(seq 1 50); do
      kill -0 "$pid" >/dev/null 2>&1 || break
      sleep 0.02
    done
    kill -KILL "$pid" >/dev/null 2>&1 || true
  fi
  ip netns delete "$namespace" >/dev/null 2>&1 || true
  ip link delete "$host_device" >/dev/null 2>&1 || true
  delete_nftables
  if test -d "$state_root/jailer/firecracker/$id"; then
    find "$state_root/jailer/firecracker/$id" -depth -delete
  fi
  if test -d "$cgroup"; then
    rmdir "$cgroup" >/dev/null 2>&1 || true
  fi
}

resolve_destination() {
  local destination="$1"
  if [[ "$destination" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(/[0-9]+)?$ ]]; then
    printf '%s\n' "$destination"
    return
  fi
  getent ahostsv4 "$destination" | awk '{print $1}' | sort -u
}

append_policy_rules() {
  local encoded="$1"
  local verdict="$2"
  local old_ifs="$IFS"
  local entry destination ports address
  IFS=';'
  read -r -a entries <<< "$encoded"
  IFS="$old_ifs"
  for entry in "${entries[@]}"; do
    test -n "$entry" || continue
    destination="${entry%%|*}"
    ports="${entry#*|}"
    test "$destination" != "$entry"
    [[ "$ports" =~ ^[0-9]+(,[0-9]+)*$ ]]
    addresses="$(resolve_destination "$destination")"
    test -n "$addresses"
    while IFS= read -r address; do
      printf '  iifname "%s" ip daddr %s tcp dport { %s } %s\n' \
        "$host_device" "$address" "${ports//,/ , }" "$verdict" >> "$nft_file"
    done <<< "$addresses"
  done
}

install_nftables() {
  delete_nftables
  if test "$egress_mode" != none; then
    test "$(sysctl -n net.ipv4.ip_forward)" = 1
  fi
  nft_file="$(mktemp "$state_root/.nft-worker-$worker_index.XXXXXX")"
  trap 'rm -f "$nft_file"' RETURN
  {
    printf 'table inet %s {\n' "$filter_table"
    printf ' chain forward { type filter hook forward priority 0; policy accept;\n'
    printf '  oifname "%s" ct state established,related accept\n' "$host_device"
    printf '  oifname "%s" drop\n' "$host_device"
    printf '  iifname "%s" ip daddr { 0.0.0.0/8, 100.100.100.200/32, 127.0.0.0/8, 168.63.129.16/32, 169.254.0.0/16, 224.0.0.0/4, 240.0.0.0/4 } drop\n' "$host_device"
  } > "$nft_file"
  if test "$egress_mode" != none && test -n "$dns_servers"; then
    old_ifs="$IFS"
    IFS=','
    read -r -a resolvers <<< "$dns_servers"
    IFS="$old_ifs"
    for resolver in "${resolvers[@]}"; do
      [[ "$resolver" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]
      printf '  iifname "%s" ip daddr %s udp dport 53 accept\n' "$host_device" "$resolver" >> "$nft_file"
      printf '  iifname "%s" ip daddr %s tcp dport 53 accept\n' "$host_device" "$resolver" >> "$nft_file"
    done
  fi
  if test "$egress_mode" != none; then
    append_policy_rules "$egress_deny" drop
  fi
  if test "$egress_mode" = public; then
    printf '  iifname "%s" ip daddr { 10.0.0.0/8, 100.64.0.0/10, 172.16.0.0/12, 192.168.0.0/16 } drop\n' "$host_device" >> "$nft_file"
    printf '  iifname "%s" meta l4proto tcp accept\n' "$host_device" >> "$nft_file"
  elif test "$egress_mode" = restricted; then
    test -n "$egress_allow"
    append_policy_rules "$egress_allow" accept
  fi
  {
    printf '  iifname "%s" drop\n' "$host_device"
    printf ' }\n}\n'
  } >> "$nft_file"
  if test "$egress_mode" != none; then
    {
      printf 'table ip %s {\n' "$nat_table"
      printf ' chain postrouting { type nat hook postrouting priority srcnat; policy accept;\n'
      printf '  ip saddr %s/32 masquerade\n' "$guest"
      printf ' }\n}\n'
    } >> "$nft_file"
  fi
  nft -f "$nft_file"
  rm -f "$nft_file"
  trap - RETURN
}

start_worker() {
  mkdir -p "$jail_root"
  install -m 0644 "$kernel" "$jail_root/vmlinux"
  ln "$rootfs" "$jail_root/rootfs.ext4" 2>/dev/null || install -m 0644 "$rootfs" "$jail_root/rootfs.ext4"
  umask 077
  cat > "$jail_root/config.json" <<EOF
{"boot-source":{"kernel_image_path":"/vmlinux","boot_args":"console=ttyS0 reboot=k panic=1 pci=off nomodules root=/dev/vda ro init=/sbin/runku-init runku.ip=$guest runku.gateway=$gateway runku.token=$ipc_token"},"drives":[{"drive_id":"rootfs","path_on_host":"/rootfs.ext4","is_root_device":true,"is_read_only":true}],"machine-config":{"vcpu_count":$vcpu_count,"mem_size_mib":$memory_mib,"smt":false},"network-interfaces":[{"iface_id":"eth0","guest_mac":"$mac","host_dev_name":"tap0"}]}
EOF
  chown "$uid:$gid" "$jail_root/config.json"
  chmod 0400 "$jail_root/config.json"

  ip netns add "$namespace"
  ip link add "$host_device" type veth peer name "$peer_device"
  ip link set "$peer_device" netns "$namespace"
  ip address add "$gateway/30" dev "$host_device"
  ip link set "$host_device" up
  ip netns exec "$namespace" ip link add br0 type bridge
  ip netns exec "$namespace" ip tuntap add tap0 mode tap user "$uid"
  ip netns exec "$namespace" ip link set "$peer_device" master br0
  ip netns exec "$namespace" ip link set tap0 master br0
  ip netns exec "$namespace" ip link set lo up
  ip netns exec "$namespace" ip link set br0 up
  ip netns exec "$namespace" ip link set "$peer_device" up
  ip netns exec "$namespace" ip link set tap0 up
  install_nftables

  cgroup_arguments=()
  if test -n "$cpu_set"; then
    [[ "$cpu_set" =~ ^[0-9,-]+$ ]]
    cgroup_arguments+=(--cgroup "cpuset.mems=0" --cgroup "cpuset.cpus=$cpu_set")
  fi
  "$jailer" \
    --id "$id" --exec-file "$firecracker" --uid "$uid" --gid "$gid" \
    --chroot-base-dir "$state_root/jailer" --netns "/var/run/netns/$namespace" \
    --cgroup-version 2 --parent-cgroup runku-firecracker \
    --cgroup "cpu.max=$((vcpu_count * 100000)) 100000" \
    --cgroup "memory.max=$(((memory_mib + 128) * 1024 * 1024))" \
    --cgroup "pids.max=$((vcpu_count + 16))" \
    "${cgroup_arguments[@]}" \
    --resource-limit no-file=1024 --resource-limit fsize=1048576 \
    --new-pid-ns --daemonize \
    -- --api-sock /firecracker.socket --config-file /config.json
  for _ in $(seq 1 250); do
    test -s "$pid_file" && kill -0 "$(cat "$pid_file")" >/dev/null 2>&1 && return 0
    sleep 0.02
  done
  return 1
}

case "$action" in
  ensure)
    if test -s "$pid_file" && kill -0 "$(cat "$pid_file")" >/dev/null 2>&1; then
      exit 0
    fi
    stop_worker
    start_worker
    ;;
  replace)
    stop_worker
    start_worker
    ;;
  shutdown)
    stop_worker
    ;;
esac
