#!/usr/bin/env bash
#
# Provisions a throwaway GCP environment to measure Wiggle's real start-rate ceiling, deploys the
# current checkout to it, runs RateCeilingBench, and tears it all down.
#
#   Cloud SQL PostgreSQL (private IP)  <--  server VM (wiggle server)  <--  worker VM (N WorkerMain processes)
#                                                                    <--  load VM   (RateCeilingBench)
#
#   export GCP_PROJECT=my-sandbox-project
#   deploy/gcp/ceiling.sh up        # network, Cloud SQL, three VMs (~10-15 min, Cloud SQL dominates)
#   deploy/gcp/ceiling.sh deploy    # ship `git archive $REF`, build on every VM, start the server
#   deploy/gcp/ceiling.sh run       # workers + ceiling ladder; results land in deploy/gcp/results/<ts>/
#   deploy/gcp/ceiling.sh down      # delete everything `up` created
#
#   deploy/gcp/ceiling.sh status | ssh server|worker|load | logs server|worker|load
#
# Everything is named "$PREFIX-*" so several environments can coexist. See deploy/gcp/README.md for
# every knob; the defaults aim at the 5k-10k starts/s question.
set -euo pipefail
cd "$(dirname "$0")"
HERE=$(pwd)
REPO=$(git rev-parse --show-toplevel)

: "${GCP_PROJECT:?set GCP_PROJECT (deliberately not taken from gcloud config)}"
REGION=${REGION:-us-central1}
ZONE=${ZONE:-${REGION}-a}
PREFIX=${PREFIX:-wiggle-ceiling}

DB_VERSION=${DB_VERSION:-POSTGRES_16}
DB_TIER=${DB_TIER:-db-custom-8-32768}
DB_DISK_GB=${DB_DISK_GB:-250}
DB_HA=${DB_HA:-false}
DB_MAX_CONNECTIONS=${DB_MAX_CONNECTIONS:-500}

DEFAULT_MACHINES=c3-standard-8
SERVER_MACHINE=${SERVER_MACHINE:-$DEFAULT_MACHINES}
WORKER_MACHINE=${WORKER_MACHINE:-$DEFAULT_MACHINES}
LOAD_MACHINE=${LOAD_MACHINE:-$DEFAULT_MACHINES}
VM_IMAGE_FAMILY=${VM_IMAGE_FAMILY:-ubuntu-2404-lts-amd64}

REF=${REF:-HEAD}
SERVER_POOL=${SERVER_POOL:-128}
SERVER_HEAP_PERCENT=${SERVER_HEAP_PERCENT:-75}
SERVER_ENV=${SERVER_ENV:-}

WORKERS=${WORKERS:-4}
WORKER_CONCURRENCY=${WORKER_CONCURRENCY:-256}
WORKER_LOCAL_BATCH=${WORKER_LOCAL_BATCH:-64}
BENCH_RATES=${BENCH_RATES:-500,1000,2000,3000,4000,5000,7500,10000,15000,20000}
BENCH_STAGE_SECONDS=${BENCH_STAGE_SECONDS:-30}
BENCH_CONFIRM_SECONDS=${BENCH_CONFIRM_SECONDS:-60}
BENCH_THREADS=${BENCH_THREADS:-256}

NET=$PREFIX-net
SUBNET=$PREFIX-subnet
PSA_RANGE=$PREFIX-psa
DB=$PREFIX-db
SERVER_VM=$PREFIX-server
WORKER_VM=$PREFIX-worker
LOAD_VM=$PREFIX-load
ALL_VMS=("$SERVER_VM" "$WORKER_VM" "$LOAD_VM")
STATE_DIR=$HERE/.state
STATE=$STATE_DIR/$PREFIX.env

gc() { gcloud --project "$GCP_PROJECT" --quiet "$@"; }
log() { printf '\n== %s ==\n' "$*" >&2; }
exists() { "$@" >/dev/null 2>&1; }
vm_ssh() { local vm=$1; shift; gc compute ssh "$vm" --zone "$ZONE" --tunnel-through-iap --command "$*"; }
vm_scp() { gc compute scp --zone "$ZONE" --tunnel-through-iap "$@"; }
internal_ip() { gc compute instances describe "$1" --zone "$ZONE" --format='value(networkInterfaces[0].networkIP)'; }
machine_of() { gc compute instances describe "$1" --zone "$ZONE" --format='value(machineType.basename())' 2>/dev/null; }

create_vm() {
    local vm=$1 candidates=$2 startup=$3 machine err
    err=$(mktemp)
    for machine in ${candidates//,/ }; do
        echo "creating $vm as $machine in $ZONE" >&2
        if gc compute instances create "$vm" --zone "$ZONE" --machine-type "$machine" \
            --subnet "$SUBNET" --image-family "$VM_IMAGE_FAMILY" --image-project ubuntu-os-cloud \
            --boot-disk-size 50GB --boot-disk-type pd-balanced \
            --metadata-from-file startup-script="$startup" 2>"$err"; then
            rm -f "$err"; return 0
        fi
        if grep -qE 'ZONE_RESOURCE_POOL_EXHAUSTED|resource_availability|not available in zone|does not exist in zone|is not supported' "$err"; then
            echo "  $machine unavailable in $ZONE, trying the next type" >&2
            continue
        fi
        cat "$err" >&2; rm -f "$err"; return 1
    done
    echo "none of [$candidates] is available in $ZONE; set SERVER_MACHINE/WORKER_MACHINE/LOAD_MACHINE or another ZONE" >&2
    rm -f "$err"; return 1
}

peering_of() { gc services vpc-peerings list --network "$1" --format='value(peering)' 2>/dev/null; }

db_ip() { gc sql instances describe "$DB" --format='value(ipAddresses[0].ipAddress)'; }

load_state() {
    [ -f "$STATE" ] || { echo "no state at $STATE -- run '$0 up' first" >&2; exit 1; }
    # shellcheck disable=SC1090
    source "$STATE"
}

vm_startup_script() {
    cat <<'SH'
#!/bin/bash
set -e
[ -f /var/lib/wiggle-ready ] && exit 0
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q openjdk-21-jdk-headless sysstat postgresql-client curl unzip
id wiggle >/dev/null 2>&1 || useradd --system --create-home --home-dir /var/lib/wiggle wiggle
mkdir -p /opt/wiggle /etc/wiggle
cat > /etc/sysctl.d/90-wiggle.conf <<EOF
net.core.somaxconn=4096
net.ipv4.ip_local_port_range=10240 65535
EOF
sysctl --system >/dev/null
touch /var/lib/wiggle-ready
SH
}

cmd_up() {
    log "enabling APIs on $GCP_PROJECT"
    gc services enable compute.googleapis.com sqladmin.googleapis.com servicenetworking.googleapis.com iap.googleapis.com

    log "network $NET"
    exists gc compute networks describe "$NET" \
        || gc compute networks create "$NET" --subnet-mode=custom
    exists gc compute networks subnets describe "$SUBNET" --region "$REGION" \
        || gc compute networks subnets create "$SUBNET" --network "$NET" --region "$REGION" --range 10.42.0.0/24
    exists gc compute firewall-rules describe "$PREFIX-internal" \
        || gc compute firewall-rules create "$PREFIX-internal" --network "$NET" \
            --allow tcp,udp,icmp --source-ranges 10.42.0.0/24
    exists gc compute firewall-rules describe "$PREFIX-iap-ssh" \
        || gc compute firewall-rules create "$PREFIX-iap-ssh" --network "$NET" \
            --allow tcp:22 --source-ranges 35.235.240.0/20

    log "private services access for Cloud SQL"
    exists gc compute addresses describe "$PSA_RANGE" --global \
        || gc compute addresses create "$PSA_RANGE" --global --purpose VPC_PEERING --prefix-length 20 --network "$NET"
    gc services vpc-peerings connect --service servicenetworking.googleapis.com \
        --ranges "$PSA_RANGE" --network "$NET" 2>/dev/null \
        || gc services vpc-peerings update --service servicenetworking.googleapis.com \
            --ranges "$PSA_RANGE" --network "$NET" --force

    mkdir -p "$STATE_DIR"
    if [ ! -f "$STATE" ]; then
        printf 'DB_PASSWORD=%s\n' "$(openssl rand -hex 24)" > "$STATE"
        chmod 600 "$STATE"
    fi
    load_state

    log "Cloud SQL $DB ($DB_VERSION, $DB_TIER, ${DB_DISK_GB}GB SSD, HA=$DB_HA)"
    if ! exists gc sql instances describe "$DB"; then
        local availability=zonal
        [ "$DB_HA" = true ] && availability=regional
        gc sql instances create "$DB" \
            --database-version "$DB_VERSION" --edition enterprise --tier "$DB_TIER" \
            --zone "$ZONE" --availability-type "$availability" \
            --storage-type SSD --storage-size "$DB_DISK_GB" --no-storage-auto-increase \
            --network "projects/$GCP_PROJECT/global/networks/$NET" --no-assign-ip \
            --database-flags "max_connections=$DB_MAX_CONNECTIONS,track_io_timing=on"
    fi
    exists gc sql databases describe wiggle --instance "$DB" \
        || gc sql databases create wiggle --instance "$DB"
    gc sql users set-password wiggle --instance "$DB" --password "$DB_PASSWORD" >/dev/null 2>&1 \
        || gc sql users create wiggle --instance "$DB" --password "$DB_PASSWORD"

    log "VMs $SERVER_VM ($SERVER_MACHINE), $WORKER_VM ($WORKER_MACHINE), $LOAD_VM ($LOAD_MACHINE)"
    local startup; startup=$(mktemp); vm_startup_script > "$startup"
    local vm machine
    for vm in "${ALL_VMS[@]}"; do
        case $vm in
            "$SERVER_VM") machine=$SERVER_MACHINE ;;
            "$WORKER_VM") machine=$WORKER_MACHINE ;;
            *) machine=$LOAD_MACHINE ;;
        esac
        exists gc compute instances describe "$vm" --zone "$ZONE" || create_vm "$vm" "$machine" "$startup"
    done
    rm -f "$startup"

    for vm in "${ALL_VMS[@]}"; do
        log "waiting for $vm to finish bootstrapping"
        for _ in $(seq 60); do
            vm_ssh "$vm" "test -f /var/lib/wiggle-ready" 2>/dev/null && break
            sleep 10
        done
        vm_ssh "$vm" "test -f /var/lib/wiggle-ready" || { echo "$vm never became ready" >&2; exit 1; }
    done

    cmd_status
    echo "next: $0 deploy"
}

cmd_deploy() {
    load_state
    if [ -n "$(git -C "$REPO" status --porcelain --untracked-files=no)" ]; then
        echo "warning: uncommitted changes are NOT shipped; deploying $REF ($(git -C "$REPO" rev-parse --short "$REF"))" >&2
    fi
    local sha; sha=$(git -C "$REPO" rev-parse --short "$REF")
    local tarball; tarball=$(mktemp -d)/wiggle-src.tgz
    git -C "$REPO" archive --format tar.gz -o "$tarball" "$REF"

    local build="set -e; sudo rm -rf /tmp/wiggle-src; mkdir /tmp/wiggle-src; tar -xzf /tmp/wiggle-src.tgz -C /tmp/wiggle-src;
        cd /tmp/wiggle-src; chmod +x gradlew"

    log "building the server ($sha) on $SERVER_VM"
    vm_scp "$tarball" "$SERVER_VM:/tmp/wiggle-src.tgz"
    vm_ssh "$SERVER_VM" "$build; ./gradlew --no-daemon -q --console=plain -PskipDashboard :dist:installDist;
        sudo rm -rf /opt/wiggle/server; sudo mkdir -p /opt/wiggle/server;
        sudo cp -r dist/build/install/wiggle-server/lib /opt/wiggle/server/; echo $sha | sudo tee /opt/wiggle/server/REVISION >/dev/null"

    local vm
    for vm in "$WORKER_VM" "$LOAD_VM"; do
        log "building the example module ($sha) on $vm"
        vm_scp "$tarball" "$vm:/tmp/wiggle-src.tgz"
        vm_ssh "$vm" "$build; ./gradlew --no-daemon -q --console=plain -PskipDashboard :example:installDist;
            sudo rm -rf /opt/wiggle/example; sudo mkdir -p /opt/wiggle/example;
            sudo cp -r example/build/install/example/lib /opt/wiggle/example/; echo $sha | sudo tee /opt/wiggle/example/REVISION >/dev/null"
    done
    rm -f "$tarball"

    local dbip; dbip=$(db_ip)
    local env; env=$(mktemp)
    {
        echo "WIGGLE_PORT=8080"
        echo "WIGGLE_DASHBOARD_PORT=8090"
        echo "WIGGLE_JDBC_URL=jdbc:postgresql://$dbip:5432/wiggle"
        echo "WIGGLE_JDBC_USER=wiggle"
        echo "WIGGLE_JDBC_PASSWORD=$DB_PASSWORD"
        echo "WIGGLE_JDBC_POOL_SIZE=$SERVER_POOL"
        echo "JAVA_OPTS=-XX:MaxRAMPercentage=$SERVER_HEAP_PERCENT -XX:StartFlightRecording=disk=true,maxage=1h,settings=profile"
        for kv in $SERVER_ENV; do echo "$kv"; done
    } > "$env"
    {
        echo "PGHOST=$dbip"
        echo "PGUSER=wiggle"
        echo "PGPASSWORD=$DB_PASSWORD"
        echo "PGDATABASE=wiggle"
    } > "$env.pg"
    vm_scp "$env" "$SERVER_VM:/tmp/server.env"
    vm_scp "$env.pg" "$SERVER_VM:/tmp/pg.env"
    rm -f "$env" "$env.pg"

    log "starting wiggle-server on $SERVER_VM"
    vm_ssh "$SERVER_VM" "set -e
        sudo install -m 600 -o wiggle /tmp/server.env /etc/wiggle/server.env
        sudo install -m 600 -o \$(id -un) /tmp/pg.env /etc/wiggle/pg.env; rm -f /tmp/server.env /tmp/pg.env
        sudo tee /etc/systemd/system/wiggle-server.service >/dev/null <<'UNIT'
[Unit]
Description=Wiggle server
After=network-online.target
[Service]
User=wiggle
WorkingDirectory=/var/lib/wiggle
EnvironmentFile=/etc/wiggle/server.env
ExecStart=/usr/bin/java \$JAVA_OPTS -cp /opt/wiggle/server/lib/* com.wiggle.dist.Main
LimitNOFILE=1048576
Restart=no
[Install]
WantedBy=multi-user.target
UNIT
        sudo systemctl daemon-reload
        sudo systemctl restart wiggle-server
        for i in \$(seq 60); do curl -fs localhost:8090/healthz >/dev/null && break; sleep 2; done
        curl -fs localhost:8090/healthz >/dev/null || { sudo journalctl -u wiggle-server -n 80 --no-pager; exit 1; }
        echo 'server healthy (revision '\$(cat /opt/wiggle/server/REVISION)')'"
    echo "next: $0 run"
}

cmd_run() {
    load_state
    local ts; ts=$(date +%Y%m%d-%H%M%S)
    local out=$HERE/results/$ts
    local rdir=/var/tmp/wiggle-run-$ts
    local server_ip; server_ip=$(internal_ip "$SERVER_VM")
    mkdir -p "$out"

    cat > "$out/params.env" <<EOF
revision=$(vm_ssh "$SERVER_VM" "cat /opt/wiggle/server/REVISION" 2>/dev/null)
region=$REGION zone=$ZONE
db=$DB_VERSION tier=$DB_TIER disk=${DB_DISK_GB}GB-SSD ha=$DB_HA max_connections=$DB_MAX_CONNECTIONS
server_machine=$(machine_of "$SERVER_VM") pool=$SERVER_POOL heap=${SERVER_HEAP_PERCENT}% extra_env="$SERVER_ENV"
worker_machine=$(machine_of "$WORKER_VM") workers=$WORKERS concurrency=$WORKER_CONCURRENCY local_batch=$WORKER_LOCAL_BATCH
load_machine=$(machine_of "$LOAD_VM") rates=$BENCH_RATES stage=${BENCH_STAGE_SECONDS}s confirm=${BENCH_CONFIRM_SECONDS}s threads=$BENCH_THREADS
EOF
    cat "$out/params.env"

    log "resetting pg_stat_statements; starting resource sampling"
    vm_ssh "$SERVER_VM" "set -a; . /etc/wiggle/pg.env; set +a
        psql -qAt -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements' -c 'SELECT pg_stat_statements_reset()' >/dev/null || echo 'pg_stat_statements unavailable'
        mkdir -p $rdir; nohup mpstat 5 > $rdir/server-mpstat.txt 2>&1 & echo \$! > $rdir/mpstat.pid"

    log "starting $WORKERS workers x $WORKER_CONCURRENCY slots on $WORKER_VM"
    vm_ssh "$WORKER_VM" "set -e; mkdir -p $rdir
        for i in \$(seq $WORKERS); do
            sudo systemctl stop wiggle-worker-\$i 2>/dev/null || true
            sudo systemctl reset-failed wiggle-worker-\$i 2>/dev/null || true
            sudo systemd-run --quiet --unit wiggle-worker-\$i --uid wiggle -p LimitNOFILE=1048576 \
                -E WIGGLE_URL=$server_ip:8080 -E WIGGLE_WORKER_ID=ceiling-worker-\$i \
                -E WIGGLE_WORKER_CONCURRENCY=$WORKER_CONCURRENCY -E WIGGLE_LOCAL_BATCH_SIZE=$WORKER_LOCAL_BATCH \
                /usr/bin/java -XX:MaxRAMPercentage=$((75 / WORKERS)) -cp '/opt/wiggle/example/lib/*' com.wiggle.order.WorkerMain
        done
        sleep 5
        for i in \$(seq $WORKERS); do systemctl is-active -q wiggle-worker-\$i || { sudo journalctl -u wiggle-worker-\$i -n 40 --no-pager; exit 1; }; done
        nohup mpstat 5 > $rdir/worker-mpstat.txt 2>&1 & echo \$! > $rdir/mpstat.pid"

    log "running RateCeilingBench on $LOAD_VM (rates $BENCH_RATES) -- the run survives a dropped ssh session"
    vm_ssh "$LOAD_VM" "set -e; mkdir -p $rdir
        nohup mpstat 5 > $rdir/load-mpstat.txt 2>&1 & echo \$! > $rdir/mpstat.pid
        sudo systemctl reset-failed wiggle-bench 2>/dev/null || true
        sudo systemd-run --quiet --unit wiggle-bench --uid \$(id -u) -p StandardOutput=file:$rdir/bench.log -p StandardError=inherit \
            -E WIGGLE_SERVER_URL=$server_ip:8080 -E BENCH_RATES=$BENCH_RATES -E BENCH_STAGE_SECONDS=$BENCH_STAGE_SECONDS \
            -E BENCH_CONFIRM_SECONDS=$BENCH_CONFIRM_SECONDS -E BENCH_THREADS=$BENCH_THREADS \
            /usr/bin/java -XX:MaxRAMPercentage=75 -cp '/opt/wiggle/example/lib/*' com.wiggle.order.RateCeilingBench
        sleep 1
        tail -n +1 -F $rdir/bench.log 2>/dev/null & tailer=\$!
        while systemctl is-active -q wiggle-bench; do sleep 5; done
        sleep 1; kill \$tailer" || echo "(ssh dropped -- the bench keeps running; re-run '$0 collect $ts' when it finishes)" >&2

    cmd_collect "$ts"
}

cmd_collect() {
    load_state
    local ts=${1:?usage: collect <timestamp>}
    local out=$HERE/results/$ts
    local rdir=/var/tmp/wiggle-run-$ts
    mkdir -p "$out"

    log "stopping workers and samplers; capturing server JFR and pg_stat_statements"
    vm_ssh "$LOAD_VM" "kill \$(cat $rdir/mpstat.pid) 2>/dev/null || true
        systemctl is-active -q wiggle-bench && echo 'warning: bench still running' >&2 || true
        cd /var/tmp && tar -czf wiggle-run-$ts.tgz wiggle-run-$ts"
    vm_ssh "$WORKER_VM" "kill \$(cat $rdir/mpstat.pid) 2>/dev/null || true
        for u in \$(systemctl list-units --plain --no-legend 'wiggle-worker-*' | awk '{print \$1}'); do sudo systemctl stop \$u; done
        sudo journalctl -u 'wiggle-worker-*' --since '-2h' --no-pager > $rdir/worker-journal.txt
        cd /var/tmp && tar -czf wiggle-run-$ts.tgz wiggle-run-$ts"
    vm_ssh "$SERVER_VM" "set -a; . /etc/wiggle/pg.env; set +a
        kill \$(cat $rdir/mpstat.pid) 2>/dev/null || true
        psql -P pager=off -c \"SELECT calls, round(total_exec_time::numeric/1000,1) AS total_s,
               round(mean_exec_time::numeric,3) AS mean_ms, rows, left(regexp_replace(query, '\\s+', ' ', 'g'), 160) AS query
            FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            ORDER BY total_exec_time DESC LIMIT 25\" > $rdir/pg_stat_statements.txt 2>&1 || true
        pid=\$(systemctl show -p MainPID --value wiggle-server)
        sudo -u wiggle jcmd \$pid JFR.dump name=1 filename=/var/lib/wiggle/server-$ts.jfr >/dev/null 2>&1 \
            && sudo mv /var/lib/wiggle/server-$ts.jfr $rdir/server.jfr && sudo chown \$(id -un) $rdir/server.jfr || echo 'no JFR dump'
        sudo journalctl -u wiggle-server --since '-2h' --no-pager > $rdir/server-journal.txt
        cd /var/tmp && tar -czf wiggle-run-$ts.tgz wiggle-run-$ts"

    local vm
    for vm in "${ALL_VMS[@]}"; do
        vm_scp "$vm:/var/tmp/wiggle-run-$ts.tgz" "$out/$vm.tgz"
        tar -xzf "$out/$vm.tgz" -C "$out" --strip-components 1 && rm -f "$out/$vm.tgz"
    done
    echo
    grep -E '^== ceiling|^summary|^  ' "$out/bench.log" 2>/dev/null || true
    echo "results: $out (bench.log, pg_stat_statements.txt, *-mpstat.txt, server.jfr, *-journal.txt)"
    echo "Cloud SQL CPU/IOPS for the run window: https://console.cloud.google.com/sql/instances/$DB/system-insights?project=$GCP_PROJECT"
}

cmd_status() {
    echo "project=$GCP_PROJECT region=$REGION zone=$ZONE prefix=$PREFIX"
    gc sql instances list --filter "name=$DB" --format 'table(name,databaseVersion,settings.tier,state,ipAddresses[0].ipAddress)' 2>/dev/null || true
    gc compute instances list --filter "name~^$PREFIX-" --format 'table(name,machineType.basename(),status,networkInterfaces[0].networkIP)' 2>/dev/null || true
}

cmd_logs() {
    case ${1:-server} in
        server) vm_ssh "$SERVER_VM" "sudo journalctl -u wiggle-server -n 200 -f" ;;
        worker) vm_ssh "$WORKER_VM" "sudo journalctl -u 'wiggle-worker-*' -n 200 -f" ;;
        load) vm_ssh "$LOAD_VM" "tail -n 200 -F \$(ls -td /var/tmp/wiggle-run-*/ | head -1)bench.log" ;;
        *) echo "usage: logs server|worker|load" >&2; exit 2 ;;
    esac
}

cmd_ssh() {
    case ${1:-} in
        server) gc compute ssh "$SERVER_VM" --zone "$ZONE" --tunnel-through-iap ;;
        worker) gc compute ssh "$WORKER_VM" --zone "$ZONE" --tunnel-through-iap ;;
        load) gc compute ssh "$LOAD_VM" --zone "$ZONE" --tunnel-through-iap ;;
        *) echo "usage: ssh server|worker|load" >&2; exit 2 ;;
    esac
}

cmd_down() {
    log "deleting everything named $PREFIX-* in $GCP_PROJECT"
    gc compute instances delete "${ALL_VMS[@]}" --zone "$ZONE" 2>/dev/null || true
    exists gc sql instances describe "$DB" && gc sql instances delete "$DB"
    local err; err=$(mktemp)
    for attempt in $(seq 12); do
        exists gc compute networks describe "$NET" || break
        [ -z "$(peering_of "$NET")" ] && break
        gc services vpc-peerings delete --service servicenetworking.googleapis.com --network "$NET" 2>"$err" && break
        if [ "$attempt" = 12 ]; then cat "$err" >&2; break; fi
        echo "peering still in use (Cloud SQL releases it a few minutes after deletion); retrying in 30s" >&2
        sleep 30
    done
    rm -f "$err"
    gc compute addresses delete "$PSA_RANGE" --global 2>/dev/null || true
    gc compute firewall-rules delete "$PREFIX-internal" "$PREFIX-iap-ssh" 2>/dev/null || true
    gc compute networks subnets delete "$SUBNET" --region "$REGION" 2>/dev/null || true
    if exists gc compute networks describe "$NET"; then
        gc compute networks delete "$NET" 2>/dev/null || echo "network $NET not deleted yet; re-run '$0 down' in a few minutes" >&2
    fi
    rm -f "$STATE"
    cmd_status
}

case ${1:-} in
    up) cmd_up ;;
    deploy) cmd_deploy ;;
    run) cmd_run ;;
    collect) shift; cmd_collect "$@" ;;
    status) cmd_status ;;
    logs) shift; cmd_logs "$@" ;;
    ssh) shift; cmd_ssh "$@" ;;
    down) cmd_down ;;
    *) sed -n '2,20p' "$0"; exit 2 ;;
esac
