#!/usr/bin/env bash
#
# Verifies the COORDINATOR + NAMESPACE storage layer against real databases -- one per backend.
#
# The coordinator's durable state (policy CAS, node roster, definition registry, namespace registry)
# lives in JdbcCoordinatorStore over the coord_* schema, which is written in canonical PostgreSQL SQL
# and rewritten per backend by Dialect.ddl(). That portability is only *unit*-tested on H2; this runs
# the full CoordinatorStore conformance scenario against the actual database images.
#
# Backends: postgres, mysql (fast, run by default), oracle, sqlserver (heavy images, opt-in).
# Cassandra is NOT a JDBC store and does not (yet) provide a durable coordinator store -- see the note
# printed at the end.
#
#   scripts/coordinator-storage-matrix.sh                 # postgres + mysql
#   scripts/coordinator-storage-matrix.sh postgres mysql oracle sqlserver
set -uo pipefail
cd "$(dirname "$0")/.."

BACKENDS=("$@"); [ ${#BACKENDS[@]} -eq 0 ] && BACKENDS=(postgres mysql)
OUT=$(mktemp -d)
declare -a CONTAINERS=()
fail=0
cleanup() { for c in "${CONTAINERS[@]:-}"; do docker rm -f "$c" >/dev/null 2>&1; done; rm -rf "$OUT"; }
trap cleanup EXIT
command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }

[ -d dist/build/install/wiggle/lib ] || { echo "== building :dist:installDist (all backends + drivers) =="; ./gradlew :dist:installDist -q; }
CP=$(printf '%s:' dist/build/install/wiggle/lib/*.jar)

cat > "$OUT/CoordConformance.java" <<'JAVA'
import dev.wiggle.cassandra.CassandraStorage;
import dev.wiggle.jdbc.Dialect;
import dev.wiggle.jdbc.JdbcCoordinatorStore;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.mysql.MySqlDialect;
import dev.wiggle.oracle.OracleDialect;
import dev.wiggle.postgres.PostgresDialect;
import dev.wiggle.sqlserver.SqlServerDialect;
import dev.wiggle.server.ServerRole;
import dev.wiggle.server.coord.*;
import dev.wiggle.server.coord.CoordPolicy.EpochRing;
import dev.wiggle.server.coord.CoordPolicy.EpochStatus;
import dev.wiggle.server.coord.CoordPolicy.RingSlot;
import java.util.List;
import java.util.Map;

/** Runs the coordinator + namespace storage conformance against one real JDBC database. */
public class CoordConformance {
    static int checks = 0;
    static void ok(boolean c, String m) { checks++; if (!c) throw new AssertionError("FAILED: " + m); }

    public static void main(String[] a) throws Exception {
        String backend = a[0], url = a[1], user = a[2], pass = a[3];
        if (backend.equals("cassandra")) {
            try (CassandraStorage storage = CassandraStorage.fromUrl(url, user.isEmpty() ? null : user, pass)) {
                storage.migrate(ServerRole.COORDINATOR);   // create coord_* tables in the keyspace
                scenario(storage.coordinatorStore());      // a CQL store: policy CAS via LWT
            }
            System.out.println("  PASS  cassandra  (" + checks + " checks)");
            return;
        }
        Dialect d = switch (backend) {
            case "postgres" -> new PostgresDialect();
            case "mysql" -> new MySqlDialect();
            case "oracle" -> new OracleDialect();
            case "sqlserver" -> new SqlServerDialect();
            default -> throw new IllegalArgumentException("unknown backend " + backend);
        };
        try (JdbcStorage mig = new JdbcStorage(url, user, pass, 4, d)) {
            mig.migrate(ServerRole.COORDINATOR);   // create coord_* via dialect-rewritten DDL
        }
        try (CoordinatorStore s = new JdbcCoordinatorStore(url, user, pass, 4, d)) {
            scenario(s);
        }
        System.out.println("  PASS  " + backend + "  (" + checks + " checks)");
    }

    static EpochRing ring(String cell, EpochStatus st) {
        return new EpochRing(List.of(new RingSlot(0, cell, "eu-west")), st);
    }
    static CoordPolicy policy(String ns, long epoch, Map<Long, EpochRing> e) { return new CoordPolicy(ns, epoch, 0, e); }

    static void scenario(CoordinatorStore s) {
        // ---- policy CAS (the OpenEpoch/SetRing storage path) ----
        ok(s.getPolicy("acme").isEmpty(), "no policy initially");
        ok(s.casPolicy("acme", 0, policy("acme", 0, Map.of(0L, ring("cell-3", EpochStatus.OPEN)))) == 1, "create -> rev 1");
        ok(s.casPolicy("acme", 0, policy("acme", 0, Map.of(0L, ring("cell-3", EpochStatus.OPEN)))) == -1, "second create loses");
        CoordPolicy g = s.getPolicy("acme").orElseThrow();
        ok(g.revision() == 1 && g.currentEpoch() == 0, "read back rev/epoch");
        ok(g.epochs().get(0L).ring().get(0).cellId().equals("cell-3"), "ring cell round-trips");
        Map<Long, EpochRing> two = Map.of(0L, ring("cell-3", EpochStatus.DRAINING), 1L, ring("cell-5", EpochStatus.OPEN));
        ok(s.casPolicy("acme", 1, policy("acme", 1, two)) == 2, "update on rev 1 -> rev 2");
        ok(s.casPolicy("acme", 1, policy("acme", 1, two)) == -1, "stale-rev update loses");
        CoordPolicy g2 = s.getPolicy("acme").orElseThrow();
        ok(g2.epochs().size() == 2 && g2.epochs().get(0L).status() == EpochStatus.DRAINING, "epoch 0 draining");

        // ---- node roster ----
        s.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:1", "eu-west", "2.1.5", 2, 1_000));
        s.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:2", "eu-west", "2.1.5", 2, 2_000));
        ok(s.nodes("acme").size() == 1, "upsert same id");
        ok(s.nodes("acme").get(0).endpoint().equals("grpc://h:2"), "upsert replaced endpoint");
        ok(s.nodes("acme").get(0).cellId().equals("cellA"), "cell_id round-trips");
        s.upsertNode(new CoordNode("n2", "acme", "cellB", "grpc://h:3", "eu-west", "2.1.5", 2, 500));
        ok(s.expireNodes(900) == 1, "expire stale node");
        ok(s.nodes("acme").size() == 1, "one node left");

        // ---- definition registry (allocate / list / deallocate) ----
        ok(s.getDefinition("acme", "order").isEmpty(), "no def initially");
        s.putDefinition(new CoordDefinition("acme", "order", 42, "hashA", 111));
        s.putDefinition(new CoordDefinition("acme", "order", 42, "hashA", 111));   // idempotent
        ok(s.definitions("acme").size() == 1, "idempotent put");
        s.putDefinition(new CoordDefinition("acme", "order", 43, "hashB", 222));   // update
        ok(s.getDefinition("acme", "order").orElseThrow().version() == 43, "update in place");
        ok(s.removeDefinition("acme", "order"), "deallocate removes");
        ok(!s.removeDefinition("acme", "order"), "deallocate is idempotent");
        ok(s.definitions("acme").isEmpty(), "registry empty after remove");

        // ---- namespace registry (provisioning) ----
        ok(s.getNamespace("acme").isEmpty(), "no namespace record initially");
        StorageConfig sc = StorageConfig.jdbc("jdbc:postgresql://db/acme", "app", "ACME_DB_SECRET", 8);
        s.putNamespace(CoordNamespace.requested(new NamespaceSpec("acme", sc, 2, "eu-west", 8100), 1_000));
        CoordNamespace ns = s.getNamespace("acme").orElseThrow();
        ok(ns.state() == ProvisionState.REQUESTED && ns.replicas() == 2, "requested record");
        ok(ns.storage().secretRef().equals("ACME_DB_SECRET"), "secretRef stored (never the secret)");
        s.putNamespace(ns.active("grpc://acme:8100", 2_000));
        CoordNamespace act = s.getNamespace("acme").orElseThrow();
        ok(act.state() == ProvisionState.ACTIVE && act.endpoint().equals("grpc://acme:8100"), "active upsert");
        ok(s.namespaces().size() == 1, "one namespace listed");
    }
}
JAVA
javac -cp "$CP" -d "$OUT" "$OUT/CoordConformance.java" || { echo "conformance driver failed to compile" >&2; exit 1; }

run_driver() { java -cp "$OUT:$CP" CoordConformance "$@"; }
wait_ready() { for _ in $(seq 1 60); do eval "$1" >/dev/null 2>&1 && return 0; sleep 2; done; return 1; }

start_postgres() {
    local name=coord-mtx-pg; CONTAINERS+=("$name"); docker rm -f "$name" >/dev/null 2>&1
    docker run -d --name "$name" -e POSTGRES_PASSWORD=wiggle -p 5455:5432 postgres:16-alpine >/dev/null
    wait_ready "docker exec $name pg_isready -U postgres" || { echo "  FAIL postgres: not ready"; return 1; }
    docker exec "$name" psql -U postgres -c "CREATE DATABASE coord;" >/dev/null 2>&1
    run_driver postgres "jdbc:postgresql://localhost:5455/coord" postgres wiggle
}

start_mysql() {
    local name=coord-mtx-my; CONTAINERS+=("$name"); docker rm -f "$name" >/dev/null 2>&1
    docker run -d --name "$name" -e MYSQL_ROOT_PASSWORD=wiggle -e MYSQL_DATABASE=coord -p 3312:3306 mysql:8 >/dev/null
    wait_ready "docker exec $name mysqladmin ping -h localhost -uroot -pwiggle --silent" || { echo "  FAIL mysql: not ready"; return 1; }
    sleep 3
    run_driver mysql "jdbc:mysql://localhost:3312/coord?allowPublicKeyRetrieval=true&useSSL=false" root wiggle
}

start_oracle() {
    local name=coord-mtx-ora; CONTAINERS+=("$name"); docker rm -f "$name" >/dev/null 2>&1
    docker run -d --name "$name" -e ORACLE_PASSWORD=wiggle -p 15210:1521 gvenzl/oracle-free:23-slim >/dev/null
    # Oracle Free first-boot initializes the DB; it is slow (minutes), more so under emulation.
    for _ in $(seq 1 120); do docker exec "$name" healthcheck.sh >/dev/null 2>&1 && break; sleep 6; done
    docker exec "$name" healthcheck.sh >/dev/null 2>&1 || { echo "  FAIL oracle: not ready (give the image more time)"; return 1; }
    run_driver oracle "jdbc:oracle:thin:@//localhost:15210/FREEPDB1" system wiggle
}

start_sqlserver() {
    local name=coord-mtx-mss; CONTAINERS+=("$name"); docker rm -f "$name" >/dev/null 2>&1
    docker run -d --name "$name" -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Wiggle!Pass1 -p 14330:1433 \
        mcr.microsoft.com/mssql/server:2022-latest >/dev/null
    wait_ready "docker exec $name /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P Wiggle!Pass1 -C -Q 'SELECT 1'" \
        || { echo "  FAIL sqlserver: not ready"; return 1; }
    docker exec "$name" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Wiggle!Pass1' -C -Q "CREATE DATABASE coord" >/dev/null 2>&1
    run_driver sqlserver "jdbc:sqlserver://localhost:14330;databaseName=coord;encrypt=false" sa 'Wiggle!Pass1'
}

start_cassandra() {
    local name=coord-mtx-cas; CONTAINERS+=("$name"); docker rm -f "$name" >/dev/null 2>&1
    docker run -d --name "$name" -p 19042:9042 cassandra:5 >/dev/null
    for _ in $(seq 1 80); do docker exec "$name" cqlsh -e "describe keyspaces" >/dev/null 2>&1 && break; sleep 3; done
    docker exec "$name" cqlsh -e "describe keyspaces" >/dev/null 2>&1 || { echo "  FAIL cassandra: not ready (slow first boot)"; return 1; }
    run_driver cassandra "cassandra://localhost:19042/coord?dc=datacenter1&rf=1" "" ""
}

echo "== coordinator + namespace storage conformance =="
for b in "${BACKENDS[@]}"; do
    echo "-- $b --"
    case "$b" in
        postgres)  start_postgres  || fail=1 ;;
        mysql)     start_mysql     || fail=1 ;;
        oracle)    start_oracle    || fail=1 ;;
        sqlserver) start_sqlserver || fail=1 ;;
        cassandra) start_cassandra || fail=1 ;;
        *) echo "  unknown backend: $b"; fail=1 ;;
    esac || fail=1
done

echo
[ "$fail" = 0 ] && echo "MATRIX: PASS" || echo "MATRIX: FAIL"
exit "$fail"
