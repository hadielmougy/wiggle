package com.wiggle.server.store;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.tests.TestStorage;

/**
 * {@link StorageContract} against the SQL store: H2 in PostgreSQL-compatibility mode by default,
 * and whatever {@code WIGGLE_TEST_DB_URL} names when one is configured -- the real dialect, its
 * real claim path, real transactions and a real network hop. The dialect comes from the same
 * published mapping the server itself uses, so a backend added there is covered here without
 * touching this file.
 *
 * <p>On H2 the claim runs the portable compare-and-set fallback (WGL-STOR-022); the
 * {@code FOR UPDATE SKIP LOCKED} statement it cannot execute is covered by
 * {@code postgres/PostgresClaimTest}, and by this suite itself once pointed at a PostgreSQL.
 */
final class JdbcStorageContractTest extends StorageContract {

    @Override protected Storage newStorage() {
        String url = TestStorage.url("contract");
        Storage storage = new JdbcStorage(url, TestStorage.user(), TestStorage.password(), 4,
                PostgresStorageFactory.dialect(url));
        storage.migrate();
        return storage;
    }
}
