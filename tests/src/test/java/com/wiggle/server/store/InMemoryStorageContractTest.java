package com.wiggle.server.store;

/**
 * {@link StorageContract} against the in-memory store -- the backend a development run and most of
 * the suite get when no JDBC URL is configured (WGL-STOR-010). It is the one backend that cannot roll
 * back, so it is also what keeps that clause of the contract honest: such a store must not hold
 * writes back ({@link Tx#bufferWrites()}).
 */
final class InMemoryStorageContractTest extends StorageContract {

    @Override protected Storage newStorage() {
        Storage storage = new InMemoryStorage();
        storage.migrate();
        return storage;
    }
}
