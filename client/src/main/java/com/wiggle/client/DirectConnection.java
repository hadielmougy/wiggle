package com.wiggle.client;

import com.wiggle.core.Tls;

/**
 * A connection to one Wiggle cluster. {@link #client()} returns
 * the single {@link WiggleClient}; the same instance is reused across calls. Obtain one via
 * {@link WiggleConnection#direct(String)}.
 */
public final class DirectConnection implements AutoCloseable {

    private final String target;
    private final Tls.Options tls;
    private WiggleClient client;

    DirectConnection(String target, Tls.Options tls) {
        this.target = target;
        this.tls = tls == null ? Tls.Options.DISABLED : tls;
    }

    /** The client for the cluster. */
    public synchronized WiggleClient client() {
        if (client == null) {
            client = new WiggleClient(WiggleConnection.strip(target), tls);
        }
        return client;
    }

    @Override public synchronized void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
