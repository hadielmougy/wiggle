package com.wiggle.client;

import com.wiggle.core.Tls;

/**
 * A connection to one standalone Wiggle server -- no namespaces, no routing. {@link #client()} returns
 * the single {@link WiggleClient}; the same instance is reused across calls. Obtain one via
 * {@link WiggleConnection#direct(String)}.
 */
public final class DirectConnection implements AutoCloseable {

    private final String target;
    private final Tls.Options tls;
    private volatile EndpointRewriter endpointRewriter = EndpointRewriter.fromEnv();
    private WiggleClient client;

    DirectConnection(String target, Tls.Options tls) {
        this.target = target;
        this.tls = tls == null ? Tls.Options.DISABLED : tls;
    }

    /**
     * Override the resolved address before connecting -- a testing seam for when the advertised
     * address is unreachable from where the client runs. Passing {@code null} restores identity; by
     * default a rewriter is loaded from the env. See {@link EndpointRewriter}. Set before {@link #client()}.
     */
    public DirectConnection withEndpointRewriter(EndpointRewriter rewriter) {
        this.endpointRewriter = rewriter == null ? EndpointRewriter.identity() : rewriter;
        return this;
    }

    /** The client for the server -- the single entry point of a standalone deployment. */
    public synchronized WiggleClient client() {
        if (client == null) {
            String t = WiggleConnection.strip(endpointRewriter.rewrite(WiggleConnection.strip(target)));
            client = new WiggleClient(t, tls);
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
