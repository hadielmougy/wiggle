package com.wiggle.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a resolved cell target ({@code host:port}) before {@link WiggleConnection} opens a channel to it.
 * A testing seam: the coordinator may hand back an address only reachable inside the cluster (e.g. a
 * Kubernetes pod IP), while a client running outside must reach the cell through a tunnel (e.g. a
 * {@code kubectl port-forward} on {@code 127.0.0.1}). The default is identity, so production routing is
 * unaffected.
 *
 * <p>A default rewriter is loaded from {@code -Dwiggle.endpointRewrite} (or the
 * {@code WIGGLE_ENDPOINT_REWRITE} environment variable): a comma-separated list of {@code MATCH=REPLACEMENT}
 * rules, where MATCH is {@code host:port} or {@code *:port} (any host on that port) or {@code *} (anything).
 * The first matching rule wins. Example — send every resolved cell to a local port-forward:
 * <pre>WIGGLE_ENDPOINT_REWRITE=*:8080=127.0.0.1:18100</pre>
 * or map specific pod IPs when running several cells:
 * <pre>WIGGLE_ENDPOINT_REWRITE=10.244.0.7:8080=127.0.0.1:18100,10.244.0.8:8080=127.0.0.1:18101</pre>
 */
@FunctionalInterface
public interface EndpointRewriter {

    /** Return a target to connect to given the resolved one; return the input to leave it unchanged. */
    String rewrite(String resolvedTarget);

    static EndpointRewriter identity() {
        return t -> t;
    }

    /** The default rewriter from {@code wiggle.endpointRewrite} / {@code WIGGLE_ENDPOINT_REWRITE}. */
    static EndpointRewriter fromEnv() {
        return fromSpec(System.getProperty("wiggle.endpointRewrite",
                System.getenv().getOrDefault("WIGGLE_ENDPOINT_REWRITE", "")));
    }

    /** Parse a {@code MATCH=REPLACEMENT[,MATCH=REPLACEMENT...]} spec (see the class doc). */
    static EndpointRewriter fromSpec(String spec) {
        if (spec == null || spec.isBlank()) return identity();
        List<String[]> rules = new ArrayList<>();   // {matchHost, matchPort, replacement}
        for (String rule : spec.split(",")) {
            String r = rule.trim();
            int eq = r.indexOf('=');
            if (eq <= 0) continue;
            String match = r.substring(0, eq).trim();
            String repl = r.substring(eq + 1).trim();
            if (match.isEmpty() || repl.isEmpty()) continue;
            int colon = match.lastIndexOf(':');
            String host = colon < 0 ? match : match.substring(0, colon);
            String port = colon < 0 ? "" : match.substring(colon + 1);
            rules.add(new String[]{host, port, repl});
        }
        if (rules.isEmpty()) return identity();
        return target -> {
            String t = target;
            int scheme = t.indexOf("://");
            if (scheme >= 0) t = t.substring(scheme + 3);
            int colon = t.lastIndexOf(':');
            String host = colon < 0 ? t : t.substring(0, colon);
            String port = colon < 0 ? "" : t.substring(colon + 1);
            for (String[] rule : rules) {
                boolean hostOk = rule[0].equals("*") || rule[0].equals(host);
                boolean portOk = rule[1].isEmpty() || rule[1].equals(port);
                if (hostOk && portOk) return rule[2];
            }
            return target;
        };
    }
}
