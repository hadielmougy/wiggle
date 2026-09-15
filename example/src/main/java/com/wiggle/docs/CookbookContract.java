package com.wiggle.docs;

import com.wiggle.client.worker.ForFlow;

/**
 * The contract-and-handler shape quoted at the top of {@code docs/cookbook.md}.
 *
 * <p>The doc abridges the handler class rather than repeating it -- the point there is the pairing,
 * not the bodies. {@code docs:elide} is how that is said: the reader gets an ellipsis where the
 * implementation was, instead of a class that looks complete and is not.
 */
public final class CookbookContract {

    public record Signup(String email) {}
    public record Classified(String email, boolean internal) {}

    // docs:begin contract
    public interface LinearGateSteps {
        Signup     normalise(Signup s);
        Classified classify(Signup s);
        boolean    eligible(Classified c);
        void       welcome(Classified c);
    }

    @ForFlow("tcb-linear-gate")
    public static final class LinearWithGate implements LinearGateSteps {
        // docs:elide
        // docs:skip
        public Signup normalise(Signup s) { return new Signup(s.email().trim()); }

        public Classified classify(Signup s) { return new Classified(s.email(), true); }

        public boolean eligible(Classified c) { return c.internal(); }

        public void welcome(Classified c) { }
        // docs:resume
    }
    // docs:end contract

    private CookbookContract() {}
}
