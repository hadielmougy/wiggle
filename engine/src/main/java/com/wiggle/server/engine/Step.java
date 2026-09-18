package com.wiggle.server.engine;

import com.wiggle.core.Node;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Tx;

import java.util.Deque;

/**
 * One token being advanced at its node during a drive pass: the unit every drive-family
 * method operates on. {@code work} is the pass's remaining tokens; a spawning node pushes
 * its continuations there.
 */
record Step(Tx tx, LazyGraph def, Instance inst, Token token, Node node, Deque<Token> work, long now) {
}
