package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's one run-time fan-out, shared by a forEach and by the branches a step creates. Each
 * item gets a token at its start node, under an {@code ITEM} frame whose view is the item and whose
 * index is its position; all share the group {@code "<forkTokenId>#<width>"}, which the join parses
 * back for its width and which restores the fork token's payload as the base.
 */
final class FanOut {

    /** One item: where its token starts, its source key (null for a list), and its whole context. */
    record Item(String start, String key, Object input) {}

    private FanOut() {}

    static List<Token> items(Tx tx, Instance inst, Token fork, List<Item> items, long now) {
        String group = fork.id + "#" + items.size();
        String childStack = fork.pushJoinStack(group);
        List<Token> children = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            Item item = items.get(i);
            Token child = Tokens.create(inst, item.start(), childStack,
                    fork.payload.push(TokenPayload.FrameKind.ITEM, i, item.key(), Doc.of(item.input())), now);
            tx.insertToken(child);
            children.add(child);
        }
        return children;
    }
}
