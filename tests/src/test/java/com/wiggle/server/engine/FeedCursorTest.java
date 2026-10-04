package com.wiggle.server.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FeedCursorTest {

    @Test @DisplayName("a cursor writes its shards in order and reads back as written")
    void roundTrips() {
        FeedCursor c = new FeedCursor(new TreeMap<>(Map.of(3, 40L, 0, 12L)));
        assertEquals("0:12,3:40", c.format());
        assertEquals(c, FeedCursor.parse("0:12,3:40"));
        assertEquals(c, FeedCursor.parse(" 3:40 , 0:12 "), "order and spaces do not matter");
        assertEquals(41, c.with(3, 41).at(3));
        assertEquals(0, c.at(7), "a shard the cursor has not seen is at its start");
    }

    @Test @DisplayName("anything that is not a cursor is a bad request")
    void malformed() {
        for (String bad : new String[]{"", "12", "0:", ":5", "0:x", "-1:3", "0:-3", "0:1,0:2"}) {
            EngineException e = assertThrows(EngineException.class, () -> FeedCursor.parse(bad), bad);
            assertEquals(400, e.statusCode(), bad);
        }
    }
}
