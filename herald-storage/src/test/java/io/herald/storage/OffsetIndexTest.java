package io.herald.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OffsetIndexTest {

    @Test
    void emptyLookup() {
        OffsetIndex idx = new OffsetIndex();
        assertEquals(-1, idx.lookup(0));
    }

    @Test
    void sparseLookup() {
        OffsetIndex idx = new OffsetIndex();
        idx.append(0, 0);
        idx.append(100, 4096);
        idx.append(200, 8192);

        assertEquals(0, idx.lookup(0));
        assertEquals(0, idx.lookup(50));
        assertEquals(4096, idx.lookup(100));
        assertEquals(4096, idx.lookup(199));
        assertEquals(8192, idx.lookup(200));
        assertEquals(8192, idx.lookup(999));
    }
}
