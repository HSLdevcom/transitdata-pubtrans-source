package fi.hsl.transitdata.pulsarpubtransconnect;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class PubtransTableHandlerTest {
    @Test
    public void testTimestampConversion() {
        assertEquals((Long)1545695999000L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.0").get());
        assertEquals((Long)1545695999000L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.000").get());
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(""));
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(null));
    }
}
