package fi.hsl.transitdata.pulsarpubtransconnect;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PubtransTableHandlerTest {
    @Test
    public void testTimestampConversion() {
        assertEquals((Long)1545695999000L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.0").get());
        assertEquals((Long)1545695999001L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.001").get());
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(""));
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(null));
        assertEquals((Long)1540551514557L, PubtransTableHandler.toUtcEpochMs("2018-10-26 10:58:34.557").get());
    }
}
