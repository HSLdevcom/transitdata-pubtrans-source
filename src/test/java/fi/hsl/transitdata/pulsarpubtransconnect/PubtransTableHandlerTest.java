package fi.hsl.transitdata.pulsarpubtransconnect;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class PubtransTableHandlerTest {
    private static final double DELTA = 1e-15;

    @Test
    public void testTimestampConversionInUTC() {
        final String timezone = "UTC";
        assertEquals((Long) 1545695999000L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.0", timezone).get());
        assertEquals((Long) 1545695999001L,
                PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.001", timezone).get());
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs("", timezone));
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(null, timezone));
        assertEquals((Long) 1540551514557L,
                PubtransTableHandler.toUtcEpochMs("2018-10-26 10:58:34.557", timezone).get());
    }

    @Test
    public void testTimestampConversionInEET() {
        final String timezone = "Europe/Helsinki";
        assertEquals((Long) 1545688799000L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.0", timezone).get());
        assertEquals((Long) 1545688799001L,
                PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.001", timezone).get());
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs("", timezone));
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(null, timezone));
        assertEquals((Long) 1540540714557L,
                PubtransTableHandler.toUtcEpochMs("2018-10-26 10:58:34.557", timezone).get());
    }

    @Test
    public void testDaylightSavings() {
        final String timezone = "Europe/Helsinki";
        final long winterTime = PubtransTableHandler.toUtcEpochMs("2018-03-25 02:59:59.999", timezone).get();
        final long summerTime = PubtransTableHandler.toUtcEpochMs("2018-03-25 03:00:00.000", timezone).get();
        assertEquals(summerTime, winterTime + 1);

        final long summerTimeWithDaylightSavings = PubtransTableHandler
                .toUtcEpochMs("2018-03-25 04:00:00.000", timezone).get();
        assertEquals(summerTime, summerTimeWithDaylightSavings);
        assertEquals(summerTimeWithDaylightSavings, winterTime + 1);
    }

    @Test
    public void testConvertMillisecondsSeconds() {
        assertEquals(0.0f, PubtransTableHandler.getSeconds(0), DELTA);
        assertEquals(0.011f, PubtransTableHandler.getSeconds(11), DELTA);
        assertEquals(0.5f, PubtransTableHandler.getSeconds(500), DELTA);
        assertEquals(0.6f, PubtransTableHandler.getSeconds(600), DELTA);
        assertEquals(1.0f, PubtransTableHandler.getSeconds(1000), DELTA);
        assertEquals(1.501f, PubtransTableHandler.getSeconds(1501), DELTA);
        assertEquals(59.0f, PubtransTableHandler.getSeconds(59000), DELTA);
    }
}
