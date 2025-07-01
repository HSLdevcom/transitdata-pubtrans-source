package fi.hsl.transitdata.pulsarpubtransconnect;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PubtransTableHandlerTest {
    @Test
    public void testTimestampConversionInUTC() {
        final String timezone = "UTC";
        assertEquals((Long)1545695999000L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.0", timezone).get());
        assertEquals((Long)1545695999001L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.001", timezone).get());
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs("", timezone));
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(null, timezone));
        assertEquals((Long)1540551514557L, PubtransTableHandler.toUtcEpochMs("2018-10-26 10:58:34.557", timezone).get());
    }

    @Test
    public void testTimestampConversionInEET() {
        final String timezone = "Europe/Helsinki";
        assertEquals((Long)1545688799000L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.0", timezone).get());
        assertEquals((Long)1545688799001L, PubtransTableHandler.toUtcEpochMs("2018-12-24 23:59:59.001", timezone).get());
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs("", timezone));
        assertEquals(Optional.empty(), PubtransTableHandler.toUtcEpochMs(null, timezone));
        assertEquals((Long)1540540714557L, PubtransTableHandler.toUtcEpochMs("2018-10-26 10:58:34.557", timezone).get());
    }

    @Test
    public void testDaylightSavings() {
        final String timezone = "Europe/Helsinki";
        final long winterTime = PubtransTableHandler.toUtcEpochMs("2018-03-25 02:59:59.999", timezone).get();
        final long summerTime = PubtransTableHandler.toUtcEpochMs("2018-03-25 03:00:00.000", timezone).get();
        assertEquals(summerTime, winterTime + 1);

        final long summerTimeWithDaylightSavings = PubtransTableHandler.toUtcEpochMs("2018-03-25 04:00:00.000", timezone).get();
        assertEquals(summerTime, summerTimeWithDaylightSavings);
        assertEquals(summerTimeWithDaylightSavings, winterTime + 1);
    }
    
    @Test
    public void testConvertMillisecondsToMinutesAndSeconds() {
        assertEquals("0 ms", PubtransTableHandler.getMinSec(0));
        assertEquals("11 ms", PubtransTableHandler.getMinSec(11));
        assertEquals("500 ms", PubtransTableHandler.getMinSec(500));
        assertEquals("0 min 1 sec", PubtransTableHandler.getMinSec(1000));
        assertEquals("0 min 59 sec", PubtransTableHandler.getMinSec(59000));
        assertEquals("1 min 0 sec", PubtransTableHandler.getMinSec(60000));
        assertEquals("1 min 1 sec", PubtransTableHandler.getMinSec(61000));
        assertEquals("2 min 0 sec", PubtransTableHandler.getMinSec(120000));
        assertEquals("2 min 1 sec", PubtransTableHandler.getMinSec(121000));
        assertEquals("59 min 59 sec", PubtransTableHandler.getMinSec(3599000));
    }
}
