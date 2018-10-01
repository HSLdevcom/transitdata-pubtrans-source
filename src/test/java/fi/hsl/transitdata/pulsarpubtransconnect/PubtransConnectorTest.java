package fi.hsl.transitdata.pulsarpubtransconnect;

import org.junit.Test;

import java.time.OffsetDateTime;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PubtransConnectorTest {
    @Test
    public void testCacheMaxAge() {
        final OffsetDateTime now = OffsetDateTime.now();
        final int maxAgeInMins = 120;

        assertTrue(PubtransConnector.isCacheValid(now, maxAgeInMins));
        assertTrue(PubtransConnector.isCacheValid(now.minusMinutes(1), maxAgeInMins));
        assertTrue(PubtransConnector.isCacheValid(now.minusMinutes(119), maxAgeInMins));
        assertTrue(PubtransConnector.isCacheValid(now.minusMinutes(120), maxAgeInMins));

        assertFalse(PubtransConnector.isCacheValid(now.minusMinutes(121), maxAgeInMins));
        assertFalse(PubtransConnector.isCacheValid(now.minusMinutes(200), maxAgeInMins));

    }
}
