package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TestPojo {
    private long id;
    private long IsOnDatedVehicleJourneyId;
    private long IsOnMonitoredVehicleJourneyId;
}
