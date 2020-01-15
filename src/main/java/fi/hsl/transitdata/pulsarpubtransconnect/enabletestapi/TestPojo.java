package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TestPojo {
    private Long id;
    private Long IsOnDatedVehicleJourneyId;
    private Long IsOnMonitoredVehicleJourneyId;
}
